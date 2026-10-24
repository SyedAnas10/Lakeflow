"""The queue worker: one message = one file to process.

Idempotency and retry safety, which is the core of this file:

1. CLAIM with a conditional UPDATE (compare-and-set in SQL):
       UPDATE ingestion_job SET status='RUNNING', lease_until=now+5m, attempts=attempts+1
       WHERE id=:id AND (status='QUEUED' OR (status='RUNNING' AND lease_until < now))
   Only one worker can win that row. A duplicate delivery of a finished job finds
   status='SUCCEEDED', is completed without doing any work, and nothing is written twice.
   The lease is renewed at every stage. If a worker dies, its lease runs out, so a
   redelivered message can take the job over. A message that arrives while another
   worker's lease is still live is RESCHEDULED for when that lease ends, not abandoned:
   abandon makes it visible again at once, and a tight loop of abandons would burn every
   delivery attempt in milliseconds and dead-letter a healthy job.
2. DETERMINISTIC OUTPUTS: every path contains the content hash; a retry first clears its
   folder, rewrites the same files, and writes a _SUCCESS marker last. Readers trust a
   folder only when _SUCCESS exists, so a half-finished attempt is never consumed.
3. ERROR CLASSES: permanent errors (bad format, contract mismatch) are dead-lettered at
   once, because retrying cannot fix them. Anything else is treated as transient
   (network, storage throttling): the job goes back to QUEUED and the message is
   abandoned so Service Bus redelivers it, up to MaxDeliveryCount, then dead-letters it.
4. PROGRESS: each stage writes stage + progress_pct to the job row, which the API serves.
"""

from __future__ import annotations

import logging
import shutil
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import and_, delete, or_, update
from sqlalchemy.orm import sessionmaker

from lakeflow import zones
from lakeflow.db import ColumnProfile, IngestionJob, JobStatus
from lakeflow.detect import UnsupportedFileError, detect
from lakeflow.ingest import convert
from lakeflow.profile import profile_parquet
from lakeflow.queue import JobMessage, JobQueue, Received
from lakeflow.storage import Lake
from lakeflow.transform import Contract, ContractError, transform

log = logging.getLogger("lakeflow.worker")

# Renewed at every stage. A stage longer than this would need a background heartbeat.
LEASE = timedelta(minutes=5)
RESCHEDULE_BUFFER_S = 5
PERMANENT_ERRORS = (UnsupportedFileError, ContractError)


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class Worker:
    def __init__(self, session_factory: sessionmaker, lake: Lake, queue: JobQueue,
                 contract: Contract, work_dir: str | Path, max_delivery_count: int = 5) -> None:
        self.sf = session_factory
        self.lake = lake
        self.queue = queue
        self.contract = contract
        self.work_dir = Path(work_dir)
        self.max_delivery_count = max_delivery_count

    # ------------------------------------------------------------ queue loop

    def run_once(self, max_messages: int = 1, max_wait_s: float = 5) -> int:
        handled = 0
        for received in self.queue.receive(max_messages=max_messages, max_wait_s=max_wait_s):
            self.handle(received)
            handled += 1
        return handled

    def run_forever(self, idle_sleep_s: float = 1.0) -> None:
        log.info("worker started")
        while True:
            if self.run_once() == 0:
                time.sleep(idle_sleep_s)

    def handle(self, received: Received) -> None:
        job_id = received.body.job_id
        outcome = self._claim(job_id)
        if outcome == "missing":
            received.dead_letter("job not found")
            return
        if outcome == "done":
            log.info("job %s already finished; duplicate delivery ignored", job_id)
            received.complete()
            return
        if outcome == "busy":
            # Another worker holds a live lease. Come back when it ends; if that worker
            # finished, the next delivery sees SUCCEEDED and does nothing.
            self.queue.send(JobMessage(job_id, received.body.content_hash),
                            delay_s=self._lease_left_s(job_id) + RESCHEDULE_BUFFER_S)
            received.complete()
            return

        try:
            self._process(job_id, received)
        except PERMANENT_ERRORS as exc:
            log.warning("job %s failed permanently: %s", job_id, exc)
            self._fail(job_id, f"{type(exc).__name__}: {exc}")
            received.dead_letter(str(exc))
        except Exception as exc:  # noqa: BLE001 - anything else is treated as transient
            if received.delivery_count >= self.max_delivery_count:
                self._fail(job_id, f"gave up after {received.delivery_count} attempts: {exc}")
                received.dead_letter(str(exc))
            else:
                log.warning("job %s transient error, will retry: %s", job_id, exc)
                self._release(job_id, str(exc))
                received.abandon()
        else:
            received.complete()

    # ------------------------------------------------------------ state changes

    def _claim(self, job_id: str) -> str:
        now = utcnow()
        J = IngestionJob
        with self.sf() as s:
            result = s.execute(
                update(J)
                .where(J.id == job_id, or_(
                    J.status == JobStatus.QUEUED,
                    and_(J.status == JobStatus.RUNNING,
                         or_(J.lease_until.is_(None), J.lease_until < now)),
                ))
                .values(status=JobStatus.RUNNING, lease_until=now + LEASE,
                        attempts=J.attempts + 1, error=None)
            )
            s.commit()
            if result.rowcount == 1:
                return "claimed"
            job = s.get(J, job_id)
            if job is None:
                return "missing"
            if job.status in (JobStatus.SUCCEEDED, JobStatus.FAILED):
                return "done"
            return "busy"

    def _lease_left_s(self, job_id: str) -> float:
        with self.sf() as s:
            job = s.get(IngestionJob, job_id)
            if job is None or job.lease_until is None:
                return 0.0
            return max((job.lease_until - utcnow()).total_seconds(), 0.0)

    def _update(self, job_id: str, **values: object) -> None:
        with self.sf() as s:
            s.execute(update(IngestionJob).where(IngestionJob.id == job_id).values(**values))
            s.commit()

    def _progress(self, job_id: str, stage: str, pct: int, received: Received) -> None:
        received.renew_lock()
        self._update(job_id, stage=stage, progress_pct=pct, lease_until=utcnow() + LEASE)
        log.info("job %s %s %d%%", job_id, stage, pct)

    def _release(self, job_id: str, error: str) -> None:
        self._update(job_id, status=JobStatus.QUEUED, lease_until=None, error=error[:4000])

    def _fail(self, job_id: str, error: str) -> None:
        self._update(job_id, status=JobStatus.FAILED, lease_until=None, error=error[:4000])

    # ------------------------------------------------------------ the pipeline

    def _publish(self, local_dir: Path, lake_dir: str) -> None:
        """Clear, write every part, then the _SUCCESS marker last."""
        self.lake.delete_prefix(lake_dir)
        for part in sorted(local_dir.glob("*.parquet")):
            self.lake.upload_file(part, f"{lake_dir}/{part.name}")
        self.lake.write_text(f"{lake_dir}/{zones.SUCCESS_MARKER}", "")

    def _process(self, job_id: str, received: Received) -> None:
        with self.sf() as s:
            job = s.get(IngestionJob, job_id)
            assert job is not None
            source, key, digest = job.source, job.landing_key, job.content_hash
            filename, ingest_date = job.filename, job.created_at.date()
            declared = job.declared_encoding

        work = self.work_dir / job_id
        shutil.rmtree(work, ignore_errors=True)
        work.mkdir(parents=True)
        try:
            self._progress(job_id, "download", 5, received)
            local = work / filename
            self.lake.download_file(key, local)

            self._progress(job_id, "detect", 15, received)
            detected = detect(local, encoding=declared)
            self._update(job_id, file_format=detected.file_format,
                         encoding=detected.encoding, delimiter=detected.delimiter)

            self._progress(job_id, "convert", 25, received)
            converted = convert(local, work / "raw", detected)
            self._publish(work / "raw", zones.zone_dir(zones.RAW, source, ingest_date, digest))
            self._update(job_id, rows_raw=converted.rows, replaced_chars=converted.replaced_chars)

            self._progress(job_id, "profile", 45, received)
            stats = profile_parquet(converted.parquet_files)
            with self.sf() as s:  # replace, never append: safe to re-run
                s.execute(delete(ColumnProfile).where(ColumnProfile.job_id == job_id))
                s.add_all(ColumnProfile(job_id=job_id, **vars(st)) for st in stats)
                s.commit()

            self._progress(job_id, "transform", 65, received)
            result = transform(converted.parquet_files, self.contract,
                               work / "cleansed", work / "quarantine")

            self._progress(job_id, "publish", 85, received)
            self._publish(work / "cleansed",
                          zones.zone_dir(zones.CLEANSED, source, ingest_date, digest))
            self._publish(work / "quarantine",
                          zones.zone_dir(zones.QUARANTINE, source, ingest_date, digest))

            self._update(
                job_id, status=JobStatus.SUCCEEDED, stage="done", progress_pct=100,
                lease_until=None,
                rows_exact_duplicate=result.rows_exact_duplicate,
                rows_rejected=result.rows_rejected,
                rows_key_duplicate=result.rows_key_duplicate,
                rows_valid=result.rows_valid,
            )
        finally:
            shutil.rmtree(work, ignore_errors=True)
