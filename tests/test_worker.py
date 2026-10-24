"""The worker must be safe to retry: duplicates do no extra work, transient errors retry,
permanent errors go to dead-letter, and outputs are complete only when _SUCCESS exists."""

from __future__ import annotations

import hashlib
from datetime import date, timedelta
from pathlib import Path

import pytest
from sqlalchemy import func, select

from lakeflow import zones
from lakeflow.db import ColumnProfile, IngestionJob, JobStatus
from lakeflow.queue import JobMessage
from lakeflow.worker import Worker, utcnow


def _submit(sf, lake, queue, src: Path, source: str = "sap_bp") -> str:
    digest = hashlib.sha256(src.read_bytes()).hexdigest()
    key = zones.landing_key(source, date.today(), digest, src.name)
    lake.upload_file(src, key)
    with sf() as s:
        job = IngestionJob(content_hash=digest, source=source, filename=src.name,
                           landing_key=key, size_bytes=src.stat().st_size)
        s.add(job)
        s.commit()
        job_id = job.id
    queue.send(JobMessage(job_id=job_id, content_hash=digest))
    return job_id


@pytest.fixture
def worker(session_factory, lake, queue, contract, tmp_path) -> Worker:
    return Worker(session_factory, lake, queue, contract, tmp_path / "work", max_delivery_count=3)


def test_happy_path(worker, session_factory, lake, queue, bp_csv) -> None:
    src, m = bp_csv
    job_id = _submit(session_factory, lake, queue, src)
    worker.run_once()

    with session_factory() as s:
        job = s.get(IngestionJob, job_id)
        assert job.status == JobStatus.SUCCEEDED and job.progress_pct == 100
        assert (job.rows_valid, job.rows_rejected) == (m.rows_valid, m.rows_rejected)
        assert job.file_format == "csv" and job.delimiter == ";"
        n_profiles = s.scalar(select(func.count()).select_from(ColumnProfile))
        assert n_profiles == 8

    cleansed = [k for k in lake.list_keys(zones.CLEANSED)]
    assert any(k.endswith(zones.SUCCESS_MARKER) for k in cleansed)
    assert len(queue.completed) == 1 and not queue.dead_letters


def test_duplicate_delivery_does_no_work(worker, session_factory, lake, queue, bp_csv) -> None:
    src, _ = bp_csv
    job_id = _submit(session_factory, lake, queue, src)
    worker.run_once()
    queue.send(JobMessage(job_id=job_id, content_hash="x"))  # Service Bus redelivers
    worker.run_once()

    with session_factory() as s:
        job = s.get(IngestionJob, job_id)
        assert job.attempts == 1  # claimed once, the second delivery was a no-op
        assert s.scalar(select(func.count()).select_from(ColumnProfile)) == 8
    assert len(queue.completed) == 2


def test_transient_error_is_retried_then_succeeds(worker, session_factory, lake, queue,
                                                  bp_csv, monkeypatch) -> None:
    src, _ = bp_csv
    job_id = _submit(session_factory, lake, queue, src)

    real_upload = lake.upload_file
    calls = {"n": 0}

    def flaky_upload(local, key):
        if key.startswith(zones.CLEANSED) and calls["n"] == 0:
            calls["n"] += 1
            raise ConnectionError("storage throttled (503)")
        return real_upload(local, key)

    monkeypatch.setattr(lake, "upload_file", flaky_upload)

    worker.run_once()  # fails mid-publish, message abandoned
    with session_factory() as s:
        assert s.get(IngestionJob, job_id).status == JobStatus.QUEUED
    assert len(queue) == 1

    worker.run_once()  # redelivered, succeeds
    with session_factory() as s:
        job = s.get(IngestionJob, job_id)
        assert job.status == JobStatus.SUCCEEDED and job.attempts == 2
        # profiles replaced, not duplicated, by the second attempt
        assert s.scalar(select(func.count()).select_from(ColumnProfile)) == 8


def test_permanent_error_goes_to_dead_letter(worker, session_factory, lake, queue,
                                             tmp_path) -> None:
    src = tmp_path / "wrong_shape.csv"
    src.write_text("foo,bar\n1,2\n", encoding="utf-8")
    job_id = _submit(session_factory, lake, queue, src)
    worker.run_once()

    with session_factory() as s:
        job = s.get(IngestionJob, job_id)
        assert job.status == JobStatus.FAILED and "ContractError" in job.error
    assert len(queue.dead_letters) == 1 and len(queue) == 0  # not retried


def test_gives_up_after_max_deliveries(worker, session_factory, lake, queue, bp_csv,
                                       monkeypatch) -> None:
    src, _ = bp_csv
    job_id = _submit(session_factory, lake, queue, src)
    monkeypatch.setattr(lake, "download_file",
                        lambda *a: (_ for _ in ()).throw(TimeoutError("network down")))
    for _ in range(3):
        worker.run_once()
    with session_factory() as s:
        assert s.get(IngestionJob, job_id).status == JobStatus.FAILED
    assert len(queue.dead_letters) == 1


def test_busy_job_is_rescheduled_not_abandoned(worker, session_factory, lake, queue,
                                               bp_csv) -> None:
    """A second delivery while another worker holds the lease must not burn delivery
    attempts: it is rescheduled for when the lease ends and the original is completed."""
    src, _ = bp_csv
    fake_now = [1000.0]
    queue.clock = lambda: fake_now[0]
    job_id = _submit(session_factory, lake, queue, src)

    assert worker._claim(job_id) == "claimed"  # worker A is busy with it
    worker.run_once()  # worker B gets the message
    assert len(queue.completed) == 1 and not queue.dead_letters
    assert len(queue) == 1  # rescheduled copy waiting
    assert worker.run_once() == 0  # not visible yet: no hot loop

    fake_now[0] += 5 * 60 + 10  # lease ends
    with session_factory() as s:  # ...because worker A finished meanwhile
        s.get(IngestionJob, job_id).status = JobStatus.SUCCEEDED
        s.commit()
    assert worker.run_once() == 1
    assert len(queue.completed) == 2 and not queue.dead_letters


def test_crashed_workers_job_is_taken_over_after_lease(worker, session_factory, lake, queue,
                                                       bp_csv) -> None:
    src, m = bp_csv
    job_id = _submit(session_factory, lake, queue, src)
    assert worker._claim(job_id) == "claimed"
    with session_factory() as s:  # simulate a worker that died: lease in the past
        s.get(IngestionJob, job_id).lease_until = utcnow() - timedelta(seconds=1)
        s.commit()
    worker.run_once()
    with session_factory() as s:
        job = s.get(IngestionJob, job_id)
        assert job.status == JobStatus.SUCCEEDED and job.attempts == 2
        assert job.rows_valid == m.rows_valid
