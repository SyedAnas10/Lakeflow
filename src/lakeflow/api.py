"""FastAPI service: submit a file, follow its progress, read its column profile.

Note on 2 GB uploads: streaming them through an API works (we write in 1 MB chunks, never
holding the file in memory), but in production the client should upload straight to Blob
Storage with a short-lived SAS URL, and an Event Grid "blob created" event should create
the job. The API then only handles metadata. The flow below is the simple version.
"""

from __future__ import annotations

import codecs
import hashlib
import tempfile
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, UploadFile, status
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from lakeflow import zones
from lakeflow.config import Settings, load_settings
from lakeflow.db import ColumnProfile, IngestionJob, JobStatus, make_engine, make_session_factory
from lakeflow.queue import JobMessage, JobQueue, make_queue
from lakeflow.storage import Lake, make_lake

UPLOAD_CHUNK = 1024 * 1024


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    source: str
    filename: str
    status: JobStatus
    stage: str | None
    progress_pct: int
    attempts: int
    file_format: str | None
    declared_encoding: str | None
    encoding: str | None
    rows_raw: int | None
    rows_exact_duplicate: int | None
    rows_rejected: int | None
    rows_key_duplicate: int | None
    rows_valid: int | None
    replaced_chars: int | None
    error: str | None
    created_at: datetime


class ColumnProfileOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    column_name: str
    position: int
    row_count: int
    null_count: int
    null_rate: float
    distinct_approx: int
    min_length: int | None
    max_length: int | None
    inferred_type: str
    top_values: list[dict[str, object]]
    top_patterns: list[dict[str, object]]


def get_session(request: Request) -> Iterator[Session]:
    """One session per request, closed when the request ends."""
    with request.app.state.sf() as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]


def create_app(settings: Settings | None = None, *, session_factory: sessionmaker | None = None,
               lake: Lake | None = None, queue: JobQueue | None = None) -> FastAPI:
    settings = settings if settings is not None else load_settings()
    app = FastAPI(title="Lakeflow", version="0.1.0")
    # `is None`, not `or`: an empty queue object is falsy because it defines __len__,
    # so `queue or make_queue(...)` would silently build a second queue.
    if session_factory is None:
        session_factory = make_session_factory(make_engine(settings.database_url))
    if lake is None:
        lake = make_lake(settings.lake_backend, root=settings.lake_root,
                         connection_string=settings.azure_storage_connection_string,
                         container=settings.lake_container,
                         account_url=settings.azure_storage_account_url)
    if queue is None:
        queue = make_queue(settings.queue_backend,
                           connection_string=settings.servicebus_connection_string,
                           queue_name=settings.queue_name,
                           max_delivery_count=settings.max_delivery_count,
                           namespace=settings.servicebus_namespace)
    app.state.sf, app.state.lake, app.state.queue = session_factory, lake, queue

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/jobs", response_model=JobOut, status_code=status.HTTP_202_ACCEPTED)
    def submit(file: UploadFile, response: Response, session: SessionDep,
               source: Annotated[str, Query(pattern=r"^[a-z0-9_-]{1,50}$")],
               encoding: Annotated[str | None, Query(max_length=40)] = None) -> IngestionJob:
        if encoding:
            try:
                encoding = codecs.lookup(encoding).name
            except LookupError:
                raise HTTPException(422,
                                    f"unknown encoding {encoding!r}") from None
        digest = hashlib.sha256()
        size = 0
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            while chunk := file.file.read(UPLOAD_CHUNK):
                digest.update(chunk)
                size += len(chunk)
                tmp.write(chunk)
        tmp_path = Path(tmp.name)
        try:
            if size == 0:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "empty file")
            content_hash = digest.hexdigest()

            existing = session.scalar(
                select(IngestionJob).where(IngestionJob.content_hash == content_hash))
            if existing:  # same bytes already submitted: return that job, do nothing new
                response.status_code = status.HTTP_200_OK
                return existing

            filename = Path(file.filename or "upload.bin").name
            key = zones.landing_key(source, datetime.now(UTC).date(), content_hash, filename)
            app.state.lake.upload_file(tmp_path, key)  # landing first, then the DB row
            job = IngestionJob(content_hash=content_hash, source=source, filename=filename,
                               landing_key=key, size_bytes=size, declared_encoding=encoding)
            session.add(job)
            try:
                session.commit()
            except IntegrityError:  # a concurrent request with the same file won the race
                session.rollback()
                response.status_code = status.HTTP_200_OK
                return session.scalars(select(IngestionJob)
                                       .where(IngestionJob.content_hash == content_hash)).one()
            app.state.queue.send(JobMessage(job_id=job.id, content_hash=content_hash))
            return job
        finally:
            tmp_path.unlink(missing_ok=True)

    @app.get("/jobs", response_model=list[JobOut])
    def list_jobs(session: SessionDep,
                  status_filter: JobStatus | None = Query(None, alias="status"),
                  limit: int = Query(50, ge=1, le=500)) -> list[IngestionJob]:
        stmt = select(IngestionJob).order_by(IngestionJob.created_at.desc()).limit(limit)
        if status_filter:
            stmt = stmt.where(IngestionJob.status == status_filter)
        return list(session.scalars(stmt))

    @app.get("/jobs/{job_id}", response_model=JobOut)
    def get_job(job_id: str, session: SessionDep) -> IngestionJob:
        job = session.get(IngestionJob, job_id)
        if job is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
        return job

    @app.get("/jobs/{job_id}/profile", response_model=list[ColumnProfileOut])
    def get_profile(job_id: str, session: SessionDep) -> list[ColumnProfile]:
        if session.get(IngestionJob, job_id) is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
        return list(session.scalars(select(ColumnProfile)
                                    .where(ColumnProfile.job_id == job_id)
                                    .order_by(ColumnProfile.position)))

    @app.post("/jobs/{job_id}/retry", response_model=JobOut,
              status_code=status.HTTP_202_ACCEPTED)
    def retry(job_id: str, session: SessionDep) -> IngestionJob:
        """Re-send the message for a FAILED job, or a QUEUED job whose message was lost."""
        job = session.get(IngestionJob, job_id)
        if job is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
        if job.status not in (JobStatus.FAILED, JobStatus.QUEUED):
            raise HTTPException(status.HTTP_409_CONFLICT, f"job is {job.status.value}")
        job.status = JobStatus.QUEUED
        job.error = None
        session.commit()
        app.state.queue.send(JobMessage(job_id=job.id, content_hash=job.content_hash))
        return job

    return app
