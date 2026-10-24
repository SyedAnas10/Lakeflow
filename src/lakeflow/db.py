"""Relational metadata (Azure SQL / SQL Server in production, SQLite in tests).

SQLAlchemy 2 typed models: `Mapped[...]` annotations give mypy and the IDE real types.
The schema itself is owned by Alembic migrations (migrations/versions), never by
`create_all` in production, so every schema change is reviewed and reversible.
"""

from __future__ import annotations

import enum
import uuid
from datetime import datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    Unicode,
    UniqueConstraint,
    create_engine,
    func,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker


class Base(DeclarativeBase):
    pass


class JobStatus(enum.StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class IngestionJob(Base):
    __tablename__ = "ingestion_job"

    id: Mapped[str] = mapped_column(String(36), primary_key=True,
                                    default=lambda: str(uuid.uuid4()))
    # Unique: the same file submitted twice maps to the same job (idempotent submission).
    content_hash: Mapped[str] = mapped_column(String(64), unique=True)
    source: Mapped[str] = mapped_column(String(100))
    # Unicode -> NVARCHAR on SQL Server; plain VARCHAR would mangle non-Latin filenames.
    filename: Mapped[str] = mapped_column(Unicode(255))
    landing_key: Mapped[str] = mapped_column(Unicode(1024))
    # BigInteger: a 2 GB file is 2,147,483,648 bytes, one more than a 32-bit INT can hold.
    size_bytes: Mapped[int] = mapped_column(BigInteger)

    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus, native_enum=False, length=20), default=JobStatus.QUEUED, index=True
    )
    stage: Mapped[str | None] = mapped_column(String(50))
    progress_pct: Mapped[int] = mapped_column(Integer, default=0)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    # Naive UTC on purpose: SQLite and SQL Server compare it the same way.
    lease_until: Mapped[datetime | None] = mapped_column(DateTime())

    # What the sender said the encoding is (optional); `encoding` is what we actually used.
    declared_encoding: Mapped[str | None] = mapped_column(String(40))
    file_format: Mapped[str | None] = mapped_column(String(20))
    encoding: Mapped[str | None] = mapped_column(String(40))
    delimiter: Mapped[str | None] = mapped_column(String(4))
    rows_raw: Mapped[int | None] = mapped_column(Integer)
    rows_exact_duplicate: Mapped[int | None] = mapped_column(Integer)
    rows_rejected: Mapped[int | None] = mapped_column(Integer)
    rows_key_duplicate: Mapped[int | None] = mapped_column(Integer)
    rows_valid: Mapped[int | None] = mapped_column(Integer)
    replaced_chars: Mapped[int | None] = mapped_column(Integer)
    error: Mapped[str | None] = mapped_column(Unicode())  # NVARCHAR(max); NTEXT is deprecated

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True),
                                                 server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True),
                                                 server_default=func.now(), onupdate=func.now())

    profiles: Mapped[list[ColumnProfile]] = relationship(
        back_populates="job", cascade="all, delete-orphan", order_by="ColumnProfile.position"
    )


class ColumnProfile(Base):
    __tablename__ = "column_profile"
    __table_args__ = (UniqueConstraint("job_id", "column_name", name="uq_profile_job_column"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(ForeignKey("ingestion_job.id", ondelete="CASCADE"),
                                        index=True)
    column_name: Mapped[str] = mapped_column(Unicode(255))
    position: Mapped[int] = mapped_column(Integer)
    row_count: Mapped[int] = mapped_column(Integer)
    null_count: Mapped[int] = mapped_column(Integer)
    null_rate: Mapped[float] = mapped_column(Float)
    distinct_approx: Mapped[int] = mapped_column(Integer)
    min_length: Mapped[int | None] = mapped_column(Integer)
    max_length: Mapped[int | None] = mapped_column(Integer)
    inferred_type: Mapped[str] = mapped_column(String(20))
    top_values: Mapped[list[dict[str, object]]] = mapped_column(JSON)
    top_patterns: Mapped[list[dict[str, object]]] = mapped_column(JSON)

    job: Mapped[IngestionJob] = relationship(back_populates="profiles")


def make_engine(url: str) -> Engine:
    kwargs: dict[str, object] = {"pool_pre_ping": True}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    return create_engine(url, **kwargs)


def make_session_factory(engine: Engine) -> sessionmaker:
    return sessionmaker(bind=engine, expire_on_commit=False)
