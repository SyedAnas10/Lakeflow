"""Command line entry points.

  lakeflow init-db              create the database if needed and run Alembic migrations
  lakeflow api                  start the FastAPI service
  lakeflow worker               start a queue worker (run several for parallelism)
  lakeflow run FILE --source S  one-shot local demo: submit + process + print the summary
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import time
from datetime import UTC, datetime
from pathlib import Path

from lakeflow import zones
from lakeflow.config import Settings, load_settings
from lakeflow.db import IngestionJob, make_engine, make_session_factory
from lakeflow.queue import InMemoryQueue, JobMessage, make_queue
from lakeflow.storage import make_lake
from lakeflow.transform import Contract
from lakeflow.worker import Worker


def _lake(s: Settings):  # type: ignore[no-untyped-def]
    return make_lake(s.lake_backend, root=s.lake_root,
                     connection_string=s.azure_storage_connection_string,
                     container=s.lake_container, account_url=s.azure_storage_account_url)


def init_db(s: Settings) -> None:
    from alembic import command
    from alembic.config import Config
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import make_url

    url = make_url(s.database_url)
    if url.get_backend_name() == "mssql" and url.database:
        # Local SQL Server containers start with only `master`; create our database once.
        # DDL cannot take bind parameters, so the name is checked strictly first.
        name = url.database
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", name):
            raise ValueError(f"refusing unsafe database name {name!r}")
        master = create_engine(url.set(database="master"), isolation_level="AUTOCOMMIT")
        with master.connect() as conn:
            conn.execute(text(f"IF DB_ID('{name}') IS NULL CREATE DATABASE [{name}]"))
    command.upgrade(Config("alembic.ini"), "head")


def run_local(s: Settings, path: Path, source: str) -> dict[str, object]:
    engine = make_engine(s.database_url)
    sf = make_session_factory(engine)
    lake = _lake(s)
    queue = InMemoryQueue()

    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(1024 * 1024):
            digest.update(chunk)
    content_hash = digest.hexdigest()
    key = zones.landing_key(source, datetime.now(UTC).date(), content_hash, path.name)
    lake.upload_file(path, key)
    with sf() as session:
        job = IngestionJob(content_hash=content_hash, source=source, filename=path.name,
                           landing_key=key, size_bytes=path.stat().st_size)
        session.add(job)
        session.commit()
        job_id = job.id
    queue.send(JobMessage(job_id=job_id, content_hash=content_hash))

    started = time.perf_counter()
    Worker(sf, lake, queue, Contract.load(s.contract), s.work_dir).run_once()
    elapsed = time.perf_counter() - started
    with sf() as session:
        job = session.get(IngestionJob, job_id)
        assert job is not None
        return {
            "job_id": job.id, "status": job.status.value, "seconds": round(elapsed, 2),
            "format": job.file_format, "encoding": job.encoding, "delimiter": job.delimiter,
            "rows_raw": job.rows_raw, "rows_exact_duplicate": job.rows_exact_duplicate,
            "rows_rejected": job.rows_rejected, "rows_key_duplicate": job.rows_key_duplicate,
            "rows_valid": job.rows_valid, "error": job.error,
        }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(prog="lakeflow")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init-db")
    api = sub.add_parser("api")
    api.add_argument("--host", default="0.0.0.0")
    api.add_argument("--port", type=int, default=8000)
    sub.add_parser("worker")
    run = sub.add_parser("run")
    run.add_argument("file", type=Path)
    run.add_argument("--source", default="demo")
    args = ap.parse_args()
    s = load_settings()

    if args.cmd == "init-db":
        init_db(s)
    elif args.cmd == "api":
        import uvicorn

        from lakeflow.api import create_app

        uvicorn.run(create_app(s), host=args.host, port=args.port)
    elif args.cmd == "worker":
        queue = make_queue(s.queue_backend, connection_string=s.servicebus_connection_string,
                           queue_name=s.queue_name, max_delivery_count=s.max_delivery_count,
                           namespace=s.servicebus_namespace)
        sf = make_session_factory(make_engine(s.database_url))
        Worker(sf, _lake(s), queue, Contract.load(s.contract), s.work_dir,
               s.max_delivery_count).run_forever()
    elif args.cmd == "run":
        print(json.dumps(run_local(s, args.file, args.source), indent=2))


if __name__ == "__main__":
    main()
