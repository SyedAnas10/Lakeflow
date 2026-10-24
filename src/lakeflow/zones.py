"""Data-lake layout. One function per zone so every path in the system is built in one place.

landing/     original file exactly as received (immutable, never rewritten)
raw/         the same data converted to Parquet, every column kept as a string
cleansed/    typed, validated, deduplicated Parquet
quarantine/  rows that failed validation or were dropped as duplicates, with the reason
curated/     business-ready outputs (for example SAP import files)

Paths are Hive-style (key=value) so engines like DuckDB, Polars, Synapse and Spark
can prune partitions by source and date. The content hash in every path makes each
write deterministic: re-running a job overwrites the same files instead of creating copies.
"""

from __future__ import annotations

from datetime import date

LANDING = "landing"
RAW = "raw"
CLEANSED = "cleansed"
QUARANTINE = "quarantine"
CURATED = "curated"

SUCCESS_MARKER = "_SUCCESS"


def _prefix(zone: str, source: str, ingest_date: date, content_hash: str) -> str:
    return f"{zone}/source={source}/ingest_date={ingest_date.isoformat()}/{content_hash}"


def landing_key(source: str, ingest_date: date, content_hash: str, filename: str) -> str:
    return f"{_prefix(LANDING, source, ingest_date, content_hash)}/{filename}"


def zone_dir(zone: str, source: str, ingest_date: date, content_hash: str) -> str:
    return _prefix(zone, source, ingest_date, content_hash)
