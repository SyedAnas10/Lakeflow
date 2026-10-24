"""Integration test against Azurite, the local Azure Storage emulator.

Runs only when AZURITE_CONNECTION_STRING is set, e.g. with Azurite started by
`docker compose up azurite` or `npx azurite-blob --inMemoryPersistence`:

  AZURITE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;\
AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;\
BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;" pytest tests/test_azurite.py

(That key is Azurite's published, well-known development key, not a secret.)
"""

from __future__ import annotations

import os
import uuid

import polars as pl
import pytest

from lakeflow import zones
from lakeflow.storage import AzureBlobLake
from lakeflow.worker import Worker
from tests.test_worker import _submit

CONN = os.environ.get("AZURITE_CONNECTION_STRING")
pytestmark = pytest.mark.skipif(not CONN, reason="AZURITE_CONNECTION_STRING not set")


@pytest.fixture
def azure_lake() -> AzureBlobLake:
    return AzureBlobLake(CONN, f"test-{uuid.uuid4().hex[:8]}")


def test_round_trip_and_prefix_ops(azure_lake, tmp_path) -> None:
    src = tmp_path / "a.txt"
    src.write_bytes(b"x" * (5 * 1024 * 1024))  # 5 MB: the SDK uploads it as blocks
    azure_lake.upload_file(src, "landing/a/a.txt")
    azure_lake.write_text("landing/a/_SUCCESS", "")
    assert azure_lake.exists("landing/a/a.txt")
    assert azure_lake.list_keys("landing/a") == ["landing/a/_SUCCESS", "landing/a/a.txt"]

    out = tmp_path / "back.txt"
    azure_lake.download_file("landing/a/a.txt", out)
    assert out.read_bytes() == src.read_bytes()

    azure_lake.delete_prefix("landing/a")
    assert azure_lake.list_keys("landing/a") == []


def test_full_pipeline_on_azurite(azure_lake, session_factory, queue, contract, bp_csv,
                                  tmp_path) -> None:
    src, manifest = bp_csv
    job_id = _submit(session_factory, azure_lake, queue, src)
    Worker(session_factory, azure_lake, queue, contract, tmp_path / "work").run_once()

    keys = azure_lake.list_keys(zones.CLEANSED)
    assert any(k.endswith(zones.SUCCESS_MARKER) for k in keys)
    part = next(k for k in keys if k.endswith(".parquet"))
    local = tmp_path / "clean.parquet"
    azure_lake.download_file(part, local)
    assert pl.read_parquet(local).height == manifest.rows_valid
    assert job_id
