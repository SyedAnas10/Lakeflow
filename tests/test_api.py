from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from lakeflow.api import create_app
from lakeflow.config import load_settings
from lakeflow.worker import Worker


@pytest.fixture
def client(session_factory, lake, queue) -> TestClient:
    app = create_app(load_settings(), session_factory=session_factory, lake=lake, queue=queue)
    return TestClient(app)


def test_submit_process_and_read_profile(client, session_factory, lake, queue, contract,
                                         bp_csv, tmp_path) -> None:
    src, manifest = bp_csv
    with open(src, "rb") as fh:
        r = client.post("/jobs", params={"source": "sap_bp"}, files={"file": (src.name, fh)})
    assert r.status_code == 202
    job = r.json()
    assert job["status"] == "QUEUED"

    Worker(session_factory, lake, queue, contract, tmp_path / "work").run_once()

    job = client.get(f"/jobs/{job['id']}").json()
    assert job["status"] == "SUCCEEDED"
    assert job["rows_valid"] == manifest.rows_valid

    profile = client.get(f"/jobs/{job['id']}/profile").json()
    by_name = {c["column_name"]: c for c in profile}
    assert by_name["Name"]["null_count"] == manifest.rejected_required_name
    assert by_name["Credit Limit"]["inferred_type"] == "decimal"
    assert by_name["Partner ID"]["top_patterns"][0]["pattern"] == "AA9999999"


def test_same_file_twice_returns_same_job(client, bp_csv) -> None:
    src, _ = bp_csv
    first = client.post("/jobs", params={"source": "sap_bp"},
                        files={"file": (src.name, src.read_bytes())})
    second = client.post("/jobs", params={"source": "sap_bp"},
                         files={"file": ("renamed.csv", src.read_bytes())})
    assert first.status_code == 202 and second.status_code == 200
    assert first.json()["id"] == second.json()["id"]


def test_validation_errors(client) -> None:
    assert client.post("/jobs", params={"source": "Bad Name!"},
                       files={"file": ("a.csv", b"x")}).status_code == 422
    assert client.post("/jobs", params={"source": "ok"},
                       files={"file": ("a.csv", b"")}).status_code == 400
    assert client.get("/jobs/does-not-exist").status_code == 404


def test_declared_encoding_beats_detection(client, session_factory, lake, queue, contract,
                                           tmp_path) -> None:
    header = "Partner ID;Name;Email;Country;Phone;Credit Limit;Created At;Updated At\n"
    row = "BP0000001;Søren Ørsted;s@x.dk;DK;1;10;2024-01-01;2024-01-01 10:00:00\n"
    payload = (header + row).encode("cp1252")
    r = client.post("/jobs", params={"source": "dk", "encoding": "windows-1252"},
                    files={"file": ("dk.csv", payload)})
    assert r.status_code == 202 and r.json()["declared_encoding"] == "cp1252"
    Worker(session_factory, lake, queue, contract, tmp_path / "work").run_once()
    job = client.get(f"/jobs/{r.json()['id']}").json()
    assert job["status"] == "SUCCEEDED" and job["encoding"] == "cp1252"


def test_unknown_encoding_is_rejected(client) -> None:
    r = client.post("/jobs", params={"source": "x", "encoding": "klingon-8"},
                    files={"file": ("a.csv", b"a,b\n1,2\n")})
    assert r.status_code == 422
