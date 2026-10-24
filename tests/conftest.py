from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy.orm import sessionmaker

from lakeflow.db import Base, make_engine, make_session_factory
from lakeflow.queue import InMemoryQueue
from lakeflow.storage import LocalLake
from lakeflow.synth import Manifest, generate, write_csv
from lakeflow.transform import Contract

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def contract() -> Contract:
    return Contract.load(ROOT / "contracts" / "business_partner.json")


@pytest.fixture
def session_factory(tmp_path: Path) -> sessionmaker:
    engine = make_engine(f"sqlite:///{tmp_path / 'meta.db'}")
    Base.metadata.create_all(engine)
    return make_session_factory(engine)


@pytest.fixture
def lake(tmp_path: Path) -> LocalLake:
    return LocalLake(tmp_path / "lake")


@pytest.fixture
def queue() -> InMemoryQueue:
    return InMemoryQueue(max_delivery_count=3)


@pytest.fixture
def bp_csv(tmp_path: Path) -> tuple[Path, Manifest]:
    """2,000 base partners, cp1252 encoded, semicolon separated: the awkward real-world case."""
    df, manifest = generate(2_000)
    path = tmp_path / "partners.csv"
    write_csv(df, path, encoding="cp1252", delimiter=";")
    return path, manifest
