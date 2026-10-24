"""The Alembic history must build exactly the schema the models describe."""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import create_engine, inspect

from lakeflow.db import Base

ROOT = Path(__file__).resolve().parents[1]


def test_upgrade_matches_models_and_downgrade_is_clean(tmp_path, monkeypatch) -> None:
    url = f"sqlite:///{tmp_path / 'm.db'}"
    monkeypatch.setenv("LAKEFLOW_DATABASE_URL", url)
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "migrations"))

    command.upgrade(cfg, "head")
    engine = create_engine(url)
    with engine.connect() as conn:
        diff = compare_metadata(MigrationContext.configure(conn), Base.metadata)
    assert diff == [], f"models and migrations disagree: {diff}"

    command.downgrade(cfg, "base")
    assert set(inspect(engine).get_table_names()) <= {"alembic_version"}
