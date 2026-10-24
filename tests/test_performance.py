"""1M-row benchmark. Skipped by default; run with: pytest -m slow -s"""

from __future__ import annotations

import time

import pytest

from lakeflow.detect import detect
from lakeflow.ingest import convert
from lakeflow.synth import generate, write_csv
from lakeflow.transform import transform

TIME_BUDGET_S = 120  # agreed limit for 1M rows end to end on a laptop-class machine


@pytest.mark.slow
def test_one_million_rows_within_budget(tmp_path, contract) -> None:
    df, m = generate(1_000_000)
    src = tmp_path / "bp_1m.csv"
    write_csv(df, src, encoding="cp1252", delimiter=";")
    del df

    t0 = time.perf_counter()
    detected = detect(src)
    converted = convert(src, tmp_path / "raw", detected)
    t1 = time.perf_counter()
    result = transform(converted.parquet_files, contract, tmp_path / "c", tmp_path / "q")
    t2 = time.perf_counter()

    size_mb = src.stat().st_size / 1e6
    print(f"\n{m.rows_in:,} rows, {size_mb:.0f} MB cp1252 CSV: "
          f"convert {t1 - t0:.1f}s, transform {t2 - t1:.1f}s, total {t2 - t0:.1f}s")
    assert result.rows_valid == m.rows_valid
    assert t2 - t0 < TIME_BUDGET_S
