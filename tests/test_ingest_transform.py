"""Conversion and transformation must catch exactly the defects the generator planted."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from lakeflow.detect import detect
from lakeflow.ingest import convert
from lakeflow.synth import Manifest, generate, write_excel, write_xml
from lakeflow.transform import Contract, ContractError, transform


def _run(src: Path, tmp: Path, contract: Contract):
    detected = detect(src)
    converted = convert(src, tmp / "raw", detected)
    result = transform(converted.parquet_files, contract, tmp / "cleansed", tmp / "quarantine")
    return converted, result


def _assert_matches(result, m: Manifest) -> None:
    assert result.rows_in == m.rows_in
    assert result.rows_exact_duplicate == m.rows_exact_duplicate
    assert result.rows_rejected == m.rows_rejected
    assert result.rows_key_duplicate == m.rows_key_duplicate
    assert result.rows_valid == m.rows_valid


def test_csv_cp1252_end_to_end(bp_csv, tmp_path: Path, contract: Contract) -> None:
    src, manifest = bp_csv
    converted, result = _run(src, tmp_path, contract)
    assert converted.rows == manifest.rows_in
    assert converted.replaced_chars == 0
    _assert_matches(result, manifest)

    raw = pl.read_parquet(converted.parquet_files)
    assert all(dtype == pl.String for dtype in raw.schema.values())  # raw zone = strings
    # "ü" is the same byte in cp1250 and cp1252; "ø" is not. Check both, so a wrong
    # guess between close code pages cannot pass unnoticed.
    assert raw["Name"].str.contains("Müller").any()
    assert raw["Name"].str.contains("Søren").any()
    assert not raw["Name"].str.contains("ř").any()

    clean = pl.read_parquet(tmp_path / "cleansed")
    assert clean.schema["created_at"] == pl.Date
    assert clean.schema["credit_limit"] == pl.Float64
    assert clean["partner_id"].is_unique().all()
    assert not clean["name"].str.starts_with(" ").any()  # whitespace cleansed


def test_rejections_carry_reasons(bp_csv, tmp_path: Path, contract: Contract) -> None:
    src, m = bp_csv
    _run(src, tmp_path, contract)
    q = pl.read_parquet(tmp_path / "quarantine")
    counts = dict(q.group_by("reasons").len().iter_rows())
    assert counts["name:required"] == m.rejected_required_name
    assert counts["email:pattern"] == m.rejected_bad_email
    assert counts["credit_limit:min"] == m.rejected_negative_credit
    assert counts["duplicate_key"] == m.rows_key_duplicate


def test_latest_version_wins_on_key_duplicates(bp_csv, tmp_path: Path, contract) -> None:
    src, _ = bp_csv
    _run(src, tmp_path, contract)
    clean = pl.read_parquet(tmp_path / "cleansed")
    # Base partner index 20 (BP0000021) has a newer version with an "updated" email.
    row = clean.filter(pl.col("partner_id") == "BP0000021")
    assert row["email"].item().startswith("updated")


def test_day_first_dates_are_parsed(bp_csv, tmp_path: Path, contract) -> None:
    src, _ = bp_csv
    _run(src, tmp_path, contract)
    clean = pl.read_parquet(tmp_path / "cleansed")
    assert clean["created_at"].null_count() == 0


@pytest.mark.parametrize("writer", [write_excel, write_xml], ids=["excel", "xml"])
def test_excel_and_xml_give_the_same_result(writer, tmp_path: Path, contract) -> None:
    df, manifest = generate(1_000)
    src = tmp_path / "input.bin"
    writer(df, src)
    _, result = _run(src, tmp_path, contract)
    _assert_matches(result, manifest)


def test_missing_required_column_is_a_contract_error(tmp_path: Path, contract) -> None:
    src = tmp_path / "bad.csv"
    src.write_text("Partner ID,Name\nBP0000001,Ali\n", encoding="utf-8")
    with pytest.raises(ContractError, match="country"):
        _run(src, tmp_path, contract)


def test_ragged_rows_do_not_crash(tmp_path: Path, contract) -> None:
    src = tmp_path / "ragged.csv"
    header = "Partner ID,Name,Email,Country,Phone,Credit Limit,Created At,Updated At\n"
    good = "BP0000001,Ali,a@b.com,PK,1,10,2024-01-01,2024-01-01 10:00:00\n"
    short = "BP0000002,Sara\n"
    src.write_text(header + good + short, encoding="utf-8")
    _, result = _run(src, tmp_path, contract)
    assert result.rows_valid == 1 and result.rows_rejected == 1
