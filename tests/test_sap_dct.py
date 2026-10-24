from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from lakeflow.sap_dct import build_rows, load_fields

ROOT = Path(__file__).resolve().parents[1]


def test_formats_order_and_strict_length() -> None:
    fields = load_fields(ROOT / "contracts" / "sap_bp_general.json")
    df = pl.DataFrame({
        "partner_id": ["BP0000001", "BP0000002"],
        "name": ["Ali Khan Trading", "X" * 41],  # second row: 41 chars > NAME_ORG1 length 40
        "country": ["PK", "DE"],
        "email": ["a@b.com", None],
        "phone": ["+92-300-1", None],
        "credit_limit": [1500.5, 0.0],
        "created_at": [date(2024, 3, 7), date(2024, 1, 1)],
    })
    ok, bad = build_rows(df, fields)

    assert ok.columns == [f.name for f in fields]  # exact template field order
    first = ok.row(0, named=True)
    assert first["VALID_FROM"] == "20240307"  # SAP date format
    assert first["CREDIT_LIM"] == "1500.50"
    assert first["BU_TYPE"] == "2" and first["BU_GROUP"] == "BP02"

    assert bad.height == 1
    assert bad["_errors"].item() == "NAME_ORG1:length 41>40"  # rejected, never truncated
