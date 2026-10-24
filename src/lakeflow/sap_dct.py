"""Generate SAP Migration Cockpit import rows from cleansed data.

Background. In SAP S/4HANA's Migration Cockpit ("Migrate Your Data"), each migration object
(Business Partner, Product, G/L account, ...) has a downloadable template. It is an XML
Spreadsheet 2003 file with a field-list sheet and one data sheet per structure (for
Business Partner: general data, addresses, roles, ...). SAP validates every value on import:
mandatory fields, exact lengths, allowed codes, and date/number formats.

What this module does is the part that is the same for every object:
- a FIELD LIST (technical name, length, mandatory, type) taken from the template,
- a MAPPING from our cleansed columns (or constants) to those fields,
- STRICT validation: a value that is too long is an error, never silently truncated,
  because a truncated name or ID imported into SAP is far worse than a rejected row,
- SAP value formats: dates as YYYYMMDD, decimals with "." and no thousands separator,
- rows written in the exact field order of the template.

Writing those rows into the template's own data sheet is template-version specific and is
kept as a separate, thin step. The example field list in contracts/ is modelled on the
Business Partner object; confirm names and lengths against the template you download.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Literal

import polars as pl


@dataclass(frozen=True)
class DctField:
    name: str  # technical field name in the template
    length: int
    mandatory: bool = False
    kind: Literal["char", "date", "dec"] = "char"
    source: str | None = None  # cleansed column to read from
    constant: str | None = None  # or a fixed value (e.g. grouping, category)


def load_fields(path: str | Path) -> list[DctField]:
    return [DctField(**f) for f in json.loads(Path(path).read_text(encoding="utf-8"))["fields"]]


def _format(value: object, kind: str) -> str | None:
    if value is None:
        return None
    if kind == "date" and isinstance(value, (date, datetime)):
        return value.strftime("%Y%m%d")
    if kind == "dec" and isinstance(value, float):
        return f"{value:.2f}"
    return str(value).strip()


def build_rows(df: pl.DataFrame, fields: list[DctField]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Returns (rows ready for the template, rows that would fail SAP's checks + reasons)."""
    out: dict[str, list[str | None]] = {f.name: [] for f in fields}
    errors: list[str] = []
    for row in df.iter_rows(named=True):
        problems = []
        for f in fields:
            value = f.constant if f.constant is not None else _format(row.get(f.source or ""),
                                                                       f.kind)
            if f.mandatory and not value:
                problems.append(f"{f.name}:mandatory")
            elif value and len(value) > f.length:
                problems.append(f"{f.name}:length {len(value)}>{f.length}")
            out[f.name].append(value)
        errors.append(";".join(problems))
    table = pl.DataFrame(out, schema={f.name: pl.String for f in fields})
    table = table.with_columns(pl.Series("_errors", errors))
    ok = table.filter(pl.col("_errors") == "").drop("_errors")
    bad = table.filter(pl.col("_errors") != "")
    return ok, bad
