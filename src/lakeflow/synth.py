"""Synthetic business-partner data with known, injected defects.

We never use real client data on laptops or in the repo. Instead we generate data where
every defect is planted on purpose and counted in a manifest, so tests can assert that
the pipeline catches exactly what was planted, no more and no less.

Defects are assigned by `row_index % 100`, so they never overlap and the expected counts
are exact (n must be a multiple of 100):
  0-1   name missing                        -> rejected (required)
  2-3   malformed email                     -> rejected (pattern)
  4     negative credit limit               -> rejected (min)
  5-9   created_at as dd/mm/yyyy            -> valid after date parsing
  10-14 padded whitespace in id and name    -> valid after cleansing
  20-22 second, newer version of the record -> older version removed as key duplicate
  30-31 identical copy of the row           -> removed as exact duplicate
Names include accented characters (é, ü, ø, á) to exercise encoding detection.

Usage:
  python -m lakeflow.synth --rows 1000000 --out data/bp_1m.csv --encoding cp1252 --delimiter ";"
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path

import polars as pl

HEADERS = {
    "partner_id": "Partner ID", "name": "Name", "email": "Email", "country": "Country",
    "phone": "Phone", "credit_limit": "Credit Limit", "created_at": "Created At",
    "updated_at": "Updated At",
}
FIRST = ["Ali", "Sara", "Jürgen", "Zoë", "José", "Ahmed", "Fatima", "Chloé", "Søren", "Liam"]
LAST = ["Khan", "Müller", "García", "Smith", "Dubois", "Nakamura", "Hussain", "Rossi",
        "Novák", "Ahmadi"]
COUNTRIES = ["PK", "DE", "GB", "FR", "US", "AE", "NL", "ES"]


@dataclass(frozen=True)
class Manifest:
    base_rows: int
    rows_in: int
    rows_exact_duplicate: int
    rows_rejected: int
    rows_key_duplicate: int
    rows_valid: int
    rejected_required_name: int
    rejected_bad_email: int
    rejected_negative_credit: int


def generate(n: int, seed: int = 7) -> tuple[pl.DataFrame, Manifest]:
    if n % 100:
        raise ValueError("n must be a multiple of 100 so defect counts are exact")
    i = pl.col("i")
    b = (i % 100).alias("b")
    base = pl.DataFrame({"i": pl.int_range(0, n, eager=True)}).with_columns(b)

    clean = base.with_columns(
        partner_id=pl.format("BP{}", (i + 1).cast(pl.String).str.zfill(7)),
        name=pl.format("{} {} Trading",
                       pl.lit(pl.Series(FIRST)).gather(i % 10),
                       pl.lit(pl.Series(LAST)).gather((i // 10) % 10)),
        email=pl.format("contact{}@example{}.com", i, i % 7),
        country=pl.lit(pl.Series(COUNTRIES)).gather(i % 8),
        phone=pl.format("+92-3{}-{}", i % 90 + 10, i % 9_000_000 + 1_000_000),
        credit_limit=((i * 7919) % 100_000).cast(pl.Float64).round(2),
        created_at=pl.lit(date(2020, 1, 1)) + pl.duration(days=i % 2000),
        updated_at=pl.lit(datetime(2024, 1, 1)) + pl.duration(seconds=i % 500_000),
    )

    b = pl.col("b")
    defective = clean.with_columns(
        name=pl.when(b.is_between(0, 1)).then(None)
        .when(b.is_between(10, 14)).then(pl.format("   {}  ", pl.col("name")))
        .otherwise(pl.col("name")),
        partner_id=pl.when(b.is_between(10, 14)).then(pl.format(" {} ", pl.col("partner_id")))
        .otherwise(pl.col("partner_id")),
        email=pl.when(b.is_between(2, 3)).then(pl.format("contact{}-at-example.com", i))
        .otherwise(pl.col("email")),
        credit_limit=pl.when(b == 4).then(pl.lit("-500.00"))
        .otherwise(pl.col("credit_limit").cast(pl.String)),
        created_at=pl.when(b.is_between(5, 9)).then(pl.col("created_at").dt.strftime("%d/%m/%Y"))
        .otherwise(pl.col("created_at").dt.strftime("%Y-%m-%d")),
        updated_at=pl.col("updated_at").dt.strftime("%Y-%m-%d %H:%M:%S"),
    )

    newer = (
        clean.filter(b.is_between(20, 22))
        .with_columns(
            email=pl.format("updated{}@example.com", i),
            credit_limit=pl.col("credit_limit").cast(pl.String),
            created_at=pl.col("created_at").dt.strftime("%Y-%m-%d"),
            updated_at=(pl.col("updated_at") + pl.duration(days=1))
            .dt.strftime("%Y-%m-%d %H:%M:%S"),
        )
    )
    exact_copies = defective.filter(b.is_between(30, 31))

    cols = list(HEADERS)
    df = (
        pl.concat([defective.select(cols), newer.select(cols), exact_copies.select(cols)])
        .sample(fraction=1.0, shuffle=True, seed=seed)
        .rename(HEADERS)
    )
    p = n // 100
    manifest = Manifest(
        base_rows=n, rows_in=df.height, rows_exact_duplicate=2 * p, rows_rejected=5 * p,
        rows_key_duplicate=3 * p, rows_valid=95 * p, rejected_required_name=2 * p,
        rejected_bad_email=2 * p, rejected_negative_credit=p,
    )
    return df, manifest


def write_csv(df: pl.DataFrame, path: Path, encoding: str = "utf-8", delimiter: str = ",") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if encoding.lower().replace("-", "") in ("utf8",):
        df.write_csv(path, separator=delimiter)
    else:
        path.write_bytes(df.write_csv(separator=delimiter).encode(encoding))


def write_excel(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_excel(path, worksheet="Partners")


def write_xml(df: pl.DataFrame, path: Path) -> None:
    from xml.sax.saxutils import escape

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write('<?xml version="1.0" encoding="UTF-8"?>\n<Partners>\n')
        for row in df.iter_rows(named=True):
            fh.write("  <Partner>")
            for key, value in row.items():
                tag = key.replace(" ", "_")
                fh.write(f"<{tag}>{escape(value) if value is not None else ''}</{tag}>")
            fh.write("</Partner>\n")
        fh.write("</Partners>\n")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--rows", type=int, default=100_000)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--format", choices=["csv", "excel", "xml"], default="csv")
    ap.add_argument("--encoding", default="utf-8")
    ap.add_argument("--delimiter", default=",")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    df, manifest = generate(args.rows, args.seed)
    if args.format == "csv":
        write_csv(df, args.out, args.encoding, args.delimiter)
    elif args.format == "excel":
        write_excel(df, args.out)
    else:
        write_xml(df, args.out)
    args.out.with_suffix(".manifest.json").write_text(json.dumps(asdict(manifest), indent=2))
    print(json.dumps(asdict(manifest), indent=2))


if __name__ == "__main__":
    main()
