"""Cleansing, typing, validation and deduplication, driven by a data contract (JSON).

Pipeline, in this order and for these reasons:
1. Normalise headers      " Partner ID " -> "partner_id", so files from different senders line up.
2. Cleanse strings        trim, collapse inner spaces, "" / "NULL" / "N/A" -> null.
3. Drop exact duplicates  identical rows are noise; removing them first is cheap.
4. Cast to types          keep the original text; if text was present but the cast gave null,
                          that is a type error we can report (never a silent null).
5. Validate rules         required, regex pattern, min/max, max length. Every failing rule is
                          collected per row, so a rejected row lists ALL its problems.
6. Split                  rows with no errors -> cleansed; others -> quarantine with reasons.
7. Deduplicate by key     on valid rows only, keep the latest version by `keep_latest_by`.
                          Done after validation so a broken newer record cannot knock out a
                          good older one.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import polars as pl

DType = Literal["str", "int", "float", "date", "datetime", "bool"]
NULL_TOKENS = ["", "NULL", "null", "N/A", "n/a", "NA", "-"]
DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y", "%Y%m%d"]
DATETIME_FORMATS = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M"]
ERRORS = "_errors"


class ContractError(ValueError):
    """The file does not fit the contract at all (for example a required column is missing)."""


@dataclass(frozen=True)
class ColumnRule:
    name: str
    dtype: DType = "str"
    required: bool = False
    pattern: str | None = None
    min: float | None = None
    max: float | None = None
    max_length: int | None = None


@dataclass(frozen=True)
class Contract:
    name: str
    columns: list[ColumnRule]
    key: list[str] = field(default_factory=list)
    keep_latest_by: str | None = None

    @classmethod
    def load(cls, path: str | Path) -> Contract:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(
            name=raw["name"],
            columns=[ColumnRule(**c) for c in raw["columns"]],
            key=raw.get("key", []),
            keep_latest_by=raw.get("keep_latest_by"),
        )


@dataclass(frozen=True)
class TransformResult:
    rows_in: int
    rows_exact_duplicate: int
    rows_rejected: int
    rows_key_duplicate: int
    rows_valid: int


def normalise_header(name: str) -> str:
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()
    return name or "unnamed"


def _cleanse(col: str) -> pl.Expr:
    text = pl.col(col).str.strip_chars().str.replace_all(r"\s+", " ")
    return pl.when(text.is_in(NULL_TOKENS)).then(None).otherwise(text).alias(col)


def _parse_any(col: str, formats: list[str], to: Literal["date", "datetime"]) -> pl.Expr:
    """Try each format; the first that parses wins. Ambiguity note: 03/04/2024 is read as
    3 April because day-first formats come first. That choice belongs in the contract."""
    s = pl.col(col)
    if to == "date":
        attempts = [s.str.to_date(f, strict=False) for f in formats]
    else:
        attempts = [s.str.to_datetime(f, strict=False) for f in formats]
    return pl.coalesce(attempts)


def _cast(rule: ColumnRule) -> pl.Expr:
    s = pl.col(rule.name)
    if rule.dtype == "int":
        return s.cast(pl.Int64, strict=False)
    if rule.dtype == "float":
        return s.str.replace_all(",", "").cast(pl.Float64, strict=False)
    if rule.dtype == "bool":
        low = s.str.to_lowercase()
        return (
            pl.when(low.is_in(["true", "1", "yes", "y"])).then(True)
            .when(low.is_in(["false", "0", "no", "n"])).then(False)
            .otherwise(None)
        )
    if rule.dtype == "date":
        return _parse_any(rule.name, DATE_FORMATS, "date")
    if rule.dtype == "datetime":
        return _parse_any(rule.name, DATETIME_FORMATS, "datetime")
    return s


def _rule_errors(rule: ColumnRule) -> list[pl.Expr]:
    """One expression per rule: the error label when the rule fails, else null."""
    raw, typed = pl.col(rule.name), pl.col(f"__typed_{rule.name}")
    checks: list[tuple[pl.Expr, str]] = []
    if rule.required:
        checks.append((raw.is_null(), "required"))
    if rule.dtype != "str":
        checks.append((raw.is_not_null() & typed.is_null(), f"type:{rule.dtype}"))
    if rule.pattern:
        checks.append((raw.is_not_null() & ~raw.str.contains(rule.pattern), "pattern"))
    if rule.max_length is not None:
        checks.append((raw.str.len_chars() > rule.max_length, "max_length"))
    if rule.min is not None:
        checks.append((typed < rule.min, "min"))
    if rule.max is not None:
        checks.append((typed > rule.max, "max"))
    return [
        pl.when(cond.fill_null(False)).then(pl.lit(f"{rule.name}:{label}")).otherwise(None)
        for cond, label in checks
    ]


def transform(raw_parquet: list[Path], contract: Contract, cleansed_out: Path,
              quarantine_out: Path) -> TransformResult:
    lf = pl.scan_parquet(raw_parquet, missing_columns="insert")
    lf = lf.rename({c: normalise_header(c) for c in lf.collect_schema().names()})

    present = set(lf.collect_schema().names())
    missing = [c.name for c in contract.columns if c.required and c.name not in present]
    if missing:
        raise ContractError(f"missing required columns: {missing}")
    rules = [c for c in contract.columns if c.name in present]
    names = [r.name for r in rules]

    # Materialise once. Conversion already streamed; a typed 1M-row frame is a few hundred MB.
    # Beyond that, split the input by hash(key) into partitions and run this per partition.
    df = lf.select(names).with_columns([_cleanse(n) for n in names]).collect(engine="streaming")
    rows_in = df.height

    df = df.unique(maintain_order=True)
    rows_exact_dup = rows_in - df.height

    df = df.with_columns([_cast(r).alias(f"__typed_{r.name}") for r in rules])
    error_exprs = [e for r in rules for e in _rule_errors(r)]
    df = df.with_columns(
        pl.concat_list(error_exprs).list.drop_nulls().alias(ERRORS) if error_exprs
        else pl.lit([], dtype=pl.List(pl.String)).alias(ERRORS)
    )
    has_errors = pl.col(ERRORS).list.len() > 0

    quarantine_out.mkdir(parents=True, exist_ok=True)
    cleansed_out.mkdir(parents=True, exist_ok=True)

    rejected = df.filter(has_errors).select(
        *names, pl.col(ERRORS).list.join(";").alias("reasons")
    )

    valid = df.filter(~has_errors).select(
        [pl.col(f"__typed_{n}").alias(n) for n in names]
    )
    key_dupes = pl.DataFrame()
    if contract.key:
        if contract.keep_latest_by:
            valid = valid.sort(contract.keep_latest_by, descending=True, nulls_last=True)
        is_first = pl.int_range(pl.len()).over(contract.key) == 0
        key_dupes = valid.filter(~is_first)
        valid = valid.filter(is_first)

    if key_dupes.height:
        dup_rows = key_dupes.select(
            [pl.col(n).cast(pl.String) for n in names]
        ).with_columns(pl.lit("duplicate_key").alias("reasons"))
        rejected = pl.concat([rejected, dup_rows], how="vertical")

    valid.write_parquet(cleansed_out / "part-00000.parquet", compression="zstd")
    rejected.write_parquet(quarantine_out / "part-00000.parquet", compression="zstd")

    return TransformResult(
        rows_in=rows_in,
        rows_exact_duplicate=rows_exact_dup,
        rows_rejected=rejected.height - key_dupes.height,
        rows_key_duplicate=key_dupes.height,
        rows_valid=valid.height,
    )
