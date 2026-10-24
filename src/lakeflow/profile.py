"""Column profiling with DuckDB SQL over Parquet.

We profile the RAW zone (all strings, exactly as received), because that is what tells a
data owner what their source really looks like before any rule touches it.

Why DuckDB here: Parquet is columnar, so a query on one column reads only that column's
pages from disk. Profiling 20 columns of a 1M-row file is 20 cheap scans, not 20 full
reads. It also keeps the profiling logic in plain SQL that anyone on the team can review.

Per column we return: null rate, approximate distinct count, min/max length, the share of
values that parse as integer / decimal / date (to suggest a type), the top values, and the
top "shapes" (letters -> A, digits -> 9), which exposes format problems at a glance:
"AA-9999" vs "99999" in a postcode column.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import duckdb

TOP_N = 5


@dataclass
class ColumnStats:
    column_name: str
    position: int
    row_count: int
    null_count: int
    null_rate: float
    distinct_approx: int
    min_length: int | None
    max_length: int | None
    inferred_type: str
    top_values: list[dict[str, object]] = field(default_factory=list)
    top_patterns: list[dict[str, object]] = field(default_factory=list)


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'  # quote identifiers safely


def _infer(non_null: int, ints: int, decimals: int, dates: int) -> str:
    if non_null == 0:
        return "empty"
    for label, hits in (("integer", ints), ("decimal", decimals), ("date", dates)):
        if hits / non_null >= 0.95:  # tolerate a few bad values; they show up in validation
            return label
    return "string"


def profile_parquet(files: list[Path]) -> list[ColumnStats]:
    con = duckdb.connect()
    paths = [str(p) for p in files]
    con.read_parquet(paths, union_by_name=True).create_view("src")
    columns = [row[0] for row in con.execute("DESCRIBE src").fetchall()]

    results: list[ColumnStats] = []
    for pos, col in enumerate(columns):
        c = _q(col)
        # One pass per column for the scalar stats.
        row = con.execute(f"""
            WITH v AS (SELECT NULLIF(TRIM(CAST({c} AS VARCHAR)), '') AS x FROM src)
            SELECT
                COUNT(*)                                             AS row_count,
                COUNT(*) FILTER (WHERE x IS NULL)                    AS null_count,
                APPROX_COUNT_DISTINCT(x)                             AS distinct_approx,
                MIN(LENGTH(x))                                       AS min_len,
                MAX(LENGTH(x))                                       AS max_len,
                COUNT(*) FILTER (WHERE REGEXP_FULL_MATCH(x, '-?[0-9]+')) AS ints,
                COUNT(TRY_CAST(REPLACE(x, ',', '') AS DOUBLE))       AS decimals,
                COUNT(COALESCE(TRY_CAST(x AS DATE),
                               TRY_STRPTIME(x, '%d/%m/%Y')))         AS dates
            FROM v
        """).fetchone()
        assert row is not None
        rows, nulls, distinct, min_len, max_len, ints, decimals, dates = row
        non_null = rows - nulls

        top_values = con.execute(f"""
            SELECT CAST({c} AS VARCHAR) AS value, COUNT(*) AS n
            FROM src WHERE {c} IS NOT NULL
            GROUP BY 1 ORDER BY n DESC, value LIMIT {TOP_N}
        """).fetchall()
        top_patterns = con.execute(f"""
            SELECT REGEXP_REPLACE(REGEXP_REPLACE(CAST({c} AS VARCHAR),
                       '[A-Za-z]', 'A', 'g'), '[0-9]', '9', 'g') AS pattern,
                   COUNT(*) AS n
            FROM src WHERE {c} IS NOT NULL
            GROUP BY 1 ORDER BY n DESC, pattern LIMIT {TOP_N}
        """).fetchall()

        results.append(ColumnStats(
            column_name=col,
            position=pos,
            row_count=rows,
            null_count=nulls,
            null_rate=round(nulls / rows, 6) if rows else 0.0,
            distinct_approx=int(distinct or 0),
            min_length=min_len,
            max_length=max_len,
            inferred_type=_infer(non_null, ints, decimals, dates),
            top_values=[{"value": v, "count": n} for v, n in top_values],
            top_patterns=[{"pattern": p, "count": n} for p, n in top_patterns],
        ))
    con.close()
    return results
