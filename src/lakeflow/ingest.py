"""Convert a source file to Parquet with every column kept as a string (the raw zone).

Why strings? Type inference guesses from the first rows. If row 900,000 has "N/A" in a
numeric column, an inferred Int64 either crashes the load or silently turns the value into
null, and you lose the evidence. Landing everything as text keeps the data exactly as
received; typing and validation happen later in cleansing, where failures are recorded.

Memory: CSV is streamed end to end (scan_csv -> sink_parquet) so a 2 GB file never sits in
RAM. Non-UTF-8 text is first re-encoded to UTF-8 in 8 MB chunks, because Polars reads only
UTF-8. XML is streamed with lxml.iterparse in batches. Excel is the exception: an .xlsx is
a zip of XML parts and the reader loads one sheet at a time.
"""

from __future__ import annotations

import codecs
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from lxml import etree

from lakeflow.detect import Detected, UnsupportedFileError

CHUNK = 8 * 1024 * 1024
XML_BATCH_ROWS = 50_000


@dataclass(frozen=True)
class ConvertResult:
    parquet_files: list[Path]
    rows: int
    replaced_chars: int  # characters that could not be decoded and became U+FFFD


def transcode_to_utf8(src: Path, dst: Path, encoding: str) -> int:
    """Stream-decode src and write UTF-8. Returns how many characters were undecodable."""
    decoder = codecs.getincrementaldecoder(encoding)(errors="replace")
    replaced = 0
    with open(src, "rb") as fin, open(dst, "w", encoding="utf-8", newline="") as fout:
        while chunk := fin.read(CHUNK):
            text = decoder.decode(chunk)
            replaced += text.count("�")
            fout.write(text)
        tail = decoder.decode(b"", final=True)
        replaced += tail.count("�")
        fout.write(tail)
    return replaced


def csv_to_parquet(src: Path, out_dir: Path, detected: Detected) -> ConvertResult:
    out_dir.mkdir(parents=True, exist_ok=True)
    replaced = 0
    text_file = src
    if detected.encoding not in ("utf-8", None):
        text_file = out_dir / "_utf8.csv"
        replaced = transcode_to_utf8(src, text_file, detected.encoding)

    target = out_dir / "part-00000.parquet"
    (
        pl.scan_csv(
            text_file,
            separator=detected.delimiter or ",",
            infer_schema=False,  # every column is a string
            truncate_ragged_lines=True,
            missing_columns="insert",
        )
        .sink_parquet(target, compression="zstd")
    )
    if text_file != src:
        text_file.unlink()
    rows = pl.scan_parquet(target).select(pl.len()).collect().item()
    return ConvertResult([target], rows, replaced)


def excel_to_parquet(src: Path, out_dir: Path) -> ConvertResult:
    out_dir.mkdir(parents=True, exist_ok=True)
    # sheet_id=0 returns every sheet; infer_schema_length=0 keeps all values as strings.
    sheets = pl.read_excel(src, sheet_id=0, engine="calamine", infer_schema_length=0)
    files: list[Path] = []
    rows = 0
    for i, (name, df) in enumerate(sheets.items()):
        if df.height == 0:
            continue
        df = df.with_columns(pl.all().cast(pl.String)).with_columns(pl.lit(name).alias("_sheet"))
        target = out_dir / f"part-{i:05d}.parquet"
        df.write_parquet(target, compression="zstd")
        files.append(target)
        rows += df.height
    if not files:
        raise UnsupportedFileError("Excel workbook has no data rows")
    return ConvertResult(files, rows, 0)


def _local(tag: str) -> str:
    return etree.QName(tag).localname  # strip XML namespaces: {ns}Partner -> Partner


def _record_tag(src: Path) -> str:
    """The record element is the first child of the root, e.g. <Partners><Partner>..."""
    for _event, elem in etree.iterparse(str(src), events=("start",), huge_tree=True):
        if elem.getparent() is not None:
            return _local(elem.tag)
    raise UnsupportedFileError("XML has no record elements")


def _iter_records(src: Path, record_tag: str) -> Iterator[dict[str, str | None]]:
    for _, elem in etree.iterparse(str(src), events=("end",), huge_tree=True):
        if _local(elem.tag) != record_tag:
            continue
        record: dict[str, str | None] = {f"@{k}": v for k, v in elem.attrib.items()}
        for child in elem:
            text = child.text.strip() if child.text else None
            record[_local(child.tag)] = text or None
        yield record
        # Free memory as we go, otherwise lxml keeps the whole tree alive.
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]


def xml_to_parquet(src: Path, out_dir: Path) -> ConvertResult:
    """Two streaming passes: collect the column names, then write with a fixed schema."""
    out_dir.mkdir(parents=True, exist_ok=True)
    tag = _record_tag(src)
    columns: dict[str, None] = {}
    for rec in _iter_records(src, tag):
        for key in rec:
            columns.setdefault(key, None)
    schema = pa.schema([(c, pa.string()) for c in columns])

    target = out_dir / "part-00000.parquet"
    rows = 0
    batch: list[dict[str, str | None]] = []
    with pq.ParquetWriter(target, schema, compression="zstd") as writer:
        for rec in _iter_records(src, tag):
            batch.append(rec)
            if len(batch) >= XML_BATCH_ROWS:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                rows += len(batch)
                batch.clear()
        if batch:
            writer.write_table(pa.Table.from_pylist(batch, schema=schema))
            rows += len(batch)
    return ConvertResult([target], rows, 0)


def convert(src: Path, out_dir: Path, detected: Detected) -> ConvertResult:
    if detected.file_format == "csv":
        return csv_to_parquet(src, out_dir, detected)
    if detected.file_format == "excel":
        return excel_to_parquet(src, out_dir)
    if detected.file_format == "xml":
        return xml_to_parquet(src, out_dir)
    raise UnsupportedFileError(f"unsupported format {detected.file_format}")
