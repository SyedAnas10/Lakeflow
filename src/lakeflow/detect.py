"""Work out what a file is from its bytes, never from its extension.

Order of checks:
1. Magic bytes: xlsx is a zip ("PK\\x03\\x04"), legacy xls is an OLE2 container.
2. Byte-order mark (BOM): tells us UTF-8-sig, UTF-16 or UTF-32 with certainty.
3. Strict UTF-8 decode of a sample: valid UTF-8 is very unlikely to be an accident.
4. Statistical guess (charset-normalizer) for legacy encodings such as cp1252 / latin-1.
5. Then: starts with "<" -> XML, otherwise delimited text; the delimiter is sniffed.
"""

from __future__ import annotations

import codecs
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from charset_normalizer import from_bytes

SAMPLE_BYTES = 1024 * 1024  # 1 MB is plenty to decide, and cheap even for a 2 GB file
PREFERRED_LEGACY_ENCODINGS = ["cp1252"]

FileFormat = Literal["csv", "excel", "xml"]

# UTF-32 LE must be checked before UTF-16 LE: its BOM starts with the UTF-16 LE BOM.
_BOMS: list[tuple[bytes, str]] = [
    (codecs.BOM_UTF8, "utf-8-sig"),
    (codecs.BOM_UTF32_LE, "utf-32"),
    (codecs.BOM_UTF32_BE, "utf-32"),
    (codecs.BOM_UTF16_LE, "utf-16"),
    (codecs.BOM_UTF16_BE, "utf-16"),
]


class UnsupportedFileError(ValueError):
    """The file can never be processed (permanent failure, not worth retrying)."""


@dataclass(frozen=True)
class Detected:
    file_format: FileFormat
    encoding: str | None = None
    delimiter: str | None = None


def read_sample(path: Path, size: int = SAMPLE_BYTES) -> bytes:
    with open(path, "rb") as fh:
        return fh.read(size)


def detect_encoding(sample: bytes) -> str:
    for bom, name in _BOMS:
        if sample.startswith(bom):
            return name
    # Incremental decoder with final=False: a sample cut in the middle of a multi-byte
    # character is not treated as an error.
    try:
        codecs.getincrementaldecoder("utf-8")().decode(sample, final=False)
        return "utf-8"
    except UnicodeDecodeError:
        pass
    # Statistical guessing is unreliable between close relatives: cp1250 (Central
    # European) and cp1252 (Western European) agree on "ü" but not on "ø", so a wrong
    # pick silently turns "Søren" into "Sřren". Our sources are Western European ERP
    # exports, so cp1252 wins whenever it is among the plausible candidates. Sources
    # that know their encoding should declare it instead (see `detect(..., encoding=)`).
    matches = from_bytes(sample)
    candidates = {m.encoding for m in matches}
    for preferred in PREFERRED_LEGACY_ENCODINGS:
        if preferred in candidates:
            return preferred
    best = matches.best()
    return best.encoding if best is not None else "cp1252"


def detect_delimiter(text: str) -> str:
    head = "\n".join(text.splitlines()[:50])
    try:
        return csv.Sniffer().sniff(head, delimiters=",;\t|").delimiter
    except csv.Error:
        return ","


def detect(path: Path, encoding: str | None = None) -> Detected:
    """`encoding`: the encoding the source declared, if any. A declaration always beats a
    guess; detection is the fallback for senders who do not tell us."""
    sample = read_sample(path)
    if not sample:
        raise UnsupportedFileError("file is empty")
    if sample.startswith(b"PK\x03\x04"):
        return Detected("excel")
    if sample.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return Detected("excel")

    if encoding:
        codecs.lookup(encoding)  # raises LookupError for an unknown name
    encoding = encoding or detect_encoding(sample)
    text = codecs.getincrementaldecoder(encoding)(errors="replace").decode(sample, final=False)
    if text.lstrip("﻿ \t\r\n").startswith("<"):
        return Detected("xml", encoding=encoding)
    if "\x00" in text:
        raise UnsupportedFileError("binary content that is not Excel")
    return Detected("csv", encoding=encoding, delimiter=detect_delimiter(text))
