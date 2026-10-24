from __future__ import annotations

import codecs
from pathlib import Path

import pytest

from lakeflow.detect import UnsupportedFileError, detect, detect_encoding
from lakeflow.synth import generate, write_excel


def test_utf8_comma(tmp_path: Path) -> None:
    p = tmp_path / "a.txt"
    p.write_text("id,name\n1,Zoë\n", encoding="utf-8")
    d = detect(p)
    assert (d.file_format, d.encoding, d.delimiter) == ("csv", "utf-8", ",")


def test_cp1252_semicolon_is_not_mistaken_for_utf8(tmp_path: Path) -> None:
    p = tmp_path / "a.csv"
    p.write_bytes("id;name;city\n1;Jürgen Müller;Köln\n2;Chloé;Orléans\n".encode("cp1252") * 50)
    d = detect(p)
    assert d.file_format == "csv"
    assert d.delimiter == ";"
    assert d.encoding is not None and d.encoding != "utf-8"
    # Whatever legacy name was chosen, it must decode the umlauts correctly.
    assert "Müller" in p.read_bytes().decode(d.encoding)


@pytest.mark.parametrize("bom,expected", [
    (codecs.BOM_UTF8, "utf-8-sig"),
    (codecs.BOM_UTF16_LE, "utf-16"),
    (codecs.BOM_UTF32_LE, "utf-32"),
])
def test_bom_wins(bom: bytes, expected: str) -> None:
    assert detect_encoding(bom + b"abc") == expected


def test_tab_delimiter(tmp_path: Path) -> None:
    p = tmp_path / "a.tsv"
    p.write_text("id\tname\tcountry\n1\tAli\tPK\n2\tSara\tDE\n", encoding="utf-8")
    assert detect(p).delimiter == "\t"


def test_extension_is_ignored_for_excel(tmp_path: Path) -> None:
    df, _ = generate(100)
    p = tmp_path / "looks_like.csv"  # wrong extension on purpose
    write_excel(df, p)
    assert detect(p).file_format == "excel"


def test_xml(tmp_path: Path) -> None:
    p = tmp_path / "x.dat"
    p.write_text('<?xml version="1.0"?><Rows><Row><a>1</a></Row></Rows>', encoding="utf-8")
    assert detect(p).file_format == "xml"


def test_empty_file_is_permanent_error(tmp_path: Path) -> None:
    p = tmp_path / "empty.csv"
    p.write_bytes(b"")
    with pytest.raises(UnsupportedFileError):
        detect(p)
