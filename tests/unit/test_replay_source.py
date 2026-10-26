"""ReplayRecordSource: `.json` objects and lists, `.jsonl` lines, and directories in name order."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from fire.adapters.sources.replay import ReplayRecordSource
from fire.core.ports.record_source import RawRecord, RecordSource


async def _read(path: Path) -> list[RawRecord]:
    return [raw async for raw in ReplayRecordSource("samples", path).records()]


async def _records(path: Path) -> list[dict[str, Any] | None]:
    raws = await _read(path)
    assert {(raw.source, raw.region) for raw in raws} <= {("samples", None)}
    return [None if raw.record is None else dict(raw.record) for raw in raws]


def _write(path: Path, content: Any) -> Path:
    text = content if isinstance(content, str) else json.dumps(content)
    path.write_text(text, encoding="utf-8")
    return path


@pytest.mark.unit
def test_is_a_record_source(tmp_path: Path) -> None:
    assert isinstance(ReplayRecordSource("samples", tmp_path), RecordSource)


@pytest.mark.unit
async def test_json_object_is_one_record(tmp_path: Path) -> None:
    path = _write(tmp_path / "one.json", {"id": 1})
    assert await _records(path) == [{"id": 1}]


@pytest.mark.unit
async def test_json_list_gives_one_record_per_element(tmp_path: Path) -> None:
    path = _write(tmp_path / "many.json", [{"id": 1}, "text", None, [1], {"id": 2}])
    assert await _records(path) == [{"id": 1}, None, None, None, {"id": 2}]


@pytest.mark.unit
@pytest.mark.parametrize("content", ["{not json", '"text"', "42", "null", ""], ids=repr)
async def test_json_file_that_is_not_an_object_or_list_is_one_undecodable_record(
    tmp_path: Path, content: str
) -> None:
    assert await _records(_write(tmp_path / "bad.json", content)) == [None]


@pytest.mark.unit
async def test_jsonl_gives_one_record_per_line_and_skips_blank_lines(tmp_path: Path) -> None:
    path = _write(tmp_path / "lines.jsonl", '{"id": 1}\n\n  \n{broken\n[1]\n{"id": 2}\n')
    assert await _records(path) == [{"id": 1}, None, None, {"id": 2}]


@pytest.mark.unit
async def test_file_that_is_not_utf8_is_one_undecodable_record(tmp_path: Path) -> None:
    path = tmp_path / "latin1.json"
    path.write_bytes('{"name": "café"}'.encode("latin-1"))
    assert await _records(path) == [None]


@pytest.mark.unit
async def test_json_nested_too_deep_is_undecodable(tmp_path: Path) -> None:
    path = _write(tmp_path / "deep.json", "[" * 100_000 + "]" * 100_000)
    assert await _records(path) == [None]


@pytest.mark.unit
async def test_directory_is_read_in_name_order_and_not_recursively(tmp_path: Path) -> None:
    _write(tmp_path / "b.jsonl", '{"id": "b1"}\n{"id": "b2"}\n')
    _write(tmp_path / "a.json", {"id": "a"})
    _write(tmp_path / "c.json", [{"id": "c1"}, {"id": "c2"}])
    _write(tmp_path / "notes.txt", '{"id": "ignored"}')
    _write(tmp_path / "data.json.bak", '{"id": "ignored"}')
    (tmp_path / "nested").mkdir()
    _write(tmp_path / "nested" / "d.json", {"id": "ignored"})
    (tmp_path / "dir.json").mkdir()

    records = await _records(tmp_path)

    assert records == [{"id": "a"}, {"id": "b1"}, {"id": "b2"}, {"id": "c1"}, {"id": "c2"}]


@pytest.mark.unit
async def test_empty_directory_has_no_records(tmp_path: Path) -> None:
    assert await _records(tmp_path) == []


@pytest.mark.unit
async def test_ack_is_a_no_op(tmp_path: Path) -> None:
    [raw] = await _read(_write(tmp_path / "one.json", {"id": 1}))
    assert await raw.ack() is None
