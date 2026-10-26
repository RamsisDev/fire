"""Replay RecordSource: JSON objects from `.json` and `.jsonl` files, read once, in name order."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

from fire.core.ports.record_source import RawRecord

SUFFIXES = frozenset({".json", ".jsonl"})


class ReplayRecordSource:
    """Reads a `.json` file (an object or a list), a `.jsonl` file, or a directory of them.

    A directory is read in name order and not recursively; other files in it are ignored. Each
    object is one record. An element or line that is not an object, and a file or line that is
    not JSON, is one record with no object, which the pipeline counts as `undecodable`. Blank
    `.jsonl` lines are not records. Replay records have no region.
    """

    def __init__(self, source: str, path: Path) -> None:
        self._source = source
        self._path = path

    async def records(self) -> AsyncIterator[RawRecord]:
        for file in self._files():
            text = await asyncio.to_thread(_read_text, file)
            for record in _decode(file.suffix, text):
                yield RawRecord(source=self._source, region=None, record=record)

    def _files(self) -> list[Path]:
        if not self._path.is_dir():
            return [self._path]
        return sorted(
            path for path in self._path.iterdir() if path.suffix in SUFFIXES and path.is_file()
        )


def _read_text(path: Path) -> str | None:
    """The file's text; None when it is not UTF-8."""
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return None


def _decode(suffix: str, text: str | None) -> Iterator[dict[str, Any] | None]:
    if text is None:
        yield None
    elif suffix == ".jsonl":
        for line in text.splitlines():
            if line.strip():
                yield _object(_parse(line))
    else:
        document = _parse(text)
        if isinstance(document, list):
            yield from (_object(element) for element in document)
        else:
            yield _object(document)


_UNPARSED = object()


def _parse(text: str) -> Any:
    try:
        return json.loads(text)
    except (ValueError, RecursionError):
        return _UNPARSED


def _object(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None
