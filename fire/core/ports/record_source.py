"""Port: where the tentacle reads raw records. Records never leave the tentacle process."""

from __future__ import annotations

from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable


class StartPosition(StrEnum):
    """Where a live source starts: what is written after start, or what it still retains."""

    LATEST = "latest"
    OLDEST = "oldest"


class RecordSkipReason(StrEnum):
    """Why a source read a record it has nothing to build from: a stream record that removes
    an item, one its filter leaves out, a queue message sent before `--start latest`, or a
    queue message that is no notification of the source's topic.
    """

    REMOVE_RECORD = "remove_record"
    FILTERED = "filtered"
    BEFORE_START = "before_start"
    NOT_A_NOTIFICATION = "not_a_notification"


async def _nothing_to_ack() -> None:
    return None


@dataclass(frozen=True)
class RawRecord:
    """One record as its source read it.

    `record` is the decoded JSON object, or None when the wire form did not decode to an
    object. `region` is where it was read, None for a source without regions. `ack` settles
    the record at its source, for example by deleting a queue message. `skipped` says why the
    source gives the record nothing to build from; such a record carries no object. Neither
    `record` nor `ack` appears in `repr`, so a logged record never carries its values.
    """

    source: str
    region: str | None
    record: Mapping[str, Any] | None = field(repr=False)
    ack: Callable[[], Awaitable[None]] = field(default=_nothing_to_ack, repr=False, compare=False)
    skipped: RecordSkipReason | None = None

    def __post_init__(self) -> None:
        if self.skipped is not None and self.record is not None:
            raise ValueError("a skipped record carries no object")


@runtime_checkable
class RecordSource(Protocol):
    """The records of one declared source, in the order it reads them.

    A finite source, such as a replay, ends when it has read everything; a live source runs
    until its task is cancelled. Adapters live in `fire/adapters/sources/`.

    Contract: docs/specs/tentacle.md#record-pipeline.
    """

    def records(self) -> AsyncIterator[RawRecord]: ...


@runtime_checkable
class NotingRecordSource(RecordSource, Protocol):
    """A record source that also counts, by reason code, what happened apart from records, such
    as a shard position it lost. The runner logs `noted` with the source's counters.
    """

    noted: Counter[str]
