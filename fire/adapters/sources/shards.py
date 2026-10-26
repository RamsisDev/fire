"""Shard following for the stream sources, Kinesis data streams and DynamoDB Streams: per region,
an iterator per shard polled with GetRecords, through splits, merges, failed calls, and expired
iterators.

Contract: docs/specs/tentacle.md#kinds (`kinesis`, `dynamodb_stream`).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from botocore.exceptions import ClientError

from fire.adapters.sources.aws import READ_ERRORS, FailureLog, ReadProblem, error_code
from fire.core.ports.record_source import StartPosition

LATEST = "LATEST"
TRIM_HORIZON = "TRIM_HORIZON"
AFTER_SEQUENCE_NUMBER = "AFTER_SEQUENCE_NUMBER"
AT_TIMESTAMP = "AT_TIMESTAMP"
PAGE_LIMIT = 1000
SHARD_REFRESH_S = 60.0
POSITION_LOST = "position_lost"
# The stream compares an AT_TIMESTAMP with its own arrival times, so a resume starts this much
# earlier than the tentacle's clock says, and never later than now minus it.
CLOCK_SKEW_S = 60
_EXPIRED = "ExpiredIteratorException"
_TRIMMED = "TrimmedDataAccessException"
# Also Kinesis's answer to an AT_TIMESTAMP in its future.
_INVALID_ARGUMENT = "InvalidArgumentException"
# The iterator is no longer valid; the position it was taken from still is.
_INVALID = frozenset({_EXPIRED, "ResourceNotFoundException"})


@dataclass(frozen=True)
class Shard:
    shard_id: str
    # The shard has an ending sequence number: a split or merge closed it.
    closed: bool


@dataclass(frozen=True)
class Page:
    records: list[Any]
    # None once a closed shard has been read to its end.
    next_iterator: str | None


class ShardApi(Protocol):
    """One region's stream in the calls a follower makes. Each call blocks, so the follower
    runs it in a worker thread.
    """

    # Whether `iterator` takes AT_TIMESTAMP: Kinesis does, DynamoDB Streams does not.
    timestamps: bool

    def locate(self) -> str:
        """The stream to read, by name or ARN; raises `ReadProblem` when there is none."""
        ...

    def shards(self, stream: str) -> list[Shard]: ...

    def iterator(
        self, stream: str, shard_id: str, kind: str, after: str | None, at: datetime | None
    ) -> str: ...

    def page(self, iterator: str) -> Page: ...

    def sequence(self, record: Any) -> str: ...


@dataclass
class _Position:
    kind: str
    iterator: str | None = None
    # The sequence number of the last record yielded.
    after: str | None = None
    # When the last page was asked for, or the first iterator: no record written since can be
    # behind the shard's iterator.
    since: datetime | None = None
    # The kind of the iterator being asked for, until the stream issues it.
    requested: str | None = None


async def follow(
    api: ShardApi,
    *,
    start: StartPosition,
    poll_s: float,
    failures: FailureLog,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    clock: Callable[[], float] = lambda: asyncio.get_running_loop().time(),
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    note: Callable[[str], None] = lambda _reason: None,
) -> AsyncIterator[Any]:
    """The records of every shard of one region's stream, one page per shard and pass, a pass
    every `poll_s`, until cancelled.

    With `StartPosition.LATEST`, the shards of the first listing that succeeds start at their
    tip and closed ones are not read; with `OLDEST` they start at their oldest record. Shards
    listed later, the children of a split or merge or the shards of a new stream, start at
    their oldest record; a parent and its children are read in no set order. A shard read to
    its end is dropped and the shards are listed again; they are also listed every
    `SHARD_REFRESH_S`.

    A failed call is logged once through `failures` and retried on the next pass, with the same
    iterator unless the error invalidated it. A new iterator resumes after the last record
    yielded; a shard that started at its tip and has yielded nothing resumes `AT_TIMESTAMP` of
    its last page, or of its first iterator, `CLOCK_SKEW_S` earlier, where the stream allows
    it, and otherwise at its tip again, which is logged through `failures` and passed to `note`
    as `position_lost`; so is a refused `AT_TIMESTAMP`, after which the shard starts at its tip.
    Trimmed data, or a sequence number the stream refuses (a stream recreated under the same
    name), restarts the shard at its oldest record.
    """
    stream: str | None = None
    positions: dict[str, _Position] = {}
    ended: set[str] = set()
    first = True
    refresh_at: float | None = None
    while True:
        if refresh_at is None or clock() >= refresh_at:
            try:
                located = await asyncio.to_thread(api.locate)
                listed = await asyncio.to_thread(api.shards, located)
            except (*READ_ERRORS, ReadProblem) as error:
                failures.failed(error)
            else:
                if located != stream:
                    positions.clear()
                    ended.clear()
                stream = located
                _track(listed, positions, ended, start=start, first=first)
                first = False
                refresh_at = clock() + SHARD_REFRESH_S
        if stream is not None:
            for shard_id, position in list(positions.items()):
                try:
                    page = await _page(api, stream, shard_id, position, failures, now, note)
                except READ_ERRORS as error:
                    failures.failed(error)
                    if _invalidate(position, error):
                        failures.failed(ReadProblem(POSITION_LOST))
                        note(POSITION_LOST)
                    continue
                for record in page.records:
                    position.after = api.sequence(record)
                    yield record
                position.iterator = page.next_iterator
                if page.next_iterator is None:
                    del positions[shard_id]
                    ended.add(shard_id)
                    refresh_at = None
        failures.end_pass()
        await sleep(poll_s)


def _track(
    listed: list[Shard],
    positions: dict[str, _Position],
    ended: set[str],
    *,
    start: StartPosition,
    first: bool,
) -> None:
    """Starts a position for each shard not seen yet and forgets shards no longer listed."""
    ids = {shard.shard_id for shard in listed}
    for gone in positions.keys() - ids:
        del positions[gone]
    ended.intersection_update(ids)
    for shard in listed:
        if shard.shard_id in positions or shard.shard_id in ended:
            continue
        if first and start is StartPosition.LATEST:
            if shard.closed:
                ended.add(shard.shard_id)
                continue
            positions[shard.shard_id] = _Position(LATEST)
        else:
            positions[shard.shard_id] = _Position(TRIM_HORIZON)


def _invalidate(position: _Position, error: Exception) -> bool:
    """Drops what a failed call made unusable; throttling, timeouts, and 5xx keep the iterator.

    True when the shard's position is lost: the stream refused an `AT_TIMESTAMP`, so the
    shard starts at its tip again.
    """
    code = error_code(error) if isinstance(error, ClientError) else None
    requested, position.requested = position.requested, None
    if code == _INVALID_ARGUMENT and requested == AT_TIMESTAMP:
        position.kind, position.iterator = LATEST, None
        position.after = position.since = None
        return True
    if code in (_TRIMMED, _INVALID_ARGUMENT):
        position.kind, position.iterator = TRIM_HORIZON, None
        position.after = position.since = None
    elif code in _INVALID:
        position.iterator = None
    return False


async def _page(
    api: ShardApi,
    stream: str,
    shard_id: str,
    position: _Position,
    failures: FailureLog,
    now: Callable[[], datetime],
    note: Callable[[str], None],
) -> Page:
    """The next page of a shard; an expired iterator is replaced once."""
    try:
        return await _fetch(api, stream, shard_id, position, failures, now, note)
    except ClientError as error:
        if error_code(error) != _EXPIRED:
            raise
    position.iterator = None
    return await _fetch(api, stream, shard_id, position, failures, now, note)


async def _fetch(
    api: ShardApi,
    stream: str,
    shard_id: str,
    position: _Position,
    failures: FailureLog,
    now: Callable[[], datetime],
    note: Callable[[str], None],
) -> Page:
    if position.iterator is None:
        issued = now()
        kind, after, at = _resume(api, position, failures, issued, note)
        position.requested = kind
        position.iterator = await asyncio.to_thread(
            api.iterator, stream, shard_id, kind, after, at
        )
        position.requested = None
        if position.since is None:
            position.since = issued
    asked = now()
    page = await asyncio.to_thread(api.page, position.iterator)
    position.since = asked
    return page


def _resume(
    api: ShardApi,
    position: _Position,
    failures: FailureLog,
    now: datetime,
    note: Callable[[str], None],
) -> tuple[str, str | None, datetime | None]:
    """Where a new iterator starts: after the last record yielded; for a shard that started at
    its tip and yielded nothing since an iterator was issued, `CLOCK_SKEW_S` before the time of
    its last page, and never later than `CLOCK_SKEW_S` before now. Nothing was yielded from
    that shard, so the margin can re-read records from before it, never repeat a yielded one.
    """
    if position.after is not None:
        return AFTER_SEQUENCE_NUMBER, position.after, None
    if position.kind == LATEST and position.since is not None:
        if api.timestamps:
            return AT_TIMESTAMP, None, min(position.since, now) - timedelta(seconds=CLOCK_SKEW_S)
        failures.failed(ReadProblem(POSITION_LOST))
        note(POSITION_LOST)
    return position.kind, None, None
