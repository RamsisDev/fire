"""DynamoDB stream RecordSource: each item a table writes, in each of its regions, read from the
new image of its stream record as plain JSON.

Per region, the table's latest stream (DescribeTable) is followed shard by shard
(`shards.follow`). Every boto3 call runs in a worker thread. Logs hold the source id, regions,
reason codes, exception type names, and AWS error codes; never the table name, a key, an image,
or the text of an exception. Contract: docs/specs/tentacle.md#kinds (`dynamodb_stream`).
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from fire.adapters.sources.aws import FailureLog, ReadProblem, merge
from fire.adapters.sources.shards import PAGE_LIMIT, Page, Shard, follow
from fire.core.domain.envelope import lookup
from fire.core.ports.record_source import RawRecord, RecordSkipReason, StartPosition

logger = logging.getLogger(__name__)

NO_STREAM = "no_stream"
NO_NEW_IMAGE = "no_new_image"
_NEW_IMAGE_VIEWS = frozenset({"NEW_IMAGE", "NEW_AND_OLD_IMAGES"})
_INTEGER = re.compile(r"-?[0-9]+")


@dataclass(frozen=True)
class StreamFilter:
    """Keeps the records whose new image holds the string `equals` at the dotted `attribute`."""

    attribute: str
    equals: str


@dataclass(frozen=True)
class RegionClients:
    tables: Any
    streams: Any


class DynamoDbStreamRecordSource:
    """Reads the stream of `table` in every region of `clients`.

    `INSERT` and `MODIFY` records are read from their new image; a `REMOVE` record is skipped
    as `remove_record`, and with `stream_filter` an image without the filter's string as
    `filtered`. An image that is not valid DynamoDB JSON is a record with no object. A table
    with no enabled stream, or whose stream carries no new image, is a read failure of that
    region, logged once and retried. Positions live in memory, and acks do nothing. `noted`
    counts `position_lost`.
    """

    def __init__(
        self,
        *,
        source: str,
        table: str,
        clients: Mapping[str, RegionClients],
        start: StartPosition,
        poll_s: float,
        stream_filter: StreamFilter | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._source = source
        self._table = table
        self._clients = dict(clients)
        self._start = start
        self._poll_s = poll_s
        self._filter = stream_filter
        self._sleep = sleep
        self.noted: Counter[str] = Counter()

    def _note(self, reason: str) -> None:
        self.noted[reason] += 1

    def __repr__(self) -> str:
        return f"DynamoDbStreamRecordSource(source={self._source!r})"

    async def records(self) -> AsyncIterator[RawRecord]:
        regions = [self._region(region, clients) for region, clients in self._clients.items()]
        async for raw in merge(regions):
            yield raw

    async def _region(self, region: str, clients: RegionClients) -> AsyncIterator[RawRecord]:
        records = follow(
            _TableStream(clients, self._table),
            start=self._start,
            poll_s=self._poll_s,
            failures=FailureLog(logger, "event read failed", self._source, region),
            sleep=self._sleep,
            note=self._note,
        )
        async for record in records:
            yield self._raw(region, record)

    def _raw(self, region: str, record: Mapping[str, Any]) -> RawRecord:
        if record.get("eventName") == "REMOVE":
            return RawRecord(self._source, region, None, skipped=RecordSkipReason.REMOVE_RECORD)
        image = plain_image(record.get("dynamodb", {}).get("NewImage"))
        kept = self._filter is None or lookup(image or {}, self._filter.attribute) == (
            self._filter.equals
        )
        if image is not None and not kept:
            return RawRecord(self._source, region, None, skipped=RecordSkipReason.FILTERED)
        return RawRecord(self._source, region, image)


class _TableStream:
    def __init__(self, clients: RegionClients, table: str) -> None:
        self._clients = clients
        self._table = table

    timestamps = False

    def locate(self) -> str:
        table = self._clients.tables.describe_table(TableName=self._table)["Table"]
        specification = table.get("StreamSpecification") or {}
        arn = table.get("LatestStreamArn")
        if not arn or not specification.get("StreamEnabled"):
            raise ReadProblem(NO_STREAM)
        if specification.get("StreamViewType") not in _NEW_IMAGE_VIEWS:
            raise ReadProblem(NO_NEW_IMAGE)
        return arn

    def shards(self, stream: str) -> list[Shard]:
        shards: list[Shard] = []
        request: dict[str, Any] = {"StreamArn": stream}
        while True:
            description = self._clients.streams.describe_stream(**request)["StreamDescription"]
            shards += [
                Shard(item["ShardId"], "EndingSequenceNumber" in item["SequenceNumberRange"])
                for item in description.get("Shards", ())
            ]
            last = description.get("LastEvaluatedShardId")
            if not last:
                return shards
            request = {"StreamArn": stream, "ExclusiveStartShardId": last}

    def iterator(
        self, stream: str, shard_id: str, kind: str, after: str | None, at: datetime | None
    ) -> str:
        request = {"StreamArn": stream, "ShardId": shard_id, "ShardIteratorType": kind}
        if after is not None:
            request["SequenceNumber"] = after
        return self._clients.streams.get_shard_iterator(**request)["ShardIterator"]

    def page(self, iterator: str) -> Page:
        response = self._clients.streams.get_records(ShardIterator=iterator, Limit=PAGE_LIMIT)
        return Page(response.get("Records", []), response.get("NextShardIterator"))

    def sequence(self, record: Any) -> str:
        return record["dynamodb"]["SequenceNumber"]


def plain_image(image: object) -> dict[str, Any] | None:
    """An image in DynamoDB JSON as plain JSON; None when it is not one.

    An `N` is an integer when its text has no `.` or exponent, else a float. Sets become
    arrays. `B` and `BS` stay bytes, which the generalizer types `unknown`.
    """
    if not isinstance(image, Mapping):
        return None
    try:
        return {name: _plain(value) for name, value in image.items()}
    except (ValueError, TypeError, RecursionError):
        return None


def _plain(value: object) -> Any:
    if not isinstance(value, Mapping) or len(value) != 1:
        raise ValueError("not an attribute value")
    [(tag, inner)] = value.items()
    match tag:
        case "S":
            return _typed(inner, str)
        case "N":
            return _number(inner)
        case "BOOL":
            return _typed(inner, bool)
        case "NULL":
            return None
        case "B":
            return bytes(_typed(inner, bytes | bytearray))
        case "M":
            return {name: _plain(item) for name, item in _typed(inner, Mapping).items()}
        case "L":
            return [_plain(item) for item in _typed(inner, list)]
        case "SS":
            return [_typed(item, str) for item in _typed(inner, list)]
        case "NS":
            return [_number(item) for item in _typed(inner, list)]
        case "BS":
            return [bytes(_typed(item, bytes | bytearray)) for item in _typed(inner, list)]
    raise ValueError("unknown attribute type")


def _typed(value: object, kind: Any) -> Any:
    if not isinstance(value, kind):
        raise TypeError("attribute value of the wrong type")
    return value


def _number(text: object) -> int | float:
    if not isinstance(text, str):
        raise TypeError("a number is sent as text")
    if _INTEGER.fullmatch(text):
        return int(text)
    number = float(text)
    if not math.isfinite(number):
        raise ValueError("not a finite number")
    return number
