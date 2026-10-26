"""Kinesis RecordSource: every record of a data stream, in each of its regions, decoded as JSON.

Regions are read in parallel, each by `shards.follow`: one GetRecords per shard and pass. Every
boto3 call runs in a worker thread. Logs hold the source id, regions, exception type names, and
AWS error codes; never the stream name, a partition key, a record, or the text of an exception.
Contract: docs/specs/tentacle.md#kinds (`kinesis`).
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from datetime import datetime
from typing import Any

from fire.adapters.sources.aws import FailureLog, merge
from fire.adapters.sources.shards import PAGE_LIMIT, Page, Shard, follow
from fire.core.ports.record_source import RawRecord, StartPosition

logger = logging.getLogger(__name__)


class KinesisRecordSource:
    """Reads `stream` in every region of `clients`, a Kinesis client per region.

    A record's data that is no UTF-8 JSON object is a record with no object, which the
    pipeline counts as `undecodable`. Positions live in memory, and acks do nothing. `noted`
    counts `position_lost`.
    """

    def __init__(
        self,
        *,
        source: str,
        stream: str,
        clients: Mapping[str, Any],
        start: StartPosition,
        poll_s: float,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._source = source
        self._stream = stream
        self._clients = dict(clients)
        self._start = start
        self._poll_s = poll_s
        self._sleep = sleep
        self.noted: Counter[str] = Counter()

    def _note(self, reason: str) -> None:
        self.noted[reason] += 1

    def __repr__(self) -> str:
        return f"KinesisRecordSource(source={self._source!r})"

    async def records(self) -> AsyncIterator[RawRecord]:
        regions = [self._region(region, client) for region, client in self._clients.items()]
        async for raw in merge(regions):
            yield raw

    async def _region(self, region: str, client: Any) -> AsyncIterator[RawRecord]:
        records = follow(
            _KinesisStream(client, self._stream),
            start=self._start,
            poll_s=self._poll_s,
            failures=FailureLog(logger, "event read failed", self._source, region),
            sleep=self._sleep,
            note=self._note,
        )
        async for record in records:
            yield RawRecord(source=self._source, region=region, record=_decoded(record))


class _KinesisStream:
    def __init__(self, client: Any, stream: str) -> None:
        self._client = client
        self._stream = stream

    timestamps = True

    def locate(self) -> str:
        return self._stream

    def shards(self, stream: str) -> list[Shard]:
        shards: list[Shard] = []
        request: dict[str, Any] = {"StreamName": stream}
        while True:
            response = self._client.list_shards(**request)
            shards += [
                Shard(item["ShardId"], "EndingSequenceNumber" in item["SequenceNumberRange"])
                for item in response.get("Shards", ())
            ]
            token = response.get("NextToken")
            if not token:
                return shards
            request = {"NextToken": token}

    def iterator(
        self, stream: str, shard_id: str, kind: str, after: str | None, at: datetime | None
    ) -> str:
        request: dict[str, Any] = {
            "StreamName": stream,
            "ShardId": shard_id,
            "ShardIteratorType": kind,
        }
        if after is not None:
            request["StartingSequenceNumber"] = after
        if at is not None:
            request["Timestamp"] = at
        return self._client.get_shard_iterator(**request)["ShardIterator"]

    def page(self, iterator: str) -> Page:
        response = self._client.get_records(ShardIterator=iterator, Limit=PAGE_LIMIT)
        return Page(response.get("Records", []), response.get("NextShardIterator"))

    def sequence(self, record: Any) -> str:
        return record["SequenceNumber"]


def _decoded(record: Mapping[str, Any]) -> dict[str, Any] | None:
    data = record.get("Data")
    if not isinstance(data, bytes):
        return None
    try:
        document = json.loads(data.decode("utf-8"))
    except (ValueError, RecursionError):
        return None
    return document if isinstance(document, dict) else None
