"""KinesisRecordSource on moto: shards and regions, `latest` and `oldest`, decoding, a failing
stream logged once, and privacy.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
import pytest
from aws_streams import (
    OTHER_REGION,
    REGION,
    Passes,
    create_stream,
    drain,
    isolate_aws,
    nothing,
    put_records,
)
from canary import Canary
from moto import mock_aws

from fire.adapters.sources.kinesis import KinesisRecordSource, _decoded, _KinesisStream
from fire.adapters.sources.shards import AT_TIMESTAMP
from fire.core.ports.record_source import NotingRecordSource, RecordSource, StartPosition

SOURCE = "domain-events"
STREAM = "domain-events-stream"
LOGGER = "fire.adapters.sources.kinesis"


@pytest.fixture
def aws(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    isolate_aws(monkeypatch, tmp_path)
    with mock_aws():
        yield


def _source(
    *steps: Callable[[], object],
    regions: tuple[str, ...] = (REGION,),
    start: StartPosition = StartPosition.OLDEST,
    stream: str = STREAM,
) -> KinesisRecordSource:
    return KinesisRecordSource(
        source=SOURCE,
        stream=stream,
        clients={region: boto3.client("kinesis", region_name=region) for region in regions},
        start=start,
        poll_s=1,
        sleep=Passes(*steps),
    )


def _warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [record.getMessage() for record in caplog.records if record.name == LOGGER]


@pytest.mark.unit
def test_is_a_record_source(aws: None) -> None:
    assert isinstance(_source(), RecordSource)
    assert isinstance(_source(), NotingRecordSource)


@pytest.mark.unit
async def test_records_of_every_shard_decode_as_json_objects(aws: None) -> None:
    client = create_stream(STREAM, shards=2)
    records = [{"id": f"e-{index}", "payload": {"n": index}} for index in range(6)]
    keys = iter(f"order-{index}" for index in range(6))
    put_records(client, STREAM, records, partition_key=lambda: next(keys))

    read = await drain(_source(nothing).records())

    assert sorted(raw.record["id"] for raw in read) == [record["id"] for record in records]
    assert {(raw.source, raw.region) for raw in read} == {(SOURCE, REGION)}
    assert [await raw.ack() for raw in read] == [None] * len(read)


@pytest.mark.unit
@pytest.mark.parametrize(
    "data",
    [b"{not json", b"[1, 2]", b'"text"', b"null", '{"x": "café"}'.encode("latin-1")],
    ids=["not-json", "array", "string", "null", "latin-1"],
)
async def test_data_that_is_no_utf8_json_object_has_no_record(aws: None, data: bytes) -> None:
    client = create_stream(STREAM)
    put_records(client, STREAM, [data, {"id": "e-1"}])

    read = await drain(_source().records())

    assert [raw.record for raw in read] == [None, {"id": "e-1"}]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("start", "expected"),
    [(StartPosition.LATEST, ["e-2"]), (StartPosition.OLDEST, ["e-1", "e-2"])],
)
async def test_latest_reads_what_is_written_after_the_start(
    aws: None, start: StartPosition, expected: list[str]
) -> None:
    client = create_stream(STREAM)
    put_records(client, STREAM, [{"id": "e-1"}])

    def write() -> None:
        put_records(client, STREAM, [{"id": "e-2"}])

    read = await drain(_source(write, nothing, start=start).records())

    assert [raw.record["id"] for raw in read] == expected


@pytest.mark.unit
async def test_each_region_is_read_and_its_records_carry_it(aws: None) -> None:
    for region in (REGION, OTHER_REGION):
        put_records(create_stream(STREAM, region), STREAM, [{"id": "e-1"}, {"id": f"{region}"}])

    # Both regions share the sleep, so each gets a few passes before it stops them.
    read = await drain(_source(*[nothing] * 5, regions=(REGION, OTHER_REGION)).records())

    assert sorted((raw.region, raw.record["id"]) for raw in read) == sorted(
        [(REGION, "e-1"), (REGION, REGION), (OTHER_REGION, "e-1"), (OTHER_REGION, OTHER_REGION)]
    )


@pytest.mark.unit
async def test_a_missing_stream_is_logged_once_and_read_once_it_exists(
    aws: None, caplog: pytest.LogCaptureFixture
) -> None:
    def create() -> None:
        put_records(create_stream(STREAM), STREAM, [{"id": "e-1"}])

    read = await drain(_source(nothing, nothing, create, nothing).records())

    assert [raw.record["id"] for raw in read] == ["e-1"]
    assert _warnings(caplog) == [
        f"event read failed: source={SOURCE} region={REGION} "
        "error=ClientError:ResourceNotFoundException"
    ]


@pytest.mark.unit
async def test_no_canary_reaches_logs_or_reprs(
    aws: None, canary: Canary, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    stream = canary.token()
    record = canary.plant({"id": "x", "payload": {"name": "x", "n": 1}})

    def create() -> None:
        client = create_stream(stream)
        put_records(client, stream, [record, canary.token().encode()], canary.token)

    source = _source(create, nothing, stream=stream)
    read: list[Any] = await drain(source.records())

    assert [raw.record for raw in read] == [record, None]
    assert len(_warnings(caplog)) == 1
    assert canary.leaks("\n".join([caplog.text, repr(source), *map(repr, read)])) == []


@pytest.mark.unit
def test_an_iterator_at_a_timestamp_reads_what_was_written_since(aws: None) -> None:
    client = create_stream(STREAM)
    put_records(client, STREAM, [{"id": "e-1"}])
    since = datetime.now(UTC)
    time.sleep(0.01)
    put_records(client, STREAM, [{"id": "e-2"}])
    stream = _KinesisStream(client, STREAM)
    [shard] = stream.shards(STREAM)

    iterator = stream.iterator(STREAM, shard.shard_id, AT_TIMESTAMP, None, since)

    assert [_decoded(record) for record in stream.page(iterator).records] == [{"id": "e-2"}]
