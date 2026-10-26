"""DynamoDbStreamRecordSource: DynamoDB JSON to plain JSON, then on moto the new image of each
write, `REMOVE` and `filter`, `latest` and `oldest`, a table without a usable stream logged
once, regions, and privacy.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import boto3
import pytest
from aws_streams import (
    OTHER_REGION,
    REGION,
    Passes,
    create_table,
    drain,
    isolate_aws,
    nothing,
    put_items,
)
from canary import Canary
from moto import mock_aws

from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.adapters.sources.dynamodb_stream import (
    DynamoDbStreamRecordSource,
    RegionClients,
    StreamFilter,
    plain_image,
)
from fire.core.domain.dossier import ValueType
from fire.core.ports.record_source import (
    NotingRecordSource,
    RecordSkipReason,
    RecordSource,
    StartPosition,
)

SOURCE = "order-domain-events"
TABLE = "order_domain_events"
LOGGER = "fire.adapters.sources.dynamodb_stream"
WARNING = f"event read failed: source={SOURCE} region={REGION} error="


@pytest.mark.unit
def test_dynamodb_json_becomes_plain_json() -> None:
    image = {
        "PK": {"S": "ORDER#1"},
        "count": {"N": "3"},
        "negative": {"N": "-12"},
        "price": {"N": "95.5"},
        "huge": {"N": "1E+30"},
        "flag": {"BOOL": True},
        "nothing": {"NULL": True},
        "blob": {"B": b"\x00\x01"},
        "tags": {"SS": ["a", "b"]},
        "sizes": {"NS": ["1", "2.5"]},
        "blobs": {"BS": [b"\x02"]},
        "lines": {"L": [{"M": {"sku": {"S": "x"}, "qty": {"N": "1"}}}, {"S": "y"}]},
        "empty": {"M": {}},
    }

    assert plain_image(image) == {
        "PK": "ORDER#1",
        "count": 3,
        "negative": -12,
        "price": 95.5,
        "huge": 1e30,
        "flag": True,
        "nothing": None,
        "blob": b"\x00\x01",
        "tags": ["a", "b"],
        "sizes": [1, 2.5],
        "blobs": [b"\x02"],
        "lines": [{"sku": "x", "qty": 1}, "y"],
        "empty": {},
    }


@pytest.mark.unit
def test_numbers_and_bytes_get_their_value_types() -> None:
    image = plain_image({"n": {"N": "1"}, "x": {"N": "1.25"}, "b": {"B": b"\x00"}})
    assert image is not None
    shape = FlatStarShapeGeneralizer().generalize(image)
    assert {field.path: field.value_type for field in shape.fields} == {
        "n": ValueType.INTEGER,
        "x": ValueType.NUMBER,
        "b": ValueType.UNKNOWN,
    }


@pytest.mark.unit
@pytest.mark.parametrize(
    "image",
    [
        None,
        "text",
        {"a": "plain"},
        {"a": {}},
        {"a": {"S": "x", "N": "1"}},
        {"a": {"XX": "x"}},
        {"a": {"S": 1}},
        {"a": {"N": 1}},
        {"a": {"N": "one"}},
        {"a": {"N": "NaN"}},
        {"a": {"N": "Infinity"}},
        {"a": {"BOOL": "true"}},
        {"a": {"M": []}},
        {"a": {"L": {}}},
        {"a": {"SS": [1]}},
        {"a": {"B": "AAE="}},
    ],
)
def test_what_is_no_dynamodb_json_image_is_none(image: Any) -> None:
    assert plain_image(image) is None


@pytest.fixture
def aws(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    isolate_aws(monkeypatch, tmp_path)
    with mock_aws():
        yield


def _source(
    *steps: Callable[[], object],
    regions: tuple[str, ...] = (REGION,),
    start: StartPosition = StartPosition.OLDEST,
    stream_filter: StreamFilter | None = None,
    table: str = TABLE,
) -> DynamoDbStreamRecordSource:
    clients = {
        region: RegionClients(
            boto3.client("dynamodb", region_name=region),
            boto3.client("dynamodbstreams", region_name=region),
        )
        for region in regions
    }
    return DynamoDbStreamRecordSource(
        source=SOURCE,
        table=table,
        clients=clients,
        start=start,
        poll_s=1,
        stream_filter=stream_filter,
        sleep=Passes(*steps),
    )


def _event(sk: str, **extra: Any) -> dict[str, Any]:
    return {"PK": "ORDER#1", "SK": f"EVENT#{sk}", "event_type": "order.created", **extra}


def _warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [record.getMessage() for record in caplog.records if record.name == LOGGER]


@pytest.mark.unit
def test_is_a_record_source(aws: None) -> None:
    assert isinstance(_source(), RecordSource)
    assert isinstance(_source(), NotingRecordSource)


@pytest.mark.unit
async def test_inserts_and_modifies_read_their_new_image_and_removes_are_skipped(
    aws: None,
) -> None:
    client = create_table(TABLE)
    put_items(client, TABLE, [_event("01A", payload={"n": 1})])
    put_items(client, TABLE, [_event("01A", payload={"n": 2})])
    client.delete_item(TableName=TABLE, Key={"PK": {"S": "ORDER#1"}, "SK": {"S": "EVENT#01A"}})

    read = await drain(_source(nothing).records())

    assert [(raw.record, raw.skipped) for raw in read] == [
        (_event("01A", payload={"n": 1}), None),
        (_event("01A", payload={"n": 2}), None),
        (None, RecordSkipReason.REMOVE_RECORD),
    ]
    assert {(raw.source, raw.region) for raw in read} == {(SOURCE, REGION)}


@pytest.mark.unit
async def test_a_filter_keeps_images_holding_its_string_at_its_path(aws: None) -> None:
    client = create_table(TABLE)
    put_items(
        client,
        TABLE,
        [
            _event("01A", task={"status": "ABANDONED"}),
            _event("01B", task={"status": "DONE"}),
            _event("01C", task={"status": ["ABANDONED"]}),
            _event("01D"),
        ],
    )

    read = await drain(_source(stream_filter=StreamFilter("task.status", "ABANDONED")).records())

    assert [(raw.record and raw.record["SK"], raw.skipped) for raw in read] == [
        ("EVENT#01A", None),
        (None, RecordSkipReason.FILTERED),
        (None, RecordSkipReason.FILTERED),
        (None, RecordSkipReason.FILTERED),
    ]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("start", "expected"),
    [(StartPosition.LATEST, ["EVENT#02"]), (StartPosition.OLDEST, ["EVENT#01", "EVENT#02"])],
)
async def test_latest_reads_what_is_written_after_the_start(
    aws: None, start: StartPosition, expected: list[str]
) -> None:
    client = create_table(TABLE)
    put_items(client, TABLE, [_event("01")])

    read = await drain(
        _source(lambda: put_items(client, TABLE, [_event("02")]), nothing, start=start).records()
    )

    assert [raw.record["SK"] for raw in read] == expected


@pytest.mark.unit
@pytest.mark.parametrize(
    ("view", "reason"),
    [(None, "no_stream"), ("KEYS_ONLY", "no_new_image"), ("OLD_IMAGE", "no_new_image")],
)
async def test_a_stream_without_new_images_is_logged_once_per_region(
    aws: None, caplog: pytest.LogCaptureFixture, view: str | None, reason: str
) -> None:
    put_items(create_table(TABLE, view=view), TABLE, [_event("01")])

    read = await drain(_source(nothing, nothing).records())

    assert read == []
    assert _warnings(caplog) == [WARNING + reason]


@pytest.mark.unit
async def test_a_missing_table_is_logged_once_and_read_once_it_exists(
    aws: None, caplog: pytest.LogCaptureFixture
) -> None:
    def create() -> None:
        put_items(create_table(TABLE), TABLE, [_event("01")])

    read = await drain(_source(nothing, create, nothing).records())

    assert [raw.record["SK"] for raw in read] == ["EVENT#01"]
    assert _warnings(caplog) == [f"{WARNING}ClientError:ResourceNotFoundException"]


@pytest.mark.unit
async def test_each_region_is_read_and_its_records_carry_it(aws: None) -> None:
    for region in (REGION, OTHER_REGION):
        put_items(create_table(TABLE, region), TABLE, [_event("01")])

    read = await drain(_source(*[nothing] * 5, regions=(REGION, OTHER_REGION)).records())

    assert sorted((raw.region, raw.record["SK"]) for raw in read) == [
        (REGION, "EVENT#01"),
        (OTHER_REGION, "EVENT#01"),
    ]


@pytest.mark.unit
async def test_no_canary_reaches_logs_or_reprs(
    aws: None, canary: Canary, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    table = canary.token()
    record = canary.plant(_event("x", payload={"name": "x", "n": 1}))

    def create() -> None:
        client = create_table(table)
        put_items(client, table, [record])
        client.delete_item(
            TableName=table, Key={"PK": {"S": record["PK"]}, "SK": {"S": record["SK"]}}
        )

    source = _source(create, nothing, table=table)
    read = await drain(source.records())

    assert [raw.record for raw in read] == [record, None]
    assert len(_warnings(caplog)) == 1
    assert canary.leaks("\n".join([caplog.text, repr(source), *map(repr, read)])) == []
