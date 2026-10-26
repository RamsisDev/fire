"""Event sources end to end: Kinesis streams, DynamoDB streams, and SNS topics on moto reach the
body as event channels; one event read from two regions of a source counts once, one carried by
two sources counts once per source; rejected and skipped records are counted; a fan-out queue
takes nothing from the system's own queue; and no planted value leaks.

The body runs in-process through `httpx.ASGITransport`. A test waits for the counters line of
each source to show every record read and none pending, then stops the tentacle.
"""

from __future__ import annotations

import copy
import json
import logging
from collections.abc import AsyncIterator, Callable, Mapping
from pathlib import Path
from typing import Any

import boto3
import pytest
from aws_streams import (
    OTHER_REGION,
    REGION,
    create_stream,
    create_table,
    fanout,
    isolate_aws,
    publish,
    put_items,
    put_records,
    queue_depth,
)
from canary import WIRE_FORMS, Canary, WireContext
from event_harness import Body, counters, streams, streams_text
from moto import mock_aws

import fire.apps.incident_engine.app as app_module
from fire.adapters.sources.dynamodb_stream import plain_image
from fire.adapters.sources.sns_fanout import queue_name
from fire.apps.tentacle.runner import EXIT_OK

SYSTEM = "shop"
STREAM, TABLE, TOPIC = "orders-stream", "orders_events", "carrier-topic"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "acme" / "events"
EVENTS = [
    ("01JABCDEFGHJKMNPQRSTVWXYZ0", "order.created", {"order_id": "o-1", "lines": [{"qty": 1}]}),
    ("01JABCDEFGHJKMNPQRSTVWXYZ1", "order.charged", {"order_id": "o-1", "amount": 9500}),
]
AT = "2026-10-11T16:30:00+00:00"


def _kinesis(event_id: str, event_type: str, payload: Any) -> dict[str, Any]:
    return {"id": event_id, "eventType": event_type, "occurredAt": AT, "payload": payload}


def _item(event_id: str, event_type: str, payload: Any) -> dict[str, Any]:
    return {
        "PK": "ORDER#o-1",
        "SK": f"EVENT#{event_id}",
        "event_type": event_type,
        "created_at": AT,
        "payload": payload,
    }


def _message(event_id: str, event_type: str, payload: Any) -> dict[str, Any]:
    return {"id": event_id, "event_type": event_type, "occurred_at": AT, **payload}


KINDS: dict[str, dict[str, Any]] = {
    "kinesis": {
        "source": {
            "id": "stream-events",
            "kind": "kinesis",
            "direction": "internal",
            "stream_env": "STREAM",
            "poll_s": 0.05,
            "envelope": {
                "event_id": "id",
                "event_type": "eventType",
                "occurred_at": "occurredAt",
                "work_id": "id",
                "payload": "payload",
            },
        },
        "env": ("STREAM", STREAM),
        "record": _kinesis,
    },
    "dynamodb_stream": {
        "source": {
            "id": "table-events",
            "kind": "dynamodb_stream",
            "direction": "internal",
            "table_env": "TABLE",
            "poll_s": 0.05,
            "envelope": {
                "event_id": {"path": "SK", "strip_prefix": "EVENT#"},
                "event_type": "event_type",
                "occurred_at": "created_at",
                "work_id": {"path": "SK", "strip_prefix": "EVENT#"},
                "payload": "payload",
            },
        },
        "env": ("TABLE", TABLE),
        "record": _item,
    },
    "sns_fanout": {
        "source": {
            "id": "topic-events",
            "kind": "sns_fanout",
            "direction": "inbound",
            "topic_env": "TOPIC",
            "wait_s": 0,
            "poll_s": 0.05,
            # SNS gives each publish its own MessageId; an id inside the message is what two
            # regions' topics share.
            "envelope": {
                "event_id": "Message.id",
                "event_type": "Message.event_type",
                "occurred_at": "Message.occurred_at",
                "work_id": "Message.id",
                "payload": "Message",
            },
        },
        "env": ("TOPIC", TOPIC),
        "record": _message,
    },
}


def _write(kind: str, records: list[Any], region: str = REGION) -> None:
    """Creates the kind's resource in `region` and writes `records` as that kind reads them."""
    if kind == "kinesis":
        put_records(create_stream(STREAM, region), STREAM, records)
    elif kind == "dynamodb_stream":
        put_items(create_table(TABLE, region), TABLE, records)
    else:
        source_id = KINDS[kind]["source"]["id"]
        sns, topic_arn = fanout(TOPIC, queue_name(SYSTEM, source_id), region)
        publish(sns, topic_arn, records)


def _system(*kinds: str, regions: tuple[str, ...] = (REGION,)) -> dict[str, Any]:
    return {
        "system": SYSTEM,
        "display_name": "Shop",
        "notify": {"slack": {"channel_id": "C0123456789"}},
        "sources": [{**KINDS[kind]["source"], "regions": list(regions)} for kind in kinds],
    }


def _environ(*kinds: str) -> dict[str, str]:
    return dict(KINDS[kind]["env"] for kind in kinds)


def _records(kind: str) -> list[Any]:
    build: Callable[..., Any] = KINDS[kind]["record"]
    return [build(*event) for event in EVENTS]


@pytest.fixture
async def body(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> AsyncIterator[Callable[[dict[str, Any]], Body]]:
    caplog.set_level(logging.INFO)
    isolate_aws(monkeypatch, tmp_path)
    made: list[Body] = []

    def configure(system: dict[str, Any]) -> Body:
        made.append(Body.configure(tmp_path, monkeypatch, caplog, system))
        return made[-1]

    with mock_aws():
        yield configure
        for each in made:
            await each.finish()


def _source(kind: str) -> str:
    return KINDS[kind]["source"]["id"]


def _channel(kind: str, event_type: str) -> str:
    return f"event:{KINDS[kind]['source']['direction']}:{event_type}:payload"


@pytest.mark.integration
@pytest.mark.parametrize("kind", sorted(KINDS))
async def test_each_kind_reaches_the_body_as_event_channels(
    body: Callable[[dict[str, Any]], Body], caplog: pytest.LogCaptureFixture, kind: str
) -> None:
    _write(kind, _records(kind))
    harness = body(_system(kind))
    tentacle = harness.start(_environ(kind))

    await harness.read_and_sent({_source(kind): 2})
    assert await tentacle.finish() == EXIT_OK

    assert await streams(SYSTEM) == {
        (_source(kind), _channel(kind, event_type)): 1 for _, event_type, _ in EVENTS
    }
    shapes = tentacle.transport.shapes()
    assert sorted((shape["observation_id"], shape["work_id"]) for shape in shapes) == [
        (event_id, event_id) for event_id, _, _ in EVENTS
    ]
    assert {(shape["region"], shape["occurred_at"]) for shape in shapes} == {
        (REGION, "2026-10-11T16:30:00Z")
    }
    assert (
        f"source={_source(kind)} read=2 built=2 sent=2 unsent=0 pending=0 rejected=- skipped=- "
        "noted=- body_duplicate=0 body_rejected=-"
    ) in caplog.text


@pytest.mark.integration
@pytest.mark.parametrize("kind", sorted(KINDS))
async def test_one_event_read_from_two_regions_counts_once_per_source(
    body: Callable[[dict[str, Any]], Body], kind: str
) -> None:
    for region in (REGION, OTHER_REGION):
        _write(kind, _records(kind), region)
    harness = body(_system(kind, regions=(REGION, OTHER_REGION)))
    tentacle = harness.start(_environ(kind))

    await harness.read_and_sent({_source(kind): 4})
    assert await tentacle.finish() == EXIT_OK

    assert set((await streams(SYSTEM)).values()) == {1}
    assert len(tentacle.transport.shapes()) == 2
    assert (
        f"source={_source(kind)} read=4 built=2 sent=2 unsent=0 pending=0 rejected=- "
        "skipped=local_duplicate:2 noted=- body_duplicate=0 body_rejected=-"
    ) in counters(harness.caplog.messages)[_source(kind)]


@pytest.mark.integration
async def test_two_sources_carrying_one_event_give_one_observation_each(
    body: Callable[[dict[str, Any]], Body],
) -> None:
    kinds = ("kinesis", "dynamodb_stream")
    for kind in kinds:
        _write(kind, _records(kind))
    harness = body(_system(*kinds))
    tentacle = harness.start(_environ(*kinds))

    await harness.read_and_sent({_source(kind): 2 for kind in kinds})
    assert await tentacle.finish() == EXIT_OK

    assert await streams(SYSTEM) == {
        (_source(kind), _channel(kind, event_type)): 1
        for kind in kinds
        for _, event_type, _ in EVENTS
    }
    by_source: dict[str, set[str]] = {}
    for shape in tentacle.transport.shapes():
        by_source.setdefault(shape["source"], set()).add(shape["observation_id"])
    assert by_source == {_source(kind): {event_id for event_id, _, _ in EVENTS} for kind in kinds}


@pytest.mark.integration
async def test_rejected_and_skipped_records_are_counted_and_a_second_run_adds_only_duplicates(
    body: Callable[[dict[str, Any]], Body], caplog: pytest.LogCaptureFixture
) -> None:
    kinds = ("kinesis", "dynamodb_stream")
    no_id = _kinesis(*EVENTS[0])
    del no_id["id"]
    put_records(create_stream(STREAM), STREAM, [*_records("kinesis"), b"{x", no_id])
    table = create_table(TABLE)
    put_items(table, TABLE, [*_records("dynamodb_stream"), {"PK": "ORDER#o-2", "SK": "STATE"}])
    table.delete_item(TableName=TABLE, Key={"PK": {"S": "ORDER#o-2"}, "SK": {"S": "STATE"}})
    harness = body(_system(*kinds))
    expected = {
        "stream-events": "read=4 built=2 sent=2 unsent=0 pending=0 "
        "rejected=missing_event_id:1,undecodable:1 skipped=- noted=- ",
        "table-events": "read=4 built=2 sent=2 unsent=0 pending=0 "
        "rejected=missing_event_id:1 skipped=remove_record:1 noted=- ",
    }

    first = harness.start(_environ(*kinds))
    await harness.read_and_sent({_source(kind): 4 for kind in kinds})
    assert await first.finish() == EXIT_OK
    for source, line in expected.items():
        assert f"source={source} {line}body_duplicate=0 body_rejected=-" in caplog.text
    observed = await streams(SYSTEM)

    caplog.clear()
    second = harness.start(_environ(*kinds))
    await harness.read_and_sent({_source(kind): 4 for kind in kinds})
    assert await second.finish() == EXIT_OK
    for source, line in expected.items():
        assert f"source={source} {line}body_duplicate=2 body_rejected=-" in caplog.text
    assert await streams(SYSTEM) == observed


@pytest.mark.integration
async def test_a_fanout_queue_deletes_what_it_settled_and_the_systems_queue_keeps_everything(
    body: Callable[[dict[str, Any]], Body], caplog: pytest.LogCaptureFixture
) -> None:
    fire_queue = queue_name(SYSTEM, _source("sns_fanout"))
    sns, topic_arn = fanout(TOPIC, fire_queue)
    sqs = boto3.client("sqs", region_name=REGION)
    own = sqs.create_queue(QueueName="shop-carrier-events-queue")["QueueUrl"]
    own_arn = sqs.get_queue_attributes(QueueUrl=own, AttributeNames=["QueueArn"])
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=own_arn["Attributes"]["QueueArn"])
    publish(sns, topic_arn, [*_records("sns_fanout"), "not json", {"id": "x"}])
    harness = body(_system("sns_fanout"))
    tentacle = harness.start(_environ("sns_fanout"))

    await harness.read_and_sent({_source("sns_fanout"): 4})
    assert await tentacle.finish() == EXIT_OK

    assert (
        "source=topic-events read=4 built=2 sent=2 unsent=0 pending=0 "
        "rejected=missing_event_id:1,missing_event_type:1 skipped=- "
    ) in caplog.text
    assert queue_depth(fire_queue) == 0
    assert queue_depth("shop-carrier-events-queue") == 4


@pytest.mark.integration
async def test_a_fanout_source_with_raw_delivery_reads_the_message_as_the_body(
    body: Callable[[dict[str, Any]], Body],
) -> None:
    sns, topic_arn = fanout(TOPIC, queue_name(SYSTEM, _source("sns_fanout")), raw_delivery=True)
    publish(sns, topic_arn, _records("sns_fanout"))
    system = _system("sns_fanout")
    system["sources"][0]["raw_delivery"] = True
    harness = body(system)
    tentacle = harness.start(_environ("sns_fanout"))

    await harness.read_and_sent({_source("sns_fanout"): 2})
    assert await tentacle.finish() == EXIT_OK

    assert set(await streams(SYSTEM)) == {
        (_source("sns_fanout"), _channel("sns_fanout", event_type)) for _, event_type, _ in EVENTS
    }


# The canary: fixture records per kind, every value planted, valid, rejected, undecodable,
# and duplicate records, and planted values around them (keys, partition keys, SNS attributes).


def _planted_kinesis(canary: Canary, sample: Mapping[str, Any]) -> dict[str, Any]:
    record = canary.plant(copy.deepcopy(dict(sample)))
    record["id"] = canary.envelope_id()
    record["eventType"] = canary.envelope_name()
    record["occurredAt"] = canary.envelope_timestamp()
    return record


def _planted_item(canary: Canary, sample: Mapping[str, Any]) -> dict[str, Any]:
    record = canary.plant(copy.deepcopy(dict(sample)))
    record["SK"] = f"EVENT#{canary.envelope_id()}"
    record["event_type"] = canary.envelope_name()
    record["created_at"] = canary.envelope_timestamp()
    return record


def _planted_message(canary: Canary, sample: Mapping[str, Any]) -> dict[str, Any]:
    record = canary.plant(copy.deepcopy(dict(sample)))
    record["id"] = canary.envelope_id()
    record["event_type"] = canary.envelope_name()
    record["occurred_at"] = canary.envelope_timestamp()
    return record


def _plain_item(document: Mapping[str, Any]) -> dict[str, Any]:
    image = plain_image(document)
    assert image is not None
    return image


CANARY: dict[str, dict[str, Any]] = {
    "kinesis": {
        "fixtures": "domain-events",
        "plant": _planted_kinesis,
        "id_key": ("id",),
        # One undecodable record per broken line.
        "counts": "read=9 built=5 sent=5 unsent=0 pending=0 "
        "rejected=bad_name:1,missing_event_id:1,undecodable:1 skipped=local_duplicate:1 ",
    },
    "dynamodb_stream": {
        "fixtures": "domain-event-items",
        "plant": _planted_item,
        "id_key": ("SK",),
        # A broken line is an item put, rejected without an envelope, then removed.
        "counts": "read=10 built=5 sent=5 unsent=0 pending=0 "
        "rejected=bad_name:1,missing_event_id:2 skipped=local_duplicate:1,remove_record:1 ",
    },
    "sns_fanout": {
        "fixtures": "shipment-events",
        "plant": _planted_message,
        "id_key": ("id",),
        # A broken line is a notification whose message is no JSON, so it holds no id.
        "counts": "read=7 built=3 sent=3 unsent=0 pending=0 "
        "rejected=bad_name:1,missing_event_id:2 skipped=local_duplicate:1 ",
    },
}


def _samples(kind: str) -> list[dict[str, Any]]:
    paths = sorted((FIXTURES / CANARY[kind]["fixtures"]).glob("*.json"))
    documents = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
    return [_plain_item(doc) for doc in documents] if kind == "dynamodb_stream" else documents


@pytest.mark.integration
@pytest.mark.parametrize("kind", sorted(CANARY))
async def test_event_source_canary(
    body: Callable[[dict[str, Any]], Body],
    canary: Canary,
    caplog: pytest.LogCaptureFixture,
    capfd: pytest.CaptureFixture[str],
    kind: str,
) -> None:
    caplog.set_level(logging.DEBUG)
    plant: Callable[[Canary, Mapping[str, Any]], dict[str, Any]] = CANARY[kind]["plant"]
    samples = _samples(kind)
    valid = [plant(canary, sample) for sample in samples]
    [id_key] = CANARY[kind]["id_key"]

    missing_id = plant(canary, samples[0])
    if kind == "dynamodb_stream":
        # SK is the table's key; one without the prefix counts as missing.
        missing_id["SK"] = canary.token()
    else:
        missing_id.pop(id_key)
    bad_name = plant(canary, samples[-1])
    bad_name["eventType" if kind == "kinesis" else "event_type"] = f"{canary.token()} spaced"
    for record in (missing_id, bad_name):
        canary.rejected(*(value for value in _envelope_values(record) if value))
    records = [valid[0], missing_id, *valid[1:], bad_name, copy.deepcopy(valid[0])]
    broken = [f'{{"{id_key}": "{canary.token()}", "x": "{canary.token()}"']

    harness = body(_system(kind))
    context = WireContext(SYSTEM, _source(kind), canary.token)
    location = WIRE_FORMS[kind](Path(), records, broken, context)
    tentacle = harness.start({KINDS[kind]["env"][0]: location})
    read = int(CANARY[kind]["counts"].split()[0].removeprefix("read="))
    await harness.read_and_sent({_source(kind): read})
    assert await tentacle.finish() == EXIT_OK

    assert f"source={_source(kind)} {CANARY[kind]['counts']}" in caplog.text
    shapes = tentacle.transport.shapes()
    assert len(shapes) == len(valid)
    for shape in shapes:
        assert canary.observation_leaks(shape) == []
    for posted in tentacle.transport.bodies:
        assert canary.leaks(posted, envelope=False) == []
    catalog_state = repr(vars(app_module.get_container().catalog))
    assert canary.leaks(catalog_state, envelope=False) == []
    text = await streams_text(SYSTEM)
    assert [leak for leak in canary.leaks(text) if leak not in canary.names] == []
    captured = capfd.readouterr()
    for output in (caplog.text, captured.out, captured.err):
        assert canary.leaks(output) == []
        assert location not in output


def _envelope_values(record: Mapping[str, Any]) -> list[str]:
    keys = ("id", "SK", "eventType", "event_type", "occurredAt", "created_at", "occurred_at")
    values = [record.get(key) for key in keys]
    return [value.removeprefix("EVENT#") for value in values if isinstance(value, str)]
