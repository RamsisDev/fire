"""SnsFanoutRecordSource on moto: notifications with and without raw delivery, bodies that are no
notification of the topic, `latest` and `oldest`, acks that delete, a missing queue logged once,
regions, and privacy.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import boto3
import pytest
from aws_streams import (
    OTHER_REGION,
    REGION,
    Passes,
    drain,
    fanout,
    isolate_aws,
    nothing,
    publish,
    queue_depth,
)
from canary import Canary
from moto import mock_aws

from fire.adapters.sources.sns_fanout import SnsFanoutRecordSource, queue_name
from fire.core.ports.record_source import RecordSkipReason, RecordSource, StartPosition

SOURCE = "shipment-events"
TOPIC = "shipment-events-topic"
QUEUE = queue_name("shop", SOURCE)
LOGGER = "fire.adapters.sources.sns_fanout"
DELIVERED = {"event_type": "delivered", "occurred_at": "2026-10-11T16:30:00+00:00"}


@pytest.fixture
def aws(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    isolate_aws(monkeypatch, tmp_path)
    with mock_aws():
        yield


def _source(
    *steps: Callable[[], object],
    regions: tuple[str, ...] = (REGION,),
    start: StartPosition = StartPosition.OLDEST,
    raw_delivery: bool = False,
    topic: str = TOPIC,
    queue: str = QUEUE,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> SnsFanoutRecordSource:
    return SnsFanoutRecordSource(
        source=SOURCE,
        topic=topic,
        queue=queue,
        clients={region: boto3.client("sqs", region_name=region) for region in regions},
        raw_delivery=raw_delivery,
        start=start,
        wait_s=0,
        visibility_s=300,
        poll_s=1,
        clock=clock,
        sleep=Passes(*steps),
    )


def _warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [record.getMessage() for record in caplog.records if record.name == LOGGER]


@pytest.mark.unit
def test_the_queue_is_named_after_the_system_and_the_source() -> None:
    assert queue_name("acme", "shipment-events") == "fire-acme-shipment-events"


@pytest.mark.unit
def test_is_a_record_source(aws: None) -> None:
    assert isinstance(_source(), RecordSource)


@pytest.mark.unit
async def test_a_notification_is_read_with_its_message_decoded(aws: None) -> None:
    sns, topic_arn = fanout(TOPIC, QUEUE)
    publish(sns, topic_arn, [DELIVERED, "plain text"])

    read = await drain(_source().records())

    records = [raw.record for raw in read]
    assert [record["Message"] for record in records] == [DELIVERED, "plain text"]
    assert {record["Type"] for record in records} == {"Notification"}
    assert {record["TopicArn"] for record in records} == {topic_arn}
    assert all(record["MessageId"] and record["Timestamp"] for record in records)
    assert {(raw.source, raw.region, raw.skipped) for raw in read} == {(SOURCE, REGION, None)}


@pytest.mark.unit
async def test_with_raw_delivery_the_body_is_the_message(aws: None) -> None:
    sns, topic_arn = fanout(TOPIC, QUEUE, raw_delivery=True)
    publish(sns, topic_arn, [DELIVERED, "plain text"])

    read = await drain(_source(raw_delivery=True).records())

    records = [raw.record for raw in read]
    assert [record["Message"] for record in records] == [DELIVERED, "plain text"]
    assert [sorted(record) for record in records] == [["Message", "MessageId", "Timestamp"]] * 2
    assert records[0]["Timestamp"].endswith("Z")
    datetime.fromisoformat(records[0]["Timestamp"])


@pytest.mark.unit
@pytest.mark.parametrize(
    "body",
    [
        "not json",
        json.dumps(["a"]),
        json.dumps({"Message": "x"}),
        json.dumps({"Type": "Notification", "Message": {"x": 1}, "TopicArn": f"a:{TOPIC}"}),
        json.dumps({"Type": "Notification", "Message": "{}"}),
    ],
    ids=["not-json", "array", "no-type", "message-object", "no-topic"],
)
async def test_a_body_that_is_no_sns_message_has_no_record(aws: None, body: str) -> None:
    sqs = boto3.client("sqs", region_name=REGION)
    url = sqs.create_queue(QueueName=QUEUE)["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody=body)

    [raw] = await drain(_source().records())

    assert (raw.record, raw.skipped) == (None, None)
    await raw.ack()
    assert queue_depth(QUEUE) == 0


@pytest.mark.unit
@pytest.mark.parametrize(
    ("body", "deleted"),
    [
        (
            {"Type": "Notification", "Message": "{}", "TopicArn": "arn:aws:sns:x:1:other"},
            True,
        ),
        ({"Type": "UnsubscribeConfirmation", "Message": "x", "TopicArn": f"a:{TOPIC}"}, True),
        ({"Type": "SubscriptionConfirmation", "Token": "t", "TopicArn": f"a:{TOPIC}"}, False),
    ],
    ids=["other-topic", "unsubscribe", "confirmation"],
)
async def test_another_sns_message_is_skipped_and_a_confirmation_kept(
    aws: None, body: dict[str, str], deleted: bool
) -> None:
    sqs = boto3.client("sqs", region_name=REGION)
    url = sqs.create_queue(QueueName=QUEUE)["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody=json.dumps(body))

    [raw] = await drain(_source().records())
    await raw.ack()

    assert (raw.record, raw.skipped) == (None, RecordSkipReason.NOT_A_NOTIFICATION)
    assert queue_depth(QUEUE) == (0 if deleted else 1)


@pytest.mark.unit
async def test_latest_skips_messages_sent_before_the_start_second(aws: None) -> None:
    sns, topic_arn = fanout(TOPIC, QUEUE)
    publish(sns, topic_arn, [{"n": 1}])
    # Into the next second, so the start floored to the second is after the first message.
    time.sleep(1.01 - datetime.now(UTC).microsecond / 1_000_000)

    read = await drain(
        _source(
            lambda: publish(sns, topic_arn, [{"n": 2}]), nothing, start=StartPosition.LATEST
        ).records()
    )

    assert [(raw.record and raw.record["Message"], raw.skipped) for raw in read] == [
        (None, RecordSkipReason.BEFORE_START),
        ({"n": 2}, None),
    ]


@pytest.mark.unit
async def test_oldest_reads_what_the_queue_already_holds(aws: None) -> None:
    sns, topic_arn = fanout(TOPIC, QUEUE)
    publish(sns, topic_arn, [{"n": 1}])

    read = await drain(
        _source(
            start=StartPosition.OLDEST, clock=lambda: datetime.now(UTC) + timedelta(hours=1)
        ).records()
    )

    assert [raw.record["Message"] for raw in read] == [{"n": 1}]


@pytest.mark.unit
async def test_an_ack_deletes_the_message_and_an_unacked_one_stays(aws: None) -> None:
    sns, topic_arn = fanout(TOPIC, QUEUE)
    publish(sns, topic_arn, [{"n": 1}, {"n": 2}])

    acked, kept = await drain(_source().records())
    await acked.ack()

    assert queue_depth(QUEUE) == 1
    assert kept.record["Message"] == {"n": 2}


@pytest.mark.unit
async def test_a_missing_queue_is_logged_once_and_read_once_it_exists(
    aws: None, caplog: pytest.LogCaptureFixture
) -> None:
    def create() -> None:
        sns, topic_arn = fanout(TOPIC, QUEUE)
        publish(sns, topic_arn, [{"n": 1}])

    read = await drain(_source(nothing, create, nothing).records())

    assert [raw.record["Message"] for raw in read] == [{"n": 1}]
    assert _warnings(caplog) == [
        f"event read failed: source={SOURCE} region={REGION} "
        "error=ClientError:AWS.SimpleQueueService.NonExistentQueue"
    ]


@pytest.mark.unit
async def test_each_region_is_read_and_its_records_carry_it(aws: None) -> None:
    for region in (REGION, OTHER_REGION):
        sns, topic_arn = fanout(TOPIC, QUEUE, region)
        publish(sns, topic_arn, [{"region": region}])

    read = await drain(_source(*[nothing] * 5, regions=(REGION, OTHER_REGION)).records())

    assert sorted((raw.region, raw.record["Message"]["region"]) for raw in read) == [
        (REGION, REGION),
        (OTHER_REGION, OTHER_REGION),
    ]


@pytest.mark.unit
async def test_no_canary_reaches_logs_or_reprs(
    aws: None, canary: Canary, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    topic, queue = canary.token(), f"fire-{canary.token()}"
    message = canary.plant({"event_type": "x", "detail": {"name": "x", "n": 1}})

    def create() -> None:
        sns, topic_arn = fanout(topic, queue)
        publish(sns, topic_arn, [message, canary.string()], canary.token)

    source = _source(create, nothing, topic=topic, queue=queue)
    read = await drain(source.records())

    assert read[0].record is not None and read[0].record["Message"] == message
    assert len(_warnings(caplog)) == 1
    assert canary.leaks("\n".join([caplog.text, repr(source), *map(repr, read)])) == []
