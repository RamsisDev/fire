"""Helpers for the event source tests on moto: an AWS environment from inside the repo, writers
that put records where each kind reads them, and a sleep that ends a live source after its
passes.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3
import pytest
from boto3.dynamodb.types import TypeSerializer

from fire.adapters.sources.aws import quiet_boto

REGION = "us-east-1"
OTHER_REGION = "us-west-2"
_SERIALIZER = TypeSerializer()


def isolate_aws(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Fake credentials; no endpoint, profile, or AWS file from outside the repo; boto's loggers
    held at INFO, as the tentacle holds them.
    """
    quiet_boto()
    for name in (
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_S3",
        "AWS_PROFILE",
        "AWS_REGION",
        "AWS_SESSION_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "aws-config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "aws-credentials"))


class Stop(Exception):
    """Ends a test's run of an endless source."""


class Passes:
    """A source's sleep: runs the next step after each pass, and stops after the last."""

    def __init__(self, *steps: Callable[[], object]) -> None:
        self._steps = list(steps)
        self.slept: list[float] = []

    @property
    def done(self) -> int:
        return len(self.slept)

    async def __call__(self, seconds: float) -> None:
        self.slept.append(seconds)
        if self.done > len(self._steps):
            raise Stop
        self._steps[self.done - 1]()


def nothing() -> None:
    return None


async def drain(records: Any) -> list[Any]:
    """Every record an endless source yields until its sleep raises `Stop`."""
    read: list[Any] = []
    with pytest.raises(Stop):
        async for raw in records:
            read.append(raw)
    return read


# Kinesis


def create_stream(stream: str, region: str = REGION, shards: int = 1) -> Any:
    client = boto3.client("kinesis", region_name=region)
    client.create_stream(StreamName=stream, ShardCount=shards)
    return client


def put_records(
    client: Any, stream: str, records: Iterable[Any], partition_key: Callable[[], str] = str
) -> None:
    """Puts each record as its JSON, or as is when it is bytes."""
    for record in records:
        data = record if isinstance(record, bytes) else json.dumps(record).encode()
        client.put_record(StreamName=stream, Data=data, PartitionKey=partition_key() or "p")


# DynamoDB


def create_table(table: str, region: str = REGION, view: str | None = "NEW_AND_OLD_IMAGES") -> Any:
    client = boto3.client("dynamodb", region_name=region)
    stream = {"StreamEnabled": True, "StreamViewType": view} if view else {"StreamEnabled": False}
    client.create_table(
        TableName=table,
        KeySchema=[
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification=stream,
    )
    return client


def item(record: Mapping[str, Any]) -> dict[str, Any]:
    """A plain JSON object in DynamoDB JSON; floats as their text."""

    def numbers(value: Any) -> Any:
        if isinstance(value, float):
            return Decimal(str(value))
        if isinstance(value, dict):
            return {key: numbers(inner) for key, inner in value.items()}
        if isinstance(value, list):
            return [numbers(inner) for inner in value]
        return value

    return {key: _SERIALIZER.serialize(numbers(value)) for key, value in record.items()}


def put_items(client: Any, table: str, records: Iterable[Mapping[str, Any]]) -> None:
    for record in records:
        client.put_item(TableName=table, Item=item(record))


# SNS over SQS


def fanout(
    topic: str, queue: str, region: str = REGION, *, raw_delivery: bool = False
) -> tuple[Any, str]:
    """Creates the topic and the queue, subscribes the queue; returns the SNS client and the
    topic ARN.
    """
    sns = boto3.client("sns", region_name=region)
    sqs = boto3.client("sqs", region_name=region)
    topic_arn = sns.create_topic(Name=topic)["TopicArn"]
    url = sqs.create_queue(QueueName=queue)["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queue_arn["Attributes"]["QueueArn"],
        Attributes={"RawMessageDelivery": "true" if raw_delivery else "false"},
    )
    return sns, topic_arn


def publish(
    sns: Any,
    topic_arn: str,
    messages: Iterable[Any],
    attribute: Callable[[], str] | None = None,
) -> None:
    """Publishes each message as its JSON, or as is when it is a string."""
    for message in messages:
        text = message if isinstance(message, str) else json.dumps(message)
        extra: dict[str, Any] = {}
        if attribute is not None:
            extra["Subject"] = attribute()
            extra["MessageAttributes"] = {
                "trace": {"DataType": "String", "StringValue": attribute()}
            }
        sns.publish(TopicArn=topic_arn, Message=text, **extra)


def queue_depth(queue: str, region: str = REGION) -> int:
    sqs = boto3.client("sqs", region_name=region)
    url = sqs.get_queue_url(QueueName=queue)["QueueUrl"]
    attributes = sqs.get_queue_attributes(
        QueueUrl=url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    return int(attributes["ApproximateNumberOfMessages"]) + int(
        attributes["ApproximateNumberOfMessagesNotVisible"]
    )
