"""SNS fan-out RecordSource: the notifications of a topic, read from a queue FIRE owns that is
subscribed to it, in each region, so the system's own queues lose nothing.

The source never creates, subscribes, or deletes a queue: `fire-{system}-{source}` must exist
in every region, subscribed to the topic (docs/specs/tentacle.md#kinds, `sns_fanout`). It
receives from that queue and deletes a message once its record is settled. Every boto3 call
runs in a worker thread. Logs hold the source id, regions, exception type names, and AWS error
codes; never the topic or queue name, a message, an attribute, or the text of an exception.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from datetime import UTC, datetime
from typing import Any

from fire.adapters.sources.aws import READ_ERRORS, FailureLog, merge
from fire.core.ports.record_source import RawRecord, RecordSkipReason, StartPosition

logger = logging.getLogger(__name__)

MAX_MESSAGES = 10


def queue_name(system: str, source: str) -> str:
    """The queue FIRE owns for one `sns_fanout` source, the same in every region."""
    return f"fire-{system}-{source}"


class SnsFanoutRecordSource:
    """Receives from `queue` in every region of `clients`, an SQS client per region.

    Without `raw_delivery`, a message body must be an SNS notification of `topic`; its record
    is the notification with `Message` decoded when it is JSON. Another SNS message (a
    subscription confirmation, a notification of another topic) is skipped as
    `not_a_notification`, and any other body is a record with no object. With `raw_delivery`,
    the body is the message, and its record is `{"MessageId", "Timestamp", "Message"}` built
    from the SQS message, the same paths. With `StartPosition.LATEST`, a message sent before
    the start, to the second, is skipped as `before_start`. An ack deletes the message, except
    a subscription confirmation's: its token is what confirms a subscription across accounts.
    """

    def __init__(
        self,
        *,
        source: str,
        topic: str,
        queue: str,
        clients: Mapping[str, Any],
        raw_delivery: bool,
        start: StartPosition,
        wait_s: int,
        visibility_s: int,
        poll_s: float,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._source = source
        self._topic = topic
        self._queue = queue
        self._clients = dict(clients)
        self._raw_delivery = raw_delivery
        self._start = start
        self._wait_s = wait_s
        self._visibility_s = visibility_s
        self._poll_s = poll_s
        self._clock = clock
        self._sleep = sleep

    def __repr__(self) -> str:
        return f"SnsFanoutRecordSource(source={self._source!r})"

    async def records(self) -> AsyncIterator[RawRecord]:
        since = self._clock().replace(microsecond=0)
        regions = [self._region(region, client, since) for region, client in self._clients.items()]
        async for raw in merge(regions):
            yield raw

    async def _region(self, region: str, client: Any, since: datetime) -> AsyncIterator[RawRecord]:
        """Receives until cancelled; waits `poll_s` after a receive that failed or got nothing."""
        failures = FailureLog(logger, "event read failed", self._source, region)
        url: str | None = None
        while True:
            try:
                if url is None:
                    found = await asyncio.to_thread(client.get_queue_url, QueueName=self._queue)
                    url = found["QueueUrl"]
                response = await asyncio.to_thread(
                    client.receive_message,
                    QueueUrl=url,
                    MaxNumberOfMessages=MAX_MESSAGES,
                    WaitTimeSeconds=self._wait_s,
                    VisibilityTimeout=self._visibility_s,
                    AttributeNames=["SentTimestamp"],
                )
            except READ_ERRORS as error:
                url = None
                failures.failed(error)
                failures.end_pass()
                await self._sleep(self._poll_s)
                continue
            failures.end_pass()
            messages = response.get("Messages", [])
            for message in messages:
                yield self._raw(region, client, url, message, since)
            if not messages:
                await self._sleep(self._poll_s)

    def _raw(
        self, region: str, client: Any, url: str, message: Mapping[str, Any], since: datetime
    ) -> RawRecord:
        ack = _deleter(client, url, message["ReceiptHandle"])
        sent = _sent_at(message)
        if self._start is StartPosition.LATEST and sent is not None and sent < since:
            return RawRecord(
                self._source, region, None, ack, skipped=RecordSkipReason.BEFORE_START
            )
        body = message.get("Body")
        if self._raw_delivery:
            return RawRecord(
                self._source, region, _raw_record(body, message.get("MessageId"), sent), ack
            )
        return self._notification(region, body, ack)

    def _notification(
        self, region: str, body: object, ack: Callable[[], Awaitable[None]]
    ) -> RawRecord:
        notification = _json(body)
        kind = notification.get("Type") if isinstance(notification, dict) else None
        if not isinstance(kind, str):
            return RawRecord(self._source, region, None, ack)
        not_one = RecordSkipReason.NOT_A_NOTIFICATION
        if kind == "SubscriptionConfirmation":
            return RawRecord(self._source, region, None, skipped=not_one)
        if kind != "Notification":
            return RawRecord(self._source, region, None, ack, skipped=not_one)
        message, topic_arn = notification.get("Message"), notification.get("TopicArn")
        if not isinstance(message, str) or not isinstance(topic_arn, str):
            return RawRecord(self._source, region, None, ack)
        if topic_arn.rpartition(":")[2] != self._topic:
            return RawRecord(self._source, region, None, ack, skipped=not_one)
        return RawRecord(self._source, region, {**notification, "Message": _message(message)}, ack)


def _deleter(client: Any, url: str, handle: str) -> Callable[[], Awaitable[None]]:
    async def ack() -> None:
        await asyncio.to_thread(client.delete_message, QueueUrl=url, ReceiptHandle=handle)

    return ack


def _sent_at(message: Mapping[str, Any]) -> datetime | None:
    sent = (message.get("Attributes") or {}).get("SentTimestamp")
    if not isinstance(sent, str) or not sent.isdigit():
        return None
    return datetime.fromtimestamp(int(sent) / 1000, UTC)


def _raw_record(body: object, message_id: object, sent: datetime | None) -> dict[str, Any]:
    record: dict[str, Any] = {"MessageId": message_id, "Message": _message(body)}
    if sent is not None:
        record["Timestamp"] = sent.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return record


_UNPARSED = object()


def _json(text: object) -> Any:
    if not isinstance(text, str):
        return _UNPARSED
    try:
        return json.loads(text)
    except (ValueError, RecursionError):
        return _UNPARSED


def _message(text: object) -> Any:
    """The message decoded when it is JSON; else the text as it came."""
    decoded = _json(text)
    return text if decoded is _UNPARSED else decoded
