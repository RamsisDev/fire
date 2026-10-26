"""S3 capture CaptureSource: the request and response pairs a system's clients write to a
bucket, each read once.

Every `poll_s` the source lists each rule's `responses/` and reads each response once it is
`settle_s` old, with the request of the same name under `requests/`. Every boto3 call runs in
a worker thread. Logs hold the source id, exception type names, and S3 error codes; never a
key, the bucket name, metadata, a body, or the text of an exception.
Contract: docs/specs/tentacle.md#kinds (`s3_capture`).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from fire.adapters.sources.aws import (
    CONNECT_TIMEOUT_S,
    READ_ERRORS,
    READ_TIMEOUT_S,
    FailureLog,
    error_code,
)
from fire.core.domain.capture import REQUESTS, RESPONSES, CaptureRule
from fire.core.ports.capture_source import ObjectState, RawCapture
from fire.core.ports.record_source import StartPosition

logger = logging.getLogger(__name__)

ENDPOINT_ENVS = ("AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL")
# `NoSuchBucket` is also a 404, and is a failure: the bucket is not there to hold the half.
_NOT_FOUND = frozenset({"NoSuchKey", "404"})


def make_s3_client() -> Any:
    """An S3 client from the standard AWS environment, with the timeouts of `make_client`;
    path-style addressing when AWS_ENDPOINT_URL_S3 or AWS_ENDPOINT_URL is set (an S3-compatible
    endpoint such as a local AWS emulator).
    """
    behind_endpoint = any(os.environ.get(name) for name in ENDPOINT_ENVS)
    s3 = {"addressing_style": "path"} if behind_endpoint else None
    config = Config(s3=s3, connect_timeout=CONNECT_TIMEOUT_S, read_timeout=READ_TIMEOUT_S)
    return boto3.client("s3", config=config)


@dataclass(frozen=True)
class _Listed:
    key: str
    last_modified: datetime
    size: int


@dataclass(frozen=True)
class _Half:
    """One half as fetched: its state, its bytes when read, and its metadata `action`."""

    state: ObjectState
    data: bytes | None = None
    action: str | None = None


class S3CaptureSource:
    """Reads each rule's pairs from `bucket`, in rule order, then listing order.

    A response is read once it is `settle_s` old, and only once: the keys read stay in memory
    until their objects leave the bucket. With `StartPosition.LATEST`, a response written
    before the source started, to the whole second S3 keeps, is never read. A half over
    `max_object_bytes` is `too_large` and not read. A pair whose read fails is read on the next
    pass; a failed listing ends its rule's pass.
    """

    def __init__(
        self,
        *,
        source: str,
        bucket: str,
        rules: Sequence[CaptureRule],
        client: Any,
        start: StartPosition,
        poll_s: float,
        settle_s: float,
        max_object_bytes: int,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._source = source
        self._bucket = bucket
        self._rules = tuple(rules)
        self._client = client
        self._start = start
        self._poll_s = poll_s
        self._settle = timedelta(seconds=settle_s)
        self._max_object_bytes = max_object_bytes
        self._clock = clock
        self._sleep = sleep
        self._read: dict[str, set[str]] = {rule.prefix: set() for rule in self._rules}
        self._failures = FailureLog(logger, "capture read failed", source)

    def __repr__(self) -> str:
        return f"S3CaptureSource(source={self._source!r})"

    async def pairs(self) -> AsyncIterator[RawCapture]:
        """New pairs, one pass over the rules every `poll_s`, until cancelled."""
        since = self._clock().replace(microsecond=0)
        while True:
            for rule in self._rules:
                async for capture in self._rule_pairs(rule, since):
                    yield capture
            self._failures.end_pass()
            await self._sleep(self._poll_s)

    async def _rule_pairs(self, rule: CaptureRule, since: datetime) -> AsyncIterator[RawCapture]:
        """The rule's unread settled pairs in listing order; a failed listing ends them."""
        responses = f"{rule.prefix}{RESPONSES}/"
        try:
            listed = await asyncio.to_thread(self._list, responses)
        except READ_ERRORS as error:
            self._failures.failed(error)
            return
        read = self._read[rule.prefix]
        read.intersection_update(item.key for item in listed)
        for item in listed:
            name = item.key[len(responses) :]
            if not name or name.endswith("/") or item.key in read:
                continue
            if self._start is StartPosition.LATEST and item.last_modified < since:
                continue
            if self._clock() - item.last_modified < self._settle:
                continue
            try:
                capture = await self._pair(rule, name, item.size)
            except READ_ERRORS as error:
                self._failures.failed(error)
                continue
            read.add(item.key)
            yield capture

    async def _pair(self, rule: CaptureRule, name: str, size: int) -> RawCapture:
        if size > self._max_object_bytes:
            response = _Half(ObjectState.TOO_LARGE)
        else:
            response = await asyncio.to_thread(self._get, f"{rule.prefix}{RESPONSES}/{name}")
        request = await asyncio.to_thread(self._get, f"{rule.prefix}{REQUESTS}/{name}")
        response_state, response_object = _decoded(response)
        request_state, request_object = _decoded(request)
        return RawCapture(
            source=self._source,
            rule=rule,
            name=name,
            response_state=response_state,
            response=response_object,
            request_state=request_state,
            request=request_object,
            metadata_action=response.action or request.action,
        )

    def _list(self, prefix: str) -> list[_Listed]:
        """Every object under `prefix`, all pages, in listing order."""
        paginator = self._client.get_paginator("list_objects_v2")
        return [
            _Listed(key=item["Key"], last_modified=item["LastModified"], size=item["Size"])
            for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix)
            for item in page.get("Contents", ())
        ]

    def _get(self, key: str) -> _Half:
        """One half's bytes; `missing` without the key, `too_large` with its body unread."""
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
        except ClientError as error:
            if error_code(error) in _NOT_FOUND:
                return _Half(ObjectState.MISSING)
            raise
        with contextlib.closing(response["Body"]) as body:
            action = _action(response.get("Metadata"))
            if response.get("ContentLength", 0) > self._max_object_bytes:
                return _Half(ObjectState.TOO_LARGE, action=action)
            return _Half(ObjectState.FOUND, data=body.read(), action=action)


def _decoded(half: _Half) -> tuple[ObjectState, dict[str, Any] | None]:
    """The half's state and object; `undecodable` when its bytes are no UTF-8 JSON object."""
    if half.data is None:
        return half.state, None
    try:
        document = json.loads(half.data.decode("utf-8"))
    except (ValueError, RecursionError):
        return ObjectState.UNDECODABLE, None
    if not isinstance(document, dict):
        return ObjectState.UNDECODABLE, None
    return ObjectState.FOUND, document


def _action(metadata: object) -> str | None:
    """The object metadata `action` when it is a non-empty string; boto3 lowercases keys."""
    if not isinstance(metadata, dict):
        return None
    action = metadata.get("action")
    return action if isinstance(action, str) and action else None
