"""S3CaptureSource: pairs in rule then listing order, both halves' states, settle, the start
position, failed calls, and privacy, on moto's S3 or a stub client; and `make_s3_client`.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import boto3
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from canary import Canary
from moto import mock_aws
from spans import Clock

from fire.adapters.sources.aws import quiet_boto
from fire.adapters.sources.s3_capture import S3CaptureSource, make_s3_client
from fire.core.domain.capture import CaptureLayout, CaptureRule
from fire.core.ports.capture_source import CaptureSource, ObjectState, RawCapture
from fire.core.ports.record_source import StartPosition

SOURCE = "acme-captures"
BUCKET = "capture-bucket"
LOGGER = "fire.adapters.sources.s3_capture"
WARNING = "capture read failed: source=acme-captures error="
START = datetime(2026, 10, 11, 12, 0, 0, tzinfo=UTC)
BILLING = CaptureRule(
    prefix="billing-payloads/", label="billing", layout=CaptureLayout.WORK_UNIT_ENTITY
)
NOTIFICATIONS = CaptureRule(
    prefix="notification-payloads/", label="notifications", layout=CaptureLayout.WORK_UNIT_ACTION
)
FIRST = "wu-1/ord-1_20261011_120000_000001.json"
SECOND = "wu-1/ord-2_20261011_120000_000002.json"
THIRD = "wu-1/ord-3_20261011_120000_000003.json"
CONFIRMATION = "wu-2/send_order_confirmation_20261011_120000_000004.json"
REQUEST = {"method": "POST", "url": "https://partner.example/orders", "body": {"id": "o-1"}}
RESPONSE = {"status_code": 200, "body": {"accepted": True}}

FOUND = ObjectState.FOUND
MISSING = ObjectState.MISSING
UNDECODABLE = ObjectState.UNDECODABLE
TOO_LARGE = ObjectState.TOO_LARGE


@pytest.fixture
def aws(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "aws-config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "aws-credentials"))


@pytest.fixture
def s3(aws: None) -> Iterator[Any]:
    """A boto3 client on moto's S3, with `BUCKET` created."""
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket=BUCKET)
        yield client


class _Stop(Exception):
    """Ends a test's run of the endless `pairs()`."""


class _Passes:
    """The source's sleep: runs the next step after each pass, and stops after the last."""

    def __init__(self, *steps: Callable[[], object]) -> None:
        self._steps = list(steps)
        self.slept: list[float] = []

    @property
    def done(self) -> int:
        return len(self.slept)

    async def __call__(self, seconds: float) -> None:
        self.slept.append(seconds)
        if self.done > len(self._steps):
            raise _Stop
        self._steps[self.done - 1]()


def _nothing() -> None:
    return None


def _source(
    client: Any,
    *rules: CaptureRule,
    sleep: Callable[[float], Awaitable[None]],
    clock: Callable[[], datetime],
    start: StartPosition = StartPosition.OLDEST,
    settle_s: float = 0,
    max_object_bytes: int = 1_048_576,
    bucket: str = BUCKET,
) -> S3CaptureSource:
    return S3CaptureSource(
        source=SOURCE,
        bucket=bucket,
        rules=rules,
        client=client,
        start=start,
        poll_s=5,
        settle_s=settle_s,
        max_object_bytes=max_object_bytes,
        clock=clock,
        sleep=sleep,
    )


def _clock() -> Clock:
    """Just after now, so every object written so far has settled with `settle_s=0`."""
    return Clock(datetime.now(UTC) + timedelta(seconds=1))


async def _run(source: S3CaptureSource, passes: _Passes) -> list[tuple[int, RawCapture]]:
    """Each pair the source yields until `passes` stops it, with the pass that read it."""
    read: list[tuple[int, RawCapture]] = []
    with pytest.raises(_Stop):
        async for raw in source.pairs():
            read.append((passes.done, raw))
    return read


async def _read(
    client: Any,
    *rules: CaptureRule,
    steps: Sequence[Callable[[], object]] = (),
    clock: Clock | None = None,
    **options: Any,
) -> list[tuple[int, RawCapture]]:
    """The pairs of one pass, then one more pass per step."""
    passes = _Passes(*steps)
    source = _source(client, *rules, sleep=passes, clock=clock or _clock(), **options)
    return await _run(source, passes)


def _bytes(body: Any) -> bytes:
    return body if isinstance(body, bytes) else json.dumps(body).encode()


def _put(
    client: Any,
    key: str,
    body: Any,
    metadata: Mapping[str, str] | None = None,
    bucket: str = BUCKET,
) -> None:
    client.put_object(Bucket=bucket, Key=key, Body=_bytes(body), Metadata=dict(metadata or {}))


def _put_pair(
    client: Any,
    rule: CaptureRule,
    name: str,
    *,
    request: Any = REQUEST,
    response: Any = RESPONSE,
    request_metadata: Mapping[str, str] | None = None,
    response_metadata: Mapping[str, str] | None = None,
    bucket: str = BUCKET,
) -> None:
    """Writes the request, unless None, then the response, as a capturing client does."""
    if request is not None:
        _put(client, f"{rule.prefix}requests/{name}", request, request_metadata, bucket)
    _put(client, f"{rule.prefix}responses/{name}", response, response_metadata, bucket)


def _warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    records = [record for record in caplog.records if record.name == LOGGER]
    assert {record.levelno for record in records} <= {logging.WARNING}
    return [record.getMessage() for record in records]


def _client_error(code: str, message: str = "") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": message}}, "GetObject")


@dataclass
class _Object:
    data: bytes
    last_modified: datetime = START
    metadata: dict[str, str] = field(default_factory=dict)
    # The size listings report, for an object rewritten larger after it was listed.
    listed_size: int | None = None


def _pair_objects(
    rule: CaptureRule,
    name: str,
    *,
    request: Any = REQUEST,
    response: Any = RESPONSE,
    request_metadata: Mapping[str, str] | None = None,
    last_modified: datetime = START,
) -> dict[str, _Object]:
    return {
        f"{rule.prefix}requests/{name}": _Object(
            _bytes(request), last_modified, dict(request_metadata or {})
        ),
        f"{rule.prefix}responses/{name}": _Object(_bytes(response), last_modified),
    }


class _Body:
    """A response body that records whether it was read and closed."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self.was_read = False
        self.closed = False

    def read(self) -> bytes:
        self.was_read = True
        return self._data

    def close(self) -> None:
        self.closed = True


class _StubS3:
    """An S3 client over `objects`, listed in two pages, that records its GETs.

    Each listing takes the next of `list_errors` (None lists); each GET of a key takes the
    next of its `get_errors`.
    """

    def __init__(self, objects: Mapping[str, _Object] | None = None) -> None:
        self.objects = dict(objects or {})
        self.list_errors: list[Exception | None] = []
        self.get_errors: dict[str, list[Exception]] = {}
        self.gets: list[str] = []
        self.bodies: dict[str, _Body] = {}

    def get_paginator(self, operation: str) -> _StubS3:
        assert operation == "list_objects_v2"
        return self

    def paginate(self, *, Bucket: str, Prefix: str) -> Iterator[dict[str, Any]]:
        error = self.list_errors.pop(0) if self.list_errors else None
        if error is not None:
            raise error
        keys = sorted(key for key in self.objects if key.startswith(Prefix))
        for page in (keys[:1], keys[1:]):
            contents = [
                {
                    "Key": key,
                    "LastModified": self.objects[key].last_modified,
                    "Size": self.objects[key].listed_size or len(self.objects[key].data),
                    "ETag": '"0"',
                }
                for key in page
            ]
            yield {"Contents": contents} if contents else {"KeyCount": 0}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.gets.append(Key)
        if self.get_errors.get(Key):
            raise self.get_errors[Key].pop(0)
        stored = self.objects.get(Key)
        if stored is None:
            raise _client_error("NoSuchKey")
        body = self.bodies[Key] = _Body(stored.data)
        return {"Body": body, "ContentLength": len(stored.data), "Metadata": stored.metadata}


@pytest.mark.unit
def test_is_a_capture_source() -> None:
    source = _source(_StubS3(), BILLING, sleep=_Passes(), clock=_clock())
    assert isinstance(source, CaptureSource)


@pytest.mark.unit
async def test_pairs_come_in_rule_order_then_listing_order_and_each_once(s3: Any) -> None:
    for rule, name in ((BILLING, SECOND), (BILLING, FIRST), (NOTIFICATIONS, CONFIRMATION)):
        _put_pair(s3, rule, name, request={"request": name}, response={"response": name})

    read = await _read(s3, NOTIFICATIONS, BILLING, steps=(_nothing,))

    assert [(number, raw.source, raw.rule, raw.name) for number, raw in read] == [
        (0, SOURCE, NOTIFICATIONS, CONFIRMATION),
        (0, SOURCE, BILLING, FIRST),
        (0, SOURCE, BILLING, SECOND),
    ]
    assert [(raw.response_key, raw.request_key) for _, raw in read] == [
        (
            f"notification-payloads/responses/{CONFIRMATION}",
            f"notification-payloads/requests/{CONFIRMATION}",
        ),
        (f"billing-payloads/responses/{FIRST}", f"billing-payloads/requests/{FIRST}"),
        (f"billing-payloads/responses/{SECOND}", f"billing-payloads/requests/{SECOND}"),
    ]
    for _, raw in read:
        assert (raw.response_state, raw.response) == (FOUND, {"response": raw.name})
        assert (raw.request_state, raw.request) == (FOUND, {"request": raw.name})
        assert raw.metadata_action is None
        assert await raw.ack() is None


@pytest.mark.unit
async def test_metadata_action_comes_from_the_response_else_the_request(s3: Any) -> None:
    _put_pair(
        s3,
        BILLING,
        FIRST,
        request_metadata={"action": "quote"},
        response_metadata={"action": "authorize"},
    )
    _put_pair(s3, BILLING, SECOND, request_metadata={"action": "quote"})
    _put_pair(s3, BILLING, THIRD, response_metadata={"channel": "email"})

    read = await _read(s3, BILLING)

    assert [(raw.name, raw.metadata_action) for _, raw in read] == [
        (FIRST, "authorize"),
        (SECOND, "quote"),
        (THIRD, None),
    ]


@pytest.mark.unit
async def test_a_response_without_its_request_has_a_missing_request(s3: Any) -> None:
    _put_pair(s3, BILLING, FIRST, request=None, response_metadata={"action": "authorize"})

    [(_, raw)] = await _read(s3, BILLING)

    assert (raw.response_state, raw.response) == (FOUND, RESPONSE)
    assert (raw.request_state, raw.request) == (MISSING, None)
    assert raw.metadata_action == "authorize"


@pytest.mark.unit
@pytest.mark.parametrize("code", ["NoSuchKey", "404"])
async def test_a_response_gone_before_its_get_is_missing_and_read_once(code: str) -> None:
    stub = _StubS3(_pair_objects(BILLING, FIRST))
    stub.get_errors[f"{BILLING.prefix}responses/{FIRST}"] = [_client_error(code)]

    read = await _read(stub, BILLING, steps=(_nothing,))

    assert [(number, raw.name) for number, raw in read] == [(0, FIRST)]
    [(_, raw)] = read
    assert (raw.response_state, raw.response) == (MISSING, None)
    assert (raw.request_state, raw.request) == (FOUND, REQUEST)


@pytest.mark.unit
@pytest.mark.parametrize(
    "body",
    [
        '{"name": "café"}'.encode("latin-1"),
        json.dumps({"id": 1}).encode("utf-16"),
        b"{not json",
        b"",
        b"[1, 2]",
        b'"text"',
        b"42",
        b"null",
        b"[" * 100_000 + b"]" * 100_000,
    ],
    ids=["latin-1", "utf-16", "not-json", "empty", "array", "string", "number", "null", "deep"],
)
async def test_a_half_that_is_no_utf8_json_object_is_undecodable(s3: Any, body: bytes) -> None:
    _put_pair(s3, BILLING, FIRST, request=body, response=body)
    _put_pair(s3, BILLING, SECOND, request=body)

    read = await _read(s3, BILLING)

    assert [
        (raw.response_state, raw.response, raw.request_state, raw.request) for _, raw in read
    ] == [(UNDECODABLE, None, UNDECODABLE, None), (FOUND, RESPONSE, UNDECODABLE, None)]


@pytest.mark.unit
async def test_a_response_over_max_object_bytes_is_too_large_and_never_fetched() -> None:
    request = {"id": 1}
    stub = _StubS3(
        _pair_objects(
            BILLING,
            FIRST,
            request=request,
            response={"padding": "x" * 64},
            request_metadata={"action": "quote"},
        )
    )

    [(_, raw)] = await _read(stub, BILLING, max_object_bytes=len(_bytes(request)))

    assert stub.gets == [f"{BILLING.prefix}requests/{FIRST}"]
    assert (raw.response_state, raw.response) == (TOO_LARGE, None)
    assert (raw.request_state, raw.request) == (FOUND, request)
    assert raw.metadata_action == "quote"


@pytest.mark.unit
async def test_a_request_over_max_object_bytes_is_too_large(s3: Any) -> None:
    at_limit = {"a": "x" * 100}
    _put_pair(s3, BILLING, FIRST, request={"a": "x" * 101}, request_metadata={"action": "quote"})
    _put_pair(s3, BILLING, SECOND, request=at_limit)

    read = await _read(s3, BILLING, max_object_bytes=len(_bytes(at_limit)))

    assert [
        (raw.response_state, raw.response, raw.request_state, raw.request) for _, raw in read
    ] == [(FOUND, RESPONSE, TOO_LARGE, None), (FOUND, RESPONSE, FOUND, at_limit)]
    assert read[0][1].metadata_action == "quote"


@pytest.mark.unit
async def test_a_too_large_request_is_closed_unread_and_every_body_is_closed() -> None:
    stub = _StubS3(_pair_objects(BILLING, FIRST, request={"padding": "x" * 64}, response={}))

    [(_, raw)] = await _read(stub, BILLING, max_object_bytes=32)

    assert raw.request_state is TOO_LARGE
    request = stub.bodies[f"{BILLING.prefix}requests/{FIRST}"]
    response = stub.bodies[f"{BILLING.prefix}responses/{FIRST}"]
    assert (request.was_read, request.closed) == (False, True)
    assert (response.was_read, response.closed) == (True, True)


@pytest.mark.unit
async def test_a_response_grown_past_max_object_bytes_since_its_listing_is_too_large() -> None:
    objects = _pair_objects(BILLING, FIRST, request={}, response={"padding": "x" * 64})
    response_key = f"{BILLING.prefix}responses/{FIRST}"
    objects[response_key].listed_size = 2
    stub = _StubS3(objects)

    [(_, raw)] = await _read(stub, BILLING, max_object_bytes=32)

    assert (raw.response_state, raw.response) == (TOO_LARGE, None)
    assert (stub.bodies[response_key].was_read, stub.bodies[response_key].closed) == (False, True)


@pytest.mark.unit
async def test_a_key_is_forgotten_once_its_object_leaves_the_bucket() -> None:
    objects = _pair_objects(BILLING, FIRST)
    stub = _StubS3(objects)

    read = await _read(
        stub,
        BILLING,
        steps=(stub.objects.clear, lambda: stub.objects.update(objects), _nothing),
    )

    assert [(number, raw.name) for number, raw in read] == [(0, FIRST), (2, FIRST)]


@pytest.mark.unit
async def test_a_response_is_read_once_settle_s_old_and_only_once(s3: Any) -> None:
    _put_pair(s3, BILLING, FIRST)
    [listed] = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{BILLING.prefix}responses/")["Contents"]
    clock = Clock(listed["LastModified"] + timedelta(seconds=9.999))

    read = await _read(
        s3,
        BILLING,
        settle_s=10,
        clock=clock,
        steps=(lambda: clock.advance(0.001), lambda: clock.advance(60)),
    )

    assert [(number, raw.name) for number, raw in read] == [(1, FIRST)]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("start", "expected"),
    [
        (StartPosition.LATEST, [(0, SECOND), (1, THIRD)]),
        (StartPosition.OLDEST, [(0, FIRST), (0, SECOND), (1, THIRD)]),
    ],
)
async def test_latest_reads_only_responses_written_since_the_start_second(
    start: StartPosition, expected: list[tuple[int, str]]
) -> None:
    stub = _StubS3(
        {
            **_pair_objects(BILLING, FIRST, last_modified=START - timedelta(seconds=1)),
            **_pair_objects(BILLING, SECOND, last_modified=START),
            **_pair_objects(BILLING, THIRD, last_modified=START + timedelta(seconds=3)),
        }
    )
    clock = Clock(START + timedelta(seconds=0.5))

    read = await _read(
        stub, BILLING, start=start, clock=clock, steps=(lambda: clock.advance(10), _nothing)
    )

    assert [(number, raw.name) for number, raw in read] == expected


@pytest.mark.unit
async def test_folder_markers_requests_and_index_markers_give_no_pair(s3: Any) -> None:
    for key in (f"{BILLING.prefix}responses/", f"{BILLING.prefix}responses/wu-1/"):
        _put(s3, key, b"")
    for folder in ("requests", "index"):
        _put(s3, f"{BILLING.prefix}{folder}/{FIRST}", REQUEST)
    _put(s3, f"{BILLING.prefix}{SECOND}", RESPONSE)
    # The pipeline, not the source, counts a key that matches no layout.
    _put_pair(s3, NOTIFICATIONS, "stray.json")

    read = await _read(s3, BILLING, NOTIFICATIONS, steps=(_nothing,))

    assert [(number, raw.rule, raw.name) for number, raw in read] == [
        (0, NOTIFICATIONS, "stray.json")
    ]


@pytest.mark.unit
async def test_a_failed_listing_is_logged_once_and_polling_goes_on(
    aws: None, caplog: pytest.LogCaptureFixture
) -> None:
    with mock_aws():
        client = boto3.client("s3")

        def create_the_bucket() -> None:
            client.create_bucket(Bucket=BUCKET)
            _put_pair(client, BILLING, FIRST)

        read = await _read(
            client, BILLING, steps=(_nothing, _nothing, create_the_bucket, _nothing)
        )

    assert [(number, raw.name) for number, raw in read] == [(3, FIRST)]
    assert _warnings(caplog) == [f"{WARNING}ClientError:NoSuchBucket"]


@pytest.mark.unit
async def test_a_failed_listing_leaves_the_other_rules_of_its_pass(
    caplog: pytest.LogCaptureFixture,
) -> None:
    stub = _StubS3({**_pair_objects(BILLING, FIRST), **_pair_objects(NOTIFICATIONS, CONFIRMATION)})
    stub.list_errors = [_client_error("AccessDenied")]

    read = await _read(stub, BILLING, NOTIFICATIONS, steps=(_nothing,))

    assert [(number, raw.name) for number, raw in read] == [(0, CONFIRMATION), (1, FIRST)]
    assert _warnings(caplog) == [f"{WARNING}ClientError:AccessDenied"]


@pytest.mark.unit
async def test_an_error_is_logged_once_until_a_pass_completes_without_one(
    caplog: pytest.LogCaptureFixture,
) -> None:
    stub = _StubS3()
    stub.list_errors = [
        _client_error("NoSuchBucket"),
        _client_error("NoSuchBucket"),
        EndpointConnectionError(endpoint_url="http://s3.example"),
        _client_error("NoSuchBucket"),
        None,
        _client_error("NoSuchBucket"),
    ]

    await _read(stub, BILLING, steps=(_nothing,) * 5)

    assert _warnings(caplog) == [
        f"{WARNING}ClientError:NoSuchBucket",
        f"{WARNING}EndpointConnectionError",
        f"{WARNING}ClientError:NoSuchBucket",
    ]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("error", "text"),
    [
        (lambda: _client_error("InternalError"), "ClientError:InternalError"),
        (lambda: _client_error("NoSuchBucket"), "ClientError:NoSuchBucket"),
        (
            lambda: EndpointConnectionError(endpoint_url="http://s3.example"),
            "EndpointConnectionError",
        ),
        (ConnectionResetError, "ConnectionResetError"),
    ],
    ids=["internal-error", "no-such-bucket", "endpoint", "os-error"],
)
@pytest.mark.parametrize("half", ["responses", "requests"])
async def test_a_failed_get_leaves_only_its_pair_to_the_next_pass(
    caplog: pytest.LogCaptureFixture,
    error: Callable[[], Exception],
    text: str,
    half: str,
) -> None:
    stub = _StubS3(
        {
            **_pair_objects(BILLING, FIRST),
            **_pair_objects(BILLING, SECOND),
            **_pair_objects(NOTIFICATIONS, CONFIRMATION),
        }
    )
    stub.get_errors[f"{BILLING.prefix}{half}/{FIRST}"] = [error()]

    read = await _read(stub, BILLING, NOTIFICATIONS, steps=(_nothing, _nothing))

    assert [(number, raw.name) for number, raw in read] == [
        (0, SECOND),
        (0, CONFIRMATION),
        (1, FIRST),
    ]
    assert {(raw.response_state, raw.request_state) for _, raw in read} == {(FOUND, FOUND)}
    assert _warnings(caplog) == [WARNING + text]


@pytest.mark.unit
async def test_an_object_whose_get_always_fails_does_not_block_its_rule(
    caplog: pytest.LogCaptureFixture,
) -> None:
    failing = f"{BILLING.prefix}responses/{FIRST}"
    stub = _StubS3({**_pair_objects(BILLING, FIRST), **_pair_objects(BILLING, SECOND)})
    stub.get_errors[failing] = [_client_error("AccessDenied") for _ in range(3)]

    read = await _read(stub, BILLING, steps=(_nothing, _nothing))

    assert [(number, raw.name) for number, raw in read] == [(0, SECOND)]
    assert stub.gets.count(failing) == 3
    assert _warnings(caplog) == [f"{WARNING}ClientError:AccessDenied"]


@pytest.mark.unit
@pytest.mark.parametrize("call", ["listing", "get"])
async def test_a_programming_error_propagates(call: str) -> None:
    stub = _StubS3(_pair_objects(BILLING, FIRST))
    if call == "listing":
        stub.list_errors = [TypeError()]
    else:
        stub.get_errors[f"{BILLING.prefix}responses/{FIRST}"] = [KeyError("Body")]
    source = _source(stub, BILLING, sleep=_Passes(), clock=_clock())

    with pytest.raises((TypeError, KeyError)):
        async for _ in source.pairs():
            pass


@pytest.mark.unit
async def test_cancelling_its_task_stops_the_source() -> None:
    slept = asyncio.Event()

    async def sleep(seconds: float) -> None:
        slept.set()
        await asyncio.Event().wait()

    source = _source(_StubS3(_pair_objects(BILLING, FIRST)), BILLING, sleep=sleep, clock=_clock())
    read: list[RawCapture] = []

    async def consume() -> None:
        async for raw in source.pairs():
            read.append(raw)

    task = asyncio.create_task(consume())
    await slept.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert [raw.name for raw in read] == [FIRST]


@pytest.mark.unit
@pytest.mark.parametrize("variable", ["AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3"])
def test_make_s3_client_addresses_by_path_behind_an_endpoint(
    aws: None, monkeypatch: pytest.MonkeyPatch, variable: str
) -> None:
    monkeypatch.setenv(variable, "http://localhost:4566")

    client = make_s3_client()

    assert client.meta.config.s3 == {"addressing_style": "path"}
    assert client.meta.endpoint_url == "http://localhost:4566"


@pytest.mark.unit
def test_make_s3_client_keeps_the_default_addressing_without_an_endpoint(aws: None) -> None:
    config = make_s3_client().meta.config
    assert config.s3 is None
    assert (config.connect_timeout, config.read_timeout) == (5, 30)


@pytest.mark.unit
async def test_no_canary_reaches_logs_or_reprs(
    aws: None, canary: Canary, caplog: pytest.LogCaptureFixture
) -> None:
    bucket = canary.token()
    name = f"{canary.token()}/{canary.token()}_20261011_120000_000001.json"
    request = canary.plant(
        {"method": "POST", "url": "u", "headers": {"authorization": "a"}, "body": {"id": "i"}}
    )
    response = canary.plant({"status_code": 200, "headers": {"etag": "e"}, "body": {"n": 1}})
    metadata = {"action": canary.token(), "trace": canary.token()}
    caplog.set_level(logging.DEBUG)
    clock = _clock()

    with mock_aws():
        client = boto3.client("s3")

        def create_the_bucket() -> None:
            client.create_bucket(Bucket=bucket)
            _put_pair(
                client,
                BILLING,
                name,
                request=request,
                response=response,
                request_metadata=metadata,
                response_metadata=metadata,
                bucket=bucket,
            )
            clock.advance(60)

        passes = _Passes(create_the_bucket, _nothing)
        source = _source(client, BILLING, sleep=passes, clock=clock, bucket=bucket)
        read = await _run(source, passes)

    [(_, raw)] = read
    assert (raw.response, raw.request, raw.metadata_action) == (
        response,
        request,
        metadata["action"],
    )
    assert _warnings(caplog) == [f"{WARNING}ClientError:NoSuchBucket"]
    assert canary.leaks("\n".join([caplog.text, repr(source), repr(raw)])) == []


@pytest.mark.unit
async def test_failure_logs_carry_no_exception_text(
    canary: Canary, caplog: pytest.LogCaptureFixture
) -> None:
    bucket = canary.token()
    name = f"wu-1/{canary.token()}_20261011_120000_000001.json"
    key = f"{BILLING.prefix}responses/{name}"
    stub = _StubS3(_pair_objects(BILLING, name))
    stub.list_errors = [_client_error("AccessDenied", f"denied on {bucket}")]
    stub.get_errors[key] = [
        _client_error("SlowDown", f"slow down on {bucket}/{key}"),
        _client_error(f"Odd {canary.token()}", key),
        EndpointConnectionError(endpoint_url=f"https://{bucket}.s3.amazonaws.com/{key}"),
    ]
    caplog.set_level(logging.DEBUG)
    passes = _Passes(_nothing, _nothing, _nothing, _nothing)
    source = _source(stub, BILLING, sleep=passes, clock=_clock(), bucket=bucket)

    read = await _run(source, passes)

    assert [number for number, _ in read] == [4]
    assert _warnings(caplog) == [
        f"{WARNING}ClientError:AccessDenied",
        f"{WARNING}ClientError:SlowDown",
        f"{WARNING}ClientError",
        f"{WARNING}EndpointConnectionError",
    ]
    assert canary.leaks("\n".join([caplog.text, repr(source), repr(read[0][1])])) == []
