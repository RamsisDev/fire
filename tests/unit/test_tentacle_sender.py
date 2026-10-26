"""Sender: batches per source, retries with backoff, and counts every observation."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.apps.config import TentacleConfig
from fire.apps.tentacle.counters import SourceCounters
from fire.apps.tentacle.sender import Outgoing, Sender, Settler
from fire.core.domain.dossier import Shape, ShapeField
from fire.core.domain.observation import Channel, ShapeObservation, SpanKind, SpanObservation

URL = "http://body/v0/observations"
TOKEN = "sender-test-token"
T0 = datetime(2026, 10, 11, 16, 30, tzinfo=UTC)
SHAPE = Shape(fields=[ShapeField(path="order_id")])
CHANNEL = Channel.model_validate(
    {"kind": "event", "direction": "internal", "name": "order.created", "part": "payload"}
)


def _observation(source: str = "a", observation_id: str = "obs-1") -> ShapeObservation:
    return ShapeObservation(
        system="shop",
        source=source,
        channel=CHANNEL,
        observation_id=observation_id,
        occurred_at=T0,
        observed_at=T0,
        shape=SHAPE,
        fingerprint=Sha256ShapeFingerprinter().fingerprint(SHAPE),
    )


class Body:
    """A scripted body: answers each request with the next response, then 202."""

    def __init__(self, *responses: int | Exception | httpx.Response) -> None:
        self.responses = list(responses)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        answer = self.responses.pop(0) if self.responses else 202
        if isinstance(answer, Exception):
            raise answer
        if isinstance(answer, httpx.Response):
            return answer
        if answer == 202:
            size = len(json.loads(request.content)["shapes"])
            report = {"accepted": size, "duplicate": 0, "rejected": []}
            return httpx.Response(202, json={"shapes": report, "spans": report | {"accepted": 0}})
        return httpx.Response(answer, json={"detail": "x"})

    def batches(self) -> list[list[tuple[str, str]]]:
        return [
            [
                (shape["source"], shape["observation_id"])
                for shape in json.loads(r.content)["shapes"]
            ]
            for r in self.requests
        ]


class Harness:
    def __init__(self, body: Body, jitter: float = 1.0, **config: Any) -> None:
        self.body = body
        self.sleeps: list[float] = []
        self.counters = {source: SourceCounters(source) for source in ("a", "b")}
        self.acked: list[str] = []
        self.config = TentacleConfig.model_validate(config)
        self.client = httpx.AsyncClient(transport=httpx.MockTransport(body))

        async def sleep(seconds: float) -> None:
            self.sleeps.append(seconds)

        self.sender = Sender(
            self.client,
            url=URL,
            token=TOKEN,
            config=self.config,
            counters=self.counters,
            sleep=sleep,
            jitter=lambda: jitter,
        )

    def outgoing(self, source: str = "a", observation_id: str = "obs-1") -> Outgoing:
        async def ack() -> None:
            self.acked.append(observation_id)

        self.counters[source].built += 1
        return Outgoing(_observation(source, observation_id), ack)


@pytest.mark.unit
async def test_accepted_batch_is_sent_and_acked() -> None:
    harness = Harness(Body())
    await harness.sender.send([harness.outgoing(observation_id="o1"), harness.outgoing()])

    [request] = harness.body.requests
    assert request.url == URL
    assert request.headers["X-FIRE-Token"] == TOKEN
    assert request.headers["Content-Type"] == "application/json"
    shapes = json.loads(request.content)["shapes"]
    assert [shape["observation_id"] for shape in shapes] == ["o1", "obs-1"]
    assert ShapeObservation.model_validate(shapes[1]) == _observation()
    counters = harness.counters["a"]
    assert (counters.sent, counters.unsent, counters.pending) == (2, 0, 0)
    assert harness.acked == ["o1", "obs-1"]


@pytest.mark.unit
async def test_a_null_field_keeps_its_fingerprint_on_the_wire() -> None:
    shape = FlatStarShapeGeneralizer().generalize({"adminId": None, "items": []})
    observation = _observation().model_copy(
        update={"shape": shape, "fingerprint": Sha256ShapeFingerprinter().fingerprint(shape)}
    )
    harness = Harness(Body())
    harness.counters["a"].built += 1
    await harness.sender.send([Outgoing(observation, harness.outgoing().ack)])

    [request] = harness.body.requests
    [posted] = json.loads(request.content)["shapes"]
    again = Shape.model_validate(posted["shape"])
    assert Sha256ShapeFingerprinter().fingerprint(again) == observation.fingerprint


@pytest.mark.unit
@pytest.mark.parametrize(
    "failure",
    [503, 500, 502, 408, 429, httpx.ConnectError("refused"), httpx.ReadTimeout("slow")],
    ids=["503", "500", "502", "408", "429", "connect-error", "timeout"],
)
async def test_retried_failure_then_202_is_sent(failure: int | Exception) -> None:
    harness = Harness(Body(failure))
    await harness.sender.send([harness.outgoing()])

    assert len(harness.body.requests) == 2
    assert harness.sleeps == [0.5]
    assert (harness.counters["a"].sent, harness.counters["a"].unsent) == (1, 0)
    assert harness.acked == ["obs-1"]


@pytest.mark.unit
@pytest.mark.parametrize("status", [401, 400, 403, 404, 413, 422])
async def test_refused_batch_is_not_retried_and_counted_unsent(
    status: int, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    harness = Harness(Body(status))
    await harness.sender.send([harness.outgoing(), harness.outgoing(observation_id="o2")])

    assert len(harness.body.requests) == 1
    assert harness.sleeps == []
    counters = harness.counters["a"]
    assert (counters.sent, counters.unsent, counters.pending) == (0, 2, 0)
    assert harness.acked == []
    assert f"batch unsent: source=a size=2 status={status} attempts=1" in caplog.text
    assert TOKEN not in caplog.text


@pytest.mark.unit
async def test_batch_that_runs_out_of_attempts_is_unsent(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)
    harness = Harness(Body(*[httpx.ConnectError("refused")] * 3), retry={"max_attempts": 3})
    await harness.sender.send([harness.outgoing()])

    assert len(harness.body.requests) == 3
    assert harness.sleeps == [0.5, 1.0]
    assert (harness.counters["a"].sent, harness.counters["a"].unsent) == (0, 1)
    assert "batch unsent: source=a size=1 status=ConnectError attempts=3" in caplog.text


@pytest.mark.unit
@pytest.mark.parametrize(
    ("jitter", "expected"),
    [
        (1.0, [0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]),
        (0.5, [0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 15.0, 15.0]),
        (0.0, [0.0] * 8),
    ],
)
async def test_backoff_doubles_up_to_30_s_with_full_jitter(
    jitter: float, expected: list[float]
) -> None:
    harness = Harness(Body(*[503] * 9), jitter=jitter, retry={"max_attempts": 9})
    await harness.sender.send([harness.outgoing()])
    assert harness.sleeps == expected
    assert harness.counters["a"].unsent == 1


@pytest.mark.unit
async def test_body_counts_are_kept_per_source() -> None:
    report = {
        "accepted": 1,
        "duplicate": 2,
        "rejected": [
            {"index": 0, "reason": "fingerprint_mismatch"},
            {"index": 3, "reason": "invalid", "fields": ["shape"]},
            {"index": 4, "reason": "invalid", "fields": []},
        ],
    }
    empty = {"accepted": 0, "duplicate": 0, "rejected": []}
    answer = httpx.Response(202, json={"shapes": report, "spans": empty})
    harness = Harness(Body(answer))
    await harness.sender.send([harness.outgoing("b", str(index)) for index in range(6)])

    counters = harness.counters["b"]
    assert counters.sent == 6
    assert counters.body_duplicate == 2
    assert counters.body_rejected == {"fingerprint_mismatch": 1, "invalid": 2}


SPAN = SpanObservation(
    system="shop",
    source="a",
    trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
    span_id="00f067aa0ba902b7",
    service="orders-api",
    kind=SpanKind.SERVER,
    operation="POST /v1/orders",
    status_code=None,
    error=False,
    start=T0,
    duration_ms=1.5,
)


@pytest.mark.unit
async def test_spans_and_shapes_of_a_batch_are_posted_apart() -> None:
    harness = Harness(Body())
    harness.counters["a"].built += 1
    span = Outgoing(SPAN, harness.outgoing().ack)
    await harness.sender.send([harness.outgoing(observation_id="o1"), span])

    [request] = harness.body.requests
    posted = json.loads(request.content)
    assert [shape["observation_id"] for shape in posted["shapes"]] == ["o1"]
    assert [SpanObservation.model_validate(item) for item in posted["spans"]] == [SPAN]
    assert posted["spans"][0]["parent_span_id"] is None
    assert harness.counters["a"].sent == 2


@pytest.mark.unit
async def test_body_counts_include_spans() -> None:
    shapes = {"accepted": 1, "duplicate": 0, "rejected": []}
    spans = {
        "accepted": 0,
        "duplicate": 1,
        "rejected": [{"index": 1, "reason": "unknown_source"}],
    }
    answer = httpx.Response(202, json={"shapes": shapes, "spans": spans})
    harness = Harness(Body(answer))
    harness.counters["a"].built += 2
    await harness.sender.send(
        [
            harness.outgoing(),
            Outgoing(SPAN, harness.outgoing().ack),
            Outgoing(SPAN, harness.outgoing().ack),
        ]
    )

    counters = harness.counters["a"]
    assert (counters.body_duplicate, counters.body_rejected) == (1, {"unknown_source": 1})


@pytest.mark.unit
async def test_unreadable_report_still_counts_the_batch_sent(
    caplog: pytest.LogCaptureFixture,
) -> None:
    harness = Harness(Body(httpx.Response(202, text="accepted")))
    await harness.sender.send([harness.outgoing()])
    assert harness.counters["a"].sent == 1
    assert harness.acked == ["obs-1"]
    assert "body report unreadable: source=a size=1" in caplog.text


@pytest.mark.unit
async def test_failing_ack_is_logged_and_does_not_stop_the_batch(
    caplog: pytest.LogCaptureFixture,
) -> None:
    harness = Harness(Body())

    async def broken_ack() -> None:
        raise RuntimeError("planted-ack-detail")

    harness.counters["a"].built += 1
    await harness.sender.send([Outgoing(_observation(), broken_ack), harness.outgoing("a", "o2")])

    assert harness.counters["a"].sent == 2
    assert harness.acked == ["o2"]
    assert "ack failed: source=a error=RuntimeError" in caplog.text
    assert "planted-ack-detail" not in caplog.text


@pytest.mark.unit
async def test_a_failing_ack_is_logged_once_per_type_until_an_ack_succeeds(
    caplog: pytest.LogCaptureFixture,
) -> None:
    settle = Settler()

    async def broken() -> None:
        raise RuntimeError("planted-ack-detail")

    async def refused() -> None:
        raise ConnectionError

    async def fine() -> None:
        return None

    for ack in (broken, broken, refused, broken, fine, broken):
        await settle(ack, "a")
    await settle(broken, "b")

    assert [record.getMessage() for record in caplog.records] == [
        "ack failed: source=a error=RuntimeError",
        "ack failed: source=a error=ConnectionError",
        "ack failed: source=a error=RuntimeError",
        "ack failed: source=b error=RuntimeError",
    ]


@pytest.mark.unit
async def test_an_unsent_batch_tells_each_observation() -> None:
    harness = Harness(Body(httpx.Response(401)))
    told: list[str] = []
    harness.counters["a"].built += 2
    batch = [
        Outgoing(
            _observation("a", f"o{index}"),
            harness.outgoing().ack,
            lambda i=index: told.append(f"o{i}"),
        )
        for index in (1, 2)
    ]

    await harness.sender.send(batch)

    assert told == ["o1", "o2"]
    assert harness.acked == []


async def _run(harness: Harness, feed: Callable[[asyncio.Queue[Outgoing | None]], Any]) -> None:
    queue: asyncio.Queue[Outgoing | None] = asyncio.Queue()
    running = asyncio.create_task(harness.sender.run(queue))
    await feed(queue)
    await asyncio.wait_for(running, 5)


@pytest.mark.unit
async def test_run_batches_per_source_by_size_and_flushes_the_rest_at_the_end() -> None:
    harness = Harness(Body(), batch_size=2, flush_ms=60_000)

    async def feed(queue: asyncio.Queue[Outgoing | None]) -> None:
        for source, observation_id in [("a", "1"), ("b", "2"), ("a", "3"), ("a", "4"), ("b", "5")]:
            queue.put_nowait(harness.outgoing(source, observation_id))
        queue.put_nowait(harness.outgoing("a", "6"))
        queue.put_nowait(None)

    await _run(harness, feed)

    assert harness.body.batches() == [
        [("a", "1"), ("a", "3")],
        [("b", "2"), ("b", "5")],
        [("a", "4"), ("a", "6")],
    ]
    assert harness.counters["a"].sent == 4
    assert harness.counters["b"].sent == 2


@pytest.mark.unit
async def test_run_sends_a_partial_batch_after_flush_ms() -> None:
    harness = Harness(Body(), batch_size=100, flush_ms=20)

    async def feed(queue: asyncio.Queue[Outgoing | None]) -> None:
        queue.put_nowait(harness.outgoing("a", "1"))
        queue.put_nowait(harness.outgoing("b", "2"))
        await asyncio.sleep(0.3)
        assert harness.body.batches() == [[("a", "1")], [("b", "2")]]
        queue.put_nowait(harness.outgoing("a", "3"))
        queue.put_nowait(None)

    await _run(harness, feed)

    assert harness.body.batches()[-1] == [("a", "3")]
