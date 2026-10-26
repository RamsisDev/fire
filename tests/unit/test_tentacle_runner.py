"""The tentacle process: startup checks, exit codes, backpressure, shutdown, and counters."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import signal
import socket
import subprocess
import sys
from collections import Counter
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from botocore.exceptions import ProfileNotFound

import fire.apps.tentacle.sources as sources_module
from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.adapters.sources.dynamodb_stream import DynamoDbStreamRecordSource, StreamFilter
from fire.adapters.sources.s3_capture import S3CaptureSource
from fire.adapters.sources.sns_fanout import SnsFanoutRecordSource
from fire.apps.config import TentacleConfig
from fire.apps.tentacle.__main__ import main
from fire.apps.tentacle.runner import (
    EXIT_OK,
    EXIT_STARTUP_ERROR,
    EXIT_UNSENT,
    Plan,
    PlannedCapture,
    PlannedProxy,
    PlannedReceiver,
    PlannedSource,
    _serve,
    prepare,
    run,
    run_tentacle,
)
from fire.core.domain.capture import (
    CaptureEvent,
    CaptureIndex,
    CaptureLayout,
    CaptureRule,
    SpanRef,
)
from fire.core.domain.envelope import Envelope
from fire.core.domain.observation import ChannelDirection, SpanKind
from fire.core.domain.operation import OperationInputs, OperationNamer
from fire.core.ports.capture_source import ObjectState, RawCapture
from fire.core.ports.exchange_source import (
    ExchangeNote,
    ExchangeSink,
    ExchangeSkipReason,
    HttpExchange,
    HttpMessage,
)
from fire.core.ports.record_source import (
    NotingRecordSource,
    RawRecord,
    RecordSkipReason,
    StartPosition,
)
from fire.core.ports.span_source import (
    RawLink,
    RawSpan,
    SpanBatchResult,
    SpanRequestNote,
    SpanSink,
)
from fire.core.services.capture_pipeline import CapturePipeline
from fire.core.services.exchange_pipeline import ExchangePipeline
from fire.core.services.record_pipeline import RecordPipeline
from fire.core.services.span_pipeline import SpanPipeline

TOKEN = "runner-test-token"
ENVELOPE = {"event_id": "id", "event_type": "type", "occurred_at": "at", "payload": "data"}


def _record(observation_id: str, event_type: str = "order.created") -> dict[str, Any]:
    return {
        "id": observation_id,
        "type": event_type,
        "at": "2026-10-11T16:30:00Z",
        "data": {"order_id": f"order-{observation_id}"},
    }


def _replay_source(source_id: str = "samples", path_env: str = "REPLAY") -> dict[str, Any]:
    return {
        "id": source_id,
        "kind": "replay",
        "direction": "internal",
        "path_env": path_env,
        "envelope": ENVELOPE,
    }


KINESIS_SOURCE = {
    "id": "events",
    "kind": "kinesis",
    "direction": "internal",
    "stream_env": "STREAM",
    "regions": ["us-east-1"],
    "envelope": ENVELOPE,
}


def _configure(
    tmp_path: Path,
    sources: list[dict[str, Any]] | None = None,
    tentacle: dict[str, Any] | None = None,
) -> Path:
    config = tmp_path / "config"
    (config / "systems").mkdir(parents=True)
    fire_yml = config / "fire.yml"
    fire_yml.write_text(yaml.safe_dump({"tentacle": tentacle or {}}), encoding="utf-8")
    system = {
        "system": "shop",
        "display_name": "Shop",
        "notify": {"slack": {"channel_id": "C0123456789"}},
        "sources": [_replay_source(), KINESIS_SOURCE] if sources is None else sources,
    }
    (config / "systems" / "shop.yml").write_text(yaml.safe_dump(system), encoding="utf-8")
    return fire_yml


def _replay_file(tmp_path: Path, records: list[Any]) -> Path:
    path = tmp_path / "records.json"
    path.write_text(json.dumps(records), encoding="utf-8")
    return path


def _environ(replay: Path | None = None, **extra: str) -> dict[str, str]:
    environ = {"FIRE_API_TOKEN": TOKEN, "FIRE_BODY_URL": "http://body"}
    if replay is not None:
        environ["REPLAY"] = str(replay)
    environ.update(extra)
    return environ


class Body:
    """Answers 202 with an all-accepted report, or a fixed status; can hold requests."""

    def __init__(self, status: int = 202) -> None:
        self.status = status
        self.shapes: list[dict[str, Any]] = []
        self.spans: list[dict[str, Any]] = []
        self.release = asyncio.Event()
        self.release.set()

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        await self.release.wait()
        posted = json.loads(request.content)
        if self.status != 202:
            return httpx.Response(self.status, json={"detail": "x"})
        self.shapes.extend(posted["shapes"])
        self.spans.extend(posted["spans"])
        report = {
            kind: {"accepted": len(posted[kind]), "duplicate": 0, "rejected": []}
            for kind in ("shapes", "spans")
        }
        return httpx.Response(202, json=report)

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self)


def _run(fire_yml: Path, environ: Mapping[str, str], body: Body | None = None, **kw: Any) -> int:
    transport = (body or Body()).transport()
    return run_tentacle("shop", config_path=fire_yml, environ=environ, transport=transport, **kw)


STARTUP_ERRORS = {
    "unknown-system": ({"system": "billing"}, None, "no config for system 'billing'"),
    "not-a-system-id": ({"system": "../shop"}, None, "is not a system id"),
    "unknown-source": ({"source": "orders"}, None, "declares no source 'orders'"),
    "token-unset": ({}, {"FIRE_API_TOKEN": ""}, "FIRE_API_TOKEN is not set"),
    "body-url-unset": ({}, {"FIRE_BODY_URL": ""}, "FIRE_BODY_URL is not set"),
    "body-url-ftp": ({}, {"FIRE_BODY_URL": "ftp://body"}, "not an http or https URL"),
    "body-url-no-scheme": ({}, {"FIRE_BODY_URL": "body:8000"}, "not an http or https URL"),
    "named-source-env-unset": (
        {"source": "samples"},
        {"REPLAY": ""},
        "source samples: REPLAY is not set",
    ),
    "kinesis-stream-not-a-name": (
        {"source": "events"},
        {"STREAM": "orders stream/x"},
        "source events: STREAM is not a Kinesis stream name",
    ),
    "no-source-to-run": ({}, {"REPLAY": ""}, "system 'shop' has no source to run"),
    "replay-path-missing": ({}, {"REPLAY": "/nonexistent/records.json"}, "REPLAY names no"),
}


@pytest.mark.unit
@pytest.mark.parametrize(
    ("kwargs", "environ", "message"), list(STARTUP_ERRORS.values()), ids=list(STARTUP_ERRORS)
)
def test_startup_errors_exit_2(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    kwargs: dict[str, Any],
    environ: dict[str, str] | None,
    message: str,
) -> None:
    fire_yml = _configure(tmp_path)
    system = kwargs.pop("system", "shop")
    env = _environ(_replay_file(tmp_path, [_record("1")]), **(environ or {}))
    body = Body()

    code = run_tentacle(
        system, config_path=fire_yml, environ=env, transport=body.transport(), **kwargs
    )

    assert code == EXIT_STARTUP_ERROR
    [error] = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("tentacle not started: ")
    ]
    assert message in error
    assert body.shapes == []
    assert TOKEN not in caplog.text


@pytest.mark.unit
def test_replay_path_with_another_suffix_is_a_startup_error(tmp_path: Path) -> None:
    other = tmp_path / "records.txt"
    other.write_text("{}", encoding="utf-8")
    assert _run(_configure(tmp_path), _environ(other)) == EXIT_STARTUP_ERROR


@pytest.mark.unit
@pytest.mark.parametrize(
    ("fire_yml", "system_yml"),
    [
        ({"tentacle": {"batch_size": 0}}, None),
        (None, "sources: [{id: x, kind: kinesis, stream_env: S, regions: [r], dedupe_key: id}]"),
        ("tentacle: [unclosed", None),
    ],
    ids=["bad-fire-yml", "dedupe-key", "bad-yaml"],
)
def test_config_errors_exit_2(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    fire_yml: dict[str, Any] | str | None,
    system_yml: str | None,
) -> None:
    path = _configure(tmp_path)
    if fire_yml is not None:
        text = fire_yml if isinstance(fire_yml, str) else yaml.safe_dump(fire_yml)
        path.write_text(text, encoding="utf-8")
    if system_yml is not None:
        base = "system: shop\ndisplay_name: Shop\nnotify: {slack: {channel_id: C0123456789}}\n"
        (path.parent / "systems" / "shop.yml").write_text(base + system_yml, encoding="utf-8")

    assert _run(path, _environ(_replay_file(tmp_path, []))) == EXIT_STARTUP_ERROR
    assert "tentacle not started:" in caplog.text


@pytest.mark.unit
def test_sources_that_cannot_run_are_skipped_and_said(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    sources = [_replay_source(), _replay_source("unset", "UNSET"), KINESIS_SOURCE]
    fire_yml = _configure(tmp_path, sources)
    environ = _environ(_replay_file(tmp_path, [_record("1")]))

    assert _run(fire_yml, environ) == EXIT_OK
    assert "source skipped: source=unset reason=UNSET is not set" in caplog.text
    assert "source skipped: source=events reason=STREAM is not set" in caplog.text
    assert "tentacle started: system=shop sources=samples" in caplog.text


@pytest.mark.unit
def test_named_source_runs_alone(tmp_path: Path) -> None:
    sources = [_replay_source(), _replay_source("other", "OTHER")]
    fire_yml = _configure(tmp_path, sources)
    other = tmp_path / "other.json"
    other.write_text(json.dumps([_record("9")]), encoding="utf-8")
    body = Body()
    environ = _environ(_replay_file(tmp_path, [_record("1")]), OTHER=str(other))

    assert _run(fire_yml, environ, body, source="other") == EXIT_OK
    assert [(shape["source"], shape["observation_id"]) for shape in body.shapes] == [
        ("other", "9")
    ]


@pytest.mark.unit
def test_every_record_settled_exits_0_and_logs_its_counters(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    records = [
        _record("1"),
        _record("2"),
        {"type": "x"},
        "text",
        _record("1"),
        _record("3", "a b"),
    ]
    body = Body()

    assert _run(_configure(tmp_path), _environ(_replay_file(tmp_path, records)), body) == EXIT_OK

    assert sorted(shape["observation_id"] for shape in body.shapes) == ["1", "2"]
    assert (
        "tentacle counters: source=samples read=6 built=2 sent=2 unsent=0 pending=0 "
        "rejected=bad_name:1,missing_event_id:1,undecodable:1 skipped=local_duplicate:1 "
        "noted=- body_duplicate=0 body_rejected=-"
    ) in caplog.text


@pytest.mark.unit
def test_refused_batch_exits_1(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    environ = _environ(_replay_file(tmp_path, [_record("1"), _record("2")]))

    assert _run(_configure(tmp_path), environ, Body(status=401)) == EXIT_UNSENT
    assert "batch unsent: source=samples size=2 status=401 attempts=1" in caplog.text
    assert "source=samples read=2 built=2 sent=0 unsent=2 pending=0" in caplog.text


@pytest.mark.unit
def test_drain_past_shutdown_s_counts_pending_as_unsent(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    body = Body()
    body.release.clear()
    fire_yml = _configure(tmp_path, tentacle={"shutdown_s": 0.2})
    environ = _environ(_replay_file(tmp_path, [_record("1"), _record("2")]))

    assert _run(fire_yml, environ, body) == EXIT_UNSENT
    assert "shutdown drain timed out after 0.2s" in caplog.text
    assert "unsent at shutdown: source=samples count=2" in caplog.text
    assert "source=samples read=2 built=2 sent=0 unsent=2 pending=0" in caplog.text


class ScriptedSource:
    """Yields records, counting what it handed out; optionally fails or waits forever."""

    def __init__(
        self, records: list[Any], *, fail: bool = False, live: bool = False, acks: list[str]
    ) -> None:
        self.records_to_yield = records
        self.fail = fail
        self.live = live
        self.acks = acks
        self.yielded = 0

    async def records(self) -> AsyncIterator[RawRecord]:
        for record in self.records_to_yield:
            self.yielded += 1
            yield RawRecord(source="scripted", region=None, record=record, ack=self._ack(record))
        if self.fail:
            raise RuntimeError("planted-failure-detail")
        if self.live:
            await asyncio.Event().wait()

    def _ack(self, record: Any) -> Any:
        async def ack() -> None:
            self.acks.append(record["id"] if isinstance(record, dict) and "id" in record else "-")

        return ack


def _plan(source: ScriptedSource, **tentacle: Any) -> Plan:
    pipeline = RecordPipeline(
        system="shop",
        source="scripted",
        direction=ChannelDirection.INTERNAL,
        envelope=Envelope.model_validate(ENVELOPE),
        sample_rate=1.0,
        generalizer=FlatStarShapeGeneralizer(),
        fingerprinter=Sha256ShapeFingerprinter(),
    )
    return Plan(
        system="shop",
        sources=[PlannedSource(id="scripted", records=source, pipeline=pipeline)],
        tentacle=TentacleConfig.model_validate(tentacle),
        url="http://body/v0/observations",
        token=TOKEN,
    )


@pytest.mark.unit
async def test_full_buffer_pauses_reading_instead_of_dropping() -> None:
    source = ScriptedSource([_record(str(index)) for index in range(20)], acks=[])
    body = Body()
    body.release.clear()
    running = asyncio.create_task(
        run(
            _plan(source, buffer_size=2, batch_size=1, flush_ms=1),
            asyncio.Event(),
            transport=body.transport(),
        )
    )

    await asyncio.sleep(0.2)
    # One observation in flight, two in the buffer, and one waiting to enter it.
    assert source.yielded == 4
    body.release.set()

    assert await asyncio.wait_for(running, 5) == EXIT_OK
    assert source.yielded == 20
    assert len(body.shapes) == 20


@pytest.mark.unit
async def test_records_are_acked_after_202_and_settled_ones_at_once() -> None:
    acks: list[str] = []
    source = ScriptedSource([_record("1"), {"id": "2"}, _record("3")], acks=acks)
    body = Body()
    body.release.clear()
    running = asyncio.create_task(
        run(_plan(source, flush_ms=1), asyncio.Event(), transport=body.transport())
    )

    await asyncio.sleep(0.1)
    assert acks == ["2"]
    body.release.set()

    assert await asyncio.wait_for(running, 5) == EXIT_OK
    assert sorted(acks) == ["1", "2", "3"]


@pytest.mark.unit
async def test_refused_records_are_not_acked() -> None:
    acks: list[str] = []
    source = ScriptedSource([_record("1"), {"id": "2"}], acks=acks)
    plan = _plan(source)
    assert await run(plan, asyncio.Event(), transport=Body(401).transport()) == EXIT_UNSENT
    assert acks == ["2"]


@pytest.mark.unit
async def test_failing_source_exits_1_by_exception_type_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    body = Body()
    source = ScriptedSource([_record("1")], fail=True, acks=[])

    assert await run(_plan(source), asyncio.Event(), transport=body.transport()) == EXIT_UNSENT
    assert "source failed: source=scripted error=RuntimeError" in caplog.text
    assert "planted-failure-detail" not in caplog.text
    assert [shape["observation_id"] for shape in body.shapes] == ["1"]


@pytest.mark.unit
async def test_stop_ends_a_live_source_and_drains() -> None:
    body = Body()
    stop = asyncio.Event()
    source = ScriptedSource([_record("1"), _record("2")], live=True, acks=[])
    running = asyncio.create_task(run(_plan(source, flush_ms=1), stop, transport=body.transport()))

    await asyncio.sleep(0.1)
    assert not running.done()
    stop.set()

    assert await asyncio.wait_for(running, 5) == EXIT_OK
    assert len(body.shapes) == 2


@pytest.mark.unit
async def test_sigterm_stops_the_tentacle() -> None:
    body = Body()
    source = ScriptedSource([_record("1")], live=True, acks=[])
    loop = asyncio.get_running_loop()
    loop.call_later(0.2, os.kill, os.getpid(), signal.SIGTERM)

    assert await asyncio.wait_for(_serve(_plan(source), body.transport()), 5) == EXIT_OK
    assert len(body.shapes) == 1


# Run in a child process: a signal nothing handles would end pytest itself.
REPEATED_SIGNAL = """
import asyncio, os, signal, sys
from pathlib import Path
from fire.apps.tentacle.runner import _serve, prepare

config, port, signum = Path(sys.argv[1]), sys.argv[2], int(sys.argv[3])
body = "http://127.0.0.1:9"
environ = {"FIRE_API_TOKEN": "t", "FIRE_BODY_URL": body, "LISTEN": f"127.0.0.1:{port}"}
plan = prepare("shop", config_path=config, environ=environ)

async def scenario():
    asyncio.get_running_loop().call_later(0.3, os.kill, os.getpid(), signum)
    code = await _serve(plan, None)
    os.kill(os.getpid(), signum)
    await asyncio.sleep(0.2)
    return code

sys.exit(asyncio.run(scenario()))
"""


@pytest.mark.unit
@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM], ids=["sigint", "sigterm"])
def test_a_signal_repeated_while_the_loop_shuts_down_changes_nothing(
    tmp_path: Path, signum: signal.Signals
) -> None:
    config = tmp_path / "fire.yml"
    config.write_text(yaml.safe_dump({"tentacle": {"flush_ms": 50}}), encoding="utf-8")
    (tmp_path / "systems").mkdir()
    system = {
        "system": "shop",
        "display_name": "Shop",
        "notify": {"slack": {"channel_id": "C0000000000"}},
        "sources": [_otlp_source()],
    }
    (tmp_path / "systems" / "shop.yml").write_text(yaml.safe_dump(system), encoding="utf-8")

    done = subprocess.run(
        [sys.executable, "-c", REPEATED_SIGNAL, str(config), str(_free_port()), str(int(signum))],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert done.returncode == EXIT_OK, done.stderr
    assert "Traceback" not in done.stderr


@pytest.mark.unit
async def test_counters_are_logged_periodically(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    stop = asyncio.Event()
    source = ScriptedSource([_record("1")], live=True, acks=[])
    running = asyncio.create_task(
        run(_plan(source), stop, transport=Body().transport(), counters_interval_s=0.05)
    )
    await asyncio.sleep(0.3)
    stop.set()
    await asyncio.wait_for(running, 5)

    assert caplog.text.count("tentacle counters: source=scripted") >= 3


class ScriptedProxy:
    """Reports scripted calls to its sink, then serves until cancelled or fails."""

    def __init__(self, calls: list[Callable[[ExchangeSink], None]], *, fail: bool = False):
        self.calls = calls
        self.fail = fail
        self.cancelled = False

    async def serve(self, sink: ExchangeSink) -> None:
        for call in self.calls:
            call(sink)
        if self.fail:
            raise RuntimeError("planted-proxy-detail")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise


def _message(body: bytes, content_type: str | None = "application/json") -> HttpMessage:
    return HttpMessage(
        received_at=datetime(2026, 10, 11, 16, 30, tzinfo=UTC),
        content_type=content_type,
        content_encoding=None,
        body=body,
    )


def _exchange(
    method: str, request: bytes, status: int, response: bytes, exchange_id: str
) -> HttpExchange:
    return HttpExchange(
        exchange_id=exchange_id,
        method=method,
        template="/v1/orders/{order_id}",
        status_code=status,
        request=_message(request),
        response=_message(response),
    )


def _proxy_plan(proxy: ScriptedProxy, **tentacle: Any) -> Plan:
    pipeline = ExchangePipeline(
        system="shop",
        source="api",
        sample_rate=1.0,
        max_body_bytes=1024,
        generalizer=FlatStarShapeGeneralizer(),
        fingerprinter=Sha256ShapeFingerprinter(),
    )
    return Plan(
        system="shop",
        sources=[PlannedProxy(id="api", proxy=proxy, pipeline=pipeline)],
        tentacle=TentacleConfig.model_validate(tentacle),
        url="http://body/v0/observations",
        token=TOKEN,
    )


async def _serve_briefly(plan: Plan, body: Body) -> int:
    stop = asyncio.Event()
    running = asyncio.create_task(run(plan, stop, transport=body.transport()))
    await asyncio.sleep(0.1)
    stop.set()
    return await asyncio.wait_for(running, 5)


@pytest.mark.unit
async def test_proxy_exchanges_are_shaped_sent_and_counted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    shaped = _exchange("PUT", b'{"qty": 1}', 201, b'{"id": "x"}', "a" * 32)
    empty = _exchange("GET", b"", 204, b"", "b" * 32)
    proxy = ScriptedProxy(
        [
            lambda sink: sink.exchange(shaped),
            lambda sink: sink.skip(ExchangeSkipReason.UNMATCHED_ROUTE),
            lambda sink: sink.note(ExchangeNote.INVALID_TRACEPARENT),
            lambda sink: sink.exchange(empty),
        ]
    )
    body = Body()

    assert await _serve_briefly(_proxy_plan(proxy, flush_ms=1), body) == EXIT_OK

    assert proxy.cancelled
    assert sorted((shape["observation_id"], shape["source"]) for shape in body.shapes) == [
        ("a" * 32 + ":request", "api"),
        ("a" * 32 + ":response", "api"),
    ]
    assert (
        "source=api read=3 built=2 sent=2 unsent=0 pending=0 rejected=- "
        "skipped=no_body:2,unmatched_route:1 noted=invalid_traceparent:1 "
        "body_duplicate=0 body_rejected=-"
    ) in caplog.text


@pytest.mark.unit
async def test_proxy_observations_that_find_the_buffer_full_are_skipped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    shaped = _exchange("PUT", b'{"qty": 1}', 201, b'{"id": "x"}', "c" * 32)
    proxy = ScriptedProxy([lambda sink: sink.exchange(shaped)])

    assert await _serve_briefly(_proxy_plan(proxy, buffer_size=1, flush_ms=1), Body()) == EXIT_OK
    assert "read=1 built=1 sent=1 unsent=0 pending=0 rejected=- skipped=buffer_full:1" in (
        caplog.text
    )


@pytest.mark.unit
async def test_failing_proxy_exits_1_by_exception_type_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    proxy = ScriptedProxy([], fail=True)

    assert await run(_proxy_plan(proxy), asyncio.Event(), transport=Body().transport()) == (
        EXIT_UNSENT
    )
    assert "source failed: source=api error=RuntimeError" in caplog.text
    assert "planted-proxy-detail" not in caplog.text


def _proxy_source(port: int, upstream_env: str = "UPSTREAM") -> dict[str, Any]:
    return {"id": "api", "kind": "http_proxy", "listen_port": port, "upstream_env": upstream_env}


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.mark.unit
@pytest.mark.parametrize("upstream", ["localhost:8020", "ftp://upstream", "http://"])
def test_proxy_upstream_must_be_an_http_url(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, upstream: str
) -> None:
    fire_yml = _configure(tmp_path, [_proxy_source(port=8021)])

    assert _run(fire_yml, _environ(UPSTREAM=upstream)) == EXIT_STARTUP_ERROR
    assert "source api: UPSTREAM is not an http or https URL" in caplog.text
    assert upstream not in caplog.text


@pytest.mark.unit
def test_proxy_on_a_busy_port_is_a_startup_error(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with socket.socket() as busy:
        busy.bind(("127.0.0.1", 0))
        busy.listen()
        port = busy.getsockname()[1]
        fire_yml = _configure(tmp_path, [_proxy_source(port=port)])

        assert _run(fire_yml, _environ(UPSTREAM="http://localhost:1")) == EXIT_STARTUP_ERROR
    assert f"source api: cannot listen on 127.0.0.1:{port}" in caplog.text


@pytest.mark.unit
def test_startup_error_after_a_proxy_releases_its_port(tmp_path: Path) -> None:
    port = _free_port()
    fire_yml = _configure(tmp_path, [_proxy_source(port), _replay_source()])
    environ = _environ(UPSTREAM="http://localhost:8020", REPLAY="/nonexistent/records.json")

    assert _run(fire_yml, environ) == EXIT_STARTUP_ERROR
    with socket.socket() as again:
        again.bind(("127.0.0.1", port))


@pytest.mark.unit
def test_prepare_opens_a_proxy_with_its_pipeline(tmp_path: Path) -> None:
    fire_yml = _configure(tmp_path, [_replay_source(), _proxy_source(_free_port())])
    environ = _environ(_replay_file(tmp_path, []), UPSTREAM="http://localhost:8020")

    plan = prepare("shop", config_path=fire_yml, environ=environ)
    try:
        assert [type(planned).__name__ for planned in plan.sources] == [
            "PlannedSource",
            "PlannedProxy",
        ]
    finally:
        for planned in plan.sources:
            if isinstance(planned, PlannedProxy):
                planned.proxy.close()


TRACE = "4bf92f3577b34da6a3ce929d0e0e4736"
START_NS = 1_791_768_368_257_502_981


def _raw_span(span_id: str, **overrides: Any) -> RawSpan:
    span = RawSpan(
        trace_id=TRACE,
        span_id=span_id,
        parent_span_id=None,
        service="orders-api",
        kind=SpanKind.CLIENT,
        start_ns=START_NS,
        end_ns=START_NS + 1_000_000,
        error=False,
        naming=OperationInputs(SpanKind.CLIENT, "POST", method="POST", url="https://x.net/v1"),
        status_code=200,
    )
    return replace(span, **overrides)


class ScriptedReceiver:
    """Reports scripted batches and notes to its sink, then serves until cancelled or fails."""

    def __init__(
        self,
        batches: list[list[RawSpan]],
        notes: tuple[SpanRequestNote, ...] = (),
        *,
        fail: bool = False,
    ) -> None:
        self.batches = batches
        self.notes = notes
        self.fail = fail
        self.results: list[SpanBatchResult] = []
        self.closed = False

    async def serve(self, sink: SpanSink) -> None:
        self.results = [sink.spans(batch) for batch in self.batches]
        for note in self.notes:
            sink.note(note)
        if self.fail:
            raise RuntimeError("planted-receiver-detail")
        await asyncio.Event().wait()

    def close(self) -> None:
        self.closed = True


def _planned_receiver(
    receiver: ScriptedReceiver, capture_event: CaptureEvent | None = None
) -> PlannedReceiver:
    pipeline = SpanPipeline(
        system="shop",
        source="traces",
        sample_rate=1.0,
        namer=OperationNamer(),
        generalizer=FlatStarShapeGeneralizer(),
        fingerprinter=Sha256ShapeFingerprinter(),
    )
    return PlannedReceiver(
        id="traces", receiver=receiver, pipeline=pipeline, capture_event=capture_event
    )


def _receiver_plan(
    receiver: ScriptedReceiver, capture_event: CaptureEvent | None = None, **tentacle: Any
) -> Plan:
    return Plan(
        system="shop",
        sources=[_planned_receiver(receiver, capture_event)],
        tentacle=TentacleConfig.model_validate(tentacle),
        url="http://body/v0/observations",
        token=TOKEN,
    )


@pytest.mark.unit
async def test_received_spans_are_mapped_sent_and_counted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    batches = [
        [_raw_span("a" * 16, request_body='{"qty": 1}'), _raw_span("b" * 16, trace_id=None)],
        [_raw_span("c" * 16, request_body="", links=(RawLink(TRACE, "x"),))],
    ]
    receiver = ScriptedReceiver(batches, (SpanRequestNote.MALFORMED_REQUEST,))
    body = Body()

    assert await _serve_briefly(_receiver_plan(receiver, flush_ms=1), body) == EXIT_OK

    assert receiver.results == [SpanBatchResult(True, 1), SpanBatchResult(True, 0)]
    assert sorted(span["span_id"] for span in body.spans) == ["a" * 16, "c" * 16]
    assert [span["operation"] for span in body.spans] == ["POST x.net/v1"] * 2
    assert [shape["observation_id"] for shape in body.shapes] == [f"{TRACE}:{'a' * 16}:request"]
    assert (
        "source=traces read=3 built=3 sent=3 unsent=0 pending=0 rejected=invalid_ids:1 "
        "skipped=body_empty:1 noted=invalid_link:1,malformed_request:1 "
        "body_duplicate=0 body_rejected=-"
    ) in caplog.text


@pytest.mark.unit
async def test_a_batch_that_does_not_fit_the_buffer_is_not_taken(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    too_many = [_raw_span(f"{index + 1:016x}") for index in range(3)]
    receiver = ScriptedReceiver([too_many, too_many[:2]])
    body = Body()

    assert await _serve_briefly(_receiver_plan(receiver, buffer_size=2, flush_ms=1), body) == (
        EXIT_OK
    )
    assert receiver.results == [SpanBatchResult(False), SpanBatchResult(True)]
    assert len(body.spans) == 2
    assert "source=traces read=2 built=2 sent=2 unsent=0 pending=0 rejected=- skipped=- " in (
        caplog.text
    )
    assert "noted=buffer_full:1" in caplog.text


@pytest.mark.unit
async def test_failing_receiver_exits_1_by_exception_type_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    receiver = ScriptedReceiver([], fail=True)
    plan = _receiver_plan(receiver)

    assert await run(plan, asyncio.Event(), transport=Body().transport()) == EXIT_UNSENT
    assert "source failed: source=traces error=RuntimeError" in caplog.text
    assert "planted-receiver-detail" not in caplog.text


def _otlp_source(listen_env: str = "LISTEN") -> dict[str, Any]:
    return {"id": "traces", "kind": "otlp", "listen_env": listen_env}


@pytest.mark.unit
def test_an_otlp_source_without_its_address_is_skipped(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    fire_yml = _configure(tmp_path, [_replay_source(), _otlp_source()])
    environ = _environ(_replay_file(tmp_path, [_record("1")]))

    assert _run(fire_yml, environ) == EXIT_OK
    assert "source skipped: source=traces reason=LISTEN is not set" in caplog.text


@pytest.mark.unit
@pytest.mark.parametrize(
    "address",
    ["4318", "127.0.0.1", "127.0.0.1:", ":4318", "127.0.0.1:0", "127.0.0.1:70000", "h:43a"],
)
def test_an_otlp_listen_address_must_be_host_and_port(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, address: str
) -> None:
    fire_yml = _configure(tmp_path, [_otlp_source()])

    assert _run(fire_yml, _environ(LISTEN=address)) == EXIT_STARTUP_ERROR
    assert "source traces: LISTEN is not a host:port address" in caplog.text


@pytest.mark.unit
def test_an_otlp_receiver_on_a_busy_port_is_a_startup_error(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with socket.socket() as busy:
        busy.bind(("127.0.0.1", 0))
        busy.listen()
        port = busy.getsockname()[1]
        fire_yml = _configure(tmp_path, [_otlp_source()])

        assert _run(fire_yml, _environ(LISTEN=f"127.0.0.1:{port}")) == EXIT_STARTUP_ERROR
    assert f"source traces: cannot listen on 127.0.0.1:{port}" in caplog.text


@pytest.mark.unit
def test_prepare_opens_a_receiver_and_releases_it_on_a_later_startup_error(
    tmp_path: Path,
) -> None:
    port = _free_port()
    fire_yml = _configure(tmp_path, [_otlp_source(), _replay_source()])
    environ = _environ(_replay_file(tmp_path, []), LISTEN=f"127.0.0.1:{port}")
    plan = prepare("shop", config_path=fire_yml, environ=environ)
    try:
        assert [type(planned).__name__ for planned in plan.sources] == [
            "PlannedReceiver",
            "PlannedSource",
        ]
    finally:
        for planned in plan.sources:
            if isinstance(planned, PlannedReceiver):
                planned.receiver.close()

    environ["REPLAY"] = "/nonexistent/records.json"
    assert _run(fire_yml, environ) == EXIT_STARTUP_ERROR
    with socket.socket() as again:
        again.bind(("127.0.0.1", port))


CAPTURE_EVENT = {
    "name": "capture.written",
    "request_uri": "acme.capture.request_uri",
    "response_uri": "acme.capture.response_uri",
    "ttl_s": 60,
}
ULID = "[0-7][0-9A-HJKMNP-TV-Z]{25}"
WORK = "01JABCDEFGHJKMNPQRSTVWXYZ0"
STAMP = "20261011_163001_123456"
NOTIFICATIONS = CaptureRule(
    prefix="notification-payloads/", label="notifications", layout=CaptureLayout.WORK_UNIT_ACTION
)
CONFIRMATION = f"{WORK}/confirm_email_{STAMP}.json"
CANCEL = f"{WORK}/cancel_email_{STAMP}.json"
CHANNEL = "http:outbound:notifications:confirm_email"
PAIR_ID = f"notifications:{WORK}:confirm_email:{STAMP}"
CAPTURE_SPAN = SpanRef(TRACE, "e" * 16)


def _capture_source(source_id: str = "captures", bucket_env: str = "BUCKET") -> dict[str, Any]:
    return {
        "id": source_id,
        "kind": "s3_capture",
        "bucket_env": bucket_env,
        "work_id_pattern": ULID,
        "captures": [NOTIFICATIONS.model_dump(mode="json")],
    }


class _NoAws:
    """An S3 client that fails a test if the tentacle calls it."""

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"S3 called at startup: {name}")


@pytest.fixture
def no_aws(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sources_module, "make_s3_client", _NoAws)


@pytest.mark.unit
def test_an_s3_capture_source_without_its_bucket_is_skipped(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, no_aws: None
) -> None:
    fire_yml = _configure(tmp_path, [_replay_source(), _capture_source()])
    environ = _environ(_replay_file(tmp_path, [_record("1")]))

    assert _run(fire_yml, environ) == EXIT_OK
    assert "source skipped: source=captures reason=BUCKET is not set" in caplog.text


@pytest.mark.unit
@pytest.mark.parametrize(
    "bucket",
    [
        "q7",
        "q" * 64,
        "Planted-Bucket",
        "-planted-bucket",
        "planted-bucket-",
        "planted_bucket",
        "planted bucket",
        "s3://planted-bucket",
        "planted-bucket/prefix",
    ],
    ids=[
        "short",
        "long",
        "uppercase",
        "leading-dash",
        "trailing-dash",
        "underscore",
        "space",
        "uri",
        "slash",
    ],
)
def test_an_s3_capture_bucket_must_be_a_bucket_name(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, no_aws: None, bucket: str
) -> None:
    fire_yml = _configure(tmp_path, [_capture_source()])

    assert _run(fire_yml, _environ(BUCKET=bucket)) == EXIT_STARTUP_ERROR
    assert "source captures: BUCKET is not an S3 bucket name" in caplog.text
    assert bucket not in caplog.text


@pytest.mark.unit
def test_an_aws_environment_without_a_client_is_a_startup_error_by_type(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    def no_profile() -> Any:
        raise ProfileNotFound(profile="planted-profile")

    monkeypatch.setattr(sources_module, "make_s3_client", no_profile)
    fire_yml = _configure(tmp_path, [_capture_source()])

    assert _run(fire_yml, _environ(BUCKET="capture-data")) == EXIT_STARTUP_ERROR
    assert "source captures: cannot build an S3 client (ProfileNotFound)" in caplog.text
    assert "planted-profile" not in caplog.text
    assert "capture-data" not in caplog.text


def _pair(name: str = CONFIRMATION, **overrides: Any) -> RawCapture:
    raw = RawCapture(
        source="captures",
        rule=NOTIFICATIONS,
        name=name,
        response_state=ObjectState.FOUND,
        response={"status_code": 201, "body": {"id": "x"}},
        request_state=ObjectState.FOUND,
        request={"method": "POST", "body": {"qty": 1}},
    )
    return replace(raw, **overrides)


def _response_uri(name: str) -> str:
    return f"s3://capture-data/{NOTIFICATIONS.prefix}responses/{name}"


@pytest.mark.unit
def test_prepare_plans_capture_sources_on_one_index_without_calling_aws(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, no_aws: None
) -> None:
    opened: list[dict[str, Any]] = []
    real_receiver = sources_module.OtlpReceiver

    def receiver(**kwargs: Any) -> Any:
        opened.append(kwargs)
        return real_receiver(**kwargs)

    monkeypatch.setattr(sources_module, "OtlpReceiver", receiver)
    traces = {**_otlp_source(), "capture_event": CAPTURE_EVENT}
    sources = [traces, _capture_source(), _capture_source("more", "MORE_BUCKET")]
    fire_yml = _configure(tmp_path, sources)
    environ = _environ(
        LISTEN=f"127.0.0.1:{_free_port()}", BUCKET="capture-data", MORE_BUCKET="more.data"
    )

    plan = prepare("shop", config_path=fire_yml, environ=environ)
    try:
        received, *captures = plan.sources
        assert isinstance(received, PlannedReceiver)
        assert received.capture_event == CaptureEvent.model_validate(CAPTURE_EVENT)
        assert [kwargs["capture_event"] for kwargs in opened] == [received.capture_event]
        assert [type(planned).__name__ for planned in captures] == ["PlannedCapture"] * 2
        assert all(isinstance(planned, PlannedCapture) for planned in captures)
        assert all(isinstance(planned.source, S3CaptureSource) for planned in captures)

        plan.capture_index.put(_response_uri(CONFIRMATION), CAPTURE_SPAN, 60, datetime.now(UTC))
        unattributed = f"unattributed/confirm_email_{STAMP}.json"
        for planned in captures:
            assert isinstance(planned, PlannedCapture)
            joined = planned.pipeline.process(_pair())
            assert {(s.work_id, s.trace_id, s.span_id) for s in joined.shapes} == {
                (WORK, CAPTURE_SPAN.trace_id, CAPTURE_SPAN.span_id)
            }
            unjoined = planned.pipeline.process(_pair(unattributed))
            assert {(s.work_id, s.trace_id) for s in unjoined.shapes} == {(None, None)}
    finally:
        for planned in plan.sources:
            if isinstance(planned, PlannedReceiver):
                planned.receiver.close()


def _indexed(index: CaptureIndex, *uris: str, at: datetime | None = None) -> list[Any]:
    now = at or datetime.now(UTC)
    return [index.get(uri, now) for uri in uris]


@pytest.mark.unit
async def test_capture_uris_of_spans_sent_are_indexed_and_noted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    sent = (_response_uri(CONFIRMATION), f"{NOTIFICATIONS.prefix}requests/{CONFIRMATION}")
    invalid = (_response_uri(CANCEL),)
    no_service = (f"{NOTIFICATIONS.prefix}requests/{CANCEL}",)
    batch = [
        _raw_span("E" * 16, trace_id=TRACE.upper(), capture_uris=(*sent, "s3://bucket-only")),
        _raw_span("b" * 16, trace_id=None, capture_uris=invalid),
        _raw_span("c" * 16, service=None, capture_uris=no_service),
    ]
    receiver = ScriptedReceiver([batch])
    plan = _receiver_plan(receiver, CaptureEvent.model_validate(CAPTURE_EVENT), flush_ms=1)

    assert await _serve_briefly(plan, Body()) == EXIT_OK

    assert receiver.results == [SpanBatchResult(True, 2)]
    assert len(plan.capture_index) == 2
    assert _indexed(plan.capture_index, *sent) == [CAPTURE_SPAN, CAPTURE_SPAN]
    assert _indexed(plan.capture_index, *invalid, *no_service) == [None, None]
    later = datetime.now(UTC) + timedelta(seconds=61)
    assert _indexed(plan.capture_index, *sent, at=later) == [None, None]
    assert "rejected=invalid_ids:1,no_service:1 skipped=- noted=capture_indexed:2 " in caplog.text


@pytest.mark.unit
async def test_a_batch_that_is_not_taken_indexes_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    batch = [
        _raw_span(f"{index + 1:016x}", capture_uris=(_response_uri(f"{index}.json"),))
        for index in range(3)
    ]
    receiver = ScriptedReceiver([batch])
    plan = _receiver_plan(
        receiver, CaptureEvent.model_validate(CAPTURE_EVENT), buffer_size=2, flush_ms=1
    )

    assert await _serve_briefly(plan, Body()) == EXIT_OK

    assert receiver.results == [SpanBatchResult(False)]
    assert len(plan.capture_index) == 0
    assert "noted=buffer_full:1 " in caplog.text


@pytest.mark.unit
async def test_a_receiver_without_a_capture_event_indexes_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    receiver = ScriptedReceiver(
        [[_raw_span("a" * 16, capture_uris=(_response_uri(CONFIRMATION),))]]
    )
    plan = _receiver_plan(receiver, flush_ms=1)

    assert await _serve_briefly(plan, Body()) == EXIT_OK

    assert len(plan.capture_index) == 0
    assert "source=traces read=1 built=1 sent=1 unsent=0 pending=0 rejected=- skipped=- " in (
        caplog.text
    )
    assert "noted=- " in caplog.text


class ScriptedCaptures:
    """Yields pairs, counting what it handed out and recording acks; optionally fails or waits
    forever.
    """

    def __init__(
        self,
        pairs: list[RawCapture],
        *,
        fail: bool = False,
        live: bool = False,
        acks: list[str] | None = None,
    ) -> None:
        self.pairs_to_yield = pairs
        self.fail = fail
        self.live = live
        self.acks = [] if acks is None else acks
        self.yielded = 0

    async def pairs(self) -> AsyncIterator[RawCapture]:
        await asyncio.sleep(0)
        for raw in self.pairs_to_yield:
            self.yielded += 1
            yield replace(raw, ack=self._ack(raw.name))
        if self.fail:
            raise RuntimeError("planted-capture-detail")
        if self.live:
            await asyncio.Event().wait()

    def _ack(self, name: str) -> Any:
        async def ack() -> None:
            self.acks.append(name)

        return ack


def _planned_capture(source: ScriptedCaptures, index: CaptureIndex) -> PlannedCapture:
    pipeline = CapturePipeline(
        system="shop",
        source="captures",
        sample_rate=1.0,
        work_id_pattern=re.compile(ULID),
        index=index,
        generalizer=FlatStarShapeGeneralizer(),
        fingerprinter=Sha256ShapeFingerprinter(),
    )
    return PlannedCapture(id="captures", source=source, pipeline=pipeline)


def _capture_plan(source: ScriptedCaptures, **tentacle: Any) -> Plan:
    index = CaptureIndex()
    return Plan(
        system="shop",
        sources=[_planned_capture(source, index)],
        tentacle=TentacleConfig.model_validate(tentacle),
        url="http://body/v0/observations",
        token=TOKEN,
        capture_index=index,
    )


@pytest.mark.unit
async def test_capture_pairs_are_shaped_sent_counted_and_acked(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    missing = ObjectState.MISSING
    no_response = f"{WORK}/notify_{STAMP}.json"
    nothing = f"{WORK}/resend_{STAMP}.json"
    unattributed = f"unattributed/confirm_email_{STAMP}.json"
    pairs = [
        _pair(),
        _pair("stray.json"),
        _pair(f"{WORK}/550e8400-e29b-41d4-a716-446655440000_{STAMP}.json"),
        _pair(no_response, request_state=missing, request=None, response={"status_code": None}),
        _pair(
            nothing,
            request_state=missing,
            request=None,
            response_state=ObjectState.UNDECODABLE,
            response=None,
        ),
        _pair(CANCEL, request={"body": "not json"}),
        _pair(),
        _pair(unattributed),
    ]
    source = ScriptedCaptures(pairs)
    plan = _capture_plan(source, flush_ms=1)
    plan.capture_index.put(_response_uri(CANCEL), CAPTURE_SPAN, 60, datetime.now(UTC))
    body = Body()
    body.release.clear()
    running = asyncio.create_task(run(plan, asyncio.Event(), transport=body.transport()))

    await asyncio.sleep(0.1)
    assert sorted(source.acks) == sorted(["stray.json", pairs[2].name, nothing, CONFIRMATION])
    body.release.set()

    assert await asyncio.wait_for(running, 5) == EXIT_OK
    assert sorted(source.acks) == sorted(
        ["stray.json", pairs[2].name, nothing, CONFIRMATION, CONFIRMATION, CONFIRMATION]
        + [no_response, CANCEL, unattributed, unattributed]
    )
    shapes = {shape["observation_id"]: shape for shape in body.shapes}
    assert sorted(shapes) == [
        f"notifications:-:confirm_email:{STAMP}:request",
        f"notifications:-:confirm_email:{STAMP}:response",
        f"notifications:{WORK}:cancel_email:{STAMP}:response",
        f"{PAIR_ID}:request",
        f"{PAIR_ID}:response",
        f"notifications:{WORK}:notify:{STAMP}:response",
    ]
    assert shapes[f"{PAIR_ID}:request"]["channel"] == {
        "kind": "http",
        "direction": "outbound",
        "name": "notifications:confirm_email",
        "part": "request",
        "status_class": None,
    }
    assert (
        shapes[f"notifications:{WORK}:notify:{STAMP}:response"]["channel"]["status_class"]
        == "none"
    )
    joined = shapes[f"notifications:{WORK}:cancel_email:{STAMP}:response"]
    assert (joined["trace_id"], joined["span_id"], joined["work_id"]) == (
        CAPTURE_SPAN.trace_id,
        CAPTURE_SPAN.span_id,
        WORK,
    )
    assert shapes[f"{PAIR_ID}:request"]["trace_id"] is None
    assert shapes[f"notifications:-:confirm_email:{STAMP}:request"]["work_id"] is None
    assert (
        "tentacle counters: source=captures read=8 built=6 sent=6 unsent=0 pending=0 "
        "rejected=bad_action:1,no_layout:1 "
        "skipped=body_not_json:1,local_duplicate:1,object_missing:2,object_undecodable:1 "
        "noted=unjoined:3 body_duplicate=0 body_rejected=-"
    ) in caplog.text


@pytest.mark.unit
async def test_a_full_buffer_pauses_capture_reading() -> None:
    source = ScriptedCaptures(
        [_pair(f"{WORK}/confirm_email_20261011_163001_{index:06d}.json") for index in range(10)]
    )
    body = Body()
    body.release.clear()
    plan = _capture_plan(source, buffer_size=2, batch_size=1, flush_ms=1)
    running = asyncio.create_task(run(plan, asyncio.Event(), transport=body.transport()))

    await asyncio.sleep(0.2)
    # One shape in flight, two in the buffer, and the second pair's last one waiting.
    assert source.yielded == 2
    body.release.set()

    assert await asyncio.wait_for(running, 5) == EXIT_OK
    assert source.yielded == 10
    assert len(body.shapes) == 20


@pytest.mark.unit
async def test_failing_capture_source_exits_1_by_exception_type_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    body = Body()
    source = ScriptedCaptures([_pair()], fail=True)

    assert await run(_capture_plan(source), asyncio.Event(), transport=body.transport()) == (
        EXIT_UNSENT
    )
    assert "source failed: source=captures error=RuntimeError" in caplog.text
    assert "planted-capture-detail" not in caplog.text
    assert sorted(shape["observation_id"] for shape in body.shapes) == [
        f"{PAIR_ID}:request",
        f"{PAIR_ID}:response",
    ]


@pytest.mark.unit
async def test_stop_ends_a_live_capture_source_and_drains() -> None:
    body = Body()
    source = ScriptedCaptures([_pair()], live=True)

    assert await _serve_briefly(_capture_plan(source, flush_ms=1), body) == EXIT_OK
    assert len(body.shapes) == 2


@pytest.mark.unit
async def test_a_receivers_capture_events_join_the_pairs_of_a_capture_source(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    uri = _response_uri(CONFIRMATION)
    receiver = ScriptedReceiver([[_raw_span(CAPTURE_SPAN.span_id, capture_uris=(uri,))]])
    captures = ScriptedCaptures([_pair()], live=True)
    index = CaptureIndex()
    plan = Plan(
        system="shop",
        sources=[
            _planned_receiver(receiver, CaptureEvent.model_validate(CAPTURE_EVENT)),
            _planned_capture(captures, index),
        ],
        tentacle=TentacleConfig.model_validate({"flush_ms": 1}),
        url="http://body/v0/observations",
        token=TOKEN,
        capture_index=index,
    )
    body = Body()

    assert await _serve_briefly(plan, body) == EXIT_OK

    assert {(shape["trace_id"], shape["span_id"]) for shape in body.shapes} == {
        (CAPTURE_SPAN.trace_id, CAPTURE_SPAN.span_id)
    }
    assert "source=traces read=1 built=1 sent=1 " in caplog.text
    assert "noted=capture_indexed:1 " in caplog.text
    assert "source=captures read=1 built=2 sent=2 unsent=0 pending=0 rejected=- " in (caplog.text)
    assert "skipped=- noted=- body_duplicate=0" in caplog.text


class Copies:
    """Yields copies of one record, each acked under its label; waits on `gate` before the
    labels after it, then stays live.
    """

    def __init__(self, labels: list[str], *, gate: asyncio.Event | None = None, after: list[str]):
        self.labels, self.gate, self.after = labels, gate, after
        self.acks: list[str] = []

    async def records(self) -> AsyncIterator[RawRecord]:
        for label in self.labels:
            yield self._copy(label)
        if self.gate is not None:
            await self.gate.wait()
        for label in self.after:
            yield self._copy(label)
        await asyncio.Event().wait()

    def _copy(self, label: str) -> RawRecord:
        async def ack() -> None:
            self.acks.append(label)

        return RawRecord("scripted", "us-east-1", _record("1"), ack)


async def _until(condition: Callable[[], object]) -> None:
    async with asyncio.timeout(5):
        while not condition():
            await asyncio.sleep(0.01)


@pytest.mark.unit
async def test_a_copy_of_a_record_in_flight_is_acked_once_the_first_is_accepted() -> None:
    source = Copies(["first", "redelivered"], after=[])
    body = Body()
    body.release.clear()
    stop = asyncio.Event()
    running = asyncio.create_task(
        run(_plan(source, flush_ms=1), stop, transport=body.transport())  # type: ignore[arg-type]
    )

    await asyncio.sleep(0.1)
    assert source.acks == []
    body.release.set()
    await _until(lambda: len(source.acks) == 2)
    stop.set()

    assert await asyncio.wait_for(running, 5) == EXIT_OK
    assert source.acks == ["first", "redelivered"]
    assert len(body.shapes) == 1


@pytest.mark.unit
async def test_a_copy_held_for_an_unsent_record_is_dropped_and_the_next_copy_is_sent(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    gate = asyncio.Event()
    source = Copies(["first", "redelivered"], gate=gate, after=["again"])
    body = Body(status=401)
    body.release.clear()
    stop = asyncio.Event()
    running = asyncio.create_task(
        run(_plan(source, flush_ms=1), stop, transport=body.transport())  # type: ignore[arg-type]
    )

    await asyncio.sleep(0.1)
    body.release.set()
    await _until(lambda: "batch unsent: source=scripted" in caplog.text)
    body.status = 202
    gate.set()
    await _until(lambda: source.acks)
    stop.set()

    assert await asyncio.wait_for(running, 5) == EXIT_UNSENT
    assert source.acks == ["again"]
    assert len(body.shapes) == 1
    assert (
        "source=scripted read=3 built=2 sent=1 unsent=1 pending=0 rejected=- "
        "skipped=local_duplicate:1 "
    ) in caplog.text


TABLE_SOURCE = {
    "id": "items",
    "kind": "dynamodb_stream",
    "direction": "internal",
    "table_env": "TABLE",
    "regions": ["us-east-1", "us-west-2"],
    "filter": {"attribute": "status", "equals": "ABANDONED"},
    "envelope": ENVELOPE,
}
TOPIC_SOURCE = {
    "id": "shipment-events",
    "kind": "sns_fanout",
    "direction": "inbound",
    "topic_env": "TOPIC",
    "regions": ["us-east-1"],
    "raw_delivery": True,
    "envelope": ENVELOPE,
}


@pytest.fixture
def made_clients(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str]]:
    """Each client a reader makes, by service and region; calling one fails the test."""
    made: list[tuple[str, str]] = []

    def make(service: str, region: str) -> _NoAws:
        made.append((service, region))
        return _NoAws()

    monkeypatch.setattr(sources_module, "make_client", make)
    return made


@pytest.mark.unit
def test_prepare_plans_event_sources_without_calling_aws(
    tmp_path: Path, made_clients: list[tuple[str, str]]
) -> None:
    fire_yml = _configure(tmp_path, [KINESIS_SOURCE, TABLE_SOURCE, TOPIC_SOURCE])
    environ = _environ(STREAM="acme-dev-domain-events", TABLE="events.table", TOPIC="shipments_1")

    plan = prepare("shop", start=StartPosition.OLDEST, config_path=fire_yml, environ=environ)

    assert [(planned.id, type(planned.records).__name__) for planned in plan.sources] == [
        ("events", "KinesisRecordSource"),
        ("items", "DynamoDbStreamRecordSource"),
        ("shipment-events", "SnsFanoutRecordSource"),
    ]
    assert made_clients == [
        ("kinesis", "us-east-1"),
        ("dynamodb", "us-east-1"),
        ("dynamodb", "us-west-2"),
        ("dynamodbstreams", "us-east-1"),
        ("dynamodbstreams", "us-west-2"),
        ("sqs", "us-east-1"),
    ]
    topic = plan.sources[2].records
    assert isinstance(topic, SnsFanoutRecordSource)
    assert (topic._queue, topic._raw_delivery, topic._start) == (
        "fire-shop-shipment-events",
        True,
        StartPosition.OLDEST,
    )
    table = plan.sources[1].records
    assert isinstance(table, DynamoDbStreamRecordSource)
    assert table._filter == StreamFilter("status", "ABANDONED")


EVENT_SOURCE_ERRORS = {
    "table-not-a-name": (TABLE_SOURCE, {"TABLE": "pt"}, "TABLE is not a DynamoDB table name"),
    "topic-not-a-name": (TOPIC_SOURCE, {"TOPIC": "arn:aws:sns:x"}, "TOPIC is not an SNS topic"),
    "fifo-topic": (TOPIC_SOURCE, {"TOPIC": "events.fifo"}, "TOPIC names a FIFO topic"),
    "queue-too-long": (
        {**TOPIC_SOURCE, "id": "c" * 71},
        {"TOPIC": "shipments"},
        f"source id {'c' * 71} makes a queue name over 80 characters",
    ),
}


@pytest.mark.unit
@pytest.mark.parametrize(
    ("source", "environ", "message"),
    list(EVENT_SOURCE_ERRORS.values()),
    ids=list(EVENT_SOURCE_ERRORS),
)
def test_an_event_source_naming_nothing_it_can_read_is_a_startup_error(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    made_clients: list[tuple[str, str]],
    source: dict[str, Any],
    environ: dict[str, str],
    message: str,
) -> None:
    fire_yml = _configure(tmp_path, [source])

    assert _run(fire_yml, _environ(**environ)) == EXIT_STARTUP_ERROR
    assert f"source {source['id']}: {message}" in caplog.text
    [value] = environ.values()
    assert value not in caplog.text
    assert made_clients == []


@pytest.mark.unit
def test_startup_holds_the_boto_loggers_at_info(
    tmp_path: Path, made_clients: list[tuple[str, str]]
) -> None:
    loggers = [logging.getLogger(name) for name in ("boto3", "botocore", "urllib3")]
    levels = [logger.level for logger in loggers]
    try:
        for logger in loggers:
            logger.setLevel(logging.DEBUG)
        fire_yml = _configure(tmp_path, [KINESIS_SOURCE])

        prepare("shop", config_path=fire_yml, environ=_environ(STREAM="domain-events"))

        assert [logger.level for logger in loggers] == [logging.INFO] * 3
    finally:
        for logger, level in zip(loggers, levels, strict=True):
            logger.setLevel(level)


@pytest.mark.unit
def test_an_aws_environment_without_event_clients_is_a_startup_error_by_type(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    def no_profile(service: str, region: str) -> Any:
        raise ProfileNotFound(profile="planted-profile")

    monkeypatch.setattr(sources_module, "make_client", no_profile)
    fire_yml = _configure(tmp_path, [KINESIS_SOURCE])

    assert _run(fire_yml, _environ(STREAM="domain-events")) == EXIT_STARTUP_ERROR
    assert "source events: cannot build a kinesis client (ProfileNotFound)" in caplog.text
    assert "planted-profile" not in caplog.text


@pytest.mark.unit
async def test_what_a_noting_source_notes_shows_in_its_counters_line(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)

    class Noting:
        def __init__(self) -> None:
            self.noted: Counter[str] = Counter()

        async def records(self) -> AsyncIterator[RawRecord]:
            self.noted["position_lost"] += 1
            yield RawRecord("scripted", "us-east-1", _record("1"))

    source = Noting()
    assert isinstance(source, NotingRecordSource)

    assert await run(_plan(source, flush_ms=1), asyncio.Event(), transport=Body().transport()) == (  # type: ignore[arg-type]
        EXIT_OK
    )
    assert "source=scripted read=1 built=1 sent=1 " in caplog.text
    assert " noted=position_lost:1 " in caplog.text


@pytest.mark.unit
async def test_records_skipped_by_their_source_are_counted_and_acked(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    acks: list[str] = []

    class Skipping:
        async def records(self) -> AsyncIterator[RawRecord]:
            for index, skip in enumerate(
                [RecordSkipReason.REMOVE_RECORD, RecordSkipReason.FILTERED, None]
            ):
                record = None if skip else _record(str(index))

                async def ack(index: int = index) -> None:
                    acks.append(str(index))

                yield RawRecord("scripted", "us-west-2", record, ack, skipped=skip)

    plan = _plan(Skipping(), flush_ms=1)  # type: ignore[arg-type]
    body = Body()

    assert await run(plan, asyncio.Event(), transport=body.transport()) == EXIT_OK
    assert sorted(acks) == ["0", "1", "2"]
    assert [shape["region"] for shape in body.shapes] == ["us-west-2"]
    assert (
        "source=scripted read=3 built=1 sent=1 unsent=0 pending=0 rejected=- "
        "skipped=filtered:1,remove_record:1 "
    ) in caplog.text


@pytest.mark.unit
@pytest.mark.parametrize(
    "argv",
    [[], ["--source", "x"], ["--system", "shop", "--start", "middle"]],
    ids=["no-system", "source-only", "bad-start"],
)
def test_bad_arguments_exit_2(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as caught:
        main(argv)
    assert caught.value.code == EXIT_STARTUP_ERROR
