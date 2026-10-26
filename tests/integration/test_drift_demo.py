"""L1: the drift demo's corpora, dev/corpora/, replayed by dev/replay_corpus.py.

The script's own replay functions run here: the tentacle's `s3_capture` or `otlp` source of the
example system `acme`, the repo's _template.yml written to `tmp_path` as acme.yml with a fast
`s3_capture` poll, sends to the body, which runs in-process through `httpx.ASGITransport` on a
hand-moved clock. S3 is moto's, as in the script, with one empty bucket per replay.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import re
import sys
from collections import Counter
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

import boto3
import httpx
import pytest
import yaml
from moto import mock_aws
from spans import Clock

import fire.apps.incident_engine.app as app_module
from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.adapters.sources.otlp import parse_traces
from fire.apps.config import OtlpSourceConfig, S3CaptureSourceConfig, SystemConfig
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.apps.tentacle.runner import EXIT_OK, EXIT_UNSENT, StartupError
from fire.core.domain.operation import OperationNamer
from fire.core.services.span_pipeline import SpanPipeline

REPO = Path(__file__).resolve().parents[2]
FIXTURES = REPO / "tests" / "fixtures" / "acme"
TEMPLATE_YML = REPO / "config" / "systems" / "_template.yml"
ACME = SystemConfig.model_validate(yaml.safe_load(TEMPLATE_YML.read_text(encoding="utf-8")))
[CAPTURES] = [source for source in ACME.sources if isinstance(source, S3CaptureSourceConfig)]
[TRACES] = [source for source in ACME.sources if isinstance(source, OtlpSourceConfig)]
MIN_SAMPLES = ACME.drift.min_samples
SETTLE = ACME.paths.settle_s
TOKEN = "drift-demo-token"
HEADERS = {"X-FIRE-Token": TOKEN}
ENVIRON = {"FIRE_API_TOKEN": TOKEN, "FIRE_BODY_URL": "http://body"}
NOTIFY = "[fire] notify incident="
SHIPPING = "http:outbound:shipping:create"
ROUTE = "POST /v1/orders/checkout"
WORKER = "worker.distribute_work"
BILLING = "POST billing.partner.example.com/v2/invoices/place"
MORE = 99


def _load_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "replay_corpus", REPO / "dev" / "replay_corpus.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


replay_corpus = _load_script()


def _read(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def fire_yml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock: Clock) -> Iterator[Path]:
    """The repo's _template.yml as acme.yml with a fast `s3_capture` poll, and a body built
    from it.
    """
    system = yaml.safe_load(TEMPLATE_YML.read_text(encoding="utf-8"))
    for source in system["sources"]:
        if source["kind"] == "s3_capture":
            source.update(poll_s=0.05, settle_s=0)
    path = tmp_path / "fire.yml"
    path.write_text(yaml.safe_dump({"tentacle": {"flush_ms": 10}}), encoding="utf-8")
    (tmp_path / "systems").mkdir()
    (tmp_path / "systems" / f"{ACME.system}.yml").write_text(
        yaml.safe_dump(system), encoding="utf-8"
    )
    monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
    monkeypatch.delenv("ACME_MANIFEST_PATH", raising=False)
    app_module._container = build_container(path, clock=clock)
    yield path
    app_module._container = None


@pytest.fixture
async def s3(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> AsyncIterator[Any]:
    """moto's S3, with fake credentials and no endpoint, profile, or AWS file from outside."""
    for name in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_PROFILE", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "aws-config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "aws-credentials"))
    with mock_aws():
        yield boto3.client("s3")
        # S3 calls still in worker threads end inside the mock.
        await asyncio.get_running_loop().shutdown_default_executor()


async def _get(path: str) -> dict[str, Any]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://body") as client:
        response = await client.get(path, headers=HEADERS)
    assert response.status_code == 200
    return response.json()


def _headers(out: str) -> list[str]:
    return [line for line in out.splitlines() if line.startswith(NOTIFY)]


async def _replay_outbound(s3: Any, fire_yml: Path, salt: str, drift_copies: int) -> int:
    bucket = f"drift-demo-{salt.lower()}"
    s3.create_bucket(Bucket=bucket)
    return await replay_corpus.replay_outbound(
        s3,
        environ={**ENVIRON, CAPTURES.location_env: bucket},
        system=ACME.system,
        config_path=fire_yml,
        transport=httpx.ASGITransport(app=app),
        salt=salt,
        drift_copies=drift_copies,
    )


@pytest.mark.integration
async def test_the_outbound_corpus_notifies_once_for_the_shipping_action(
    fire_yml: Path, s3: Any, capfd: pytest.CaptureFixture[str]
) -> None:
    assert await _replay_outbound(s3, fire_yml, "AAAA", drift_copies=1) == EXIT_OK

    out = capfd.readouterr().out
    [header] = _headers(out)
    assert re.fullmatch(rf"{re.escape(NOTIFY)}\S+ system=acme trigger=shape_drift", header)
    assert (
        f"Shape drift on {SHIPPING}:response:2xx: ShipmentNumber (string) removed _[shape_drift]_"
    ) in out

    assert await _replay_outbound(s3, fire_yml, "BBBB", drift_copies=MORE) == EXIT_OK

    assert _headers(capfd.readouterr().out) == []
    streams = {
        stream["key"]: stream
        for stream in (await _get(f"/v0/systems/{ACME.system}/streams"))["streams"]
    }
    assert set(streams) == {f"{SHIPPING}:request", f"{SHIPPING}:response:2xx"}
    request, response = streams[f"{SHIPPING}:request"], streams[f"{SHIPPING}:response:2xx"]
    assert (request["status"], request["samples"], request["drift"]) == (
        "warm",
        2 * MIN_SAMPLES + 1 + MORE,
        [],
    )
    assert (response["status"], response["samples"]) == ("warm", 2 * MIN_SAMPLES)
    [signal] = response["drift"]
    assert signal["occurrences"] == 1 + MORE
    assert signal["diff"] == {
        "added": [],
        "type_changed": [],
        "removed": [{"path": "ShipmentNumber", "value_type": "string"}],
    }


@pytest.mark.integration
async def test_the_path_corpus_raises_one_path_drift_notification(
    fire_yml: Path, clock: Clock, capfd: pytest.CaptureFixture[str]
) -> None:
    async def replay(salt: str, drift_copies: int) -> dict[str, Any]:
        exit_code = await replay_corpus.replay_path(
            environ=ENVIRON,
            system=ACME.system,
            config_path=fire_yml,
            transport=httpx.ASGITransport(app=app),
            salt=salt,
            drift_copies=drift_copies,
        )
        assert exit_code == EXIT_OK
        clock.advance(SETTLE)
        report = await _get(f"/v0/systems/{ACME.system}/paths")
        assert report["open_flows"] == 0
        [path] = report["paths"]
        assert path["entry"]["operation"] == ROUTE
        return path

    path = await replay("AAAA", drift_copies=1)

    out = capfd.readouterr().out
    [header] = _headers(out)
    assert re.fullmatch(rf"{re.escape(NOTIFY)}\S+ system=acme trigger=path_drift", header)
    assert f"hop_missing: {BILLING} on {ROUTE} _[path_drift]_" in out
    assert (path["status"], path["flows"], path["drifted_flows"]) == ("warm", MIN_SAMPLES, 1)

    path = await replay("BBBB", drift_copies=MORE)

    assert _headers(capfd.readouterr().out) == []
    assert (path["flows"], path["drifted_flows"]) == (2 * MIN_SAMPLES, 1 + MORE)
    [signal] = path["drift"]
    assert (signal["rule"], signal["node"]["operation"], signal["occurrences"]) == (
        "hop_missing",
        BILLING,
        1 + MORE,
    )


@pytest.mark.integration
async def test_a_refused_batch_stops_the_replay_at_once(
    fire_yml: Path, s3: Any, caplog: pytest.LogCaptureFixture
) -> None:
    s3.create_bucket(Bucket="drift-demo-refused")
    # Without the early stop, the replay waits out its own timeout, which is longer.
    async with asyncio.timeout(replay_corpus.SLACK_S / 2):
        exit_code = await replay_corpus.replay_outbound(
            s3,
            environ={
                **ENVIRON,
                "FIRE_API_TOKEN": "wrong-token",
                CAPTURES.location_env: "drift-demo-refused",
            },
            system=ACME.system,
            config_path=fire_yml,
            transport=httpx.ASGITransport(app=app),
            salt="AAAA",
        )

    assert exit_code == EXIT_UNSENT
    assert "status=401" in caplog.text
    assert "the tentacle gave a batch up" in caplog.text


@pytest.mark.integration
async def test_the_startup_checks_run_before_the_corpus_is_written(
    fire_yml: Path, s3: Any
) -> None:
    s3.create_bucket(Bucket="drift-demo-unset")
    environ = {"FIRE_API_TOKEN": TOKEN, CAPTURES.location_env: "drift-demo-unset"}

    with pytest.raises(StartupError, match="FIRE_BODY_URL is not set"):
        await replay_corpus.replay_outbound(
            s3, environ=environ, system=ACME.system, config_path=fire_yml, salt="AAAA"
        )

    assert "Contents" not in s3.list_objects_v2(Bucket="drift-demo-unset")


@pytest.mark.integration
def test_the_replay_keeps_the_shells_aws_setup_away_from_moto() -> None:
    environ = {
        "AWS_ENDPOINT_URL": "http://localhost:4566",
        "AWS_ENDPOINT_URL_S3": "http://localhost:4566",
        "AWS_PROFILE": "someone",
        "AWS_SESSION_TOKEN": "x",
        "AWS_DEFAULT_REGION": "eu-west-1",
        "AWS_CONFIG_FILE": "/home/someone/.aws/config",
        "FIRE_API_TOKEN": TOKEN,
    }

    replay_corpus.isolate_aws(environ)

    assert environ == {
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_CONFIG_FILE": os.devnull,
        "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
        "FIRE_API_TOKEN": TOKEN,
    }


def _without(pair: dict[str, Any], *keys: str) -> dict[str, Any]:
    """The pair with `keys` removed from its response body."""
    changed = json.loads(json.dumps(pair))
    for key in keys:
        del changed["response"]["document"]["body"][key]
    return changed


def _operations(documents: list[dict[str, Any]]) -> Counter[tuple[str, str]]:
    """`(trace id, operation)` of every span, operations named by the tentacle's own pipeline."""
    pipeline = SpanPipeline(
        system=ACME.system,
        source=TRACES.id,
        sample_rate=1.0,
        namer=OperationNamer(ACME.operations),
        generalizer=FlatStarShapeGeneralizer(),
        fingerprinter=Sha256ShapeFingerprinter(),
    )
    operations: Counter[tuple[str, str]] = Counter()
    for document in documents:
        for raw in parse_traces(document):
            span = pipeline.process(raw).span
            assert span is not None
            operations[(span.trace_id, span.operation)] += 1
    return operations


@pytest.mark.integration
def test_the_corpora_derive_from_the_l1_fixtures() -> None:
    shipping = _read(FIXTURES / "captures" / "shipping" / "001.json")
    assert replay_corpus.outbound_items("baseline") == [
        shipping,
        _without(shipping, "MessageText"),
        _without(shipping, "MessageText", "CustomerReference"),
    ]
    assert replay_corpus.outbound_items("drift") == [_without(shipping, "ShipmentNumber")]

    recording = [_read(path) for path in sorted((FIXTURES / "otlp" / "checkout_1").glob("*"))]
    assert replay_corpus.path_items("baseline") == [recording]
    [drifted] = replay_corpus.path_items("drift")
    recorded = _operations(recording)
    [worker_trace] = [trace for trace, operation in recorded if operation == WORKER]
    assert recorded - _operations(drifted) == Counter({(worker_trace, BILLING): 1})
    assert _operations(drifted) - recorded == Counter()
