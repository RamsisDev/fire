"""LayaReasoner maps a /v1/systemone answer onto options, and falls back on any failure."""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx
import pytest

from fire.adapters.reasoners.laya import LayaReasoner
from fire.core.domain.decision import Primitive
from fire.core.domain.dossier import (
    Dossier,
    EvidenceFragment,
    EvidenceQuality,
    ProcessHop,
    ProcessPath,
    ShapeFingerprint,
)

URL = "http://laya.test/v1/systemone"


def _dossier() -> Dossier:
    return Dossier(
        incident_id="inc-1",
        system="example",
        fingerprint=ShapeFingerprint(schema_hash="abc"),
        process_path=ProcessPath(hops=[ProcessHop(ref="api"), ProcessHop(ref="db")]),
        fragments=[
            EvidenceFragment(
                collector="origin", quality=EvidenceQuality.MISSING, summary="No Origin provided"
            )
        ],
    )


def _answer(probabilities: dict[str, float], sufficient: float = 0.9) -> dict[str, Any]:
    return {
        "answers": {
            "cause": {
                "type": "choice",
                "probabilities": probabilities,
                "confidence": 0.1,
                "answer_confidence": 0.7,
            },
            "sufficient": {"type": "noul", "noul": sufficient},
        }
    }


def _reasoner(handler: Any, **kwargs: Any) -> LayaReasoner:
    return LayaReasoner(URL, transport=httpx.MockTransport(handler), **kwargs)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_request_and_renormalised_answer() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=_answer({"hop-0": 0.6, "hop-1": 0.3}))

    decision = await _reasoner(handler, api_key="secret", model="fire").decide(_dossier())

    body = json.loads(seen[0].content)
    assert seen[0].headers["Authorization"] == "Bearer secret"
    assert body["model"] == "fire"
    assert set(body["questions"]) == {"cause", "sufficient"}
    probabilities = {p.id: p.probability for p in decision.possibilities}
    assert probabilities == pytest.approx(
        {"hop-0": 2 / 3, "hop-1": 1 / 3, "insufficient-evidence": 0}
    )
    assert sum(probabilities.values()) == pytest.approx(1.0)
    assert decision.confidence == 0.7
    assert decision.noul is False
    assert decision.primitive is Primitive.CHOICE


@pytest.mark.unit
@pytest.mark.asyncio
async def test_no_auth_header_or_model_when_unset() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=_answer({"hop-0": 1.0}))

    await _reasoner(handler).decide(_dossier())
    assert "Authorization" not in seen[0].headers
    assert "model" not in json.loads(seen[0].content)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_insufficient_evidence_answer_is_noul() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_answer({"hop-0": 1.0}, sufficient=0.2))

    decision = await _reasoner(handler).decide(_dossier())
    assert decision.noul is True
    assert decision.primitive is Primitive.NOUL


@pytest.mark.unit
@pytest.mark.asyncio
async def test_single_option_skips_the_call() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("no call expected")

    decision = await _reasoner(handler).decide(Dossier(incident_id="inc-2", system="example"))
    assert [(p.id, p.probability) for p in decision.possibilities] == [("unknown", 1.0)]


def _boom(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectTimeout("slow", request=request)


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "handler",
    [
        lambda r: httpx.Response(500, text="boom"),
        _boom,
        lambda r: httpx.Response(200, text="not json"),
        lambda r: httpx.Response(200, json={"answers": {}}),
        lambda r: httpx.Response(200, json=_answer({"other": 1.0})),
    ],
    ids=["500", "timeout", "bad-json", "no-cause", "no-matching-options"],
)
async def test_failure_falls_back_to_echo_as_noul(
    handler: Any, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING):
        decision = await _reasoner(handler).decide(_dossier())
    assert decision.noul is True
    assert decision.primitive is Primitive.NOUL
    assert len(decision.possibilities) == 3
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "example" not in warnings[0].getMessage()
