"""Reasoner backed by Laya over Jev's wire protocol (`POST /v1/systemone`)."""

from __future__ import annotations

import logging
import math
from typing import Any

import httpx

from fire.adapters.reasoners.echo import EchoReasoner
from fire.adapters.reasoners.systemone import build_options, build_request
from fire.core.domain.decision import Decision, Possibility, Primitive
from fire.core.domain.dossier import Dossier
from fire.core.ports.reasoner import Reasoner

logger = logging.getLogger(__name__)


class LayaReasoner:
    """Asks a laya-serve endpoint which option explains the failure, and whether the evidence
    is sufficient.

    Laya rounds probabilities to 4 dp, so they are renormalised over the options; an option
    missing from the response gets 0. The decision is `noul` when the `sufficient` answer is
    below `min_sufficient`. Any transport error, non-2xx status, or malformed body falls back
    to `fallback` (echo by default) with `noul=True`; the log line carries no payload.
    """

    def __init__(
        self,
        url: str,
        api_key: str | None = None,
        model: str | None = None,
        timeout_s: float = 10.0,
        min_sufficient: float = 0.5,
        transport: httpx.AsyncBaseTransport | None = None,
        fallback: Reasoner | None = None,
    ) -> None:
        self._url = url
        self._model = model
        self._min_sufficient = min_sufficient
        self._headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._client = httpx.AsyncClient(timeout=timeout_s, transport=transport)
        self._fallback: Reasoner = fallback or EchoReasoner()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def decide(self, dossier: Dossier) -> Decision:
        options = build_options(dossier)
        if len(options) == 1:
            # Nothing to choose between: no call needed.
            return await self._fallback.decide(dossier)
        body = build_request(dossier)
        if self._model:
            body["model"] = self._model
        try:
            response = await self._client.post(self._url, json=body, headers=self._headers)
            response.raise_for_status()
            return self._parse(dossier, options, response.json())
        except (httpx.HTTPError, ValueError, KeyError, TypeError, AttributeError) as exc:
            logger.warning("laya reasoner failed (%s); falling back", type(exc).__name__)
            fallback = await self._fallback.decide(dossier)
            return fallback.model_copy(update={"noul": True, "primitive": Primitive.NOUL})

    def _parse(
        self, dossier: Dossier, options: list[Possibility], payload: dict[str, Any]
    ) -> Decision:
        answers = payload["answers"]
        cause = answers["cause"]
        raw = cause["probabilities"]
        weights = [float(raw.get(option.id, 0.0)) for option in options]
        total = math.fsum(weights)
        if not math.isfinite(total) or total <= 0 or any(w < 0 for w in weights):
            raise ValueError("no usable probabilities")
        for option, weight in zip(options, weights, strict=True):
            option.probability = weight / total
        confidence = cause.get("answer_confidence")
        confidence = float(cause["confidence"] if confidence is None else confidence)
        noul = float(answers["sufficient"]["noul"]) < self._min_sufficient
        return Decision(
            incident_id=dossier.incident_id,
            possibilities=options,
            confidence=min(max(confidence, 0.0), 1.0),
            noul=noul,
            primitive=Primitive.NOUL if noul else Primitive.CHOICE,
        )
