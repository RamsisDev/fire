"""Deterministic local Reasoner for demos — not a generative LLM."""

from __future__ import annotations

from fire.adapters.reasoners.systemone import build_options, has_evidence
from fire.core.domain.decision import Decision, Primitive
from fire.core.domain.dossier import Dossier


class EchoReasoner:
    """Builds options from process-path hops, drift, burst, and DLQ fragments, and missing
    evidence.

    Possibilities, in order: one per hop; one per present `shape_drift` or `path_drift`
    fragment, then one per present `http_errors` fragment, then one per present
    `dlq_attribution` fragment, each labelled from its `data` (its `summary` when `data` is
    malformed); `insufficient-evidence` when any fragment is missing; `unknown` when there is
    none of these. Probabilities split evenly. The decision is `noul` when any fragment is
    missing or the dossier has no shape, fingerprint, drift, burst, or DLQ possibility. An
    `incident_io` fragment says what opened the incident, not what caused it: it adds no
    possibility and is no evidence.
    """

    async def decide(self, dossier: Dossier) -> Decision:
        possibilities = build_options(dossier)
        missing = dossier.missing
        evidence = has_evidence(dossier)
        confidence = 0.2 if missing or not evidence else 0.45
        noul = confidence < 0.3 or not evidence

        return Decision(
            incident_id=dossier.incident_id,
            possibilities=possibilities,
            confidence=confidence,
            noul=noul,
            primitive=Primitive.NOUL if noul else Primitive.CHOICE,
        )
