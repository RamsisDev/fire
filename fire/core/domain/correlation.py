"""Incident correlation: the trigger events one system raises inside one window are one incident,
whose dossier merges their evidence.

Contract: docs/specs/ingest-drift-and-feedback.md#correlation.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from fire.core.domain.dossier import (
    Dossier,
    DossierTrigger,
    EvidenceFragment,
    EvidenceQuality,
    ProcessHop,
    ProcessPath,
)
from fire.core.domain.incident import Incident
from fire.core.domain.trigger_event import TriggerEvent


class CorrelationPolicy(BaseModel):
    """How long an incident collects its system's triggers: the system YAML's
    `triggers.correlation`.
    """

    model_config = ConfigDict(extra="forbid")

    # Seconds of the body's clock, from the acceptance of an incident's first trigger.
    window_s: float = Field(default=300, gt=0, allow_inf_nan=False)


@dataclass(frozen=True)
class AcceptedTrigger:
    """A trigger event and when the correlator accepted it, on the body's clock."""

    event: TriggerEvent
    accepted_at: datetime


def correlated_incident(
    incident_id: str, triggers: Sequence[AcceptedTrigger]
) -> tuple[Incident, Dossier]:
    """The incident and merged dossier of one system's triggers, in acceptance order; the first
    opened the incident.

    The incident takes the first trigger's `trigger` and `summary`, and the earliest
    `started_at`. `service` is the one service every trigger that names one agrees on, else
    `None`. `shape` and `fingerprint` come together from the first trigger with either. Hops
    and fragments follow the triggers' order, and one equal to an earlier one is kept once. A
    `missing` fragment is dropped when the merged dossier holds what it says is absent: a
    `present` fragment of its collector whose `data` holds each of its `data` keys with the same
    value, or, for `shape` and `process_path`, the dossier's own shape or hops. Each
    `DossierTrigger` lists the indexes of the fragments its event gathered that were kept.
    """
    if not triggers:
        raise ValueError("an incident has at least one trigger")
    events = [accepted.event for accepted in triggers]
    opening = events[0]
    services = {event.service for event in events if event.service is not None}
    service = next(iter(services)) if len(services) == 1 else None
    structured = next(
        (event for event in events if event.shape is not None or event.fingerprint is not None),
        None,
    )
    hops: list[ProcessHop] = []
    for event in events:
        for hop in event.process_path.hops:
            if hop not in hops:
                hops.append(hop)

    kept: list[EvidenceFragment] = []
    gathered: list[list[int]] = []
    for event in events:
        indexes: list[int] = []
        for fragment in event.fragments:
            if fragment not in kept:
                kept.append(fragment)
            index = kept.index(fragment)
            if index not in indexes:
                indexes.append(index)
        gathered.append(indexes)
    position: dict[int, int] = {}
    fragments: list[EvidenceFragment] = []
    for index, fragment in enumerate(kept):
        if not _superseded(fragment, kept, has_shape=structured is not None, has_hops=bool(hops)):
            position[index] = len(fragments)
            fragments.append(fragment)

    incident = Incident(
        id=incident_id,
        system=opening.system,
        trigger=opening.trigger,
        triggers=[event.trigger for event in events],
        service=service,
        summary=opening.summary,
        started_at=min(event.started_at for event in events),
    )
    dossier = Dossier(
        incident_id=incident_id,
        system=opening.system,
        service=service,
        shape=None if structured is None else structured.shape,
        fingerprint=None if structured is None else structured.fingerprint,
        process_path=ProcessPath(hops=hops),
        fragments=fragments,
        triggers=[
            DossierTrigger(
                trigger=accepted.event.trigger,
                summary=accepted.event.summary,
                service=accepted.event.service,
                started_at=accepted.event.started_at,
                accepted_at=accepted.accepted_at,
                fragments=[position[index] for index in indexes if index in position],
            )
            for accepted, indexes in zip(triggers, gathered, strict=True)
        ],
    )
    return incident, dossier.model_copy(deep=True)


def _superseded(
    fragment: EvidenceFragment,
    fragments: Sequence[EvidenceFragment],
    *,
    has_shape: bool,
    has_hops: bool,
) -> bool:
    if fragment.quality is not EvidenceQuality.MISSING:
        return False
    if (fragment.collector == "shape" and has_shape) or (
        fragment.collector == "process_path" and has_hops
    ):
        return True
    return any(
        other.quality is EvidenceQuality.PRESENT
        and other.collector == fragment.collector
        and all(
            key in other.data and other.data[key] == value for key, value in fragment.data.items()
        )
        for other in fragments
    )
