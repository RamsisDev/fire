"""incident.io incidents: a verified webhook raises one trigger event per system its incident maps
to, once per incident.io incident.

Contract: docs/specs/ingest-drift-and-feedback.md#incidentio-webhook.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

from fire.core.domain.dossier import EvidenceFragment, EvidenceQuality
from fire.core.domain.incident import Trigger
from fire.core.domain.incident_io import (
    IncidentIoEvent,
    IncidentIoMatch,
    IncidentIoOutcome,
    IncidentIoTrigger,
    match_incident,
    read_event,
)
from fire.core.domain.trigger_event import TriggerEvent


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass
class IncidentIoReceipt:
    """What one webhook did: its outcome, its event type when handled, the incident.io incident
    id when read, and the trigger events it raised, which the caller correlates.
    """

    outcome: IncidentIoOutcome
    event_type: str | None = None
    incident_id: str | None = None
    events: list[TriggerEvent] = field(default_factory=list)


class IncidentIoService:
    """Maps incident.io incidents to systems through each system's `triggers.incident_io`.

    An incident.io incident raises at most one trigger event per system: the first created,
    updated, or status updated event that maps it does, and later ones are `duplicate`. Which
    incidents raised is kept in process memory. `counts` holds every outcome since startup.
    """

    def __init__(
        self,
        triggers: Mapping[str, IncidentIoTrigger],
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._triggers = dict(triggers)
        self._clock = clock
        self._opened: set[tuple[str, str]] = set()
        self.counts: Counter[IncidentIoOutcome] = Counter()

    def receive(self, body: bytes) -> IncidentIoReceipt:
        receipt = self._receive(body)
        self.counts[receipt.outcome] += 1
        return receipt

    def _receive(self, body: bytes) -> IncidentIoReceipt:
        event = read_event(body)
        if isinstance(event, IncidentIoOutcome):
            return IncidentIoReceipt(event)
        receipt = IncidentIoReceipt(
            IncidentIoOutcome.UNMAPPED, event.event_type, event.incident.id
        )
        matches = {
            system: found
            for system, trigger in self._triggers.items()
            if (found := match_incident(trigger, event.incident)) is not None
        }
        if not matches:
            return receipt
        receipt.outcome = IncidentIoOutcome.DUPLICATE
        for system, found in matches.items():
            key = (system, event.incident.id)
            if key in self._opened:
                continue
            self._opened.add(key)
            receipt.events.append(self._event(system, event, found))
            receipt.outcome = IncidentIoOutcome.OPENED
        return receipt

    def _event(self, system: str, event: IncidentIoEvent, found: IncidentIoMatch) -> TriggerEvent:
        incident_io = event.incident
        name = incident_io.reference or incident_io.id
        return TriggerEvent(
            system=system,
            trigger=Trigger.INCIDENT_IO,
            summary=f"incident.io incident {name}",
            started_at=incident_io.created_at or self._clock(),
            fragments=[
                _incident_io(event, found),
                EvidenceFragment(
                    collector="process_path",
                    quality=EvidenceQuality.MISSING,
                    summary="No flow linked to the incident.io incident",
                ),
                EvidenceFragment(
                    collector="shape",
                    quality=EvidenceQuality.MISSING,
                    summary="No shape linked to the incident.io incident",
                ),
            ],
        )


def _incident_io(event: IncidentIoEvent, found: IncidentIoMatch) -> EvidenceFragment:
    incident = event.incident
    status = incident.incident_status
    severity = incident.severity
    category = status.category if status else None
    rank = severity.rank if severity else None
    matched: dict[str, object] = {"rule": found.rule, "by": found.by, "value": found.value}
    if found.custom_field_id is not None:
        matched["custom_field_id"] = found.custom_field_id
    return EvidenceFragment(
        collector="incident_io",
        quality=EvidenceQuality.PRESENT,
        summary=(
            f"incident.io incident {incident.reference or incident.id}: "
            f"status={category or 'unknown'} "
            f"severity_rank={'unknown' if rank is None else rank}"
        ),
        data={
            "event_type": event.event_type,
            "incident_id": incident.id,
            "reference": incident.reference,
            "mode": incident.mode,
            "created_at": incident.created_at.isoformat() if incident.created_at else None,
            "status_id": status.id if status else None,
            "status_category": category,
            "severity_id": severity.id if severity else None,
            "severity_rank": rank,
            "incident_type_id": incident.incident_type.id if incident.incident_type else None,
            "team_ids": list(incident.team_ids),
            "matched": matched,
        },
    )
