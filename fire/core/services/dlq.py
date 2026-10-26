"""DLQ attribution: join each system's abandoned work units to the fingerprints their observations
carried, and turn a fingerprint whose abandon rate crosses `triggers.dlq` into a trigger event.

Contract: docs/specs/ingest-drift-and-feedback.md#dlq-attribution.
"""

from __future__ import annotations

import threading
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field

from pydantic import BaseModel, ConfigDict, Field

from fire.core.domain.dlq import (
    DlqTrigger,
    FingerprintOutcomes,
    StreamOutcomes,
    WorkOutcomes,
)
from fire.core.domain.dossier import EvidenceFragment, EvidenceQuality
from fire.core.domain.incident import Trigger
from fire.core.domain.observation import ShapeObservation
from fire.core.domain.trigger_event import TriggerEvent
from fire.core.ports.observation_catalog import ObservationCatalog
from fire.core.ports.shape_fingerprinter import ShapeFingerprinter

FingerprintRef = tuple[str, str, str]


class DlqFingerprint(FingerprintOutcomes):
    """A fingerprint's outcomes and whether it crosses its system's rule now."""

    firing: bool


class DlqReport(BaseModel):
    """A system's work outcomes for `GET /v0/systems/{system}/dlq`: exactly one of `sources`
    and `missing_reason`.
    """

    model_config = ConfigDict(extra="forbid")

    system: str
    missing_reason: str | None = None
    sources: list[str] = Field(default_factory=list)
    window_s: float | None = None
    rule: DlqTrigger | None = None
    abandoned: int = 0
    unattributed: list[str] = Field(default_factory=list)
    fingerprints: list[DlqFingerprint] = Field(default_factory=list)
    streams: list[StreamOutcomes] = Field(default_factory=list)


@dataclass(frozen=True)
class DlqEvaluation:
    """What one batch's abandoned work units did: the system's outcomes after it, the
    fingerprints that crossed the rule with it, and its work units no observation joins.
    """

    outcomes: WorkOutcomes
    raised: list[FingerprintOutcomes] = field(default_factory=list)
    unattributed: list[ShapeObservation] = field(default_factory=list)


class DlqService:
    """Watches each system's outcome sources and opens one trigger event per fingerprint that
    crosses its `triggers.dlq`.

    `sources` maps a system to the ids of its sources with `outcome: abandoned`; a stored shape
    from one of them with a `work_id` marks that work unit abandoned. `evaluate` runs once per
    batch that stored such shapes: it tallies the system's outcomes from the catalog and raises
    the fingerprints that cross the rule now and did not at the last evaluation, so a
    fingerprint raises again only after an evaluation finds it below the rule. The firing set
    lives in process memory. `window_s` is the catalog's retention window, which bounds every
    count.
    """

    def __init__(
        self,
        catalog: ObservationCatalog,
        fingerprinter: ShapeFingerprinter,
        sources: Mapping[str, Collection[str]],
        triggers: Mapping[str, DlqTrigger],
        window_s: float,
    ) -> None:
        self._catalog = catalog
        self._fingerprinter = fingerprinter
        self._sources = {system: frozenset(ids) for system, ids in sources.items() if ids}
        self._triggers = dict(triggers)
        self._window_s = window_s
        self._firing: dict[str, set[FingerprintRef]] = {}
        self._lock = threading.Lock()

    def abandoned(self, observation: ShapeObservation) -> bool:
        return observation.work_id is not None and observation.source in self._sources.get(
            observation.system, ()
        )

    def evaluate(self, system: str, abandoned: Sequence[ShapeObservation]) -> DlqEvaluation:
        trigger = self._triggers.get(system)
        with self._lock:
            outcomes = self._catalog.work_outcomes(system, self._sources.get(system, ()))
            above = {
                fingerprint.ref
                for fingerprint in outcomes.fingerprints
                if trigger is not None and trigger.crosses(fingerprint)
            }
            before = self._firing.get(system, set())
            self._firing[system] = above
        unattributed = set(outcomes.unattributed)
        return DlqEvaluation(
            outcomes=outcomes,
            raised=[f for f in outcomes.fingerprints if f.ref in above - before],
            unattributed=[o for o in abandoned if o.work_id in unattributed],
        )

    def report(self, system: str) -> DlqReport:
        sources = self._sources.get(system)
        if sources is None:
            return DlqReport(system=system, missing_reason="No source declares outcome: abandoned")
        outcomes = self._catalog.work_outcomes(system, sources)
        with self._lock:
            firing = set(self._firing.get(system, set()))
        return DlqReport(
            system=system,
            sources=sorted(sources),
            window_s=self._window_s,
            rule=self._triggers.get(system),
            abandoned=outcomes.abandoned,
            unattributed=outcomes.unattributed,
            fingerprints=[
                DlqFingerprint(**f.model_dump(exclude={"rate"}), firing=f.ref in firing)
                for f in outcomes.fingerprints
            ],
            streams=outcomes.streams,
        )

    def trigger_event(
        self, system: str, fingerprint: FingerprintOutcomes, outcomes: WorkOutcomes
    ) -> TriggerEvent:
        """The fingerprint's dossier evidence: `dlq_attribution`, `dlq_unattributed` when the
        window holds abandoned work units no source read, and a missing `process_path`. Only for
        a system with `triggers.dlq`.
        """
        source, stream, schema_hash = fingerprint.ref
        shape = self._catalog.shape(system, source, stream, schema_hash)
        drifted = any(
            summary.source == source
            and summary.key == stream
            and any(signal.schema_hash == schema_hash for signal in summary.drift)
            for summary in self._catalog.streams(system)
        )
        fragments = [self._attribution(system, fingerprint, outcomes, drifted)]
        if outcomes.unattributed:
            fragments.append(
                EvidenceFragment(
                    collector="dlq_unattributed",
                    quality=EvidenceQuality.MISSING,
                    summary=(
                        f"{len(outcomes.unattributed)} of {outcomes.abandoned} abandoned work "
                        "units carry no observation: no source read their event"
                    ),
                    data={
                        "abandoned": outcomes.abandoned,
                        "unattributed": len(outcomes.unattributed),
                    },
                )
            )
        fragments.append(
            EvidenceFragment(
                collector="process_path",
                quality=EvidenceQuality.MISSING,
                summary="No flow of the abandoned work units",
            )
        )
        return TriggerEvent(
            system=system,
            trigger=Trigger.DLQ,
            summary=f"Abandoned work units on {stream}",
            started_at=fingerprint.first_abandoned,
            shape=shape,
            fingerprint=None if shape is None else self._fingerprinter.fingerprint(shape),
            fragments=fragments,
        )

    def _attribution(
        self,
        system: str,
        fingerprint: FingerprintOutcomes,
        outcomes: WorkOutcomes,
        drifted: bool,
    ) -> EvidenceFragment:
        trigger = self._triggers[system]
        stream = next(
            s
            for s in outcomes.streams
            if (s.source, s.stream) == (fingerprint.source, fingerprint.stream)
        )
        return EvidenceFragment(
            collector="dlq_attribution",
            quality=EvidenceQuality.PRESENT,
            summary=(
                f"{fingerprint.abandoned} of {fingerprint.observed} work units with fingerprint "
                f"{fingerprint.schema_hash} on {fingerprint.stream} abandoned "
                f"({fingerprint.rate:.0%})"
            ),
            data={
                **fingerprint.model_dump(mode="json"),
                "drifted": drifted,
                "stream_observed": stream.observed,
                "stream_abandoned": stream.abandoned,
                "min_abandoned": trigger.min_abandoned,
                "min_rate": trigger.min_rate,
                "window_s": self._window_s,
            },
        )
