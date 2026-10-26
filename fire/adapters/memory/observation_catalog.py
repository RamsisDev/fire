"""ObservationCatalog in process memory. The dedupe index, baselines, open flows, and paths live
as long as the process; `SqliteObservationCatalog` writes all but open flows through to a file.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Collection, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from fire.core.domain.baseline import Baseline, DriftPolicy
from fire.core.domain.dlq import WorkOutcomes, WorkRef, tally_outcomes
from fire.core.domain.dossier import Shape
from fire.core.domain.drift import Admission, ShapeAdmission
from fire.core.domain.flow import (
    CompleteFlow,
    FlowAssembler,
    FlowCounters,
    Node,
    PathPolicy,
    ShapeRef,
)
from fire.core.domain.observation import (
    Channel,
    ChannelKind,
    FingerprintSummary,
    ShapeObservation,
    SpanObservation,
    StreamSummary,
    TracedShape,
    WorkEntry,
)
from fire.core.domain.path import Path, PathDrift, PathsReport, settle

# Seven days: how long a dedupe key and a work index entry are kept by default.
DEFAULT_RETENTION_S = 7 * 24 * 3600.0


@dataclass
class FingerprintState:
    shape: Shape
    observations: int
    first_observed_at: datetime
    last_observed_at: datetime


@dataclass
class StreamState:
    channel: Channel
    fingerprints: dict[str, FingerprintState] = field(default_factory=dict)
    baseline: Baseline = field(default_factory=Baseline)


@dataclass(frozen=True)
class _IndexEntry:
    """One accepted shape observation, without its shape."""

    stream_key: tuple[str, str, str]
    schema_hash: str
    occurred_at: datetime
    observed_at: datetime
    accepted_at: datetime
    region: str | None
    trace_id: str | None
    span_id: str | None
    work_id: str | None
    correlation_id: str | None


@dataclass
class FlowState:
    """One system's open flows and its paths, one per entry."""

    assembler: FlowAssembler
    paths: dict[Node, Path] = field(default_factory=dict)


def _utc_now() -> datetime:
    return datetime.now(UTC)


class MemoryObservationCatalog:
    """Thread-safe: one lock covers every read and write.

    `policies` maps a system to its drift policy and `path_policies` to its path policy; a
    system either does not name gets the defaults. `clock` is the body's clock, the current UTC
    time by default: it stamps each observation's acceptance and decides when flows settle.
    `retention_s` is how long a dedupe key, and the work and trace index entries of its shape,
    are kept after acceptance; `complete_flows` drops the ones past it.
    """

    def __init__(
        self,
        policies: Mapping[str, DriftPolicy] | None = None,
        *,
        path_policies: Mapping[str, PathPolicy] | None = None,
        clock: Callable[[], datetime] | None = None,
        retention_s: float = DEFAULT_RETENTION_S,
    ) -> None:
        self._lock = threading.Lock()
        self._policies = dict(policies or {})
        self._default_policy = DriftPolicy()
        self._path_policies = dict(path_policies or {})
        self._clock = clock or _utc_now
        self._retention = timedelta(seconds=retention_s)
        # Insertion order is acceptance order, so `_forget` stops at the first fresh entry.
        self._index: dict[tuple[str, str, str], _IndexEntry] = {}
        self._spans: dict[tuple[str, str, str], datetime] = {}
        self._streams: dict[str, dict[tuple[str, str], StreamState]] = {}
        self._flows: dict[str, FlowState] = {}
        # `(system, trace_id)` to the dedupe keys in `_index` that carry it, in the order stored.
        self._traced: dict[tuple[str, str], dict[tuple[str, str, str], None]] = {}

    def _flows_of(self, system: str) -> FlowState:
        """The system's flows, created on first use; the caller holds the lock."""
        flows = self._flows.get(system)
        if flows is None:
            policy = self._path_policies.get(system, PathPolicy())
            flows = self._flows[system] = FlowState(FlowAssembler(policy))
        return flows

    def _fresh(self, accepted_at: datetime, now: datetime) -> bool:
        return now - accepted_at < self._retention

    @contextmanager
    def _step(self) -> Iterator[None]:
        """Wraps the writes of one operation; the caller holds the lock."""
        yield

    def _claim_shape(self, observation: ShapeObservation, now: datetime) -> bool:
        """Stores the shape's index entry unless its dedupe key is fresh; the caller holds the
        lock.
        """
        key = observation.dedupe_key
        entry = self._index.get(key)
        if entry is not None and self._fresh(entry.accepted_at, now):
            return False
        if entry is not None:
            self._untrace(key, entry)
        self._index.pop(key, None)
        if observation.trace_id is not None:
            self._traced.setdefault((observation.system, observation.trace_id), {})[key] = None
        self._index[key] = _IndexEntry(
            stream_key=observation.stream_key,
            schema_hash=observation.fingerprint.schema_hash,
            occurred_at=observation.occurred_at,
            observed_at=observation.observed_at,
            accepted_at=now,
            region=observation.region,
            trace_id=observation.trace_id,
            span_id=observation.span_id,
            work_id=observation.work_id,
            correlation_id=observation.correlation_id,
        )
        return True

    def _untrace(self, key: tuple[str, str, str], entry: _IndexEntry) -> None:
        """Drops an index entry's key from the trace index; the caller holds the lock."""
        if entry.trace_id is None:
            return
        traced = (key[0], entry.trace_id)
        keys = self._traced.get(traced)
        if keys is None:
            return
        keys.pop(key, None)
        if not keys:
            del self._traced[traced]

    def _claim_span(self, observation: SpanObservation, now: datetime) -> bool:
        key = observation.dedupe_key
        accepted_at = self._spans.get(key)
        if accepted_at is not None and self._fresh(accepted_at, now):
            return False
        self._spans.pop(key, None)
        self._spans[key] = now
        return True

    def _stored_shape(
        self, observation: ShapeObservation, stream: StreamState, admission: ShapeAdmission
    ) -> None:
        """Called once a stored shape changed its stream; the caller holds the lock."""

    def _stored_paths(self, system: str, paths: Mapping[Node, Path], entries: set[Node]) -> None:
        """Called once complete flows changed the paths of `entries`; the caller holds the lock."""

    def _forget(self, now: datetime) -> None:
        """Drops the index entries past the retention window; the caller holds the lock."""
        while self._index:
            key, entry = next(iter(self._index.items()))
            if self._fresh(entry.accepted_at, now):
                break
            self._untrace(key, entry)
            del self._index[key]
        while self._spans:
            key, accepted_at = next(iter(self._spans.items()))
            if self._fresh(accepted_at, now):
                break
            del self._spans[key]

    def add_shape(self, observation: ShapeObservation) -> ShapeAdmission:
        schema_hash = observation.fingerprint.schema_hash
        observed_at = observation.observed_at
        policy = self._policies.get(observation.system, self._default_policy)
        now = self._clock()
        with self._lock, self._step():
            if not self._claim_shape(observation, now):
                return ShapeAdmission(outcome=Admission.DUPLICATE)
            streams = self._streams.setdefault(observation.system, {})
            stream = streams.setdefault(
                (observation.source, observation.channel.key), StreamState(observation.channel)
            )
            seen = stream.fingerprints.get(schema_hash)
            if seen is None:
                stream.fingerprints[schema_hash] = FingerprintState(
                    observation.shape, 1, observed_at, observed_at
                )
            else:
                seen.observations += 1
                seen.first_observed_at = min(seen.first_observed_at, observed_at)
                seen.last_observed_at = max(seen.last_observed_at, observed_at)
            admission = stream.baseline.admit(schema_hash, observation.shape, observed_at, policy)
            self._stored_shape(observation, stream, admission)
            joins = observation.trace_id is not None or observation.work_id is not None
            if joins and observation.channel.kind is not ChannelKind.OUTCOME:
                self._flows_of(observation.system).assembler.add_shape(
                    ShapeRef(
                        channel_key=observation.channel.key,
                        trace_id=observation.trace_id,
                        span_id=observation.span_id,
                        work_id=observation.work_id,
                    ),
                    now,
                )
            return admission

    def add_span(self, observation: SpanObservation) -> bool:
        now = self._clock()
        with self._lock, self._step():
            if not self._claim_span(observation, now):
                return False
            self._flows_of(observation.system).assembler.add_span(observation, now)
            return True

    def work(self, system: str, work_id: str) -> list[WorkEntry]:
        now = self._clock()
        with self._lock:
            entries = [
                WorkEntry(
                    source=source,
                    key=entry.stream_key[2],
                    observation_id=observation_id,
                    schema_hash=entry.schema_hash,
                    occurred_at=entry.occurred_at,
                    observed_at=entry.observed_at,
                    accepted_at=entry.accepted_at,
                    trace_id=entry.trace_id,
                )
                for (entry_system, source, observation_id), entry in self._index.items()
                if entry_system == system
                and entry.work_id == work_id
                and self._fresh(entry.accepted_at, now)
            ]
        return sorted(
            entries, key=lambda entry: (entry.observed_at, entry.source, entry.observation_id)
        )

    def work_outcomes(self, system: str, outcome_sources: Collection[str]) -> WorkOutcomes:
        now = self._clock()
        with self._lock:
            refs = [
                WorkRef(source, entry.stream_key[2], entry.schema_hash, work_id, entry.occurred_at)
                for (entry_system, source, _id), entry in self._index.items()
                if entry_system == system
                and (work_id := entry.work_id) is not None
                and self._fresh(entry.accepted_at, now)
            ]
        return tally_outcomes(refs, outcome_sources)

    def shape(self, system: str, source: str, key: str, schema_hash: str) -> Shape | None:
        with self._lock:
            stream = self._streams.get(system, {}).get((source, key))
            seen = None if stream is None else stream.fingerprints.get(schema_hash)
            return None if seen is None else seen.shape.model_copy(deep=True)

    def complete_flows(self) -> list[PathDrift]:
        now = self._clock()
        drifts: list[PathDrift] = []
        with self._lock, self._step():
            for system in sorted(self._flows):
                flows = self._flows[system]
                policy = self._policies.get(system, self._default_policy)
                entries: set[Node] = set()
                for flow in flows.assembler.complete(now):
                    _outcome, drift = settle(
                        flow, flows.paths, policy, flows.assembler.counters, system
                    )
                    if flow.entry is not None:
                        entries.add(Node.of(flow.entry))
                    if drift is not None:
                        drifts.append(drift)
                if entries:
                    self._stored_paths(system, flows.paths, entries)
            self._forget(now)
        return drifts

    def paths(self, system: str) -> PathsReport:
        policy = self._policies.get(system, self._default_policy)
        with self._lock:
            flows = self._flows.get(system)
            if flows is None:
                return PathsReport(system=system, open_flows=0, counters=FlowCounters(), paths=[])
            return PathsReport(
                system=system,
                open_flows=flows.assembler.open_flows,
                counters=flows.assembler.counters.model_copy(),
                paths=[
                    flows.paths[entry].summary(policy)
                    for entry in sorted(flows.paths, key=lambda entry: entry.sort_key)
                ],
            )

    def open_flows(self, system: str, trace_ids: Iterable[str]) -> list[CompleteFlow]:
        with self._lock:
            flows = self._flows.get(system)
            if flows is None:
                return []
            return flows.assembler.snapshot(trace_ids)

    def traced_shapes(self, system: str, trace_ids: Iterable[str]) -> list[TracedShape]:
        now = self._clock()
        with self._lock:
            streams = self._streams.get(system, {})
            shapes: list[TracedShape] = []
            for trace_id in dict.fromkeys(trace_ids):
                for key in self._traced.get((system, trace_id), {}):
                    entry = self._index[key]
                    if not self._fresh(entry.accepted_at, now):
                        continue
                    _system, source, channel_key = entry.stream_key
                    shapes.append(
                        TracedShape(
                            source=source,
                            channel=streams[(source, channel_key)].channel,
                            trace_id=trace_id,
                            schema_hash=entry.schema_hash,
                        )
                    )
            return shapes

    def streams(self, system: str) -> list[StreamSummary]:
        policy = self._policies.get(system, self._default_policy)
        with self._lock:
            streams = sorted(self._streams.get(system, {}).items(), key=lambda item: item[0])
            return [
                StreamSummary(
                    source=source,
                    channel=stream.channel,
                    status=stream.baseline.status(policy),
                    samples=stream.baseline.samples,
                    fingerprints=sorted(
                        (
                            FingerprintSummary(
                                schema_hash=schema_hash,
                                observations=seen.observations,
                                first_observed_at=seen.first_observed_at,
                                last_observed_at=seen.last_observed_at,
                            )
                            for schema_hash, seen in stream.fingerprints.items()
                        ),
                        key=lambda summary: (-summary.observations, summary.schema_hash),
                    ),
                    drift=[
                        signal.model_copy(deep=True)
                        for signal in sorted(
                            stream.baseline.signals.values(),
                            key=lambda signal: (signal.first_seen, signal.schema_hash),
                        )
                    ],
                )
                for (source, _key), stream in streams
            ]

    def close(self) -> None:
        """Nothing to release in memory."""
