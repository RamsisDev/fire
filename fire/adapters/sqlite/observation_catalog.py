"""ObservationCatalog in a SQLite file: the memory catalog, with the dedupe index, the work index,
baselines, drift signals, and paths written through to the file and read back when it opens.

Open flows and the flow counters stay in memory, so a restart loses them. Reads of streams,
fingerprint shapes, and paths are served from memory; the dedupe, work, and trace index, and the
work outcomes, are read from the file. One body opens one file.
"""

from __future__ import annotations

import json
import os
import sqlite3
from collections import Counter
from collections.abc import Callable, Collection, Iterable, Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from fire.adapters.memory.observation_catalog import (
    DEFAULT_RETENTION_S,
    FingerprintState,
    MemoryObservationCatalog,
    StreamState,
)
from fire.core.domain.baseline import Baseline, DriftPolicy
from fire.core.domain.dlq import WorkOutcomes, WorkRef, tally_outcomes
from fire.core.domain.dossier import HopVia, Shape, ValueType
from fire.core.domain.drift import Admission, DriftSignal, ShapeAdmission
from fire.core.domain.flow import Node, PathPolicy
from fire.core.domain.observation import (
    Channel,
    ShapeObservation,
    SpanKind,
    SpanObservation,
    TracedShape,
    WorkEntry,
)
from fire.core.domain.path import EdgeStats, NodeStats, Path, PathDriftSignal

SCHEMA_VERSION = 1

_SCHEMA = """
CREATE TABLE IF NOT EXISTS shape_index (
    system TEXT NOT NULL,
    source TEXT NOT NULL,
    observation_id TEXT NOT NULL,
    channel_key TEXT NOT NULL,
    schema_hash TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    accepted_at TEXT NOT NULL,
    region TEXT,
    trace_id TEXT,
    span_id TEXT,
    work_id TEXT,
    correlation_id TEXT,
    PRIMARY KEY (system, source, observation_id)
);
CREATE INDEX IF NOT EXISTS shape_index_work ON shape_index (system, work_id)
    WHERE work_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS shape_index_trace ON shape_index (system, trace_id)
    WHERE trace_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS shape_index_accepted ON shape_index (accepted_at);
CREATE TABLE IF NOT EXISTS span_index (
    system TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    accepted_at TEXT NOT NULL,
    PRIMARY KEY (system, trace_id, span_id)
);
CREATE INDEX IF NOT EXISTS span_index_accepted ON span_index (accepted_at);
CREATE TABLE IF NOT EXISTS streams (
    system TEXT NOT NULL,
    source TEXT NOT NULL,
    channel_key TEXT NOT NULL,
    channel TEXT NOT NULL,
    samples INTEGER NOT NULL,
    PRIMARY KEY (system, source, channel_key)
);
CREATE TABLE IF NOT EXISTS fingerprints (
    system TEXT NOT NULL,
    source TEXT NOT NULL,
    channel_key TEXT NOT NULL,
    schema_hash TEXT NOT NULL,
    shape TEXT NOT NULL,
    observations INTEGER NOT NULL,
    first_observed_at TEXT NOT NULL,
    last_observed_at TEXT NOT NULL,
    folded INTEGER NOT NULL,
    PRIMARY KEY (system, source, channel_key, schema_hash)
);
CREATE TABLE IF NOT EXISTS presence (
    system TEXT NOT NULL,
    source TEXT NOT NULL,
    channel_key TEXT NOT NULL,
    path TEXT NOT NULL,
    value_type TEXT NOT NULL,
    count INTEGER NOT NULL,
    PRIMARY KEY (system, source, channel_key, path, value_type)
);
CREATE TABLE IF NOT EXISTS drift_signals (
    system TEXT NOT NULL,
    source TEXT NOT NULL,
    channel_key TEXT NOT NULL,
    schema_hash TEXT NOT NULL,
    signal TEXT NOT NULL,
    PRIMARY KEY (system, source, channel_key, schema_hash)
);
CREATE TABLE IF NOT EXISTS paths (
    system TEXT NOT NULL,
    service TEXT NOT NULL,
    operation TEXT NOT NULL,
    path TEXT NOT NULL,
    PRIMARY KEY (system, service, operation)
);
"""


def _text(moment: datetime) -> str:
    """Fixed-width UTC, so text order is time order."""
    return moment.astimezone(UTC).isoformat(timespec="microseconds")


def _time(text: str) -> datetime:
    return datetime.fromisoformat(text)


def _dump_path(path: Path) -> str:
    return json.dumps(
        {
            "entry": path.entry.model_dump(),
            "flows": path.flows,
            "drifted_flows": path.drifted_flows,
            "broken_flows": path.broken_flows,
            "truncated_flows": path.truncated_flows,
            "nodes": [
                {
                    "node": node.model_dump(),
                    "kinds": sorted(stats.kinds),
                    "flows": stats.flows,
                    "spans": stats.spans,
                    "errors": stats.errors,
                    "channels": dict(stats.channels),
                }
                for node, stats in path.nodes.items()
            ],
            "edges": [
                {
                    "from": source.model_dump(),
                    "to": target.model_dump(),
                    "via": via,
                    "flows": stats.flows,
                    "count": stats.count,
                    "errors": stats.errors,
                }
                for (source, target, via), stats in path.edges.items()
            ],
            "flow_channels": dict(path.flow_channels),
            "signals": [signal.model_dump(mode="json") for signal in path.signals.values()],
        }
    )


def _load_path(text: str) -> Path:
    raw: dict[str, Any] = json.loads(text)
    path = Path(
        entry=Node.model_validate(raw["entry"]),
        flows=raw["flows"],
        drifted_flows=raw["drifted_flows"],
        broken_flows=raw["broken_flows"],
        truncated_flows=raw["truncated_flows"],
        flow_channels=Counter(raw["flow_channels"]),
    )
    for item in raw["nodes"]:
        path.nodes[Node.model_validate(item["node"])] = NodeStats(
            kinds={SpanKind(kind) for kind in item["kinds"]},
            flows=item["flows"],
            spans=item["spans"],
            errors=item["errors"],
            channels=Counter(item["channels"]),
        )
    for item in raw["edges"]:
        key = (Node.model_validate(item["from"]), Node.model_validate(item["to"]))
        path.edges[(*key, HopVia(item["via"]))] = EdgeStats(
            flows=item["flows"], count=item["count"], errors=item["errors"]
        )
    for item in raw["signals"]:
        signal = PathDriftSignal.model_validate(item)
        path.signals[(signal.rule, signal.node)] = signal
    return path


class SqliteObservationCatalog(MemoryObservationCatalog):
    """Thread-safe: one connection, used only under the catalog's lock, and one transaction per
    operation. Takes the memory catalog's arguments, after the file's `path`; the file and its
    directory are created when missing.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        policies: Mapping[str, DriftPolicy] | None = None,
        *,
        path_policies: Mapping[str, PathPolicy] | None = None,
        clock: Callable[[], datetime] | None = None,
        retention_s: float = DEFAULT_RETENTION_S,
    ) -> None:
        super().__init__(
            policies, path_policies=path_policies, clock=clock, retention_s=retention_s
        )
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False)
        try:
            self._open()
        except Exception:
            self._db.close()
            raise

    def _open(self) -> None:
        version = self._db.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, SCHEMA_VERSION):
            raise ValueError(
                f"catalog schema version {version} is not {SCHEMA_VERSION}: move the file aside"
            )
        self._db.execute("PRAGMA journal_mode = WAL")
        self._db.execute("PRAGMA synchronous = NORMAL")
        self._db.executescript(_SCHEMA)
        self._db.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        self._load()

    def _load(self) -> None:
        db = self._db
        for system, source, key, channel, samples in db.execute(
            "SELECT system, source, channel_key, channel, samples FROM streams"
        ):
            self._streams.setdefault(system, {})[(source, key)] = StreamState(
                Channel.model_validate_json(channel), baseline=Baseline(samples=samples)
            )
        for (
            system,
            source,
            key,
            schema_hash,
            shape,
            observations,
            first,
            last,
            folded,
        ) in db.execute(
            "SELECT system, source, channel_key, schema_hash, shape, observations, "
            "first_observed_at, last_observed_at, folded FROM fingerprints"
        ):
            stream = self._streams[system][(source, key)]
            stream.fingerprints[schema_hash] = FingerprintState(
                Shape.model_validate_json(shape), observations, _time(first), _time(last)
            )
            if folded:
                stream.baseline.fingerprints.add(schema_hash)
        for system, source, key, path, value_type, count in db.execute(
            "SELECT system, source, channel_key, path, value_type, count FROM presence"
        ):
            baseline = self._streams[system][(source, key)].baseline
            baseline.presence[(path, ValueType(value_type))] = count
        for system, source, key, schema_hash, signal in db.execute(
            "SELECT system, source, channel_key, schema_hash, signal FROM drift_signals"
        ):
            baseline = self._streams[system][(source, key)].baseline
            baseline.signals[schema_hash] = DriftSignal.model_validate_json(signal)
        for system, service, operation, path in db.execute(
            "SELECT system, service, operation, path FROM paths"
        ):
            entry = Node(service=service, operation=operation)
            self._flows_of(system).paths[entry] = _load_path(path)

    @contextmanager
    def _step(self) -> Iterator[None]:
        with self._db:
            yield

    def _claim_shape(self, observation: ShapeObservation, now: datetime) -> bool:
        row = self._db.execute(
            "SELECT accepted_at FROM shape_index "
            "WHERE system = ? AND source = ? AND observation_id = ?",
            observation.dedupe_key,
        ).fetchone()
        if row is not None and self._fresh(_time(row[0]), now):
            return False
        self._db.execute(
            "INSERT OR REPLACE INTO shape_index VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                *observation.dedupe_key,
                observation.channel.key,
                observation.fingerprint.schema_hash,
                _text(observation.occurred_at),
                _text(observation.observed_at),
                _text(now),
                observation.region,
                observation.trace_id,
                observation.span_id,
                observation.work_id,
                observation.correlation_id,
            ),
        )
        return True

    def _claim_span(self, observation: SpanObservation, now: datetime) -> bool:
        row = self._db.execute(
            "SELECT accepted_at FROM span_index WHERE system = ? AND trace_id = ? AND span_id = ?",
            observation.dedupe_key,
        ).fetchone()
        if row is not None and self._fresh(_time(row[0]), now):
            return False
        self._db.execute(
            "INSERT OR REPLACE INTO span_index VALUES (?, ?, ?, ?)",
            (*observation.dedupe_key, _text(now)),
        )
        return True

    def _stored_shape(
        self, observation: ShapeObservation, stream: StreamState, admission: ShapeAdmission
    ) -> None:
        stream_key = observation.stream_key
        schema_hash = observation.fingerprint.schema_hash
        baseline = stream.baseline
        seen = stream.fingerprints[schema_hash]
        db = self._db
        db.execute(
            "INSERT INTO streams VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT (system, source, channel_key) DO UPDATE SET samples = excluded.samples",
            (*stream_key, stream.channel.model_dump_json(), baseline.samples),
        )
        db.execute(
            "INSERT INTO fingerprints VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (system, source, channel_key, schema_hash) DO UPDATE SET "
            "observations = excluded.observations, "
            "first_observed_at = excluded.first_observed_at, "
            "last_observed_at = excluded.last_observed_at, folded = excluded.folded",
            (
                *stream_key,
                schema_hash,
                seen.shape.model_dump_json(),
                seen.observations,
                _text(seen.first_observed_at),
                _text(seen.last_observed_at),
                int(schema_hash in baseline.fingerprints),
            ),
        )
        if admission.outcome is Admission.FOLDED:
            pairs = {(f.path, f.value_type) for f in observation.shape.fields}
            db.executemany(
                "INSERT INTO presence VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (system, source, channel_key, path, value_type) "
                "DO UPDATE SET count = excluded.count",
                [
                    (*stream_key, path, value_type.value, baseline.presence[(path, value_type)])
                    for path, value_type in sorted(pairs)
                ],
            )
        else:
            db.execute(
                "INSERT INTO drift_signals VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT (system, source, channel_key, schema_hash) "
                "DO UPDATE SET signal = excluded.signal",
                (*stream_key, schema_hash, baseline.signals[schema_hash].model_dump_json()),
            )

    def _stored_paths(self, system: str, paths: Mapping[Node, Path], entries: set[Node]) -> None:
        self._db.executemany(
            "INSERT INTO paths VALUES (?, ?, ?, ?) "
            "ON CONFLICT (system, service, operation) DO UPDATE SET path = excluded.path",
            [
                (system, entry.service, entry.operation, _dump_path(paths[entry]))
                for entry in sorted(entries, key=lambda entry: entry.sort_key)
            ],
        )

    def _forget(self, now: datetime) -> None:
        cutoff = _text(now - self._retention)
        self._db.execute("DELETE FROM shape_index WHERE accepted_at <= ?", (cutoff,))
        self._db.execute("DELETE FROM span_index WHERE accepted_at <= ?", (cutoff,))

    def work(self, system: str, work_id: str) -> list[WorkEntry]:
        now = self._clock()
        with self._lock:
            rows = self._db.execute(
                "SELECT source, channel_key, observation_id, schema_hash, occurred_at, "
                "observed_at, accepted_at, trace_id FROM shape_index "
                "WHERE system = ? AND work_id = ? AND accepted_at > ?",
                (system, work_id, _text(now - self._retention)),
            ).fetchall()
        entries = [
            WorkEntry(
                source=source,
                key=key,
                observation_id=observation_id,
                schema_hash=schema_hash,
                occurred_at=_time(occurred_at),
                observed_at=_time(observed_at),
                accepted_at=_time(accepted_at),
                trace_id=trace_id,
            )
            for (
                source,
                key,
                observation_id,
                schema_hash,
                occurred_at,
                observed_at,
                accepted_at,
                trace_id,
            ) in rows
        ]
        return sorted(
            entries, key=lambda entry: (entry.observed_at, entry.source, entry.observation_id)
        )

    def work_outcomes(self, system: str, outcome_sources: Collection[str]) -> WorkOutcomes:
        now = self._clock()
        with self._lock:
            rows = self._db.execute(
                "SELECT source, channel_key, schema_hash, work_id, occurred_at FROM shape_index "
                "WHERE system = ? AND work_id IS NOT NULL AND accepted_at > ?",
                (system, _text(now - self._retention)),
            ).fetchall()
        refs = [
            WorkRef(source, key, schema_hash, work_id, _time(occurred_at))
            for source, key, schema_hash, work_id, occurred_at in rows
        ]
        return tally_outcomes(refs, outcome_sources)

    def traced_shapes(self, system: str, trace_ids: Iterable[str]) -> list[TracedShape]:
        now = self._clock()
        cutoff = _text(now - self._retention)
        with self._lock:
            streams = self._streams.get(system, {})
            shapes: list[TracedShape] = []
            for trace_id in dict.fromkeys(trace_ids):
                # A replaced row gets a new rowid, so rowid order is the order stored.
                rows = self._db.execute(
                    "SELECT source, channel_key, schema_hash FROM shape_index "
                    "WHERE system = ? AND trace_id = ? AND accepted_at > ? ORDER BY rowid",
                    (system, trace_id, cutoff),
                ).fetchall()
                shapes.extend(
                    TracedShape(
                        source=source,
                        channel=streams[(source, key)].channel,
                        trace_id=trace_id,
                        schema_hash=schema_hash,
                    )
                    for source, key, schema_hash in rows
                )
            return shapes

    def close(self) -> None:
        with self._lock:
            self._db.close()
