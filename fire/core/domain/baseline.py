"""Baselines: per stream key, what the observations folded so far carried, and its drift signals.

Contract: docs/specs/ingest-drift-and-feedback.md#baseline and #drift-rules.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from fire.core.domain.dossier import Shape, ValueType
from fire.core.domain.drift import (
    Admission,
    DriftSignal,
    FieldRef,
    ShapeAdmission,
    ShapeDiff,
    TypeChange,
)


class DriftPolicy(BaseModel):
    """When a stream's baseline is warm, and which fields it requires. Set per system."""

    model_config = ConfigDict(extra="forbid")

    # Observations folded before a stream is warm and may raise drift.
    min_samples: int = Field(default=20, ge=1)
    # A `(path, value_type)` carried by at least this share of the samples is required.
    required_ratio: float = Field(default=1.0, gt=0, le=1, allow_inf_nan=False)


class BaselineStatus(StrEnum):
    REBUILDING = "baseline rebuilding"
    WARM = "warm"


@dataclass
class Baseline:
    """One stream key's baseline: how many observations it folded, and what they carried.

    `presence` counts the observations that carried each `(path, value_type)`; a shape listing
    one pair twice counts once. `fingerprints` holds the schema hashes folded so far. `signals`
    holds one drift signal per drifted schema hash; a drifted fingerprint is never folded.
    """

    samples: int = 0
    presence: Counter[tuple[str, ValueType]] = field(default_factory=Counter)
    fingerprints: set[str] = field(default_factory=set)
    signals: dict[str, DriftSignal] = field(default_factory=dict)

    def status(self, policy: DriftPolicy) -> BaselineStatus:
        if self.samples >= policy.min_samples:
            return BaselineStatus.WARM
        return BaselineStatus.REBUILDING

    def compares(self, schema_hash: str, policy: DriftPolicy) -> bool:
        """Whether an observation with this fingerprint is compared before it folds.

        Only on a warm baseline, and only for a fingerprint it has not folded before.
        """
        return self.status(policy) is BaselineStatus.WARM and schema_hash not in self.fingerprints

    def fold(self, schema_hash: str, shape: Shape) -> None:
        self.samples += 1
        self.presence.update({(f.path, f.value_type) for f in shape.fields})
        self.fingerprints.add(schema_hash)

    def diff(self, shape: Shape, policy: DriftPolicy) -> ShapeDiff:
        """The drift rules that fire for `shape` against this baseline.

        A path reported as `type_changed` is not also reported as `removed`.
        """
        seen: defaultdict[str, set[ValueType]] = defaultdict(set)
        for path, value_type in self.presence:
            seen[path].add(value_type)
        carried = {(f.path, f.value_type) for f in shape.fields}
        now: defaultdict[str, set[ValueType]] = defaultdict(set)
        for path, value_type in carried:
            now[path].add(value_type)

        added = [
            FieldRef(path=path, value_type=value_type)
            for path, value_type in sorted(carried)
            if path not in seen
        ]
        type_changed = [
            TypeChange(path=path, old=sorted(seen[path]), new=sorted(types - seen[path]))
            for path, types in sorted(now.items())
            if path in seen and types - seen[path]
        ]
        changed = {change.path for change in type_changed}
        removed = [
            FieldRef(path=path, value_type=value_type)
            for (path, value_type), count in sorted(self.presence.items())
            if count / self.samples >= policy.required_ratio
            and (path, value_type) not in carried
            and path not in changed
        ]
        return ShapeDiff(added=added, type_changed=type_changed, removed=removed)

    def admit(
        self, schema_hash: str, shape: Shape, observed_at: datetime, policy: DriftPolicy
    ) -> ShapeAdmission:
        """Folds a stored shape, or counts it on its drift signal; never both.

        A known drifted fingerprint adds an occurrence. A fingerprint the baseline compares
        raises a signal when any rule fires. Everything else folds. The caller holds the lock
        that makes storing the observation and admitting it one step.
        """
        signal = self.signals.get(schema_hash)
        if signal is not None:
            signal.occurrences += 1
            signal.first_seen = min(signal.first_seen, observed_at)
            signal.last_seen = max(signal.last_seen, observed_at)
            return ShapeAdmission(
                outcome=Admission.DRIFT_REPEATED, signal=signal.model_copy(deep=True)
            )
        if self.compares(schema_hash, policy):
            diff = self.diff(shape, policy)
            if not diff.is_empty:
                signal = DriftSignal(
                    schema_hash=schema_hash,
                    diff=diff,
                    occurrences=1,
                    first_seen=observed_at,
                    last_seen=observed_at,
                )
                self.signals[schema_hash] = signal
                return ShapeAdmission(
                    outcome=Admission.DRIFT_RAISED, signal=signal.model_copy(deep=True)
                )
        self.fold(schema_hash, shape)
        return ShapeAdmission(outcome=Admission.FOLDED)
