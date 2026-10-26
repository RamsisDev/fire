"""Shape drift: how a new fingerprint differs from its stream's baseline, and the signal it raises.

A diff names paths and value types, never values. Contract:
docs/specs/ingest-drift-and-feedback.md#drift-rules.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from fire.core.domain.dossier import ValueType


class FieldRef(BaseModel):
    """One `(path, value_type)` of a shape."""

    model_config = ConfigDict(extra="forbid")

    path: str
    value_type: ValueType


class TypeChange(BaseModel):
    """A path the baseline saw, now carrying value types it never saw there."""

    model_config = ConfigDict(extra="forbid")

    path: str
    # Every value type the baseline saw at the path.
    old: list[ValueType] = Field(min_length=1)
    # The value types the shape carries at the path that the baseline never saw there.
    new: list[ValueType] = Field(min_length=1)


class ShapeDiff(BaseModel):
    """The drift rules that fired for one shape, each list sorted by path, then value type."""

    model_config = ConfigDict(extra="forbid")

    added: list[FieldRef] = Field(default_factory=list)
    type_changed: list[TypeChange] = Field(default_factory=list)
    removed: list[FieldRef] = Field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not (self.added or self.type_changed or self.removed)


class DriftSignal(BaseModel):
    """One drifted fingerprint of a stream key: its diff, how often it came, and when.

    `first_seen` and `last_seen` are the earliest and latest `observed_at` of its occurrences.
    """

    model_config = ConfigDict(extra="forbid")

    schema_hash: str
    diff: ShapeDiff
    occurrences: int = Field(ge=1)
    first_seen: AwareDatetime
    last_seen: AwareDatetime


class Admission(StrEnum):
    """What the catalog did with a shape observation."""

    DUPLICATE = "duplicate"
    FOLDED = "folded"
    # The first occurrence of a drifted fingerprint: the one that notifies.
    DRIFT_RAISED = "drift_raised"
    DRIFT_REPEATED = "drift_repeated"


_DRIFT = {Admission.DRIFT_RAISED, Admission.DRIFT_REPEATED}


class ShapeAdmission(BaseModel):
    """`outcome`, and on a drift outcome the signal as this occurrence left it."""

    model_config = ConfigDict(extra="forbid")

    outcome: Admission
    signal: DriftSignal | None = None

    @model_validator(mode="after")
    def _signal_only_on_drift(self) -> ShapeAdmission:
        if (self.outcome in _DRIFT) != (self.signal is not None):
            raise ValueError("a drift outcome carries its signal, and no other outcome does")
        return self

    @property
    def stored(self) -> bool:
        return self.outcome is not Admission.DUPLICATE
