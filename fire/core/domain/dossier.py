"""Incident dossier: shape + process path evidence (never raw payload values)."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from fire.core.domain.incident import Trigger
from fire.core.domain.origin import Origin


class EvidenceQuality(StrEnum):
    PRESENT = "present"
    MISSING = "missing"


class ValueType(StrEnum):
    """Formal leaf/structure types for ShapeField — never runtime values.

    Covers structure, the integer/number split, and string refinements recognized by one
    exact form each: canonical hyphenated uuid, plain email address, `scheme://` URI,
    ISO 8601 `YYYY-MM-DD` date, and RFC 3339-style datetime. Every other string is
    `string`; nothing is guessed from content (no binary or media detection).
    """

    OBJECT = "object"
    ARRAY = "array"
    NULL = "null"
    BOOLEAN = "boolean"
    INTEGER = "integer"
    NUMBER = "number"  # float / non-int numeric
    STRING = "string"
    DATETIME = "datetime"
    DATE = "date"
    UUID = "uuid"
    URI = "uri"
    EMAIL = "email"
    UNKNOWN = "unknown"


CardinalityValue = Annotated[str, Field(pattern=r"^(void|n|0|[1-9][0-9]*)$")] | None


class ShapeField(BaseModel):
    """One structural field: path + type + cardinality + optional. Never a value.

    cardinality is null, "void", "n" (any number), or a non-negative integer as a digit
    string without leading zeros ("0", "1", "42"); never a JSON number. The generalizer
    emits null for null, "void" for an object that is always empty and "1" otherwise, "0"
    for an array that is always empty and "n" otherwise, and "1" for every other leaf.

    optional is true when the key may be absent: some object at the parent path lacks it.
    It is relative to that parent, so a field under an optional key is optional only when
    some object at its own parent path lacks it.

    Unknown fields are rejected, so a `value` key fails instead of being dropped.
    """

    model_config = ConfigDict(extra="forbid")

    path: str
    value_type: ValueType = ValueType.UNKNOWN
    cardinality: CardinalityValue = "1"
    optional: bool = False


class Shape(BaseModel):
    """Reusable structure description — keys, types, cardinality, and optionality only.

    Produced by ShapeGeneralizer: flat paths with [*] for array elements, one field per
    (path, value_type), sorted by that pair. Not a fingerprint. Unknown fields are rejected.
    """

    model_config = ConfigDict(extra="forbid")

    fields: list[ShapeField] = Field(default_factory=list)


class ShapeFingerprint(BaseModel):
    """Stable id for a Shape (schema_hash ± normalized paths). Not a generalizer.

    Unknown fields are rejected.
    """

    model_config = ConfigDict(extra="forbid")

    schema_hash: str
    normalized_paths: list[str] = Field(default_factory=list)
    notes: str | None = None


class HopVia(StrEnum):
    """How an edge hop was traversed: a parent-child span edge, or a span link."""

    CHILD = "child"
    LINK = "link"


class ProcessHop(BaseModel):
    """One hop on the path by reference — shape-safe; never embeds Shape.

    A hop is either a node (`ref`) or an edge (`from_ref` and/or `to_ref`). Every ref
    is an origin_id or a service name. `via` is set on edge hops only.
    """

    ref: str | None = Field(default=None, min_length=1, description="Node: origin_id or service")
    from_ref: str | None = Field(
        default=None, min_length=1, description="Edge start: origin_id or service"
    )
    to_ref: str | None = Field(
        default=None, min_length=1, description="Edge end: origin_id or service"
    )
    operation: str | None = None
    span_kind: str | None = None
    status: str | None = None
    via: HopVia | None = None

    @model_validator(mode="after")
    def _require_one_identity_form(self) -> ProcessHop:
        is_edge = self.from_ref is not None or self.to_ref is not None
        if self.ref is not None and is_edge:
            raise ValueError("ProcessHop is either a node (ref) or an edge (from_ref/to_ref)")
        if self.ref is None and not is_edge:
            raise ValueError("ProcessHop needs ref, from_ref, or to_ref")
        if self.via is not None and not is_edge:
            raise ValueError("via marks an edge hop; a node hop has none")
        return self

    def display_label(self) -> str:
        """Human-readable hop label without inventing probabilities."""
        if self.ref is not None:
            base = self.ref
        elif self.from_ref is not None and self.to_ref is not None:
            base = f"{self.from_ref}->{self.to_ref}"
        else:
            base = self.from_ref or self.to_ref
        if self.operation:
            return f"{base}/{self.operation}"
        return base


class ProcessPath(BaseModel):
    hops: list[ProcessHop] = Field(default_factory=list)


class EvidenceFragment(BaseModel):
    collector: str
    quality: EvidenceQuality
    summary: str
    data: dict[str, Any] = Field(
        default_factory=dict,
        description="Shape-safe metadata only — never raw payload field values.",
    )


class DossierTrigger(BaseModel):
    """One trigger of a correlated incident: what it named, when it started and was accepted,
    and the indexes in the dossier's `fragments` of the evidence it gathered.
    """

    model_config = ConfigDict(extra="forbid")

    trigger: Trigger
    summary: str
    service: str | None = None
    started_at: AwareDatetime
    accepted_at: AwareDatetime
    fragments: list[int] = Field(default_factory=list)


class Dossier(BaseModel):
    model_config = ConfigDict(extra="forbid")

    incident_id: str
    system: str
    service: str | None = None
    origin: Origin | None = None
    shape: Shape | None = None
    fingerprint: ShapeFingerprint | None = None
    process_path: ProcessPath = Field(default_factory=ProcessPath)
    fragments: list[EvidenceFragment] = Field(default_factory=list)
    triggers: list[DossierTrigger] = Field(default_factory=list)

    @property
    def missing(self) -> list[EvidenceFragment]:
        """Fragments whose quality is MISSING, in `fragments` order."""
        return [f for f in self.fragments if f.quality is EvidenceQuality.MISSING]
