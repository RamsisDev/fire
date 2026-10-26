"""What the body answers a batch of observations: counts, and why each rejected item failed.

A rejection names an index, a reason, and field locations, never an item's values.
Contract: docs/specs/observations.md#ingest-endpoint.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RejectionReason(StrEnum):
    INVALID = "invalid"
    UNKNOWN_SYSTEM = "unknown_system"
    UNKNOWN_SOURCE = "unknown_source"
    FINGERPRINT_MISMATCH = "fingerprint_mismatch"


class Rejection(BaseModel):
    """One rejected item, by its position in its own list.

    `fields` lists dotted error locations and is set on `invalid` only. It may be empty: an item
    that is not an object fails as a whole.
    """

    model_config = ConfigDict(extra="forbid")

    index: int = Field(ge=0)
    reason: RejectionReason
    fields: list[str] | None = None

    @model_validator(mode="after")
    def _fields_only_on_invalid(self) -> Rejection:
        if (self.reason is RejectionReason.INVALID) != (self.fields is not None):
            raise ValueError("fields are set on an invalid item and nowhere else")
        return self


class IngestCounts(BaseModel):
    model_config = ConfigDict(extra="forbid")

    accepted: int = Field(default=0, ge=0)
    duplicate: int = Field(default=0, ge=0)
    rejected: list[Rejection] = Field(default_factory=list)


class IngestReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    shapes: IngestCounts = Field(default_factory=IngestCounts)
    spans: IngestCounts = Field(default_factory=IngestCounts)
