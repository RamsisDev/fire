"""Origin: where an observation came from — not its form."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class OriginKind(StrEnum):
    HTTP = "http"
    QUEUE = "queue"
    CRON = "cron"
    OTHER = "other"


class Origin(BaseModel):
    """Provenance box — stable ids only; never secret-bearing URLs or form fields."""

    origin_id: str
    kind: OriginKind = OriginKind.OTHER
    endpoint_ref: str = Field(description="Stable endpoint id (not a URL that may carry secrets).")
