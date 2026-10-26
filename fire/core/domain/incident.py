"""Incident identity: the system it belongs to and what opened it. No payload fields."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, Field, model_validator


class Trigger(StrEnum):
    """What opened an incident."""

    SHAPE_DRIFT = "shape_drift"
    PATH_DRIFT = "path_drift"
    INCIDENT_IO = "incident_io"
    HTTP_ERRORS = "http_errors"
    DLQ = "dlq"
    MANUAL = "manual"


class Incident(BaseModel):
    """One incident of one loaded system — no payload fields.

    `trigger` opened it; `triggers` lists every trigger it collected, the opening one first, and
    defaults to `[trigger]`.
    """

    id: str
    system: str
    trigger: Trigger
    triggers: list[Trigger] = Field(default_factory=list)
    service: str | None = None
    summary: str | None = None
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def _opening_trigger_first(self) -> Incident:
        if not self.triggers:
            self.triggers = [self.trigger]
        elif self.triggers[0] is not self.trigger:
            raise ValueError("triggers start with the trigger that opened the incident")
        return self
