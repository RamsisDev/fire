"""Where a record's envelope values are: dotted object paths, never the values themselves.

Contract: docs/specs/tentacle.md#envelope.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

# Object keys joined by `.`. No empty key, index, or wildcard.
DottedPath = Annotated[str, Field(pattern=r"^[^.\[\]*]+(\.[^.\[\]*]+)*$")]


def lookup(record: Mapping[str, Any], path: str) -> Any:
    """The value at a dotted path; None when a key is absent or a parent is not an object."""
    value: Any = record
    for key in path.split("."):
        if not isinstance(value, Mapping) or key not in value:
            return None
        value = value[key]
    return value


class _Path(BaseModel):
    """A plain string is the short form of `{path: <string>}`."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    path: DottedPath

    @model_validator(mode="before")
    @classmethod
    def _short_form(cls, data: Any) -> Any:
        return {"path": data} if isinstance(data, str) else data


class PayloadPath(_Path):
    """Where the payload is."""


class ValuePath(_Path):
    """Where an id or a name is. A value without `strip_prefix` counts as missing."""

    strip_prefix: str | None = Field(default=None, min_length=1)


class EpochUnit(StrEnum):
    MILLISECONDS = "ms"
    SECONDS = "s"


class TimestampPath(ValuePath):
    """Where `occurred_at` is: an ISO 8601 string, or an integer in `epoch` units."""

    epoch: EpochUnit | None = None

    @model_validator(mode="after")
    def _epoch_has_no_prefix(self) -> TimestampPath:
        if self.epoch is not None and self.strip_prefix is not None:
            raise ValueError("epoch reads an integer, which has no prefix to strip")
        return self


class Envelope(BaseModel):
    """Where each envelope value is in a record of an event-carrying source.

    `event_id` becomes the `observation_id`, `event_type` the channel name, and `payload` the
    value generalized into the shape. `work_id` and `correlation_id` are optional.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: ValuePath
    event_type: ValuePath
    occurred_at: TimestampPath
    payload: PayloadPath
    work_id: ValuePath | None = None
    correlation_id: ValuePath | None = None
