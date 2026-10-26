"""What the tentacle sends the body: payload shapes and spans, never values.

Contract: docs/specs/observations.md.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
)

from fire.core.domain.baseline import BaselineStatus
from fire.core.domain.dossier import Shape, ShapeFingerprint
from fire.core.domain.drift import DriftSignal

MAX_STR_LENGTH = 256

BoundedStr = Annotated[str, Field(min_length=1, max_length=MAX_STR_LENGTH)]


def _not_all_zeros(value: str) -> str:
    if set(value) == {"0"}:
        raise ValueError("an id cannot be all zeros")
    return value


TraceId = Annotated[str, Field(pattern=r"^[0-9a-f]{32}$"), AfterValidator(_not_all_zeros)]
SpanId = Annotated[str, Field(pattern=r"^[0-9a-f]{16}$"), AfterValidator(_not_all_zeros)]


class ChannelKind(StrEnum):
    HTTP = "http"
    EVENT = "event"
    # A record of what became of a unit of work, such as an abandoned task, not an event.
    OUTCOME = "outcome"


class ChannelDirection(StrEnum):
    """Relative to the watched system."""

    INBOUND = "inbound"
    INTERNAL = "internal"
    OUTBOUND = "outbound"


class ChannelPart(StrEnum):
    REQUEST = "request"
    RESPONSE = "response"
    PAYLOAD = "payload"


class StatusClass(StrEnum):
    """Status class of an HTTP response; `none` means no response came."""

    SUCCESS = "2xx"
    REDIRECTION = "3xx"
    CLIENT_ERROR = "4xx"
    SERVER_ERROR = "5xx"
    NO_RESPONSE = "none"

    @classmethod
    def of(cls, status_code: int) -> StatusClass | None:
        """The class of a status from 200 to 599; None for any other."""
        if not 200 <= status_code <= 599:
            return None
        return cls(f"{status_code // 100}xx")


_PARTS_BY_KIND = {
    ChannelKind.HTTP: {ChannelPart.REQUEST, ChannelPart.RESPONSE},
    ChannelKind.EVENT: {ChannelPart.PAYLOAD},
    ChannelKind.OUTCOME: {ChannelPart.PAYLOAD},
}


class Channel(BaseModel):
    """Where a payload was seen: transport, direction, name, part, and a response's status class.

    `http` carries a `request` or `response` part, and `event` and `outcome` a `payload`.
    `status_class` is set on a response and nowhere else. `name` is a route template, an
    outbound operation name, an event type, or an outcome's name; it may contain `:`.
    """

    model_config = ConfigDict(extra="forbid")

    kind: ChannelKind
    direction: ChannelDirection
    name: BoundedStr
    part: ChannelPart
    status_class: StatusClass | None = Field(default=None, validate_default=True)

    @field_validator("part")
    @classmethod
    def _part_fits_kind(cls, part: ChannelPart, info: ValidationInfo) -> ChannelPart:
        kind = info.data.get("kind")
        if kind is not None and part not in _PARTS_BY_KIND[kind]:
            raise ValueError(f"a {kind} channel has no {part} part")
        return part

    @field_validator("status_class")
    @classmethod
    def _status_class_only_on_responses(
        cls, status_class: StatusClass | None, info: ValidationInfo
    ) -> StatusClass | None:
        if "part" not in info.data:
            return status_class
        is_response = info.data["part"] is ChannelPart.RESPONSE
        if is_response and status_class is None:
            raise ValueError("a response needs a status_class")
        if not is_response and status_class is not None:
            raise ValueError("only a response has a status_class")
        return status_class

    @property
    def key(self) -> str:
        """`{kind}:{direction}:{name}:{part}`, plus `:{status_class}` on a response.

        Unambiguous although `name` may contain `:`: `kind` and `direction` never do, and every
        key ends in `:request`, `:payload`, or `:response:{status_class}`.
        """
        key = f"{self.kind}:{self.direction}:{self.name}:{self.part}"
        if self.status_class is not None:
            return f"{key}:{self.status_class}"
        return key


class ShapeObservation(BaseModel):
    """One payload the tentacle saw, as its shape. No field holds a payload value."""

    model_config = ConfigDict(extra="forbid")

    system: BoundedStr
    source: BoundedStr
    channel: Channel
    observation_id: BoundedStr
    occurred_at: AwareDatetime
    observed_at: AwareDatetime
    region: BoundedStr | None = None
    trace_id: TraceId | None = None
    span_id: SpanId | None = None
    work_id: BoundedStr | None = None
    correlation_id: BoundedStr | None = None
    shape: Shape
    fingerprint: ShapeFingerprint

    @field_validator("span_id")
    @classmethod
    def _span_id_needs_trace_id(cls, span_id: str | None, info: ValidationInfo) -> str | None:
        if span_id is not None and "trace_id" in info.data and info.data["trace_id"] is None:
            raise ValueError("span_id requires trace_id")
        return span_id

    @field_validator("shape")
    @classmethod
    def _no_response_has_no_fields(cls, shape: Shape, info: ValidationInfo) -> Shape:
        channel = info.data.get("channel")
        if (
            channel is not None
            and channel.status_class is StatusClass.NO_RESPONSE
            and shape.fields
        ):
            raise ValueError("a channel with no response carries an empty shape")
        return shape

    @property
    def stream_key(self) -> tuple[str, str, str]:
        """`(system, source, channel.key)`: baselines, drift, and coverage are per stream key."""
        return (self.system, self.source, self.channel.key)

    @property
    def dedupe_key(self) -> tuple[str, str, str]:
        """`(system, source, observation_id)`."""
        return (self.system, self.source, self.observation_id)


class SpanKind(StrEnum):
    SERVER = "server"
    CLIENT = "client"
    PRODUCER = "producer"
    CONSUMER = "consumer"
    INTERNAL = "internal"


class SpanLink(BaseModel):
    """A span this one links to, by id only."""

    model_config = ConfigDict(extra="forbid")

    trace_id: TraceId
    span_id: SpanId


class SpanObservation(BaseModel):
    """One span without its attributes: ids, links, names, status, and timing."""

    model_config = ConfigDict(extra="forbid")

    system: BoundedStr
    source: BoundedStr
    trace_id: TraceId
    span_id: SpanId
    parent_span_id: SpanId | None = None
    links: list[SpanLink] = Field(default_factory=list)
    service: BoundedStr
    kind: SpanKind
    operation: BoundedStr
    status_code: Annotated[int, Field(ge=100, le=599)] | None = None
    error: bool
    start: AwareDatetime
    duration_ms: float = Field(ge=0, allow_inf_nan=False)

    @property
    def dedupe_key(self) -> tuple[str, str, str]:
        """`(system, trace_id, span_id)`: one span sent by two sources is one observation."""
        return (self.system, self.trace_id, self.span_id)


class FingerprintSummary(BaseModel):
    """One fingerprint of a stream: how many observations carried it, and when they were read."""

    model_config = ConfigDict(extra="forbid")

    schema_hash: str
    observations: int = Field(ge=1)
    first_observed_at: AwareDatetime
    last_observed_at: AwareDatetime


class StreamSummary(BaseModel):
    """One stream key of a system, `(source, channel)`: its baseline and the fingerprints seen.

    `samples` counts the observations folded into the baseline; `status` is `warm` once it
    reaches the system's `drift.min_samples`. `drift` holds the stream's open drift signals,
    sorted by `first_seen`, then `schema_hash`.
    """

    model_config = ConfigDict(extra="forbid")

    source: BoundedStr
    channel: Channel
    status: BaselineStatus
    samples: int = Field(ge=0)
    fingerprints: list[FingerprintSummary] = Field(min_length=1)
    drift: list[DriftSignal] = Field(default_factory=list)

    @property
    def key(self) -> str:
        return self.channel.key

    @property
    def observations(self) -> int:
        return sum(fingerprint.observations for fingerprint in self.fingerprints)


class WorkEntry(BaseModel):
    """One accepted shape observation that carried a `work_id`, as the work index keeps it.

    `accepted_at` is the body's clock when the catalog stored it; the retention window counts
    from it.
    """

    model_config = ConfigDict(extra="forbid")

    source: BoundedStr
    key: str
    observation_id: BoundedStr
    schema_hash: str
    occurred_at: AwareDatetime
    observed_at: AwareDatetime
    accepted_at: AwareDatetime
    trace_id: TraceId | None = None


class TracedShape(BaseModel):
    """A stored shape observation of one trace, without its shape: where it was seen and its
    fingerprint.
    """

    model_config = ConfigDict(extra="forbid")

    source: BoundedStr
    channel: Channel
    trace_id: TraceId
    schema_hash: str
