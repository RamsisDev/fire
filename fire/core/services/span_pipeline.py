"""Span pipeline: one received span to a SpanObservation, and a client span's bodies to outbound
ShapeObservations.

Nothing here keeps a body or an attribute value, or puts one in an exception. Contract:
docs/specs/http-and-span-capture.md#span-mapping.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

from fire.core.domain.observation import (
    Channel,
    ChannelDirection,
    ChannelKind,
    ChannelPart,
    ShapeObservation,
    SpanKind,
    SpanLink,
    SpanObservation,
    StatusClass,
)
from fire.core.domain.operation import OperationNamer
from fire.core.ports.shape_fingerprinter import ShapeFingerprinter
from fire.core.ports.shape_generalizer import ShapeGeneralizer
from fire.core.ports.span_source import RawLink, RawSpan
from fire.core.services.record_pipeline import sampled_in

_TRACE_ID = re.compile(r"[0-9a-f]{32}")
_SPAN_ID = re.compile(r"[0-9a-f]{16}")
_TRUNCATED = re.compile(r"\.\.\.\[truncated [0-9]+ bytes\]\Z")
_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class SpanRejectReason(StrEnum):
    INVALID_IDS = "invalid_ids"
    INVALID_TIMES = "invalid_times"
    NO_SERVICE = "no_service"
    NO_OPERATION = "no_operation"
    INVALID = "invalid"


class BodySkipReason(StrEnum):
    SAMPLED_OUT = "sampled_out"
    BODY_TRUNCATED = "body_truncated"
    BODY_EMPTY = "body_empty"
    BODY_NOT_JSON = "body_not_json"
    BODY_NOT_CONTAINER = "body_not_container"
    STATUS_UNCLASSIFIED = "status_unclassified"
    SHAPING_FAILED = "shaping_failed"


class SpanNote(StrEnum):
    OPERATION_UNLISTED = "operation_unlisted"
    INVALID_LINK = "invalid_link"


@dataclass(frozen=True)
class SpanOutcome:
    """What one span gave: its observation or why it was rejected, the shapes of its bodies,
    why a body gave none, and what was counted without dropping the span.
    """

    span: SpanObservation | None = None
    rejected: SpanRejectReason | None = None
    shapes: tuple[ShapeObservation, ...] = ()
    skipped: tuple[BodySkipReason, ...] = ()
    noted: tuple[SpanNote, ...] = ()

    @property
    def observations(self) -> list[SpanObservation | ShapeObservation]:
        return ([] if self.span is None else [self.span]) + list(self.shapes)


class _Reject(Exception):
    """Carries a reason code only, never the value that failed."""

    def __init__(self, reason: SpanRejectReason) -> None:
        super().__init__(reason.value)
        self.reason = reason


class _Skip(Exception):
    """Carries a reason code only, never the body that failed."""

    def __init__(self, reason: BodySkipReason) -> None:
        super().__init__(reason.value)
        self.reason = reason


class SpanPipeline:
    """Turns one source's spans into observations; the first failing check rejects a span.

    The checks: valid ids, valid times, a service, and an operation name. Links with an invalid
    id are dropped and noted. A client span's request and response bodies are shaped when they
    are complete JSON objects or arrays; sampling hashes `{trace_id}:{span_id}`, so both parts
    are kept or dropped together. Spans are never sampled.
    """

    def __init__(
        self,
        *,
        system: str,
        source: str,
        sample_rate: float,
        namer: OperationNamer,
        generalizer: ShapeGeneralizer,
        fingerprinter: ShapeFingerprinter,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._system = system
        self._source = source
        self._sample_rate = sample_rate
        self._namer = namer
        self._generalizer = generalizer
        self._fingerprinter = fingerprinter
        self._clock = clock

    def process(self, raw: RawSpan) -> SpanOutcome:
        """Never raises."""
        try:
            span, noted = self._span(raw)
        except _Reject as reject:
            return SpanOutcome(rejected=reject.reason)
        except Exception:
            # A pydantic ValidationError quotes its input, so it is dropped here, unread.
            return SpanOutcome(rejected=SpanRejectReason.INVALID)
        if span.kind is not SpanKind.CLIENT:
            return SpanOutcome(span=span, noted=noted)
        shapes, skipped = self._bodies(raw, span)
        return SpanOutcome(span=span, shapes=shapes, skipped=skipped, noted=noted)

    def _span(self, raw: RawSpan) -> tuple[SpanObservation, tuple[SpanNote, ...]]:
        trace_id = _id(raw.trace_id, _TRACE_ID)
        span_id = _id(raw.span_id, _SPAN_ID)
        parent_span_id = None if not raw.parent_span_id else _id(raw.parent_span_id, _SPAN_ID)
        start, end = _times(raw.start_ns, raw.end_ns)
        if not raw.service:
            raise _Reject(SpanRejectReason.NO_SERVICE)
        named = self._namer.name(raw.naming)
        if named is None:
            raise _Reject(SpanRejectReason.NO_OPERATION)
        links = [link for link in map(_link, raw.links) if link is not None]
        noted = [SpanNote.INVALID_LINK] * (len(raw.links) - len(links))
        if named.unlisted:
            noted.insert(0, SpanNote.OPERATION_UNLISTED)
        status_code = (
            raw.status_code if raw.status_code and 100 <= raw.status_code <= 599 else None
        )
        span = SpanObservation(
            system=self._system,
            source=self._source,
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            links=links,
            service=raw.service,
            kind=raw.kind,
            operation=named.name,
            status_code=status_code,
            error=raw.error or (status_code is not None and status_code >= 500),
            start=_datetime(start),
            duration_ms=(end - start) / 1e6,
        )
        return span, tuple(noted)

    def _bodies(
        self, raw: RawSpan, span: SpanObservation
    ) -> tuple[tuple[ShapeObservation, ...], tuple[BodySkipReason, ...]]:
        if raw.request_body is None and raw.response_body is None:
            return (), ()
        if not sampled_in(f"{span.trace_id}:{span.span_id}", self._sample_rate):
            return (), (BodySkipReason.SAMPLED_OUT,)
        end = _datetime(raw.end_ns or 0)
        shapes: list[ShapeObservation] = []
        skipped: list[BodySkipReason] = []
        if raw.request_body is not None:
            outcome = self._part(span, ChannelPart.REQUEST, None, raw.request_body, span.start)
            (shapes if isinstance(outcome, ShapeObservation) else skipped).append(outcome)
        if span.status_code is None:
            if raw.response_body is not None:
                skipped.append(BodySkipReason.STATUS_UNCLASSIFIED)
            if shapes:
                none = self._part(span, ChannelPart.RESPONSE, StatusClass.NO_RESPONSE, None, end)
                (shapes if isinstance(none, ShapeObservation) else skipped).append(none)
        elif raw.response_body is not None:
            status_class = StatusClass.of(span.status_code)
            if status_class is None:
                skipped.append(BodySkipReason.STATUS_UNCLASSIFIED)
            else:
                outcome = self._part(
                    span, ChannelPart.RESPONSE, status_class, raw.response_body, end
                )
                (shapes if isinstance(outcome, ShapeObservation) else skipped).append(outcome)
        return tuple(shapes), tuple(skipped)

    def _part(
        self,
        span: SpanObservation,
        part: ChannelPart,
        status_class: StatusClass | None,
        body: str | None,
        occurred_at: datetime,
    ) -> ShapeObservation | BodySkipReason:
        """The body's shape on its channel; an empty shape when `body` is None."""
        try:
            value = {} if body is None else _json_body(body)
            shape = self._generalizer.generalize(value)
            return ShapeObservation(
                system=self._system,
                source=self._source,
                channel=Channel(
                    kind=ChannelKind.HTTP,
                    direction=ChannelDirection.OUTBOUND,
                    name=span.operation,
                    part=part,
                    status_class=status_class,
                ),
                observation_id=f"{span.trace_id}:{span.span_id}:{part.value}",
                occurred_at=occurred_at,
                observed_at=self._clock(),
                trace_id=span.trace_id,
                span_id=span.span_id,
                shape=shape,
                fingerprint=self._fingerprinter.fingerprint(shape),
            )
        except _Skip as skip:
            return skip.reason
        except Exception:
            # The text may quote the body, so it is dropped here, unread.
            return BodySkipReason.SHAPING_FAILED


def _id(value: str | None, pattern: re.Pattern[str]) -> str:
    """The id in lowercase hex, of the pattern's length and not all zeros."""
    lowered = value.lower() if isinstance(value, str) else ""
    if pattern.fullmatch(lowered) is None or not lowered.strip("0"):
        raise _Reject(SpanRejectReason.INVALID_IDS)
    return lowered


def _link(link: RawLink) -> SpanLink | None:
    try:
        return SpanLink(
            trace_id=_id(link.trace_id, _TRACE_ID), span_id=_id(link.span_id, _SPAN_ID)
        )
    except _Reject:
        return None


def _times(start_ns: int | None, end_ns: int | None) -> tuple[int, int]:
    if not start_ns or not end_ns or start_ns < 0 or end_ns < start_ns:
        raise _Reject(SpanRejectReason.INVALID_TIMES)
    try:
        _datetime(end_ns)
    except OverflowError:
        raise _Reject(SpanRejectReason.INVALID_TIMES) from None
    return start_ns, end_ns


def _datetime(nanoseconds: int) -> datetime:
    """Raises OverflowError past year 9999."""
    return _UNIX_EPOCH + timedelta(microseconds=nanoseconds // 1000)


def _json_body(body: str) -> Any:
    """The body as a JSON object or array.

    Checks in order: complete, not empty, JSON, a container.
    """
    if _TRUNCATED.search(body):
        raise _Skip(BodySkipReason.BODY_TRUNCATED)
    if body == "":
        raise _Skip(BodySkipReason.BODY_EMPTY)
    try:
        value = json.loads(body)
    except (ValueError, RecursionError):
        raise _Skip(BodySkipReason.BODY_NOT_JSON) from None
    if not isinstance(value, dict | list):
        raise _Skip(BodySkipReason.BODY_NOT_CONTAINER)
    return value
