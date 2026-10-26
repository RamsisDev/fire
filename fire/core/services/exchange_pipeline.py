"""Exchange pipeline: one forwarded HTTP exchange to request and response ShapeObservations.

Nothing here keeps a body or a header value, or puts one in an exception. Contract:
docs/specs/http-and-span-capture.md#shapes.
"""

from __future__ import annotations

import json
import zlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from fire.core.domain.observation import (
    Channel,
    ChannelDirection,
    ChannelKind,
    ChannelPart,
    ShapeObservation,
    StatusClass,
)
from fire.core.ports.exchange_source import HttpExchange, HttpMessage
from fire.core.ports.shape_fingerprinter import ShapeFingerprinter
from fire.core.ports.shape_generalizer import ShapeGeneralizer
from fire.core.services.record_pipeline import sampled_in

_GZIP_WBITS = zlib.MAX_WBITS | 16
_ZLIB_WBITS = zlib.MAX_WBITS
_RAW_DEFLATE_WBITS = -zlib.MAX_WBITS

# The zlib window bits to try, in order, per content coding. Servers send `deflate` both
# zlib-wrapped and raw.
_CODINGS: dict[str, tuple[int, ...]] = {
    "gzip": (_GZIP_WBITS,),
    "x-gzip": (_GZIP_WBITS,),
    "deflate": (_ZLIB_WBITS, _RAW_DEFLATE_WBITS),
}


class PartSkipReason(StrEnum):
    SAMPLED_OUT = "sampled_out"
    NO_BODY = "no_body"
    BODY_NOT_JSON = "body_not_json"
    BODY_TOO_LARGE = "body_too_large"
    BODY_ENCODING_UNSUPPORTED = "body_encoding_unsupported"
    STATUS_UNCLASSIFIED = "status_unclassified"
    SHAPING_FAILED = "shaping_failed"


@dataclass(frozen=True)
class PartSkipped:
    reason: PartSkipReason


type ExchangeOutcome = ShapeObservation | PartSkipped


class _Skip(Exception):
    """Carries a reason code only, never the value that failed."""

    def __init__(self, reason: PartSkipReason) -> None:
        super().__init__(reason.value)
        self.reason = reason


class ExchangePipeline:
    """Turns a proxy source's exchanges into ShapeObservations; the first failing check decides.

    A sampled-out exchange gives one `sampled_out`; any other gives its request part, then
    its response part. The response's status needs a class. A body is shaped when it is not
    empty, its media type is JSON, its content coding is supported, it is complete, it decodes
    within `max_body_bytes`, and it parses as an object or array. The channel is
    `http:inbound:{METHOD} {template}:{part}` and `observed_at` is the clock's time.
    """

    def __init__(
        self,
        *,
        system: str,
        source: str,
        sample_rate: float,
        max_body_bytes: int,
        generalizer: ShapeGeneralizer,
        fingerprinter: ShapeFingerprinter,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if max_body_bytes < 0:
            raise ValueError("max_body_bytes cannot be negative")
        self._system = system
        self._source = source
        self._sample_rate = sample_rate
        self._max_body_bytes = max_body_bytes
        self._generalizer = generalizer
        self._fingerprinter = fingerprinter
        self._clock = clock

    def process(self, exchange: HttpExchange) -> list[ExchangeOutcome]:
        """Never raises; sampling hashes `exchange_id`, so both parts are kept or dropped."""
        if not sampled_in(exchange.exchange_id, self._sample_rate):
            return [PartSkipped(PartSkipReason.SAMPLED_OUT)]
        return [
            self._part(exchange, ChannelPart.REQUEST),
            self._part(exchange, ChannelPart.RESPONSE),
        ]

    def _part(self, exchange: HttpExchange, part: ChannelPart) -> ExchangeOutcome:
        try:
            return self._build(exchange, part)
        except _Skip as skip:
            return PartSkipped(skip.reason)
        except Exception:
            # The text may quote a body, a header, or a template, so it is dropped here, unread.
            return PartSkipped(PartSkipReason.SHAPING_FAILED)

    def _build(self, exchange: HttpExchange, part: ChannelPart) -> ShapeObservation:
        if part is ChannelPart.REQUEST:
            message, status_class = exchange.request, None
        else:
            message, status_class = exchange.response, _status_class(exchange.status_code)
        shape = self._generalizer.generalize(_json_body(message, self._max_body_bytes))
        return ShapeObservation(
            system=self._system,
            source=self._source,
            channel=Channel(
                kind=ChannelKind.HTTP,
                direction=ChannelDirection.INBOUND,
                name=f"{exchange.method} {exchange.template}",
                part=part,
                status_class=status_class,
            ),
            observation_id=f"{exchange.exchange_id}:{part.value}",
            occurred_at=message.received_at,
            observed_at=self._clock(),
            trace_id=exchange.trace_id,
            span_id=exchange.span_id,
            shape=shape,
            fingerprint=self._fingerprinter.fingerprint(shape),
        )


def _status_class(status_code: int) -> StatusClass:
    status_class = StatusClass.of(status_code)
    if status_class is None:
        raise _Skip(PartSkipReason.STATUS_UNCLASSIFIED)
    return status_class


def _json_body(message: HttpMessage, limit: int) -> Any:
    """The body as a JSON object or array.

    Checks in order: present, media type, content coding, complete, decoded size, JSON.
    """
    if message.complete and message.body == b"":
        raise _Skip(PartSkipReason.NO_BODY)
    if not _is_json(message.content_type):
        raise _Skip(PartSkipReason.BODY_NOT_JSON)
    attempts = _wbits_to_try(message.content_encoding)
    if not message.complete:
        raise _Skip(PartSkipReason.BODY_TOO_LARGE)
    decoded = _decode(message.body, attempts, limit)
    try:
        value = json.loads(decoded)
    except (ValueError, RecursionError):
        raise _Skip(PartSkipReason.BODY_NOT_JSON) from None
    if not isinstance(value, dict | list):
        raise _Skip(PartSkipReason.BODY_NOT_JSON)
    return value


def _is_json(content_type: str | None) -> bool:
    if content_type is None:
        return False
    media_type = content_type.split(";", 1)[0].strip().lower()
    return media_type == "application/json" or media_type.endswith("+json")


def _wbits_to_try(content_encoding: str | None) -> tuple[int, ...]:
    """Empty when no coding is left once `identity` and empty tokens are dropped.

    One coding at most: a stack such as `gzip, br` is unsupported as a whole.
    """
    tokens = (token.strip().lower() for token in (content_encoding or "").split(","))
    codings = [token for token in tokens if token and token != "identity"]
    if not codings:
        return ()
    if len(codings) > 1 or codings[0] not in _CODINGS:
        raise _Skip(PartSkipReason.BODY_ENCODING_UNSUPPORTED)
    return _CODINGS[codings[0]]


def _decode(body: bytes, attempts: tuple[int, ...], limit: int) -> bytes:
    """The body decoded by the first attempt that reads a whole stream, at most `limit` bytes."""
    if not attempts:
        if len(body) > limit:
            raise _Skip(PartSkipReason.BODY_TOO_LARGE)
        return body
    for wbits in attempts:
        decoded = _inflate(body, wbits, limit)
        if decoded is not None:
            return decoded
    raise _Skip(PartSkipReason.BODY_NOT_JSON)


def _inflate(body: bytes, wbits: int, limit: int) -> bytes | None:
    """The stream inflated; None when it is corrupt, truncated, or followed by more bytes.

    Never yields more than `limit + 1` bytes, so a small bomb cannot take much memory.
    """
    inflater = zlib.decompressobj(wbits)
    try:
        decoded = inflater.decompress(body, limit + 1)
    except zlib.error:
        return None
    if len(decoded) > limit:
        raise _Skip(PartSkipReason.BODY_TOO_LARGE)
    if not inflater.eof or inflater.unused_data:
        return None
    return decoded
