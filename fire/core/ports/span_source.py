"""Port: the spans a span source receives. Attribute values never leave the tentacle process."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol, runtime_checkable

from fire.core.domain.observation import SpanKind
from fire.core.domain.operation import OperationInputs


class SpanRequestNote(StrEnum):
    """Why a span source refused a whole request; the sender knows how many spans it held."""

    UNSUPPORTED_MEDIA_TYPE = "unsupported_media_type"
    MALFORMED_REQUEST = "malformed_request"
    REQUEST_TOO_LARGE = "request_too_large"


@dataclass(frozen=True)
class RawLink:
    """A link as received; its ids are checked later."""

    trace_id: str | None
    span_id: str | None


@dataclass(frozen=True)
class RawSpan:
    """One span as received, holding only the attributes of the allowlist.

    Ids are the received strings, None when absent or not strings; they are checked later.
    Times are nanoseconds since the epoch, None when absent or not integers. `error` is True
    when the span's status is `ERROR`; the status message is never read. `status_code` is the
    HTTP response status attribute, None when absent. `request_body` and `response_body` are a
    client span's captured bodies, None when the span carries none. `capture_uris` are the
    URIs of the span's capture events, the request URI then the response URI of each event in
    event order, empty when the source reads none; they hold values and stay in the tentacle's
    memory. Bodies and URIs are not in `repr`.
    """

    trace_id: str | None
    span_id: str | None
    parent_span_id: str | None
    service: str | None
    kind: SpanKind
    start_ns: int | None
    end_ns: int | None
    error: bool
    naming: OperationInputs
    status_code: int | None = None
    links: tuple[RawLink, ...] = ()
    request_body: str | None = field(default=None, repr=False)
    response_body: str | None = field(default=None, repr=False)
    capture_uris: tuple[str, ...] = field(default=(), repr=False)


@dataclass(frozen=True)
class SpanBatchResult:
    """What a sink did with a batch: `taken` is False when it had no room, took nothing, and
    the source should ask the sender to retry. `rejected` counts the spans it refused.
    """

    taken: bool
    rejected: int = 0


class SpanSink(Protocol):
    """Where a span source reports. No call blocks, and none raises."""

    def spans(self, spans: Sequence[RawSpan]) -> SpanBatchResult:
        """One request's spans, taken whole or not at all."""
        ...

    def note(self, reason: SpanRequestNote) -> None:
        """A request the source refused."""
        ...


@runtime_checkable
class SpanSource(Protocol):
    """A server that receives spans and reports each request's spans to a sink.

    `serve` runs until its task is cancelled; it then stops accepting connections, finishes
    the requests in flight within its shutdown time, and re-raises the cancellation. An
    exception means the source failed. Adapters live in `fire/adapters/sources/`. Contract:
    docs/specs/http-and-span-capture.md#otlp-receiver.
    """

    async def serve(self, sink: SpanSink) -> None: ...

    def close(self) -> None:
        """Releases what the source holds, such as its bound socket, when `serve` never ran."""
        ...
