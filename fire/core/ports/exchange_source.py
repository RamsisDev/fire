"""Port: the HTTP exchanges an inbound proxy forwards. Bodies never leave the tentacle process."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Protocol, runtime_checkable


class ExchangeSkipReason(StrEnum):
    """Why an exchange the proxy handled gives no observation."""

    TEMPLATES_UNAVAILABLE = "templates_unavailable"
    UNMATCHED_ROUTE = "unmatched_route"
    UPSTREAM_UNREACHABLE = "upstream_unreachable"
    UPSTREAM_ABORTED = "upstream_aborted"
    CLIENT_ABORTED = "client_aborted"
    UPGRADE_UNSUPPORTED = "upgrade_unsupported"


class ExchangeNote(StrEnum):
    """What the proxy counts without dropping anything."""

    INVALID_TRACEPARENT = "invalid_traceparent"
    TEMPLATES_REFRESH_FAILED = "templates_refresh_failed"


@dataclass(frozen=True)
class HttpMessage:
    """One side of an exchange, as the proxy copied it aside.

    `received_at` is when its headers arrived. `content_type` and `content_encoding` are the
    raw header values, None when absent. `body` holds at most the proxy's `max_body_bytes`, as
    sent, not decoded; `complete` is False when the message carried more. Only `received_at`
    and `complete` appear in `repr`.
    """

    received_at: datetime
    content_type: str | None = field(repr=False)
    content_encoding: str | None = field(repr=False)
    body: bytes = field(repr=False)
    complete: bool = True


@dataclass(frozen=True)
class HttpExchange:
    """One request and its response, forwarded unchanged, with the route template it matched.

    `exchange_id` is 32 random hex characters. `method` is the request method as received and
    `template` the matched route template. `trace_id` and `span_id` are the trace id and
    parent id of the `traceparent` the upstream received, the proxy's own or a valid one from
    the client; both are None when the client's was invalid. `status_code` is the upstream's.
    """

    exchange_id: str
    method: str
    template: str
    status_code: int
    request: HttpMessage
    response: HttpMessage
    trace_id: str | None = None
    span_id: str | None = None


class ExchangeSink(Protocol):
    """Where an exchange source reports. No call blocks, and none raises."""

    def exchange(self, exchange: HttpExchange) -> None:
        """A forwarded exchange whose route matched, to shape."""
        ...

    def skip(self, reason: ExchangeSkipReason) -> None:
        """An exchange the source handled that gives no observation."""
        ...

    def note(self, reason: ExchangeNote) -> None:
        """A counted event that drops nothing."""
        ...


@runtime_checkable
class ExchangeSource(Protocol):
    """A server that forwards HTTP exchanges and reports each one to a sink.

    `serve` runs until its task is cancelled; it then stops accepting connections, finishes
    the exchanges in flight within its shutdown time, and re-raises the cancellation. An
    exception means the source failed. Adapters live in
    `fire/adapters/sources/`. Contract: docs/specs/http-and-span-capture.md#inbound-proxy.
    """

    async def serve(self, sink: ExchangeSink) -> None: ...

    def close(self) -> None:
        """Releases what the source holds, such as its bound socket, when `serve` never ran."""
        ...
