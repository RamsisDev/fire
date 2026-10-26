"""Builders of `SpanObservation`s for flow and path tests: ids from small numbers, times in
seconds after `T0`.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

from fire.core.domain.flow import CompleteFlow
from fire.core.domain.observation import SpanKind, SpanLink, SpanObservation

T0 = datetime(2026, 10, 11, 16, 40, tzinfo=UTC)
SYSTEM = "shop"
SOURCE = "traces"
SERVICE = "orders-api"
ENTRY = "POST /orders"


def trace_id(number: int) -> str:
    return f"{number:032x}"


def span_id(number: int) -> str:
    return f"{number:016x}"


def at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


class Clock:
    """The body's clock, moved by hand: pass it to the catalog or `build_container`."""

    def __init__(self, now: datetime = T0) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def span(
    trace: int,
    number: int,
    operation: str,
    *,
    kind: SpanKind = SpanKind.INTERNAL,
    service: str = SERVICE,
    parent: int | None = None,
    links: Sequence[tuple[int, int]] = (),
    error: bool = False,
    status_code: int | None = None,
    start: float = 0,
    system: str = SYSTEM,
) -> SpanObservation:
    """Span `number` of trace `trace`; `parent` is a span number in the same trace, and each
    link is `(trace, span number)`.
    """
    return SpanObservation(
        system=system,
        source=SOURCE,
        trace_id=trace_id(trace),
        span_id=span_id(number),
        parent_span_id=None if parent is None else span_id(parent),
        links=[SpanLink(trace_id=trace_id(t), span_id=span_id(s)) for t, s in links],
        service=service,
        kind=kind,
        operation=operation,
        status_code=status_code,
        error=error,
        start=at(start),
        duration_ms=1.0,
    )


def order_spans(
    trace: int,
    *calls: str,
    failing: Sequence[str] = (),
    start: float = 0,
    system: str = SYSTEM,
) -> list[SpanObservation]:
    """`POST /orders` as a `server` root of `trace`, with one `client` child per call, in order;
    a call in `failing` errs with a 502.
    """
    root = span(trace, 1, ENTRY, kind=SpanKind.SERVER, status_code=201, start=start, system=system)
    children = [
        span(
            trace,
            index + 2,
            call,
            kind=SpanKind.CLIENT,
            parent=1,
            error=call in failing,
            status_code=502 if call in failing else 200,
            start=start + index + 1,
            system=system,
        )
        for index, call in enumerate(calls)
    ]
    return [root, *children]


def order_flow(
    trace: int, *calls: str, failing: Sequence[str] = (), start: float = 0
) -> CompleteFlow:
    """`order_spans` as a complete flow of one trace."""
    spans = order_spans(trace, *calls, failing=failing, start=start)
    return CompleteFlow(head=trace_id(trace), traces=(trace_id(trace),), spans=tuple(spans))
