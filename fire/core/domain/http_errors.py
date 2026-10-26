"""5xx bursts: a route's `server` spans with an error status, counted in a sliding window.

A route is a `Node`, `(service, operation)`, and a `server` span's operation is `{METHOD}
{http.route}`. Times are the body's clock at acceptance, as for flows. Nothing here holds a
value: a burst carries names, status codes, counts, times, and trace ids.
Contract: docs/specs/ingest-drift-and-feedback.md#5xx-bursts.
"""

from __future__ import annotations

from collections import Counter, deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from fire.core.domain.flow import Node
from fire.core.domain.observation import SpanKind, SpanObservation, TraceId


class HttpErrorsTrigger(BaseModel):
    """When a route's error responses open an incident: the system YAML's
    `triggers.http_errors`.
    """

    model_config = ConfigDict(extra="forbid")

    # A response with this status or higher is an error.
    status_min: int = Field(default=500, ge=100, le=599)
    # Errors from one route within `window_s` that open an incident.
    threshold: int = Field(default=5, ge=1)
    window_s: float = Field(default=60, gt=0, allow_inf_nan=False)


class HttpErrorBurst(BaseModel):
    """`threshold` errors from one route within `window_s`: what its incident needs.

    `statuses` counts the errors per status code; `first_error` and `last_error` are the
    earliest and latest span `start` among them, and `trace_ids` their traces, sorted.
    """

    model_config = ConfigDict(extra="forbid")

    system: str
    route: Node
    status_min: int
    threshold: int
    window_s: float
    errors: int = Field(ge=1)
    statuses: dict[str, int]
    first_error: AwareDatetime
    last_error: AwareDatetime
    trace_ids: list[TraceId] = Field(min_length=1)


@dataclass(frozen=True)
class _Error:
    accepted_at: datetime
    span: SpanObservation


@dataclass
class _Route:
    # The errors still in the window, oldest first; empty while the route bursts.
    errors: deque[_Error] = field(default_factory=deque)
    bursting: bool = False
    # When the route's last error was accepted.
    last_error: datetime | None = None


class HttpErrorDetector:
    """Counts each system's route errors and returns a burst when one route reaches its
    threshold.

    `triggers` maps a system to its `triggers.http_errors`; a system it does not name is never
    counted. An error that comes while its route bursts opens nothing and keeps the burst going;
    the burst ends once `window_s` passes with no error from the route, and the next error
    counts from one. The caller passes the body's clock and holds the lock that makes each call
    one step. A duplicate span must never reach `observe`.
    """

    def __init__(self, triggers: Mapping[str, HttpErrorsTrigger]) -> None:
        self._triggers = dict(triggers)
        self._routes: dict[tuple[str, Node], _Route] = {}

    def observe(self, span: SpanObservation, now: datetime) -> HttpErrorBurst | None:
        trigger = self._triggers.get(span.system)
        if (
            trigger is None
            or span.kind is not SpanKind.SERVER
            or span.status_code is None
            or span.status_code < trigger.status_min
        ):
            return None
        window = timedelta(seconds=trigger.window_s)
        route = self._routes.setdefault((span.system, Node.of(span)), _Route())
        quiet = route.last_error is None or now - route.last_error >= window
        route.last_error = now
        if route.bursting and not quiet:
            return None
        route.bursting = False
        while route.errors and now - route.errors[0].accepted_at >= window:
            route.errors.popleft()
        route.errors.append(_Error(now, span))
        if len(route.errors) < trigger.threshold:
            return None
        spans = [error.span for error in route.errors]
        route.errors.clear()
        route.bursting = True
        statuses = Counter(str(span.status_code) for span in spans)
        return HttpErrorBurst(
            system=span.system,
            route=Node.of(span),
            status_min=trigger.status_min,
            threshold=trigger.threshold,
            window_s=trigger.window_s,
            errors=len(spans),
            statuses=dict(sorted(statuses.items())),
            first_error=min(span.start for span in spans),
            last_error=max(span.start for span in spans),
            trace_ids=sorted({span.trace_id for span in spans}),
        )
