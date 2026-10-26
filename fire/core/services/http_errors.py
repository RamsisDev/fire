"""5xx bursts: count each route's error responses from its `server` spans, and turn a burst into a
trigger event with the failing requests' flows, their shapes, and the route's baseline.

Contract: docs/specs/ingest-drift-and-feedback.md#5xx-bursts.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from fire.core.domain.dossier import EvidenceFragment, EvidenceQuality, ProcessPath
from fire.core.domain.flow import CompleteFlow
from fire.core.domain.http_errors import HttpErrorBurst, HttpErrorDetector, HttpErrorsTrigger
from fire.core.domain.incident import Trigger
from fire.core.domain.observation import (
    Channel,
    ChannelDirection,
    ChannelKind,
    ChannelPart,
    SpanObservation,
    StreamSummary,
    TracedShape,
)
from fire.core.domain.trigger_event import TriggerEvent
from fire.core.ports.observation_catalog import ObservationCatalog


def _utc_now() -> datetime:
    return datetime.now(UTC)


class HttpErrorsService:
    """Watches each stored span for its system's `triggers.http_errors`, and gathers a burst's
    evidence.

    `observe` counts one span on the body's `clock` and returns a burst when its route reaches
    the threshold. `trigger_event` reads the catalog when it is called: a burst's event holds
    four kinds of fragments, in order: `http_errors`, `failing_shapes`, one `baseline` per
    request stream of the route, and `process_path`. Evidence it cannot build is a `missing`
    fragment.
    """

    def __init__(
        self,
        catalog: ObservationCatalog,
        triggers: Mapping[str, HttpErrorsTrigger],
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._catalog = catalog
        self._detector = HttpErrorDetector(triggers)
        self._clock = clock or _utc_now
        self._lock = threading.Lock()

    def observe(self, span: SpanObservation) -> HttpErrorBurst | None:
        now = self._clock()
        with self._lock:
            return self._detector.observe(span, now)

    def trigger_event(self, burst: HttpErrorBurst) -> TriggerEvent:
        flows = self._catalog.open_flows(burst.system, burst.trace_ids)
        shapes = self._catalog.traced_shapes(burst.system, burst.trace_ids)
        streams = self._catalog.streams(burst.system)
        process_path, path_fragment = _process_path(burst, flows)
        return TriggerEvent(
            system=burst.system,
            trigger=Trigger.HTTP_ERRORS,
            summary=f"HTTP errors on {burst.route.operation}",
            started_at=burst.first_error,
            service=burst.route.service,
            process_path=process_path,
            fragments=[
                _http_errors(burst),
                _failing_shapes(burst, shapes, streams),
                *_baselines(burst, streams),
                path_fragment,
            ],
        )


def _on_route(channel: Channel, burst: HttpErrorBurst) -> bool:
    return (
        channel.kind is ChannelKind.HTTP
        and channel.direction is ChannelDirection.INBOUND
        and channel.name == burst.route.operation
    )


def _http_errors(burst: HttpErrorBurst) -> EvidenceFragment:
    dumped = burst.model_dump(mode="json")
    return EvidenceFragment(
        collector="http_errors",
        quality=EvidenceQuality.PRESENT,
        summary=(
            f"{burst.errors} responses with status >= {burst.status_min} on "
            f"{burst.route.operation} within {burst.window_s:g}s"
        ),
        data={
            key: dumped[key]
            for key in (
                "route",
                "status_min",
                "threshold",
                "window_s",
                "errors",
                "statuses",
                "first_error",
                "last_error",
                "trace_ids",
            )
        },
    )


def _failing_shapes(
    burst: HttpErrorBurst, shapes: Sequence[TracedShape], streams: Sequence[StreamSummary]
) -> EvidenceFragment:
    route = burst.route.model_dump(mode="json")
    requests = len(burst.trace_ids)
    traces: defaultdict[tuple[str, str, str], set[str]] = defaultdict(set)
    shaped: set[str] = set()
    for shape in shapes:
        if not _on_route(shape.channel, burst):
            continue
        traces[(shape.source, shape.channel.key, shape.schema_hash)].add(shape.trace_id)
        if shape.channel.part is ChannelPart.REQUEST:
            shaped.add(shape.trace_id)
    if not traces:
        return EvidenceFragment(
            collector="failing_shapes",
            quality=EvidenceQuality.MISSING,
            summary="No shape of the failing requests",
            data={"route": route, "requests": requests},
        )
    drifted = {
        (stream.source, stream.key): {signal.schema_hash for signal in stream.drift}
        for stream in streams
    }
    entries: list[dict[str, Any]] = [
        {
            "source": source,
            "stream": key,
            "schema_hash": schema_hash,
            "requests": len(carried),
            "drifted": schema_hash in drifted.get((source, key), set()),
        }
        for (source, key, schema_hash), carried in sorted(traces.items())
    ]
    return EvidenceFragment(
        collector="failing_shapes",
        quality=EvidenceQuality.PRESENT,
        summary=(
            f"{len(shaped)} of {requests} failing requests shaped: {len(entries)} fingerprints, "
            f"{sum(entry['drifted'] for entry in entries)} drifted"
        ),
        data={"route": route, "requests": requests, "shaped": len(shaped), "shapes": entries},
    )


def _baselines(burst: HttpErrorBurst, streams: Sequence[StreamSummary]) -> list[EvidenceFragment]:
    fragments: list[EvidenceFragment] = []
    for stream in streams:
        if not (_on_route(stream.channel, burst) and stream.channel.part is ChannelPart.REQUEST):
            continue
        drifted = {signal.schema_hash for signal in stream.drift}
        folded = sum(
            1 for fingerprint in stream.fingerprints if fingerprint.schema_hash not in drifted
        )
        fragments.append(
            EvidenceFragment(
                collector="baseline",
                quality=EvidenceQuality.PRESENT,
                summary=f"{stream.status}: {stream.samples} samples, {folded} fingerprints known",
                data={
                    "source": stream.source,
                    "stream": stream.key,
                    "status": stream.status.value,
                    "samples": stream.samples,
                    "fingerprints": folded,
                    "observations": stream.observations,
                    "drift_signals": len(stream.drift),
                },
            )
        )
    if fragments:
        return fragments
    return [
        EvidenceFragment(
            collector="baseline",
            quality=EvidenceQuality.MISSING,
            summary="No baseline for this route",
            data={"route": burst.route.model_dump(mode="json")},
        )
    ]


def _process_path(
    burst: HttpErrorBurst, flows: Sequence[CompleteFlow]
) -> tuple[ProcessPath, EvidenceFragment]:
    """The failing requests' open flows as one process path: their spans together, so a hop
    every flow reached is one hop with the status of all its spans.
    """
    requests = len(burst.trace_ids)
    if not flows:
        return ProcessPath(), EvidenceFragment(
            collector="process_path",
            quality=EvidenceQuality.MISSING,
            summary="No open flow of the failing requests",
        )
    together = CompleteFlow(
        head=flows[0].head,
        traces=tuple(trace for flow in flows for trace in flow.traces),
        spans=tuple(span for flow in flows for span in flow.spans),
    )
    held = len(set(burst.trace_ids) & set(together.traces))
    return together.process_path(), EvidenceFragment(
        collector="process_path",
        quality=EvidenceQuality.PRESENT,
        summary=f"Open flows of {held} of {requests} failing requests, as assembled so far",
        data={"flows": len(flows), "held": held, "requests": requests},
    )
