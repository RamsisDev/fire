"""Builders for OTLP JSON trace requests, as a collector's `otlphttp` exporter with
`encoding: json` writes them: hex ids, integer enums, and 64-bit integers as strings.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

START_NS = 1_791_768_368_257_502_981


def value(item: Any) -> dict[str, Any]:
    """An OTLP `AnyValue`."""
    if isinstance(item, bool):
        return {"boolValue": item}
    if isinstance(item, int):
        return {"intValue": str(item)}
    if isinstance(item, float):
        return {"doubleValue": item}
    if isinstance(item, list | tuple):
        return {"arrayValue": {"values": [value(element) for element in item]}}
    if isinstance(item, Mapping):
        return {"kvlistValue": {"values": attributes(item)}}
    return {"stringValue": item}


def attributes(items: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [{"key": key, "value": value(item)} for key, item in items.items()]


def event(name: str, items: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {
        "timeUnixNano": str(START_NS + 1000),
        "name": name,
        "attributes": attributes(items or {}),
    }


def link(trace_id: str, span_id: str, items: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {"traceId": trace_id, "spanId": span_id, "attributes": attributes(items or {})}


def span(
    trace_id: str,
    span_id: str,
    name: str,
    *,
    kind: int | str = 1,
    parent: str = "",
    duration_ns: int = 5_000_000,
    items: Mapping[str, Any] | None = None,
    events: Sequence[dict[str, Any]] = (),
    links: Sequence[dict[str, Any]] = (),
    status: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    built: dict[str, Any] = {
        "traceId": trace_id,
        "spanId": span_id,
        "flags": 256,
        "name": name,
        "kind": kind,
        "startTimeUnixNano": str(START_NS),
        "endTimeUnixNano": str(START_NS + duration_ns),
        "attributes": attributes(items or {}),
        "status": dict(status or {}),
    }
    if parent:
        built["parentSpanId"] = parent
    if events:
        built["events"] = list(events)
    if links:
        built["links"] = list(links)
    return built


def resource_spans(
    service: str, spans: Sequence[dict[str, Any]], resource: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    return {
        "resource": {"attributes": attributes({"service.name": service, **(resource or {})})},
        "scopeSpans": [
            {
                "scope": {"name": "opentelemetry.instrumentation.test", "version": "0.66b0"},
                "spans": list(spans),
            }
        ],
    }


def request(*resources: dict[str, Any]) -> dict[str, Any]:
    """An `ExportTraceServiceRequest`."""
    return {"resourceSpans": list(resources)}
