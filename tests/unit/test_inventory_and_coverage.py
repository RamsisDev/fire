"""SystemInventory invariants, the declared-versus-observed coverage audit, and the coverage
report of events, inbound routes, and outbound operations.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import ValidationError

from fire.core.domain.baseline import BaselineStatus
from fire.core.domain.inventory import (
    CoverageReport,
    DeclaredEvent,
    DeclaredIngress,
    DeclaredProcessor,
    MissingCoverage,
    OutboundCoverage,
    RouteCoverage,
    SystemCoverage,
    SystemInventory,
)
from fire.core.domain.observation import Channel, FingerprintSummary, StreamSummary
from fire.core.services.coverage import (
    CoverageScope,
    RouteScope,
    audit_coverage,
    report_coverage,
)


def _inventory(*names: str) -> SystemInventory:
    return SystemInventory(
        system="example",
        manifest_kind="event_chains_graph",
        events=[DeclaredEvent(name=name) for name in names],
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "field, items",
    [
        ("events", [DeclaredEvent(name="a"), DeclaredEvent(name="a")]),
        ("processors", [DeclaredProcessor(name="p"), DeclaredProcessor(name="p")]),
        ("ingress", [DeclaredIngress(id="i"), DeclaredIngress(id="i")]),
    ],
)
def test_inventory_rejects_duplicate_names(field: str, items: list) -> None:
    with pytest.raises(ValidationError, match="duplicate"):
        SystemInventory(system="example", manifest_kind="k", **{field: items})


@pytest.mark.unit
def test_inventory_rejects_empty_names() -> None:
    with pytest.raises(ValidationError):
        DeclaredEvent(name="")


@pytest.mark.unit
def test_full_coverage_is_complete() -> None:
    report = audit_coverage(_inventory("a", "b"), ["b", "a", "a"])
    assert report.is_complete
    assert report.system == "example"


@pytest.mark.unit
def test_reports_declared_unseen_and_seen_undeclared_sorted() -> None:
    report = audit_coverage(_inventory("c", "a", "b"), ["b", "z", "y"])
    assert report.declared_unseen == ["a", "c"]
    assert report.seen_undeclared == ["y", "z"]
    assert not report.is_complete


@pytest.mark.unit
def test_ignored_events_leave_both_gap_lists() -> None:
    report = audit_coverage(_inventory("a", "noisy"), ["a", "extra"], ignore=["noisy", "extra"])
    assert report.is_complete


@pytest.mark.unit
def test_references_to_unknown_names_are_reported() -> None:
    report = audit_coverage(
        _inventory("a"), ["seen"], critical=["a", "gone"], ignore=["seen", "renamed"]
    )
    assert report.unknown_references == ["gone", "renamed"]


@pytest.mark.unit
def test_nothing_observed_lists_every_declared_event() -> None:
    report = audit_coverage(_inventory("b", "a"), [])
    assert report.declared_unseen == ["a", "b"]


@pytest.mark.unit
def test_coverage_scope_holds_an_inventory_or_a_missing_reason() -> None:
    scope = CoverageScope(inventory=_inventory("a"), critical=("a",), ignore=("b",))
    assert (scope.missing_reason, scope.critical, scope.ignore) == (None, ("a",), ("b",))
    assert CoverageScope(inventory=None, missing_reason="no manifest").inventory is None


@pytest.mark.unit
@pytest.mark.parametrize(
    ("inventory", "missing_reason"),
    [(_inventory("a"), "no manifest"), (None, None)],
    ids=["both", "neither"],
)
def test_coverage_scope_rejects_both_or_neither(
    inventory: SystemInventory | None, missing_reason: str | None
) -> None:
    with pytest.raises(ValueError, match="exactly one"):
        CoverageScope(inventory=inventory, missing_reason=missing_reason)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("routes", "missing_reason"),
    [(frozenset({"GET /a"}), "no document"), (None, None)],
    ids=["both", "neither"],
)
def test_route_scope_rejects_both_or_neither(
    routes: frozenset[str] | None, missing_reason: str | None
) -> None:
    with pytest.raises(ValueError, match="exactly one"):
        RouteScope("api", routes, missing_reason)


@pytest.mark.unit
@pytest.mark.parametrize(
    "fields",
    [
        {},
        {"declared_unseen": []},
        {"missing_reason": "no document", "declared_unseen": [], "seen_undeclared": []},
        {"missing_reason": "no document", "seen_undeclared": []},
    ],
    ids=["neither", "one-list", "both", "reason-and-a-list"],
)
def test_route_coverage_holds_both_lists_or_a_missing_reason(fields: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        RouteCoverage(source="api", **fields)


@pytest.mark.unit
def test_route_coverage_rejects_an_empty_missing_reason() -> None:
    with pytest.raises(ValidationError):
        RouteCoverage(source="api", missing_reason="")


@pytest.mark.unit
@pytest.mark.parametrize(
    "events",
    [
        {"missing_reason": "no manifest", "declared_unseen": []},
        {"system": "example", "declared_unseen": [], "missing_reason": "no manifest"},
    ],
    ids=["missing-with-a-list", "report-with-a-reason"],
)
def test_coverage_events_are_a_report_or_a_reason_never_both(events: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        SystemCoverage.model_validate({"system": "example", "events": events})


@pytest.mark.unit
def test_coverage_events_validate_back_to_their_model() -> None:
    report = CoverageReport(system="example", declared_unseen=["a"])
    missing = MissingCoverage(missing_reason="no manifest")
    for events in (report, missing):
        dumped = SystemCoverage(system="example", events=events).model_dump(mode="json")
        assert SystemCoverage.model_validate(dumped).events == events


T0 = datetime(2026, 10, 11, 16, 30, tzinfo=UTC)


def _stream(source: str, kind: str, direction: str, name: str, part: str) -> StreamSummary:
    channel: dict[str, Any] = {"kind": kind, "direction": direction, "name": name, "part": part}
    if part == "response":
        channel["status_class"] = "2xx"
    return StreamSummary(
        source=source,
        channel=Channel.model_validate(channel),
        status=BaselineStatus.WARM,
        samples=1,
        fingerprints=[
            FingerprintSummary(
                schema_hash="h", observations=1, first_observed_at=T0, last_observed_at=T0
            )
        ],
    )


STREAMS = [
    _stream("api", "http", "inbound", "POST /orders", "request"),
    _stream("api", "http", "inbound", "POST /orders", "response"),
    _stream("api", "http", "inbound", "GET /unlisted", "response"),
    _stream("web", "http", "inbound", "GET /orders/{order_id}", "response"),
    _stream("events", "event", "internal", "order.created", "payload"),
    _stream("events", "event", "inbound", "inventory.stock_updated", "payload"),
    _stream("traces", "http", "outbound", "partner/orders", "response"),
    _stream("traces", "http", "outbound", "partner/orders", "request"),
    _stream("captures", "http", "outbound", "partner:submit", "request"),
    _stream("captures", "http", "outbound", "shipping:cancel", "response"),
]
API_ROUTES = frozenset({"POST /orders", "GET /orders/{order_id}", "DELETE /orders/{order_id}"})


@pytest.mark.unit
def test_report_audits_the_observed_event_channels_against_the_inventory() -> None:
    scope = CoverageScope(inventory=_inventory("order.created", "order.shipped"), ignore=("x",))
    report = report_coverage("example", scope, STREAMS)
    assert report.events == CoverageReport(
        system="example",
        declared_unseen=["order.shipped"],
        seen_undeclared=["inventory.stock_updated"],
        unknown_references=["x"],
    )


@pytest.mark.unit
def test_report_without_an_inventory_gives_the_events_missing_reason() -> None:
    report = report_coverage("example", CoverageScope(None, "no manifest"), STREAMS)
    assert report.events == MissingCoverage(missing_reason="no manifest")


@pytest.mark.unit
def test_report_compares_each_inbound_sources_routes_with_its_own_channels() -> None:
    scope = CoverageScope(
        None,
        "no manifest",
        routes=(
            RouteScope("web", None, "WEB_OPENAPI_PATH is not set"),
            RouteScope("api", API_ROUTES),
            RouteScope("idle", frozenset({"GET /b", "GET /a"})),
        ),
    )
    report = report_coverage("example", scope, STREAMS)
    assert report.inbound_routes == [
        RouteCoverage(
            source="api",
            declared_unseen=["DELETE /orders/{order_id}", "GET /orders/{order_id}"],
            seen_undeclared=["GET /unlisted"],
        ),
        RouteCoverage(source="idle", declared_unseen=["GET /a", "GET /b"], seen_undeclared=[]),
        RouteCoverage(source="web", missing_reason="WEB_OPENAPI_PATH is not set"),
    ]


@pytest.mark.unit
def test_report_lists_the_outbound_operations_per_source() -> None:
    report = report_coverage("example", CoverageScope(None, "no manifest"), STREAMS)
    assert report.outbound_operations == [
        OutboundCoverage(source="captures", operations=["partner:submit", "shipping:cancel"]),
        OutboundCoverage(source="traces", operations=["partner/orders"]),
    ]


@pytest.mark.unit
def test_report_of_a_system_with_nothing_observed() -> None:
    scope = CoverageScope(_inventory("b", "a"), routes=(RouteScope("api", API_ROUTES),))
    report = report_coverage("example", scope, [])
    assert report.model_dump(mode="json", exclude_none=True) == {
        "system": "example",
        "events": {
            "system": "example",
            "declared_unseen": ["a", "b"],
            "seen_undeclared": [],
            "unknown_references": [],
        },
        "inbound_routes": [
            {
                "source": "api",
                "declared_unseen": sorted(API_ROUTES),
                "seen_undeclared": [],
            }
        ],
        "outbound_operations": [],
    }
