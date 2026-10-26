"""Coverage audit: what a system declares versus what FIRE observes.

Contract: docs/specs/ingest-drift-and-feedback.md#coverage-and-contract.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from fire.core.domain.inventory import (
    CoverageReport,
    MissingCoverage,
    OutboundCoverage,
    RouteCoverage,
    SystemCoverage,
    SystemInventory,
)
from fire.core.domain.observation import ChannelDirection, ChannelKind, StreamSummary


@dataclass(frozen=True)
class RouteScope:
    """One inbound source's declared routes, each `METHOD template`, or why it has none."""

    source: str
    routes: frozenset[str] | None
    missing_reason: str | None = None

    def __post_init__(self) -> None:
        if (self.routes is None) == (self.missing_reason is None):
            raise ValueError("a route scope needs exactly one of routes and missing_reason")


@dataclass(frozen=True)
class CoverageScope:
    """What one system's coverage audit needs: its inventory or why it has none, its policy, and
    one `RouteScope` per inbound proxy source.
    """

    inventory: SystemInventory | None
    missing_reason: str | None = None
    critical: tuple[str, ...] = ()
    ignore: tuple[str, ...] = ()
    routes: tuple[RouteScope, ...] = ()

    def __post_init__(self) -> None:
        if (self.inventory is None) == (self.missing_reason is None):
            raise ValueError("a coverage scope needs exactly one of inventory and missing_reason")


def audit_coverage(
    inventory: SystemInventory,
    observed: Iterable[str],
    *,
    critical: Iterable[str] = (),
    ignore: Iterable[str] = (),
) -> CoverageReport:
    """Compare declared event names with observed ones.

    Ignored events are left out of both gap lists. A `critical` or `ignore` reference that is
    neither declared nor observed is reported, so config cannot go stale silently.
    """
    declared = inventory.event_names()
    seen = set(observed)
    ignored = set(ignore)
    references = set(critical) | ignored
    return CoverageReport(
        system=inventory.system,
        declared_unseen=sorted(declared - seen - ignored),
        seen_undeclared=sorted(seen - declared - ignored),
        unknown_references=sorted(references - declared - seen),
    )


def observed_events(streams: Iterable[StreamSummary]) -> set[str]:
    """The names of the `kind: event` channels, across sources."""
    return {stream.channel.name for stream in streams if stream.channel.kind is ChannelKind.EVENT}


def report_coverage(
    system: str, scope: CoverageScope, streams: Iterable[StreamSummary]
) -> SystemCoverage:
    """Declared events against observed ones, each inbound source's declared routes against the
    `http` inbound channel names observed on it, and the outbound channel names per source.
    """
    streams = list(streams)
    if scope.inventory is None:
        events: CoverageReport | MissingCoverage = MissingCoverage(
            missing_reason=scope.missing_reason
        )
    else:
        events = audit_coverage(
            scope.inventory,
            observed_events(streams),
            critical=scope.critical,
            ignore=scope.ignore,
        )
    return SystemCoverage(
        system=system,
        events=events,
        inbound_routes=[
            _routes(route_scope, streams)
            for route_scope in sorted(scope.routes, key=lambda route_scope: route_scope.source)
        ],
        outbound_operations=_outbound(streams),
    )


def _routes(scope: RouteScope, streams: list[StreamSummary]) -> RouteCoverage:
    if scope.routes is None:
        return RouteCoverage(source=scope.source, missing_reason=scope.missing_reason)
    seen = {
        stream.channel.name
        for stream in streams
        if stream.source == scope.source
        and stream.channel.kind is ChannelKind.HTTP
        and stream.channel.direction is ChannelDirection.INBOUND
    }
    return RouteCoverage(
        source=scope.source,
        declared_unseen=sorted(scope.routes - seen),
        seen_undeclared=sorted(seen - scope.routes),
    )


def _outbound(streams: list[StreamSummary]) -> list[OutboundCoverage]:
    operations: dict[str, set[str]] = {}
    for stream in streams:
        if stream.channel.direction is ChannelDirection.OUTBOUND:
            operations.setdefault(stream.source, set()).add(stream.channel.name)
    return [
        OutboundCoverage(source=source, operations=sorted(names))
        for source, names in sorted(operations.items())
    ]
