"""Paths and path drift: how a complete flow ends, what a folded flow adds, and the three rules."""

from __future__ import annotations

import logging
from dataclasses import replace

import pytest
from pydantic import ValidationError
from spans import ENTRY, SERVICE, T0, at, order_flow, span, trace_id

from fire.apps.incident_engine.wiring import log_path_drift
from fire.core.domain.baseline import BaselineStatus, DriftPolicy
from fire.core.domain.dossier import HopVia
from fire.core.domain.flow import CompleteFlow, FlowCounters, Node, ShapeRef
from fire.core.domain.observation import SpanKind
from fire.core.domain.path import (
    FlowOutcome,
    Path,
    PathDrift,
    PathDriftFiring,
    PathDriftRule,
    PathDriftSignal,
    settle,
)

SYSTEM = "shop"
POLICY = DriftPolicy(min_samples=3)
PUT = "DynamoDB.PutItem orders"
PAY = "POST payments.example.com/v1/charges"
TAX = "POST tax.example.com/v1/codes"
ROOT = Node(service=SERVICE, operation=ENTRY)
PUT_NODE = Node(service=SERVICE, operation=PUT)
PAY_NODE = Node(service=SERVICE, operation=PAY)
TAX_NODE = Node(service=SERVICE, operation=TAX)
ADDED = PathDriftRule.HOP_ADDED
MISSING = PathDriftRule.HOP_MISSING
FAILING = PathDriftRule.HOP_FAILING


def _settle(
    flow: CompleteFlow,
    paths: dict[Node, Path],
    counters: FlowCounters | None = None,
    policy: DriftPolicy = POLICY,
) -> tuple[FlowOutcome, PathDrift | None]:
    return settle(flow, paths, policy, counters or FlowCounters(), SYSTEM)


def _warm(*calls: str, flows: int = 3, failing: tuple[str, ...] = ()) -> dict[Node, Path]:
    """A path warmed by `flows` order flows with `calls`, numbered from trace 1."""
    paths: dict[Node, Path] = {}
    for number in range(1, flows + 1):
        assert _settle(order_flow(number, *calls, failing=failing), paths)[0] is FlowOutcome.FOLDED
    return paths


def _rules(drift: PathDrift | None) -> list[tuple[PathDriftRule, str, bool]]:
    assert drift is not None
    return [
        (firing.signal.rule, firing.signal.node.operation, firing.new) for firing in drift.firings
    ]


# Outcomes


@pytest.mark.unit
def test_an_orphan_counts_on_the_system_and_its_shapes_count_as_unattached() -> None:
    paths: dict[Node, Path] = {}
    counters = FlowCounters()
    orphan = CompleteFlow(
        head=trace_id(1),
        traces=(trace_id(1),),
        spans=(span(1, 1, PAY, kind=SpanKind.CLIENT),),
        shapes=(ShapeRef("http:outbound:x:request", trace_id(1)),),
    )

    assert _settle(orphan, paths, counters) == (FlowOutcome.ORPHAN, None)

    assert paths == {}
    assert counters == FlowCounters(orphan_flows=1, unattached_shapes=1)


@pytest.mark.unit
def test_a_truncated_flow_is_counted_before_a_broken_one_and_neither_folds() -> None:
    paths: dict[Node, Path] = {}
    flow = order_flow(1, PUT)
    broken = replace(flow, spans=(*flow.spans, span(1, 9, "GET /orders", kind=SpanKind.SERVER)))

    assert _settle(replace(broken, truncated=True), paths)[0] is FlowOutcome.TRUNCATED
    assert _settle(broken, paths)[0] is FlowOutcome.BROKEN

    path = paths[ROOT]
    assert (path.flows, path.truncated_flows, path.broken_flows, path.nodes) == (0, 1, 1, {})


@pytest.mark.unit
def test_a_folded_flow_adds_its_nodes_edges_and_channels() -> None:
    paths: dict[Node, Path] = {}
    flow = order_flow(1, PUT, PAY, PUT, failing=(PAY,))
    flow = replace(
        flow,
        shapes=(
            ShapeRef("http:outbound:pay:request", trace_id(1), flow.spans[2].span_id),
            ShapeRef("event:internal:order.created:payload", trace_id(1)),
            ShapeRef("http:outbound:lost:request", trace_id(1), "00000000000000ff"),
        ),
    )
    counters = FlowCounters()

    assert _settle(flow, paths, counters) == (FlowOutcome.FOLDED, None)
    _settle(order_flow(2, PUT), paths, counters)

    summary = paths[ROOT].summary(POLICY)
    assert (summary.status, summary.flows, summary.drifted_flows) == (
        BaselineStatus.REBUILDING,
        2,
        0,
    )
    assert [
        (node.operation, node.kinds, node.flows, node.spans, node.errors, node.channels)
        for node in summary.nodes
    ] == [
        (PUT, [SpanKind.CLIENT], 2, 3, 0, {}),
        (ENTRY, [SpanKind.SERVER], 2, 2, 0, {}),
        (PAY, [SpanKind.CLIENT], 1, 1, 1, {"http:outbound:pay:request": 1}),
    ]
    assert [
        (edge.from_, edge.to, edge.via, edge.flows, edge.count, edge.errors)
        for edge in summary.edges
    ] == [(1, 0, HopVia.CHILD, 2, 3, 0), (1, 2, HopVia.CHILD, 1, 1, 1)]
    assert summary.flow_channels == {"event:internal:order.created:payload": 1}
    assert counters.unattached_shapes == 1


@pytest.mark.unit
def test_a_path_summary_sorts_nodes_edges_and_channels_and_dumps_from() -> None:
    paths: dict[Node, Path] = {}
    flow = order_flow(1, TAX, PAY, PUT)
    flow = replace(
        flow,
        shapes=(
            ShapeRef("z:payload", trace_id(1)),
            ShapeRef("a:payload", trace_id(1)),
        ),
    )
    _settle(flow, paths)

    summary = paths[ROOT].summary(POLICY)

    assert [node.operation for node in summary.nodes] == [PUT, ENTRY, PAY, TAX]
    assert [(edge.from_, edge.to) for edge in summary.edges] == [(1, 0), (1, 2), (1, 3)]
    assert list(summary.flow_channels) == ["a:payload", "z:payload"]
    dumped = summary.model_dump(mode="json", by_alias=True)
    assert dumped["edges"][0] == {
        "from": 1,
        "to": 0,
        "via": "child",
        "flows": 1,
        "count": 1,
        "errors": 0,
    }
    assert dumped["entry"] == {"service": SERVICE, "operation": ENTRY}


# Warm-up


@pytest.mark.unit
def test_a_rebuilding_path_folds_every_flow_without_comparing() -> None:
    paths = _warm(PUT, flows=2)

    assert _settle(order_flow(3, PAY), paths) == (FlowOutcome.FOLDED, None)

    path = paths[ROOT]
    assert (path.status(POLICY), path.flows, path.signals) == (BaselineStatus.WARM, 3, {})
    assert path.nodes[PUT_NODE].flows == 2


@pytest.mark.unit
def test_a_warm_path_compares_the_next_flow() -> None:
    paths = _warm(PUT)
    assert paths[ROOT].status(POLICY) is BaselineStatus.WARM

    outcome, drift = _settle(order_flow(4, PUT, PAY), paths)

    assert outcome is FlowOutcome.DRIFTED
    assert _rules(drift) == [(ADDED, PAY, True)]


# Rules


@pytest.mark.unit
def test_hop_added_fires_for_a_node_the_path_never_reached() -> None:
    paths = _warm(PUT)

    _outcome, drift = _settle(order_flow(4, PUT, PAY, start=30), paths)

    assert drift is not None
    assert (drift.system, drift.entry, drift.started_at) == (SYSTEM, ROOT, at(30))
    assert drift.trace_ids == [trace_id(4)]
    [firing] = drift.firings
    assert firing == PathDriftFiring(
        signal=PathDriftSignal(
            rule=ADDED,
            node=PAY_NODE,
            occurrences=1,
            first_seen=at(30),
            last_seen=at(30),
            trace_ids=[trace_id(4)],
        ),
        new=True,
        path_flows=3,
        node_flows=0,
        node_errors=0,
        flow_spans=1,
        flow_errors=0,
    )


@pytest.mark.unit
def test_hop_missing_fires_for_a_required_node_the_flow_did_not_reach() -> None:
    paths = _warm(PUT, PAY)

    _outcome, drift = _settle(order_flow(4, PUT), paths)

    assert drift is not None
    [firing] = drift.firings
    assert (firing.signal.rule, firing.signal.node) == (MISSING, PAY_NODE)
    assert (firing.path_flows, firing.node_flows, firing.node_errors) == (3, 3, 0)
    assert (firing.flow_spans, firing.flow_errors) == (0, 0)


@pytest.mark.unit
def test_hop_failing_fires_for_a_node_with_no_errors_in_the_path() -> None:
    paths = _warm(PUT, PAY)

    _outcome, drift = _settle(order_flow(4, PUT, PAY, failing=(PAY,)), paths)

    assert drift is not None
    [firing] = drift.firings
    assert (firing.signal.rule, firing.signal.node) == (FAILING, PAY_NODE)
    assert (firing.node_flows, firing.node_errors, firing.flow_spans, firing.flow_errors) == (
        3,
        0,
        1,
        1,
    )


@pytest.mark.unit
def test_hop_failing_never_fires_for_a_node_that_erred_while_warming() -> None:
    paths: dict[Node, Path] = {}
    _settle(order_flow(1, PUT, PAY, failing=(PAY,)), paths)
    _settle(order_flow(2, PUT, PAY), paths)
    _settle(order_flow(3, PUT, PAY), paths)

    assert _settle(order_flow(4, PUT, PAY, failing=(PAY,)), paths) == (FlowOutcome.FOLDED, None)


@pytest.mark.unit
def test_required_ratio_is_a_share_of_the_paths_flows_at_its_boundary() -> None:
    policy = DriftPolicy(min_samples=4, required_ratio=0.75)

    def warmed(*flows: tuple[str, ...]) -> dict[Node, Path]:
        paths: dict[Node, Path] = {}
        for number, calls in enumerate(flows, start=1):
            _settle(order_flow(number, *calls), paths, policy=policy)
        return paths

    # TAX in 3 of 4 flows is required; in 2 of 4 it is not. PAY is in 3 of 4 both times.
    at_ratio = warmed((PUT, PAY, TAX), (PUT, PAY, TAX), (PUT, PAY, TAX), (PUT,))
    below_ratio = warmed((PUT, PAY, TAX), (PUT, PAY, TAX), (PUT, PAY), (PUT,))
    assert at_ratio[ROOT].nodes[TAX_NODE].flows == 3
    assert below_ratio[ROOT].nodes[TAX_NODE].flows == 2

    assert _rules(_settle(order_flow(5, PUT), at_ratio, policy=policy)[1]) == [
        (MISSING, PAY, True),
        (MISSING, TAX, True),
    ]
    assert _rules(_settle(order_flow(5, PUT), below_ratio, policy=policy)[1]) == [
        (MISSING, PAY, True)
    ]


@pytest.mark.unit
def test_rules_fire_together_in_rule_order_then_by_node() -> None:
    paths = _warm(PUT, PAY, TAX)

    outcome, drift = _settle(
        order_flow(4, "POST z.example.com/v1/x", "POST a.example.com/v1/x", PAY, failing=(PAY,)),
        paths,
    )

    assert outcome is FlowOutcome.DRIFTED
    assert _rules(drift) == [
        (ADDED, "POST a.example.com/v1/x", True),
        (ADDED, "POST z.example.com/v1/x", True),
        (MISSING, PUT, True),
        (MISSING, TAX, True),
        (FAILING, PAY, True),
    ]


# Signals


@pytest.mark.unit
def test_a_drifted_flow_is_not_folded() -> None:
    paths = _warm(PUT)

    _settle(order_flow(4, PUT, PAY), paths)

    path = paths[ROOT]
    assert (path.flows, path.drifted_flows) == (3, 1)
    assert PAY_NODE not in path.nodes


@pytest.mark.unit
def test_one_hundred_drifted_flows_are_one_signal_with_one_hundred_occurrences() -> None:
    paths = _warm(PUT, PAY)
    starts = [50 + (index * 37) % 100 for index in range(100)]

    results = [
        _settle(order_flow(10 + index, PUT, start=start), paths)
        for index, start in enumerate(starts)
    ]

    assert [outcome for outcome, _drift in results] == [FlowOutcome.DRIFTED] * 100
    assert [drift is not None for _outcome, drift in results] == [True] + [False] * 99
    [signal] = paths[ROOT].summary(POLICY).drift
    assert signal == PathDriftSignal(
        rule=MISSING,
        node=PAY_NODE,
        occurrences=100,
        first_seen=at(min(starts)),
        last_seen=at(max(starts)),
        trace_ids=[trace_id(10)],
    )
    assert paths[ROOT].drifted_flows == 100


@pytest.mark.unit
def test_a_flow_with_one_new_signal_carries_its_old_ones_too() -> None:
    paths = _warm(PUT, PAY)
    _settle(order_flow(4, PUT), paths)

    _outcome, drift = _settle(order_flow(5, TAX), paths)

    assert _rules(drift) == [(ADDED, TAX, True), (MISSING, PUT, True), (MISSING, PAY, False)]
    assert drift is not None
    assert drift.firings[2].signal.occurrences == 2


@pytest.mark.unit
def test_signals_sort_by_first_seen_then_rule_then_node() -> None:
    paths = _warm(PUT, PAY)
    _settle(order_flow(4, PUT, start=20), paths)
    _settle(order_flow(5, PUT, PAY, TAX, start=10), paths)
    _settle(order_flow(6, PAY, start=10), paths)

    drift = paths[ROOT].summary(POLICY).drift

    assert [(signal.first_seen, signal.rule, signal.node.operation) for signal in drift] == [
        (at(10), ADDED, TAX),
        (at(10), MISSING, PUT),
        (at(20), MISSING, PAY),
    ]


@pytest.mark.unit
def test_summaries_and_firings_are_copies() -> None:
    paths = _warm(PUT)
    _outcome, drift = _settle(order_flow(4, PUT, PAY), paths)
    assert drift is not None

    drift.firings[0].signal.occurrences = 50
    paths[ROOT].summary(POLICY).drift[0].occurrences = 60

    assert paths[ROOT].signals[(ADDED, PAY_NODE)].occurrences == 1


@pytest.mark.unit
@pytest.mark.parametrize(
    "flow",
    [
        CompleteFlow(
            head=trace_id(9),
            traces=(trace_id(9),),
            spans=(span(9, 1, TAX, kind=SpanKind.CLIENT),),
        ),
        replace(order_flow(9, TAX), truncated=True),
        replace(
            order_flow(9, TAX),
            spans=(*order_flow(9, TAX).spans, span(9, 8, "GET /x", kind=SpanKind.SERVER)),
        ),
    ],
    ids=["orphan", "truncated", "broken"],
)
def test_orphan_truncated_and_broken_flows_raise_nothing_on_a_warm_path(
    flow: CompleteFlow,
) -> None:
    paths = _warm(PUT)

    outcome, drift = _settle(flow, paths)

    assert outcome in {FlowOutcome.ORPHAN, FlowOutcome.TRUNCATED, FlowOutcome.BROKEN}
    assert drift is None
    assert paths[ROOT].signals == {}


@pytest.mark.unit
def test_a_drift_carries_the_flows_process_path() -> None:
    paths = _warm(PUT)

    _outcome, drift = _settle(order_flow(4, PUT, PAY), paths)

    assert drift is not None
    assert drift.process_path == order_flow(4, PUT, PAY).process_path()
    assert [hop.operation for hop in drift.process_path.hops] == [ENTRY, PUT, PAY]


@pytest.mark.unit
def test_log_path_drift_writes_one_info_line_with_the_entry_and_counts(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    paths = _warm(PUT, PAY)
    _settle(order_flow(4, PUT), paths)
    _outcome, drift = _settle(order_flow(5, TAX), paths)
    assert drift is not None

    log_path_drift(drift)

    assert caplog.record_tuples == [
        (
            "fire.apps.incident_engine.wiring",
            logging.INFO,
            "path drift raised: system=shop service=orders-api entry=POST /orders "
            "hop_added=1 hop_missing=2 hop_failing=0 new=2",
        )
    ]


# Models


@pytest.mark.unit
@pytest.mark.parametrize(
    ("update", "loc"),
    [
        ({"occurrences": 0}, ("occurrences",)),
        ({"trace_ids": []}, ("trace_ids",)),
        ({"trace_ids": ["not-a-trace"]}, ("trace_ids", 0)),
        ({"first_seen": "2026-10-11T16:40:00"}, ("first_seen",)),
        ({"rule": "hop_moved"}, ("rule",)),
        ({"node": {"service": SERVICE, "operation": PUT, "kind": "client"}}, ("node", "kind")),
        ({"value": "x"}, ("value",)),
    ],
    ids=["zero", "no-traces", "bad-trace", "naive", "rule", "node-extra", "extra"],
)
def test_a_path_drift_signal_rejects_bad_fields(update: dict[str, object], loc: tuple) -> None:
    valid = {
        "rule": "hop_missing",
        "node": {"service": SERVICE, "operation": PUT},
        "occurrences": 1,
        "first_seen": T0.isoformat(),
        "last_seen": T0.isoformat(),
        "trace_ids": [trace_id(1)],
    }
    with pytest.raises(ValidationError) as caught:
        PathDriftSignal.model_validate({**valid, **update})
    assert [error["loc"] for error in caught.value.errors()] == [loc]
