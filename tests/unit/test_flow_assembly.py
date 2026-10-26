"""FlowAssembler and CompleteFlow: spans into flows, entries, caps, late spans, shapes on flows,
and the dossier's process path. Every time comes from an injected clock; nothing sleeps.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError
from spans import ENTRY, SERVICE, at, span, span_id, trace_id

from fire.core.domain.dossier import HopVia, ProcessHop
from fire.core.domain.flow import (
    COMPLETED_RETENTION,
    CompleteFlow,
    EdgeReach,
    FlowAssembler,
    FlowCounters,
    Node,
    NodeReach,
    PathPolicy,
    ShapeRef,
)
from fire.core.domain.observation import SpanKind, SpanObservation

SETTLE = 60
SERVER = SpanKind.SERVER
CLIENT = SpanKind.CLIENT
INTERNAL = SpanKind.INTERNAL
PRODUCER = SpanKind.PRODUCER
CONSUMER = SpanKind.CONSUMER
WORKER = "orders-worker"
PROCESSOR = "orders-processor"
LISTENER = "listener.distribute_work"
PUT = "DynamoDB.PutItem orders"
PUBLISH = "SNS.Publish order-events"
PROCESS = "processor.process_work_unit"
PARTNER = "POST api.partner.example.com/v1/orders"
PROXY = 0xF00
ROOT = Node(service=SERVICE, operation=ENTRY)
REQUEST = "http:inbound:POST /orders:request"
RESPONSE = "http:inbound:POST /orders:response:2xx"
PARTNER_REQUEST = f"http:outbound:{PARTNER}:request"
EVENT = "event:internal:order.created:payload"
CAPTURE = "http:outbound:partner:place:request"


def _assembler(**policy: Any) -> FlowAssembler:
    return FlowAssembler(PathPolicy(settle_s=SETTLE, **policy))


def _feed(assembler: FlowAssembler, spans: list[SpanObservation], seconds: float = 0) -> None:
    for item in spans:
        assembler.add_span(item, at(seconds))


def _complete(assembler: FlowAssembler, seconds: float) -> list[CompleteFlow]:
    return assembler.complete(at(seconds))


def _shape(
    channel: str, trace: int | None = None, number: int | None = None, work: str | None = None
) -> ShapeRef:
    return ShapeRef(
        channel_key=channel,
        trace_id=None if trace is None else trace_id(trace),
        span_id=None if number is None else span_id(number),
        work_id=work,
    )


def _api() -> list[SpanObservation]:
    return [
        span(1, 1, ENTRY, kind=SERVER, status_code=201),
        span(1, 2, PUT, kind=CLIENT, parent=1, status_code=200, start=1),
    ]


def _listener() -> list[SpanObservation]:
    """Trace 2: the listener's root, linked to the API's server span, and its SNS publish."""
    return [
        span(2, 1, LISTENER, service=WORKER, links=[(1, 1)], start=5),
        span(2, 2, PUBLISH, kind=PRODUCER, service=WORKER, parent=1, status_code=200, start=6),
    ]


# Policy and node


@pytest.mark.unit
def test_path_policy_defaults() -> None:
    policy = PathPolicy()
    assert (policy.settle_s, policy.max_open_flows, policy.max_spans_per_flow) == (60, 10000, 1000)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("settle_s", 0.5),
        ("settle_s", 0),
        ("settle_s", float("nan")),
        ("settle_s", float("inf")),
        ("max_open_flows", 0),
        ("max_spans_per_flow", 0),
    ],
    ids=["settle-half", "settle-zero", "settle-nan", "settle-inf", "open-zero", "spans-zero"],
)
def test_path_policy_rejects_values_below_one(key: str, value: float) -> None:
    with pytest.raises(ValidationError) as caught:
        PathPolicy.model_validate({key: value})
    assert [error["loc"] for error in caught.value.errors()] == [(key,)]


@pytest.mark.unit
def test_path_policy_accepts_one_and_rejects_unknown_keys() -> None:
    assert PathPolicy(settle_s=1, max_open_flows=1, max_spans_per_flow=1).settle_s == 1
    with pytest.raises(ValidationError) as caught:
        PathPolicy.model_validate({"settle": 5})
    assert [error["loc"] for error in caught.value.errors()] == [("settle",)]


@pytest.mark.unit
def test_a_node_is_a_frozen_service_and_operation() -> None:
    node = Node(service=SERVICE, operation=ENTRY)
    assert node == ROOT and hash(node) == hash(ROOT)
    assert Node.of(span(1, 1, ENTRY)) == ROOT
    with pytest.raises(ValidationError):
        Node.model_validate({"service": SERVICE, "operation": ENTRY, "kind": "server"})


# Assembly and completion


@pytest.mark.unit
def test_a_span_tree_is_one_flow_with_child_edges_and_counts() -> None:
    assembler = _assembler()
    derive = Node(service=SERVICE, operation="tax.derive")
    put = Node(service=SERVICE, operation=PUT)
    tax = Node(service=SERVICE, operation="POST tax.example.com/v1/codes")
    _feed(
        assembler,
        [
            span(1, 1, ENTRY, kind=SERVER),
            span(1, 2, PUT, kind=CLIENT, parent=1),
            span(1, 3, "tax.derive", parent=1),
            span(1, 4, tax.operation, kind=CLIENT, parent=3, error=True),
            span(1, 5, PUT, kind=CLIENT, parent=1),
        ],
    )

    [flow] = _complete(assembler, SETTLE)

    assert (flow.head, flow.traces, flow.truncated, flow.broken) == (
        trace_id(1),
        (trace_id(1),),
        False,
        False,
    )
    assert flow.entry is not None and flow.entry.span_id == span_id(1)
    nodes, edges = flow.reach()
    assert nodes == {
        ROOT: NodeReach({SERVER}, 1, 0),
        put: NodeReach({CLIENT}, 2, 0),
        derive: NodeReach({INTERNAL}, 1, 0),
        tax: NodeReach({CLIENT}, 1, 1),
    }
    assert edges == {
        (ROOT, put, HopVia.CHILD): EdgeReach(2, 0),
        (ROOT, derive, HopVia.CHILD): EdgeReach(1, 0),
        (derive, tax, HopVia.CHILD): EdgeReach(1, 1),
    }
    assert assembler.open_flows == 0
    assert assembler.counters == FlowCounters()


@pytest.mark.unit
def test_a_flow_completes_only_once_settle_s_passes_with_no_span_joining_it() -> None:
    assembler = _assembler()
    _feed(assembler, _api()[:1], 0)
    _feed(assembler, _api()[1:], 30)

    assert _complete(assembler, SETTLE) == []
    assert _complete(assembler, 30 + SETTLE - 0.001) == []
    assert assembler.open_flows == 1
    [flow] = _complete(assembler, 30 + SETTLE)
    assert len(flow.spans) == 2
    assert assembler.open_flows == 0


@pytest.mark.unit
@pytest.mark.parametrize(
    "arrival",
    ["target first", "linker first", "linked child first"],
)
def test_a_linked_trace_joins_its_target_whenever_it_arrives(arrival: str) -> None:
    api, listener = _api(), _listener()
    batches = {
        "target first": [*api, *listener],
        "linker first": [*listener, *api],
        "linked child first": [listener[1], *api, listener[0]],
    }[arrival]
    assembler = _assembler()
    for seconds, item in enumerate(batches):
        assembler.add_span(item, at(seconds))

    [flow] = _complete(assembler, len(batches) - 1 + SETTLE)

    assert flow.head == trace_id(1)
    assert set(flow.traces) == {trace_id(1), trace_id(2)}
    assert flow.entry is not None and flow.entry.operation == ENTRY
    assert not flow.broken
    _nodes, edges = flow.reach()
    listener_node = Node(service=WORKER, operation=LISTENER)
    assert edges[(ROOT, listener_node, HopVia.LINK)] == EdgeReach(1, 0)
    assert assembler.counters == FlowCounters()


@pytest.mark.unit
def test_a_trace_joins_through_its_first_link_and_counts_later_ones() -> None:
    assembler = _assembler()
    _feed(assembler, [span(1, 1, ENTRY, kind=SERVER), span(2, 1, "GET /orders", kind=SERVER)])
    _feed(assembler, [span(3, 1, "batch.process", service=WORKER, links=[(1, 1), (2, 1)])], 1)
    _feed(assembler, [span(3, 2, "batch.again", service=WORKER, parent=1, links=[(2, 1)])], 2)

    flows = _complete(assembler, 2 + SETTLE)

    assert sorted(flow.traces for flow in flows) == [
        (trace_id(1), trace_id(3)),
        (trace_id(2),),
    ]
    assert assembler.counters.fan_in_links == 2
    joined = next(flow for flow in flows if flow.head == trace_id(1))
    _nodes, edges = joined.reach()
    assert [key for key in edges if key[2] is HopVia.LINK] == [
        (ROOT, Node(service=WORKER, operation="batch.process"), HopVia.LINK)
    ]


@pytest.mark.unit
def test_a_link_within_one_flow_is_an_edge() -> None:
    assembler = _assembler()
    _feed(assembler, [*_api(), *_listener()])
    _feed(assembler, [span(2, 3, "listener.ack", service=WORKER, parent=1, links=[(1, 2)])], 1)

    [flow] = _complete(assembler, 1 + SETTLE)

    _nodes, edges = flow.reach()
    put = Node(service=SERVICE, operation=PUT)
    assert (put, Node(service=WORKER, operation="listener.ack"), HopVia.LINK) in edges
    assert assembler.counters == FlowCounters()


@pytest.mark.unit
def test_a_span_of_a_completed_trace_is_late_and_reopens_nothing() -> None:
    assembler = _assembler()
    _feed(assembler, _api(), 0)
    [flow] = _complete(assembler, SETTLE)

    _feed(assembler, [span(1, 9, "retry", parent=1)], SETTLE + 1)
    # A new trace whose first link to resolve names the completed trace, then its child.
    _feed(assembler, _listener(), SETTLE + 2)

    assert assembler.counters.late_spans == 3
    assert assembler.open_flows == 0
    assert _complete(assembler, 3 * SETTLE) == []
    assert len(flow.spans) == 2


@pytest.mark.unit
def test_a_flow_whose_head_links_to_a_completed_trace_is_late_whole() -> None:
    assembler = _assembler()
    _feed(assembler, _api(), 0)
    _complete(assembler, SETTLE)
    listener_root, publish = _listener()
    _feed(assembler, [publish], SETTLE + 1)
    assembler.add_shape(_shape(EVENT, 2), at(SETTLE + 1))
    assert assembler.open_flows == 1

    _feed(assembler, [listener_root], SETTLE + 2)

    assert assembler.open_flows == 0
    assert (assembler.counters.late_spans, assembler.counters.late_shapes) == (2, 1)
    assert _complete(assembler, 3 * SETTLE) == []


@pytest.mark.unit
def test_the_completed_index_forgets_a_trace_after_its_retention() -> None:
    assembler = _assembler()
    _feed(assembler, _api(), 0)
    _complete(assembler, SETTLE)
    retention = COMPLETED_RETENTION * SETTLE

    _complete(assembler, SETTLE + retention - 1)
    _feed(assembler, [span(1, 8, "retry", parent=1)], SETTLE + retention - 1)
    assert (assembler.counters.late_spans, assembler.open_flows) == (1, 0)

    _complete(assembler, SETTLE + retention)
    _feed(assembler, [span(1, 9, "retry", parent=1)], SETTLE + retention)
    assert assembler.open_flows == 1
    [orphan] = _complete(assembler, SETTLE + retention + SETTLE)
    assert orphan.entry is None


# Entry, orphans, broken flows


@pytest.mark.unit
@pytest.mark.parametrize(
    "root",
    [
        span(1, 1, ENTRY, kind=SERVER),
        span(1, 1, ENTRY, kind=SERVER, parent=PROXY),
        span(1, 1, "orders.consume", kind=CONSUMER, links=[(9, 9)]),
        span(1, 1, "nightly.job"),
    ],
    ids=["server", "server-under-proxy-id", "consumer-with-unseen-link", "internal-no-cause"],
)
def test_a_root_that_takes_work_from_outside_is_the_entry(root: SpanObservation) -> None:
    assembler = _assembler()
    _feed(assembler, [root, span(1, 2, PUT, kind=CLIENT, parent=1)])

    [flow] = _complete(assembler, SETTLE)

    assert flow.entry == root
    assert not flow.broken


@pytest.mark.unit
@pytest.mark.parametrize(
    "root",
    [
        span(1, 1, PARTNER, kind=CLIENT),
        span(1, 1, PUBLISH, kind=PRODUCER),
        span(1, 1, LISTENER, links=[(9, 9)]),
        span(1, 1, PROCESS, parent=7),
    ],
    ids=["client", "producer", "internal-link-never-resolves", "internal-parent-never-arrives"],
)
def test_a_flow_without_an_entry_is_an_orphan(root: SpanObservation) -> None:
    assembler = _assembler()
    _feed(assembler, [root])

    [flow] = _complete(assembler, SETTLE)

    assert flow.entry is None


@pytest.mark.unit
def test_two_local_roots_make_a_flow_broken_and_the_earliest_qualifying_is_the_entry() -> None:
    assembler = _assembler()
    roots = [
        span(1, 5, "GET /b", kind=SERVER, start=1),
        span(1, 3, "GET /a", kind=SERVER, start=1),
        span(1, 4, PARTNER, kind=CLIENT, start=0),
        span(1, 6, "GET /c", kind=SERVER, start=2),
    ]
    _feed(assembler, roots)

    [flow] = _complete(assembler, SETTLE)

    assert flow.entry == roots[1]
    assert flow.broken


@pytest.mark.unit
def test_a_linked_trace_with_two_local_roots_breaks_the_flow() -> None:
    assembler = _assembler()
    _feed(assembler, [*_api(), *_listener(), span(2, 3, PROCESS, service=PROCESSOR, parent=7)])

    [flow] = _complete(assembler, SETTLE)

    assert flow.entry is not None and flow.entry.operation == ENTRY
    assert flow.broken


# Caps


@pytest.mark.unit
def test_spans_over_max_spans_per_flow_are_dropped_and_the_flow_is_truncated() -> None:
    assembler = _assembler(max_spans_per_flow=3)
    spans = [span(1, 1, ENTRY, kind=SERVER)] + [
        span(1, number, PUT, kind=CLIENT, parent=1) for number in range(2, 6)
    ]
    _feed(assembler, spans[:4], 0)
    _feed(assembler, spans[4:], 30)

    assert _complete(assembler, SETTLE) == []
    [flow] = _complete(assembler, 30 + SETTLE)

    assert flow.truncated
    assert [item.span_id for item in flow.spans] == [span_id(n) for n in (1, 2, 3)]


@pytest.mark.unit
def test_merging_two_flows_past_the_span_cap_truncates_the_flow() -> None:
    assembler = _assembler(max_spans_per_flow=3)
    listener_root, publish = _listener()
    _feed(assembler, [publish, *_api()])
    _feed(assembler, [listener_root], 1)

    [flow] = _complete(assembler, 1 + SETTLE)

    assert flow.truncated
    assert len(flow.spans) == 3


@pytest.mark.unit
def test_one_flow_more_than_max_open_flows_evicts_the_least_recently_joined() -> None:
    assembler = _assembler(max_open_flows=2)
    _feed(assembler, [span(1, 1, ENTRY, kind=SERVER)], 0)
    _feed(assembler, [span(2, 1, ENTRY, kind=SERVER)], 1)
    _feed(assembler, [span(1, 2, PUT, kind=CLIENT, parent=1)], 2)
    _feed(assembler, [span(3, 1, ENTRY, kind=SERVER)], 3)

    assert (assembler.counters.evicted_flows, assembler.open_flows) == (1, 2)
    _feed(assembler, [span(2, 2, PUT, kind=CLIENT, parent=1)], 4)
    assert assembler.counters.late_spans == 1

    flows = _complete(assembler, 3 + SETTLE)
    assert sorted(flow.head for flow in flows) == [trace_id(1), trace_id(3)]


# Shapes on flows


@pytest.mark.unit
def test_shapes_attach_by_span_by_proxy_id_or_by_trace_or_count_as_unattached() -> None:
    assembler = _assembler()
    partner = Node(service=SERVICE, operation=PARTNER)
    _feed(
        assembler,
        [
            span(1, 1, ENTRY, kind=SERVER, parent=PROXY),
            span(1, 2, PARTNER, kind=CLIENT, parent=1),
        ],
    )
    for shape in (
        _shape(REQUEST, 1, PROXY),
        _shape(RESPONSE, 1, PROXY),
        _shape(PARTNER_REQUEST, 1, 2),
        _shape(EVENT, 1),
        _shape("http:inbound:GET /other:request", 1, 0xDEAD),
    ):
        assembler.add_shape(shape, at(1))

    [flow] = _complete(assembler, SETTLE)

    node_channels, flow_channels, unattached = flow.attachments()
    assert node_channels == {ROOT: {REQUEST, RESPONSE}, partner: {PARTNER_REQUEST}}
    assert flow_channels == {EVENT}
    assert unattached == 1


@pytest.mark.unit
def test_a_shape_never_extends_its_flows_settle_time() -> None:
    assembler = _assembler()
    _feed(assembler, [span(1, 1, ENTRY, kind=SERVER, parent=PROXY)], 0)
    assembler.add_shape(_shape(RESPONSE, 1, PROXY), at(SETTLE - 1))

    [flow] = _complete(assembler, SETTLE)

    assert flow.shapes == (_shape(RESPONSE, 1, PROXY),)


@pytest.mark.unit
def test_a_shape_waits_settle_s_for_its_traces_first_span() -> None:
    assembler = _assembler()
    early = _shape(REQUEST, 1, PROXY)
    assembler.add_shape(early, at(0))
    assembler.add_shape(_shape(REQUEST, 2, PROXY), at(0))
    _feed(assembler, [span(1, 1, ENTRY, kind=SERVER, parent=PROXY)], SETTLE - 1)

    assert _complete(assembler, SETTLE) == []
    assert assembler.counters.unattached_shapes == 1
    [flow] = _complete(assembler, 2 * SETTLE - 1)
    assert flow.shapes == (early,)


@pytest.mark.unit
def test_a_shape_of_a_completed_trace_is_late() -> None:
    assembler = _assembler()
    _feed(assembler, _api(), 0)
    _complete(assembler, SETTLE)

    assembler.add_shape(_shape(RESPONSE, 1, PROXY), at(SETTLE + 1))

    assert assembler.counters.late_shapes == 1


@pytest.mark.unit
def test_a_work_id_shape_attaches_to_the_flow_the_work_index_names() -> None:
    assembler = _assembler()
    _feed(assembler, [span(1, 1, ENTRY, kind=SERVER)], 0)
    early = _shape("event:internal:order.charged:payload", work="w-1")
    later = _shape(EVENT, work="w-1")
    assembler.add_shape(early, at(1))
    assembler.add_shape(_shape(CAPTURE, 1, 1, work="w-1"), at(2))
    assembler.add_shape(later, at(3))
    assembler.add_shape(_shape(EVENT, work="w-2"), at(3))

    [flow] = _complete(assembler, SETTLE)

    node_channels, flow_channels, unattached = flow.attachments()
    assert node_channels == {ROOT: {CAPTURE}}
    assert flow_channels == {early.channel_key, later.channel_key}
    assert unattached == 0
    assert assembler.counters.unattached_shapes == 0
    _complete(assembler, SETTLE + 3)
    assert assembler.counters.unattached_shapes == 1
    assembler.add_shape(_shape(EVENT, work="w-1"), at(SETTLE + 4))
    assert assembler.counters.late_shapes == 1


# Process path


@pytest.mark.unit
def test_the_process_path_has_a_hop_per_node_and_per_edge_across_services_or_links() -> None:
    assembler = _assembler()
    _feed(
        assembler,
        [
            *_api(),
            *_listener(),
            span(2, 3, PROCESS, service=PROCESSOR, parent=1, error=True, start=7),
            span(
                2,
                4,
                PARTNER,
                kind=CLIENT,
                service=PROCESSOR,
                parent=3,
                error=True,
                status_code=502,
                start=8,
            ),
            span(
                2, 5, PARTNER, kind=CLIENT, service=PROCESSOR, parent=3, status_code=200, start=9
            ),
        ],
    )

    [flow] = _complete(assembler, SETTLE)

    assert flow.process_path().hops == [
        ProcessHop(ref=SERVICE, operation=ENTRY, span_kind="server", status="ok 201"),
        ProcessHop(ref=SERVICE, operation=PUT, span_kind="client", status="ok 200"),
        ProcessHop(
            from_ref=SERVICE,
            to_ref=WORKER,
            via=HopVia.LINK,
            operation=LISTENER,
            span_kind="internal",
            status="ok",
        ),
        ProcessHop(ref=WORKER, operation=LISTENER, span_kind="internal", status="ok"),
        ProcessHop(ref=WORKER, operation=PUBLISH, span_kind="producer", status="ok 200"),
        ProcessHop(
            from_ref=WORKER,
            to_ref=PROCESSOR,
            via=HopVia.CHILD,
            operation=PROCESS,
            span_kind="internal",
            status="error",
        ),
        ProcessHop(ref=PROCESSOR, operation=PROCESS, span_kind="internal", status="error"),
        ProcessHop(ref=PROCESSOR, operation=PARTNER, span_kind="client", status="error 502"),
    ]


@pytest.mark.unit
def test_edges_that_differ_only_in_their_from_operation_are_one_hop() -> None:
    flow = CompleteFlow(
        head=trace_id(1),
        traces=(trace_id(1),),
        spans=(
            span(1, 1, "nightly.job", service=WORKER),
            span(1, 2, "job.first", service=WORKER, parent=1, start=1),
            span(1, 3, "job.second", service=WORKER, parent=1, start=2),
            span(1, 4, "GET /v1/orders", kind=SERVER, parent=2, status_code=200, start=3),
            span(1, 5, "GET /v1/orders", kind=SERVER, parent=3, status_code=404, start=4),
        ),
    )

    hops = flow.process_path().hops

    edges = [hop for hop in hops if hop.ref is None]
    assert edges == [
        ProcessHop(
            from_ref=WORKER,
            to_ref=SERVICE,
            via=HopVia.CHILD,
            operation="GET /v1/orders",
            span_kind="server",
            status="ok 404",
        )
    ]
    assert hops[-2:] == [
        edges[0],
        ProcessHop(ref=SERVICE, operation="GET /v1/orders", span_kind="server", status="ok 404"),
    ]
