"""ObservationCatalog contract, run against the memory and the SQLite adapter: each observation
stored once, streams summarized per stream key, flows and paths, and the work index. The SQLite
adapter's restart cases close the file and open it again.
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from spans import Clock, order_spans, trace_id

from fire.adapters.memory.observation_catalog import MemoryObservationCatalog
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.adapters.sqlite.observation_catalog import SCHEMA_VERSION, SqliteObservationCatalog
from fire.core.domain.baseline import BaselineStatus, DriftPolicy
from fire.core.domain.dossier import Shape, ShapeField, ValueType
from fire.core.domain.drift import Admission, ShapeAdmission
from fire.core.domain.flow import FlowCounters, Node, PathPolicy
from fire.core.domain.observation import (
    Channel,
    ChannelDirection,
    ChannelKind,
    ChannelPart,
    ShapeObservation,
    SpanKind,
    SpanLink,
    SpanObservation,
    StatusClass,
    WorkEntry,
)
from fire.core.domain.path import PathsReport
from fire.core.ports.observation_catalog import ObservationCatalog

T0 = datetime(2026, 10, 11, 16, 30, tzinfo=UTC)
TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
SPAN_ID = "00f067aa0ba902b7"
FOLDED = ShapeAdmission(outcome=Admission.FOLDED)
DUPLICATE = ShapeAdmission(outcome=Admission.DUPLICATE)
DRIFTED = ("order_id", "coupon")
PUT = "DynamoDB.PutItem orders"
PAY = "POST pay.example.com/v1"

Make = Callable[..., ObservationCatalog]


@pytest.fixture(params=["memory", "sqlite"])
def make_catalog(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Make]:
    """Opens a catalog of the parametrized adapter, each SQLite one in its own file."""
    opened: list[ObservationCatalog] = []

    def make(*args: Any, **kwargs: Any) -> ObservationCatalog:
        if request.param == "memory":
            catalog: ObservationCatalog = MemoryObservationCatalog(*args, **kwargs)
        else:
            path = tmp_path / f"catalog-{len(opened)}.sqlite3"
            catalog = SqliteObservationCatalog(path, *args, **kwargs)
        opened.append(catalog)
        return catalog

    yield make
    for catalog in opened:
        catalog.close()


def _channel(name: str = "order.created") -> Channel:
    return Channel(
        kind=ChannelKind.EVENT,
        direction=ChannelDirection.INTERNAL,
        name=name,
        part=ChannelPart.PAYLOAD,
    )


def _shape_observation(
    observation_id: str = "obs-1",
    *,
    paths: tuple[str, ...] = ("order_id",),
    observed_at: datetime = T0,
    **overrides: Any,
) -> ShapeObservation:
    shape = Shape(fields=[ShapeField(path=path, value_type=ValueType.STRING) for path in paths])
    data: dict[str, Any] = {
        "system": "shop",
        "source": "order-events",
        "channel": _channel(),
        "observation_id": observation_id,
        "occurred_at": T0,
        "observed_at": observed_at,
        "shape": shape,
        "fingerprint": Sha256ShapeFingerprinter().fingerprint(shape),
    }
    data.update(overrides)
    return ShapeObservation(**data)


def _span(**overrides: Any) -> SpanObservation:
    data: dict[str, Any] = {
        "system": "shop",
        "source": "traces",
        "trace_id": TRACE_ID,
        "span_id": SPAN_ID,
        "service": "orders-api",
        "kind": SpanKind.SERVER,
        "operation": "POST /v1/orders",
        "error": False,
        "start": T0,
        "duration_ms": 12.5,
    }
    data.update(overrides)
    return SpanObservation(**data)


def _summary(catalog: ObservationCatalog, system: str = "shop") -> list[dict[str, Any]]:
    return [
        {
            "source": stream.source,
            "key": stream.key,
            "status": stream.status,
            "samples": stream.samples,
            "observations": stream.observations,
            "fingerprints": [
                (f.schema_hash, f.observations, f.first_observed_at, f.last_observed_at)
                for f in stream.fingerprints
            ],
            "drift": stream.drift,
        }
        for stream in catalog.streams(system)
    ]


def _baselines(
    catalog: ObservationCatalog, system: str = "shop"
) -> list[tuple[BaselineStatus, int]]:
    return [(stream.status, stream.samples) for stream in catalog.streams(system)]


def _warm(make_catalog: Make, min_samples: int = 2) -> ObservationCatalog:
    catalog = make_catalog({"shop": DriftPolicy(min_samples=min_samples)})
    for index in range(min_samples):
        catalog.add_shape(_shape_observation(f"warm-{index}"))
    return catalog


def _schema_hash(paths: tuple[str, ...]) -> str:
    return _shape_observation(paths=paths).fingerprint.schema_hash


@pytest.mark.unit
def test_catalog_implements_the_port(make_catalog: Make) -> None:
    assert isinstance(make_catalog(), ObservationCatalog)


@pytest.mark.unit
def test_first_add_stores_and_summarizes_the_stream(make_catalog: Make) -> None:
    catalog = make_catalog()
    observation = _shape_observation()

    assert catalog.add_shape(observation) == FOLDED

    [stream] = catalog.streams("shop")
    assert stream.source == "order-events"
    assert stream.channel == observation.channel
    assert stream.key == "event:internal:order.created:payload"
    assert stream.status is BaselineStatus.REBUILDING
    assert stream.samples == 1
    assert stream.observations == 1
    [fingerprint] = stream.fingerprints
    assert fingerprint.schema_hash == observation.fingerprint.schema_hash
    assert fingerprint.observations == 1
    assert fingerprint.first_observed_at == fingerprint.last_observed_at == T0


@pytest.mark.unit
def test_second_add_of_one_shape_key_is_a_duplicate_and_changes_nothing(
    make_catalog: Make,
) -> None:
    catalog = make_catalog()
    assert catalog.add_shape(_shape_observation()) == FOLDED
    before = _summary(catalog)

    resent = _shape_observation(
        paths=("order_id", "total"), observed_at=T0 + timedelta(hours=1), work_id="w-2"
    )
    assert catalog.add_shape(resent) == DUPLICATE

    assert _summary(catalog) == before
    assert catalog.work("shop", "w-2") == []


@pytest.mark.unit
def test_second_add_of_one_span_key_returns_false(make_catalog: Make) -> None:
    catalog = make_catalog()
    assert catalog.add_span(_span()) is True
    assert catalog.add_span(_span(operation="GET /v1/orders", error=True)) is False


@pytest.mark.unit
def test_concurrent_adds_store_once(make_catalog: Make) -> None:
    catalog = make_catalog()
    workers = 16
    barrier = threading.Barrier(workers)
    observation = _shape_observation()
    span = _span()

    def add(_: int) -> tuple[ShapeAdmission, bool]:
        barrier.wait()
        return catalog.add_shape(observation), catalog.add_span(span)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(add, range(workers)))

    assert [shape.stored for shape, _ in results].count(True) == 1
    assert [shape for shape, _ in results].count(DUPLICATE) == workers - 1
    assert [stored for _, stored in results].count(True) == 1
    [stream] = catalog.streams("shop")
    assert stream.observations == stream.samples == 1


@pytest.mark.unit
def test_one_event_from_two_sources_is_two_observations(make_catalog: Make) -> None:
    catalog = make_catalog()
    assert catalog.add_shape(_shape_observation(source="order-events")) == FOLDED
    assert catalog.add_shape(_shape_observation(source="order-table")) == FOLDED

    streams = catalog.streams("shop")
    assert [(stream.source, stream.observations) for stream in streams] == [
        ("order-events", 1),
        ("order-table", 1),
    ]


@pytest.mark.unit
def test_one_span_from_two_sources_is_one_observation(make_catalog: Make) -> None:
    catalog = make_catalog()
    assert catalog.add_span(_span(source="traces")) is True
    assert catalog.add_span(_span(source="traces-replica")) is False


@pytest.mark.unit
def test_one_observation_id_in_two_systems_is_two_observations(make_catalog: Make) -> None:
    catalog = make_catalog()
    assert catalog.add_shape(_shape_observation(system="shop")) == FOLDED
    assert catalog.add_shape(_shape_observation(system="billing")) == FOLDED
    assert catalog.add_span(_span(system="shop")) is True
    assert catalog.add_span(_span(system="billing")) is True

    assert [stream.observations for stream in catalog.streams("shop")] == [1]
    assert [stream.observations for stream in catalog.streams("billing")] == [1]


@pytest.mark.unit
def test_streams_of_a_system_without_observations_is_empty(make_catalog: Make) -> None:
    catalog = make_catalog()
    catalog.add_shape(_shape_observation())
    catalog.add_span(_span(system="billing"))
    assert catalog.streams("billing") == []
    assert catalog.streams("unknown") == []


@pytest.mark.unit
def test_streams_are_sorted_by_source_then_key(make_catalog: Make) -> None:
    catalog = make_catalog()
    added = [
        ("order-table", "order.created"),
        ("order-events", "order.shipped"),
        ("order-events", "order.created"),
        ("billing-events", "refund.issued"),
    ]
    for index, (source, name) in enumerate(added):
        catalog.add_shape(
            _shape_observation(f"obs-{index}", source=source, channel=_channel(name))
        )

    assert [(stream.source, stream.channel.name) for stream in catalog.streams("shop")] == [
        ("billing-events", "refund.issued"),
        ("order-events", "order.created"),
        ("order-events", "order.shipped"),
        ("order-table", "order.created"),
    ]


@pytest.mark.unit
def test_fingerprints_are_sorted_by_count_then_schema_hash(make_catalog: Make) -> None:
    catalog = make_catalog()
    forms = [("a",), ("b",), ("c",), ("c",), ("a",), ("d",), ("a",)]
    for index, paths in enumerate(forms):
        catalog.add_shape(_shape_observation(f"obs-{index}", paths=paths))

    [stream] = catalog.streams("shop")
    hashes = {paths: _shape_observation(paths=paths).fingerprint.schema_hash for paths in forms}
    tied = sorted([hashes[("b",)], hashes[("d",)]])
    assert [(f.schema_hash, f.observations) for f in stream.fingerprints] == [
        (hashes[("a",)], 3),
        (hashes[("c",)], 2),
        (tied[0], 1),
        (tied[1], 1),
    ]
    assert stream.observations == len(forms)


@pytest.mark.unit
def test_fingerprint_times_are_the_earliest_and_latest_observed_at(make_catalog: Make) -> None:
    catalog = make_catalog()
    for index, minutes in enumerate([10, 0, 30, 20]):
        catalog.add_shape(
            _shape_observation(f"obs-{index}", observed_at=T0 + timedelta(minutes=minutes))
        )

    [stream] = catalog.streams("shop")
    [fingerprint] = stream.fingerprints
    assert fingerprint.observations == 4
    assert fingerprint.first_observed_at == T0
    assert fingerprint.last_observed_at == T0 + timedelta(minutes=30)


@pytest.mark.unit
def test_stream_turns_warm_at_the_systems_min_samples(make_catalog: Make) -> None:
    catalog = make_catalog({"shop": DriftPolicy(min_samples=3)})
    catalog.add_shape(_shape_observation("obs-0"))
    catalog.add_shape(_shape_observation("obs-1"))
    assert _baselines(catalog) == [(BaselineStatus.REBUILDING, 2)]

    catalog.add_shape(_shape_observation("obs-2"))
    assert _baselines(catalog) == [(BaselineStatus.WARM, 3)]


@pytest.mark.unit
def test_drift_policies_are_per_system(make_catalog: Make) -> None:
    catalog = make_catalog({"shop": DriftPolicy(min_samples=2), "billing": DriftPolicy()})
    for system in ("shop", "billing"):
        for index in range(2):
            catalog.add_shape(_shape_observation(f"obs-{index}", system=system))

    assert _baselines(catalog, "shop") == [(BaselineStatus.WARM, 2)]
    assert _baselines(catalog, "billing") == [(BaselineStatus.REBUILDING, 2)]


@pytest.mark.unit
def test_system_without_a_policy_gets_the_default_min_samples(make_catalog: Make) -> None:
    catalog = make_catalog({"billing": DriftPolicy(min_samples=1)})
    for index in range(19):
        catalog.add_shape(_shape_observation(f"obs-{index}"))
    assert _baselines(catalog) == [(BaselineStatus.REBUILDING, 19)]

    catalog.add_shape(_shape_observation("obs-19"))
    assert _baselines(catalog) == [(BaselineStatus.WARM, 20)]


@pytest.mark.unit
def test_duplicate_add_folds_nothing(make_catalog: Make) -> None:
    catalog = make_catalog({"shop": DriftPolicy(min_samples=2)})
    assert catalog.add_shape(_shape_observation()) == FOLDED
    assert catalog.add_shape(_shape_observation(paths=("order_id", "total"))) == DUPLICATE

    assert _baselines(catalog) == [(BaselineStatus.REBUILDING, 1)]


@pytest.mark.unit
def test_samples_count_every_stored_observation_while_rebuilding(make_catalog: Make) -> None:
    catalog = make_catalog()
    forms = [("a",), ("b",), ("a",), ("a", "b")]
    for index, paths in enumerate(forms):
        catalog.add_shape(_shape_observation(f"obs-{index}", paths=paths))

    [stream] = catalog.streams("shop")
    assert len(stream.fingerprints) == 3
    assert stream.samples == stream.observations == len(forms)


@pytest.mark.unit
def test_each_stream_key_has_its_own_baseline(make_catalog: Make) -> None:
    catalog = make_catalog({"shop": DriftPolicy(min_samples=2)})
    added = [
        ("obs-1", "order-events", "order.created"),
        ("obs-1", "order-table", "order.created"),
        ("obs-2", "order-events", "order.created"),
        ("obs-3", "order-events", "order.shipped"),
    ]
    for observation_id, source, name in added:
        catalog.add_shape(
            _shape_observation(observation_id, source=source, channel=_channel(name))
        )

    streams = catalog.streams("shop")
    assert [
        (stream.source, stream.channel.name, stream.status, stream.samples) for stream in streams
    ] == [
        ("order-events", "order.created", BaselineStatus.WARM, 2),
        ("order-events", "order.shipped", BaselineStatus.REBUILDING, 1),
        ("order-table", "order.created", BaselineStatus.REBUILDING, 1),
    ]


@pytest.mark.unit
def test_streams_list_drift_signals_by_first_seen_then_schema_hash(make_catalog: Make) -> None:
    catalog = _warm(make_catalog)
    added = [
        ("late", ("order_id", "a"), 10),
        ("tie-1", ("order_id", "b"), 5),
        ("tie-2", ("order_id", "c"), 5),
        ("early", ("order_id", "a"), 1),
    ]
    for observation_id, paths, minutes in added:
        catalog.add_shape(
            _shape_observation(
                observation_id, paths=paths, observed_at=T0 + timedelta(minutes=minutes)
            )
        )

    [stream] = catalog.streams("shop")
    tied = sorted([_schema_hash(("order_id", "b")), _schema_hash(("order_id", "c"))])
    assert [(s.schema_hash, s.occurrences, s.first_seen) for s in stream.drift] == [
        (_schema_hash(("order_id", "a")), 2, T0 + timedelta(minutes=1)),
        (tied[0], 1, T0 + timedelta(minutes=5)),
        (tied[1], 1, T0 + timedelta(minutes=5)),
    ]


@pytest.mark.unit
def test_drifted_observations_count_in_fingerprints_but_not_in_samples(
    make_catalog: Make,
) -> None:
    catalog = _warm(make_catalog)
    for index in range(3):
        assert catalog.add_shape(_shape_observation(f"drift-{index}", paths=DRIFTED)).stored

    [stream] = catalog.streams("shop")
    assert (stream.status, stream.samples, stream.observations) == (BaselineStatus.WARM, 2, 5)
    assert [(f.schema_hash, f.observations) for f in stream.fingerprints] == [
        (_schema_hash(DRIFTED), 3),
        (_schema_hash(("order_id",)), 2),
    ]
    assert [(s.schema_hash, s.occurrences) for s in stream.drift] == [(_schema_hash(DRIFTED), 3)]


@pytest.mark.unit
def test_duplicate_of_a_drifted_observation_adds_no_occurrence(make_catalog: Make) -> None:
    catalog = _warm(make_catalog)
    drifted = _shape_observation("drift-0", paths=DRIFTED)
    assert catalog.add_shape(drifted).outcome is Admission.DRIFT_RAISED
    before = _summary(catalog)

    resent = _shape_observation("drift-0", paths=DRIFTED, observed_at=T0 + timedelta(hours=1))
    assert catalog.add_shape(resent) == DUPLICATE

    assert _summary(catalog) == before
    [stream] = catalog.streams("shop")
    assert [signal.occurrences for signal in stream.drift] == [1]


@pytest.mark.unit
def test_concurrent_adds_of_one_drifted_fingerprint_raise_one_signal(make_catalog: Make) -> None:
    catalog = _warm(make_catalog)
    workers = 16
    barrier = threading.Barrier(workers)

    def add(index: int) -> Admission:
        observation = _shape_observation(f"drift-{index}", paths=DRIFTED)
        barrier.wait()
        return catalog.add_shape(observation).outcome

    with ThreadPoolExecutor(max_workers=workers) as pool:
        outcomes = list(pool.map(add, range(workers)))

    assert outcomes.count(Admission.DRIFT_RAISED) == 1
    assert outcomes.count(Admission.DRIFT_REPEATED) == workers - 1
    [stream] = catalog.streams("shop")
    assert [signal.occurrences for signal in stream.drift] == [workers]
    assert (stream.samples, stream.observations) == (2, 2 + workers)


# Flows and paths


def _flow_catalog(
    make_catalog: Make, min_samples: int = 2, settle_s: float = 60
) -> tuple[ObservationCatalog, Clock]:
    clock = Clock()
    catalog = make_catalog(
        {"shop": DriftPolicy(min_samples=min_samples)},
        path_policies={"shop": PathPolicy(settle_s=settle_s)},
        clock=clock,
    )
    return catalog, clock


def _add_spans(catalog: ObservationCatalog, spans: list[SpanObservation]) -> None:
    for item in spans:
        assert catalog.add_span(item) is True


@pytest.mark.unit
def test_a_settled_flow_folds_into_its_path(make_catalog: Make) -> None:
    catalog, clock = _flow_catalog(make_catalog)
    _add_spans(catalog, order_spans(1, PUT))

    clock.advance(59)
    assert catalog.complete_flows() == []
    assert catalog.paths("shop").open_flows == 1
    clock.advance(1)
    assert catalog.complete_flows() == []

    report = catalog.paths("shop")
    assert report.open_flows == 0
    [path] = report.paths
    assert (path.entry, path.status, path.flows) == (
        Node(service="orders-api", operation="POST /orders"),
        BaselineStatus.REBUILDING,
        1,
    )


@pytest.mark.unit
def test_a_duplicate_span_does_not_extend_its_flows_settle_time(make_catalog: Make) -> None:
    catalog, clock = _flow_catalog(make_catalog)
    spans = order_spans(1, PUT)
    _add_spans(catalog, spans)

    clock.advance(30)
    assert catalog.add_span(spans[1]) is False
    clock.advance(30)
    catalog.complete_flows()

    assert catalog.paths("shop").open_flows == 0


@pytest.mark.unit
def test_complete_flows_returns_the_path_drift_of_a_flow_with_a_new_signal(
    make_catalog: Make,
) -> None:
    catalog, clock = _flow_catalog(make_catalog)
    for trace in (1, 2):
        _add_spans(catalog, order_spans(trace, PUT))
    clock.advance(60)
    assert catalog.complete_flows() == []

    _add_spans(catalog, order_spans(3, PUT, PAY))
    _add_spans(catalog, order_spans(4, PUT, PAY))
    clock.advance(60)
    [drift] = catalog.complete_flows()

    assert (drift.system, drift.entry.operation, drift.trace_ids) == (
        "shop",
        "POST /orders",
        [trace_id(3)],
    )
    [path] = catalog.paths("shop").paths
    assert (path.status, path.flows, path.drifted_flows) == (BaselineStatus.WARM, 2, 2)
    [signal] = path.drift
    assert (signal.rule, signal.node.operation, signal.occurrences) == ("hop_added", PAY, 2)


@pytest.mark.unit
def test_each_system_has_its_own_flows_and_path_policy(make_catalog: Make) -> None:
    clock = Clock()
    catalog = make_catalog(path_policies={"shop": PathPolicy(settle_s=10)}, clock=clock)
    _add_spans(catalog, order_spans(1, PUT))
    _add_spans(catalog, order_spans(1, PUT, system="billing"))

    clock.advance(10)
    catalog.complete_flows()

    assert [path.flows for path in catalog.paths("shop").paths] == [1]
    billing = catalog.paths("billing")
    assert (billing.open_flows, billing.paths) == (1, [])


@pytest.mark.unit
def test_stored_shapes_with_a_trace_join_their_flow_and_duplicates_do_not(
    make_catalog: Make,
) -> None:
    catalog, clock = _flow_catalog(make_catalog)
    spans = order_spans(1, PUT)
    _add_spans(catalog, spans)
    joined = _shape_observation(
        "shape-1", trace_id=trace_id(1), span_id=spans[1].span_id, observed_at=T0
    )
    assert catalog.add_shape(joined).stored
    assert catalog.add_shape(joined) == DUPLICATE
    assert catalog.add_shape(_shape_observation("shape-2")).stored

    clock.advance(60)
    catalog.complete_flows()

    [path] = catalog.paths("shop").paths
    put = next(node for node in path.nodes if node.operation == PUT)
    assert put.channels == {"event:internal:order.created:payload": 1}
    assert catalog.paths("shop").counters.unattached_shapes == 0


@pytest.mark.unit
def test_paths_of_a_system_without_spans_is_an_empty_report(make_catalog: Make) -> None:
    report = make_catalog().paths("shop")

    assert report == PathsReport(system="shop", open_flows=0, counters=FlowCounters(), paths=[])


@pytest.mark.unit
def test_paths_returns_copies_of_the_counters(make_catalog: Make) -> None:
    catalog, _clock = _flow_catalog(make_catalog)
    catalog.paths("shop").counters.late_spans = 5
    _add_spans(catalog, order_spans(1))

    assert catalog.paths("shop").counters.late_spans == 0


@pytest.mark.unit
def test_open_flows_snapshots_each_open_flow_holding_a_trace_once_and_completes_nothing(
    make_catalog: Make,
) -> None:
    catalog, clock = _flow_catalog(make_catalog)
    first = order_spans(1, "DynamoDB.PutItem orders")
    # Trace 2 links into trace 1, so both are one flow.
    linked = order_spans(2)[0].model_copy(
        update={
            "kind": SpanKind.INTERNAL,
            "links": [SpanLink(trace_id=trace_id(1), span_id=first[0].span_id)],
        }
    )
    _add_spans(catalog, [*first, linked, *order_spans(3)])

    [flow] = catalog.open_flows("shop", [trace_id(2), trace_id(1), trace_id(9)])

    assert (flow.head, flow.traces) == (trace_id(1), (trace_id(1), trace_id(2)))
    assert {span.dedupe_key for span in flow.spans} == {
        span.dedupe_key for span in (*first, linked)
    }
    assert catalog.paths("shop").open_flows == 2
    assert catalog.open_flows("billing", [trace_id(1)]) == []
    clock.advance(60)
    catalog.complete_flows()
    assert catalog.open_flows("shop", [trace_id(1)]) == []


@pytest.mark.unit
def test_traced_shapes_lists_the_stored_shapes_of_each_trace_in_the_order_stored(
    make_catalog: Make,
) -> None:
    catalog = make_catalog()
    request = Channel(
        kind=ChannelKind.HTTP,
        direction=ChannelDirection.INBOUND,
        name="POST /v1/orders",
        part=ChannelPart.REQUEST,
    )
    second = _shape_observation("shape-2", trace_id=trace_id(2))
    first = _shape_observation(
        "shape-1", source="api-proxy", channel=request, trace_id=trace_id(1), span_id=SPAN_ID
    )
    for observation in (
        second,
        first,
        first,
        _shape_observation("shape-3", trace_id=trace_id(2), system="billing"),
        _shape_observation("untraced"),
    ):
        catalog.add_shape(observation)

    shapes = catalog.traced_shapes("shop", [trace_id(1), trace_id(2), trace_id(1), trace_id(9)])

    assert [(shape.source, shape.channel, shape.trace_id) for shape in shapes] == [
        ("api-proxy", request, trace_id(1)),
        ("order-events", _channel(), trace_id(2)),
    ]
    assert [shape.schema_hash for shape in shapes] == [
        first.fingerprint.schema_hash,
        second.fingerprint.schema_hash,
    ]
    assert catalog.traced_shapes("payments", [trace_id(1)]) == []


@pytest.mark.unit
def test_traced_shapes_keeps_the_retention_window_and_the_order_stored(make_catalog: Make) -> None:
    clock = Clock()
    catalog = make_catalog(clock=clock, retention_s=60)
    first = _shape_observation("first", trace_id=TRACE_ID)
    catalog.add_shape(first)
    clock.advance(30)
    catalog.add_shape(_shape_observation("second", paths=DRIFTED, trace_id=TRACE_ID))

    def hashes() -> list[str]:
        return [shape.schema_hash for shape in catalog.traced_shapes("shop", [TRACE_ID])]

    clock.advance(29.999)
    assert hashes() == [_schema_hash(("order_id",)), _schema_hash(DRIFTED)]
    clock.advance(0.001)
    assert hashes() == [_schema_hash(DRIFTED)]
    catalog.complete_flows()
    assert hashes() == [_schema_hash(DRIFTED)]
    assert catalog.add_shape(first).stored
    assert hashes() == [_schema_hash(DRIFTED), _schema_hash(("order_id",))]
    clock.advance(30)
    assert hashes() == [_schema_hash(("order_id",))]
    clock.advance(30)
    assert hashes() == []


# Work index and retention


def _work_shape(observation_id: str, work_id: str | None, **overrides: Any) -> ShapeObservation:
    return _shape_observation(observation_id, work_id=work_id, **overrides)


@pytest.mark.unit
def test_work_lists_the_shapes_that_carried_the_work_id(make_catalog: Make) -> None:
    clock = Clock()
    catalog = make_catalog(clock=clock)
    response = Channel(
        kind=ChannelKind.HTTP,
        direction=ChannelDirection.OUTBOUND,
        name="partner:create",
        part=ChannelPart.RESPONSE,
        status_class=StatusClass.SUCCESS,
    )
    added = [
        _work_shape("late", "w-1", observed_at=T0 + timedelta(seconds=5)),
        _work_shape(
            "call",
            "w-1",
            source="captures",
            channel=response,
            paths=("order_number",),
            trace_id=TRACE_ID,
            observed_at=T0 + timedelta(seconds=1),
        ),
        _work_shape("tie-b", "w-1", observed_at=T0 + timedelta(seconds=1)),
        _work_shape("tie-a", "w-1", observed_at=T0 + timedelta(seconds=1)),
        _work_shape("other-work", "w-2"),
        _work_shape("no-work", None),
        _work_shape("other-system", "w-1", system="billing"),
    ]
    for observation in added:
        assert catalog.add_shape(observation).stored
    assert catalog.add_shape(_work_shape("late", "w-1")) == DUPLICATE

    entries = catalog.work("shop", "w-1")

    assert [(entry.source, entry.observation_id) for entry in entries] == [
        ("captures", "call"),
        ("order-events", "tie-a"),
        ("order-events", "tie-b"),
        ("order-events", "late"),
    ]
    assert entries[0] == WorkEntry(
        source="captures",
        key="http:outbound:partner:create:response:2xx",
        observation_id="call",
        schema_hash=_schema_hash(("order_number",)),
        occurred_at=T0,
        observed_at=T0 + timedelta(seconds=1),
        accepted_at=clock.now,
        trace_id=TRACE_ID,
    )
    assert [entry.observation_id for entry in catalog.work("billing", "w-1")] == ["other-system"]


@pytest.mark.unit
def test_work_of_an_unknown_work_id_or_system_is_empty(make_catalog: Make) -> None:
    catalog = make_catalog()
    catalog.add_shape(_work_shape("obs-1", "w-1"))

    assert catalog.work("shop", "w-9") == []
    assert catalog.work("unknown", "w-1") == []


@pytest.mark.unit
def test_work_drops_entries_older_than_the_retention_window(make_catalog: Make) -> None:
    clock = Clock()
    catalog = make_catalog(clock=clock, retention_s=60)
    catalog.add_shape(_work_shape("first", "w-1"))
    clock.advance(30)
    catalog.add_shape(_work_shape("second", "w-1"))

    clock.advance(29.999)
    assert [entry.observation_id for entry in catalog.work("shop", "w-1")] == ["first", "second"]
    clock.advance(0.001)
    assert [entry.observation_id for entry in catalog.work("shop", "w-1")] == ["second"]
    catalog.complete_flows()
    assert [entry.observation_id for entry in catalog.work("shop", "w-1")] == ["second"]
    clock.advance(30)
    assert catalog.work("shop", "w-1") == []


def _outcome(observation_id: str, work_id: str | None, **overrides: Any) -> ShapeObservation:
    return _work_shape(
        observation_id,
        work_id,
        source="task-outcomes",
        channel=Channel(
            kind=ChannelKind.OUTCOME,
            direction=ChannelDirection.INTERNAL,
            name="billing-processor",
            part=ChannelPart.PAYLOAD,
        ),
        paths=("source_queue_arn",),
        **overrides,
    )


OUTCOME_KEY = "outcome:internal:billing-processor:payload"


@pytest.mark.unit
def test_work_outcomes_join_outcome_work_ids_to_the_other_sources(make_catalog: Make) -> None:
    catalog = make_catalog()
    for observation in (
        _work_shape("e-1", "w-1"),
        _work_shape("e-2", "w-2"),
        _work_shape("e-3", "w-3", paths=("coupon",)),
        _work_shape("no-work", None),
        _work_shape("e-other", "w-1", system="billing"),
        _outcome("w-1", "w-1", occurred_at=T0 + timedelta(minutes=5)),
        _outcome("w-3", "w-3"),
        _outcome("w-9", "w-9"),
    ):
        assert catalog.add_shape(observation).stored
    assert catalog.add_shape(_outcome("w-1", "w-1")) == DUPLICATE

    outcomes = catalog.work_outcomes("shop", {"task-outcomes"})

    assert (outcomes.abandoned, outcomes.unattributed) == (3, ["w-9"])
    stream = "event:internal:order.created:payload"
    assert [
        (f.source, f.stream, f.schema_hash, f.observed, f.abandoned, f.abandoned_by)
        for f in outcomes.fingerprints
    ] == sorted(
        [
            ("order-events", stream, _schema_hash(("order_id",)), 2, 1, {OUTCOME_KEY: 1}),
            ("order-events", stream, _schema_hash(("coupon",)), 1, 1, {OUTCOME_KEY: 1}),
        ]
    )
    first = {f.schema_hash: f.first_abandoned for f in outcomes.fingerprints}
    assert first[_schema_hash(("order_id",))] == T0 + timedelta(minutes=5)
    assert [(s.stream, s.observed, s.abandoned) for s in outcomes.streams] == [(stream, 3, 2)]
    assert catalog.work_outcomes("billing", {"task-outcomes"}).abandoned == 0
    assert catalog.work_outcomes("shop", set()).fingerprints == []
    assert catalog.work_outcomes("unknown", {"task-outcomes"}).abandoned == 0


@pytest.mark.unit
def test_work_outcomes_keep_the_retention_window(make_catalog: Make) -> None:
    clock = Clock()
    catalog = make_catalog(clock=clock, retention_s=60)
    catalog.add_shape(_work_shape("e-1", "w-1"))
    clock.advance(30)
    catalog.add_shape(_outcome("w-1", "w-1"))

    assert len(catalog.work_outcomes("shop", {"task-outcomes"}).fingerprints) == 1
    clock.advance(30)
    outcomes = catalog.work_outcomes("shop", {"task-outcomes"})
    assert (outcomes.abandoned, outcomes.unattributed, outcomes.fingerprints) == (1, ["w-1"], [])
    clock.advance(30)
    assert catalog.work_outcomes("shop", {"task-outcomes"}).abandoned == 0


@pytest.mark.unit
def test_shape_returns_the_shape_a_stream_stored_for_a_fingerprint(make_catalog: Make) -> None:
    catalog = make_catalog()
    catalog.add_shape(_shape_observation(paths=("order_id", "coupon")))
    key = "event:internal:order.created:payload"
    schema_hash = _schema_hash(("order_id", "coupon"))

    shape = catalog.shape("shop", "order-events", key, schema_hash)

    assert shape is not None
    assert [field.path for field in shape.fields] == ["order_id", "coupon"]
    assert catalog.shape("shop", "order-events", key, "other") is None
    assert catalog.shape("shop", "other-source", key, schema_hash) is None
    assert catalog.shape("billing", "order-events", key, schema_hash) is None


@pytest.mark.unit
def test_outcome_shapes_join_no_flow(make_catalog: Make) -> None:
    clock = Clock()
    catalog = make_catalog(path_policies={"shop": PathPolicy(settle_s=10)}, clock=clock)
    catalog.add_shape(_work_shape("e-1", "w-1"))
    catalog.add_shape(_outcome("w-2", "w-2"))
    clock.advance(10)
    catalog.complete_flows()

    assert catalog.paths("shop").counters.unattached_shapes == 1


@pytest.mark.unit
def test_dedupe_keys_past_the_retention_window_are_stored_again(make_catalog: Make) -> None:
    clock = Clock()
    catalog = make_catalog(clock=clock, retention_s=60)
    assert catalog.add_shape(_work_shape("obs-1", "w-1")) == FOLDED
    assert catalog.add_span(_span()) is True

    clock.advance(59)
    assert catalog.add_shape(_work_shape("obs-1", "w-1")) == DUPLICATE
    assert catalog.add_span(_span()) is False
    clock.advance(1)
    assert catalog.add_shape(_work_shape("obs-1", "w-1")) == FOLDED
    assert catalog.add_span(_span()) is True

    [stream] = catalog.streams("shop")
    assert stream.observations == stream.samples == 2
    [entry] = catalog.work("shop", "w-1")
    assert entry.accepted_at == clock.now


@pytest.mark.unit
def test_retention_forgets_no_baseline_signal_or_path(make_catalog: Make) -> None:
    clock = Clock()
    catalog = make_catalog(
        {"shop": DriftPolicy(min_samples=2)},
        path_policies={"shop": PathPolicy(settle_s=10)},
        clock=clock,
        retention_s=60,
    )
    for index in range(2):
        catalog.add_shape(_shape_observation(f"warm-{index}"))
    catalog.add_shape(_shape_observation("drift-0", paths=DRIFTED))
    _add_spans(catalog, order_spans(1, PUT))
    clock.advance(10)
    catalog.complete_flows()
    before = (_summary(catalog), catalog.paths("shop"))

    clock.advance(120)
    catalog.complete_flows()

    assert (_summary(catalog), catalog.paths("shop")) == before


# The SQLite adapter across a restart


@pytest.fixture
def sqlite_path(tmp_path: Path) -> Path:
    return tmp_path / "catalog.sqlite3"


@pytest.fixture
def reopen(sqlite_path: Path) -> Iterator[Callable[..., SqliteObservationCatalog]]:
    """Closes the catalog it opened last, if still open, and opens the same file again."""
    opened: list[SqliteObservationCatalog] = []

    def open_again(*args: Any, **kwargs: Any) -> SqliteObservationCatalog:
        if opened:
            opened[-1].close()
        catalog = SqliteObservationCatalog(sqlite_path, *args, **kwargs)
        opened.append(catalog)
        return catalog

    yield open_again
    opened[-1].close()


def _flow_policies(clock: Clock) -> dict[str, Any]:
    return {
        "policies": {"shop": DriftPolicy(min_samples=2)},
        "path_policies": {"shop": PathPolicy(settle_s=60)},
        "clock": clock,
    }


@pytest.mark.unit
def test_a_warm_stream_is_still_warm_after_a_restart(
    reopen: Callable[..., SqliteObservationCatalog],
) -> None:
    catalog = reopen({"shop": DriftPolicy(min_samples=3)})
    for index in range(3):
        catalog.add_shape(_shape_observation(f"warm-{index}"))
    assert _baselines(catalog) == [(BaselineStatus.WARM, 3)]

    catalog = reopen({"shop": DriftPolicy(min_samples=3)})

    assert _baselines(catalog) == [(BaselineStatus.WARM, 3)]
    drifted = catalog.add_shape(_shape_observation("drift-0", paths=DRIFTED))
    assert drifted.outcome is Admission.DRIFT_RAISED


@pytest.mark.unit
def test_streams_baselines_and_drift_signals_survive_a_restart(
    reopen: Callable[..., SqliteObservationCatalog],
) -> None:
    policies = {"shop": DriftPolicy(min_samples=2)}
    catalog = reopen(policies)
    added = [
        ("warm-0", ("order_id",), 0),
        ("warm-1", ("order_id",), 10),
        ("drift-0", DRIFTED, 20),
        ("drift-1", DRIFTED, 5),
        ("other", ("order_id",), 30),
    ]
    for observation_id, paths, minutes in added:
        catalog.add_shape(
            _shape_observation(
                observation_id, paths=paths, observed_at=T0 + timedelta(minutes=minutes)
            )
        )
    catalog.add_shape(_shape_observation("shipped", channel=_channel("order.shipped")))
    before = _summary(catalog)

    catalog = reopen(policies)

    assert _summary(catalog) == before
    repeated = catalog.add_shape(_shape_observation("drift-2", paths=DRIFTED))
    assert repeated.outcome is Admission.DRIFT_REPEATED
    assert repeated.signal is not None and repeated.signal.occurrences == 3
    folded = catalog.add_shape(_shape_observation("warm-2"))
    assert folded == FOLDED
    removed = catalog.add_shape(_shape_observation("removed", paths=("coupon",)))
    assert removed.signal is not None
    assert [field.path for field in removed.signal.diff.removed] == ["order_id"]


@pytest.mark.unit
def test_dedupe_keys_and_the_work_and_trace_index_survive_a_restart(
    reopen: Callable[..., SqliteObservationCatalog],
) -> None:
    clock = Clock()
    catalog = reopen(clock=clock, retention_s=60)
    catalog.add_shape(_work_shape("obs-1", "w-1", trace_id=TRACE_ID))
    assert catalog.add_span(_span()) is True
    before = catalog.work("shop", "w-1")
    traced = catalog.traced_shapes("shop", [TRACE_ID])
    assert len(traced) == 1

    catalog = reopen(clock=clock, retention_s=60)

    assert catalog.add_shape(_work_shape("obs-1", "w-1")) == DUPLICATE
    assert catalog.add_span(_span()) is False
    assert catalog.work("shop", "w-1") == before
    assert catalog.traced_shapes("shop", [TRACE_ID]) == traced
    clock.advance(60)
    assert catalog.work("shop", "w-1") == []
    assert catalog.traced_shapes("shop", [TRACE_ID]) == []


@pytest.mark.unit
def test_work_outcomes_and_fingerprint_shapes_survive_a_restart(
    reopen: Callable[..., SqliteObservationCatalog],
) -> None:
    clock = Clock()
    catalog = reopen(clock=clock, retention_s=60)
    catalog.add_shape(_work_shape("e-1", "w-1"))
    catalog.add_shape(_outcome("w-1", "w-1"))
    catalog.add_shape(_outcome("w-9", "w-9"))
    before = catalog.work_outcomes("shop", {"task-outcomes"})
    key = "event:internal:order.created:payload"
    shape = catalog.shape("shop", "order-events", key, _schema_hash(("order_id",)))

    catalog = reopen(clock=clock, retention_s=60)

    assert catalog.work_outcomes("shop", {"task-outcomes"}) == before
    assert catalog.add_shape(_outcome("w-1", "w-1")) == DUPLICATE
    assert catalog.shape("shop", "order-events", key, _schema_hash(("order_id",))) == shape
    clock.advance(60)
    assert catalog.work_outcomes("shop", {"task-outcomes"}).abandoned == 0


@pytest.mark.unit
def test_paths_and_path_drift_survive_a_restart_and_open_flows_do_not(
    reopen: Callable[..., SqliteObservationCatalog],
) -> None:
    clock = Clock()
    catalog = reopen(**_flow_policies(clock))
    for trace in (1, 2):
        _add_spans(catalog, order_spans(trace, PUT))
    clock.advance(60)
    catalog.complete_flows()
    _add_spans(catalog, order_spans(3, PUT, PAY))
    clock.advance(60)
    [_drift] = catalog.complete_flows()
    _add_spans(catalog, order_spans(4, PUT))
    before = catalog.paths("shop")
    assert before.open_flows == 1

    catalog = reopen(**_flow_policies(clock))

    after = catalog.paths("shop")
    assert after.paths == before.paths
    assert (after.open_flows, after.counters) == (0, FlowCounters())
    _add_spans(catalog, order_spans(5, PUT, PAY))
    _add_spans(catalog, order_spans(6, PUT, "POST tax.example.com/v1"))
    clock.advance(60)
    [drift] = catalog.complete_flows()
    assert drift.trace_ids == [trace_id(6)]
    [path] = catalog.paths("shop").paths
    assert [(signal.node.operation, signal.occurrences) for signal in path.drift] == [
        (PAY, 2),
        ("POST tax.example.com/v1", 1),
    ]


@pytest.mark.unit
def test_complete_flows_deletes_index_rows_past_the_retention_window(
    reopen: Callable[..., SqliteObservationCatalog], sqlite_path: Path
) -> None:
    clock = Clock()
    catalog = reopen(clock=clock, retention_s=60)
    catalog.add_shape(_work_shape("old", "w-1"))
    catalog.add_span(_span())
    clock.advance(30)
    catalog.add_shape(_work_shape("new", "w-1"))

    clock.advance(30)
    catalog.complete_flows()

    with closing(sqlite3.connect(sqlite_path)) as db:
        shapes = db.execute("SELECT observation_id FROM shape_index").fetchall()
        spans = db.execute("SELECT count(*) FROM span_index").fetchone()
    assert (shapes, spans) == ([("new",)], (0,))


@pytest.mark.unit
def test_the_file_and_its_directory_are_created(tmp_path: Path) -> None:
    path = tmp_path / "var" / "fire" / "catalog.sqlite3"
    SqliteObservationCatalog(path).close()

    assert path.is_file()


@pytest.mark.unit
def test_a_file_of_another_schema_version_is_refused(sqlite_path: Path) -> None:
    with closing(sqlite3.connect(sqlite_path)) as db:
        db.execute(f"PRAGMA user_version = {SCHEMA_VERSION + 1}")

    with pytest.raises(ValueError, match="schema version"):
        SqliteObservationCatalog(sqlite_path)
