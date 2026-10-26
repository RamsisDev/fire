"""Echo reasoner builds options from path, drift fragments, and missing evidence."""

from __future__ import annotations

from typing import Any

import pytest

from fire.adapters.reasoners.echo import EchoReasoner
from fire.core.domain.decision import Primitive
from fire.core.domain.dossier import (
    Dossier,
    EvidenceFragment,
    EvidenceQuality,
    ProcessHop,
    ProcessPath,
    Shape,
    ShapeField,
    ShapeFingerprint,
    ValueType,
)

STREAM = "http:inbound:POST /v1/orders/checkout:request"
REFERENCE_AND_TIMEZONE = {
    "added": [],
    "type_changed": [{"path": "referenceOrderId", "old": ["string"], "new": ["integer"]}],
    "removed": [{"path": "timezone", "value_type": "string"}],
}
REFERENCE_AND_TIMEZONE_LABEL = (
    f"Shape drift on {STREAM}: referenceOrderId changed string -> integer; "
    "timezone (string) removed"
)
PATH_DRIFT_LABEL = "hop_missing: billing-publisher.process_work_unit on POST /v1/orders/checkout"


def _shape_drift(diff: dict[str, Any]) -> EvidenceFragment:
    counts = " ".join(
        f"{kind}={len(diff.get(kind, []))}" for kind in ("added", "type_changed", "removed")
    )
    return EvidenceFragment(
        collector="shape_drift",
        quality=EvidenceQuality.PRESENT,
        summary=f"Shape drift on {STREAM}: {counts}",
        data={
            "source": "api-proxy",
            "stream": STREAM,
            "schema_hash": "c47d",
            "diff": diff,
            "occurrences": 1,
            "first_seen": "2026-10-11T16:30:21Z",
        },
    )


def _path_drift() -> EvidenceFragment:
    return EvidenceFragment(
        collector="path_drift",
        quality=EvidenceQuality.PRESENT,
        # Differs from the label, so a test can tell a label built from `data` from the fallback.
        summary="Path drift summary",
        data={
            "rule": "hop_missing",
            "entry": {
                "service": "acme-api",
                "operation": "POST /v1/orders/checkout",
            },
            "node": {
                "service": "acme-billing-publisher",
                "operation": "billing-publisher.process_work_unit",
            },
            "path_flows": 24,
            "node_flows": 24,
            "node_errors": 0,
            "flow_spans": 0,
            "flow_errors": 0,
            "occurrences": 1,
            "first_seen": "2026-10-11T16:40:02Z",
            "trace_ids": ["4bf92f3577b34da6a3ce929d0e0e4736"],
        },
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_echo_with_shape_and_fingerprint() -> None:
    dossier = Dossier(
        incident_id="inc-1",
        system="example",
        service="checkout",
        shape=Shape(fields=[ShapeField(path="order.id", value_type=ValueType.STRING)]),
        fingerprint=ShapeFingerprint(schema_hash="abc", normalized_paths=["order.id"]),
        process_path=ProcessPath(hops=[ProcessHop(ref="api", operation="POST /orders")]),
    )
    decision = await EchoReasoner().decide(dossier)
    assert decision.noul is False
    assert decision.primitive is Primitive.CHOICE
    assert decision.confidence >= 0.3
    assert any(p.id.startswith("hop-") for p in decision.possibilities)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_echo_missing_fragment_adds_insufficient_evidence_and_sets_noul() -> None:
    dossier = Dossier(
        incident_id="inc-1",
        system="example",
        fingerprint=ShapeFingerprint(schema_hash="abc"),
        process_path=ProcessPath(hops=[ProcessHop(ref="api")]),
        fragments=[
            EvidenceFragment(
                collector="origin", quality=EvidenceQuality.MISSING, summary="No Origin provided"
            )
        ],
    )
    decision = await EchoReasoner().decide(dossier)
    insufficient = [p for p in decision.possibilities if p.id == "insufficient-evidence"]
    assert [p.rationale_refs for p in insufficient] == [["origin"]]
    assert decision.confidence == 0.2
    assert decision.noul is True
    assert decision.primitive is Primitive.NOUL


@pytest.mark.unit
@pytest.mark.asyncio
async def test_echo_splits_probability_evenly_without_rounding() -> None:
    dossier = Dossier(
        incident_id="inc-1",
        system="example",
        fingerprint=ShapeFingerprint(schema_hash="abc"),
        process_path=ProcessPath(hops=[ProcessHop(ref="api"), ProcessHop(ref="db")]),
        fragments=[
            EvidenceFragment(
                collector="origin", quality=EvidenceQuality.MISSING, summary="No Origin provided"
            )
        ],
    )
    decision = await EchoReasoner().decide(dossier)
    assert [p.probability for p in decision.possibilities] == [1 / 3, 1 / 3, 1 / 3]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_echo_present_fragments_are_not_missing_evidence() -> None:
    dossier = Dossier(
        incident_id="inc-1",
        system="example",
        fingerprint=ShapeFingerprint(schema_hash="abc"),
        process_path=ProcessPath(hops=[ProcessHop(ref="api")]),
        fragments=[
            EvidenceFragment(
                collector="shape", quality=EvidenceQuality.PRESENT, summary="Shape provided"
            )
        ],
    )
    decision = await EchoReasoner().decide(dossier)
    assert [p.id for p in decision.possibilities] == ["hop-0"]
    assert decision.confidence == 0.45
    assert decision.noul is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_echo_shape_drift_fragment_is_one_possibility_labelled_from_its_diff() -> None:
    dossier = Dossier(
        incident_id="inc-1",
        system="example",
        fragments=[_shape_drift(REFERENCE_AND_TIMEZONE)],
    )
    decision = await EchoReasoner().decide(dossier)
    [possibility] = decision.possibilities
    assert possibility.id == "drift-0"
    assert possibility.label == REFERENCE_AND_TIMEZONE_LABEL
    assert possibility.probability == 1.0
    assert possibility.rationale_refs == ["shape_drift"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_echo_shape_drift_label_lists_added_then_type_changed_then_removed() -> None:
    diff = {
        "removed": [{"path": "timezone", "value_type": "string"}],
        "type_changed": [
            {"path": "lines[*].qty", "old": ["integer", "string"], "new": ["null", "number"]}
        ],
        "added": [
            {"path": "channel", "value_type": "string"},
            {"path": "notes", "value_type": "null"},
        ],
    }
    dossier = Dossier(incident_id="inc-1", system="example", fragments=[_shape_drift(diff)])
    decision = await EchoReasoner().decide(dossier)
    assert decision.possibilities[0].label == (
        f"Shape drift on {STREAM}: channel (string) added; notes (null) added; "
        "lines[*].qty changed integer|string -> null|number; timezone (string) removed"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_echo_path_drift_label_names_rule_node_and_entry() -> None:
    dossier = Dossier(incident_id="inc-1", system="example", fragments=[_path_drift()])
    decision = await EchoReasoner().decide(dossier)
    [possibility] = decision.possibilities
    assert possibility.id == "drift-0"
    assert possibility.label == PATH_DRIFT_LABEL
    assert possibility.rationale_refs == ["path_drift"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_echo_drift_possibilities_follow_hops_in_fragment_order() -> None:
    dossier = Dossier(
        incident_id="inc-1",
        system="example",
        process_path=ProcessPath(hops=[ProcessHop(ref="api")]),
        fragments=[
            _path_drift(),
            EvidenceFragment(
                collector="baseline", quality=EvidenceQuality.PRESENT, summary="24 samples"
            ),
            _shape_drift(REFERENCE_AND_TIMEZONE),
        ],
    )
    decision = await EchoReasoner().decide(dossier)
    assert [(p.id, p.label) for p in decision.possibilities] == [
        ("hop-0", "Failure near api"),
        ("drift-0", PATH_DRIFT_LABEL),
        ("drift-1", REFERENCE_AND_TIMEZONE_LABEL),
    ]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_echo_missing_drift_fragment_only_counts_as_missing_evidence() -> None:
    dossier = Dossier(
        incident_id="inc-1",
        system="example",
        fragments=[
            EvidenceFragment(
                collector="shape_drift",
                quality=EvidenceQuality.MISSING,
                summary="Drift signal expired",
            ),
            _path_drift(),
        ],
    )
    decision = await EchoReasoner().decide(dossier)
    assert [(p.id, p.rationale_refs) for p in decision.possibilities] == [
        ("drift-0", ["path_drift"]),
        ("insufficient-evidence", ["shape_drift"]),
    ]


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("collector", "data"),
    [
        ("shape_drift", {}),
        ("shape_drift", {"diff": REFERENCE_AND_TIMEZONE}),
        ("shape_drift", {"stream": 7, "diff": REFERENCE_AND_TIMEZONE}),
        ("shape_drift", {"stream": STREAM, "diff": "referenceOrderId"}),
        (
            "shape_drift",
            {
                "stream": STREAM,
                "diff": {"type_changed": [{"path": "a", "old": "string", "new": ["integer"]}]},
            },
        ),
        ("shape_drift", {"stream": STREAM, "diff": {"added": [{"path": "a"}]}}),
        ("shape_drift", {"stream": STREAM, "diff": {}}),
        ("path_drift", {}),
        ("path_drift", {"rule": "hop_missing", "entry": {"operation": "POST /x"}}),
        (
            "path_drift",
            {"rule": "hop_missing", "entry": "POST /x", "node": {"operation": "work"}},
        ),
        (
            "path_drift",
            {"rule": "hop_missing", "entry": {"operation": "POST /x"}, "node": {"service": "w"}},
        ),
        (
            "path_drift",
            {"rule": 3, "entry": {"operation": "POST /x"}, "node": {"operation": "work"}},
        ),
    ],
    ids=[
        "shape-empty",
        "shape-no-stream",
        "shape-stream-not-str",
        "shape-diff-not-object",
        "shape-old-not-list",
        "shape-added-no-value-type",
        "shape-empty-diff",
        "path-empty",
        "path-no-node",
        "path-entry-not-object",
        "path-node-no-operation",
        "path-rule-not-str",
    ],
)
async def test_echo_malformed_drift_data_falls_back_to_summary(
    collector: str, data: dict[str, Any]
) -> None:
    fragment = EvidenceFragment(
        collector=collector,
        quality=EvidenceQuality.PRESENT,
        summary="Drift summary from the collector",
        data=data,
    )
    dossier = Dossier(incident_id="inc-1", system="example", fragments=[fragment])
    decision = await EchoReasoner().decide(dossier)
    [possibility] = decision.possibilities
    assert possibility.id == "drift-0"
    assert possibility.label == "Drift summary from the collector"
    assert possibility.rationale_refs == [collector]


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("structure", "label"),
    [
        (
            {"shape": Shape(fields=[ShapeField(path="a", value_type=ValueType.STRING)])},
            "No process path or drift evidence yet",
        ),
        (
            {"fingerprint": ShapeFingerprint(schema_hash="abc")},
            "No process path or drift evidence yet",
        ),
        ({}, "No process path, shape, or drift evidence yet"),
    ],
    ids=["shape", "fingerprint", "nothing"],
)
async def test_echo_unknown_label_names_the_evidence_that_is_absent(
    structure: dict[str, Any], label: str
) -> None:
    dossier = Dossier(incident_id="inc-1", system="example", **structure)
    decision = await EchoReasoner().decide(dossier)
    [possibility] = decision.possibilities
    assert possibility.id == "unknown"
    assert possibility.label == label
    assert possibility.probability == 1.0
    assert possibility.rationale_refs == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_echo_shape_drift_without_a_flow_is_noul() -> None:
    dossier = Dossier(
        incident_id="inc-1",
        system="example",
        fragments=[
            _shape_drift(REFERENCE_AND_TIMEZONE),
            EvidenceFragment(
                collector="process_path",
                quality=EvidenceQuality.MISSING,
                summary="No complete flow",
            ),
        ],
    )
    decision = await EchoReasoner().decide(dossier)
    assert [(p.id, p.probability) for p in decision.possibilities] == [
        ("drift-0", 0.5),
        ("insufficient-evidence", 0.5),
    ]
    assert decision.possibilities[1].rationale_refs == ["process_path"]
    assert decision.confidence == 0.2
    assert decision.noul is True
    assert decision.primitive is Primitive.NOUL


@pytest.mark.unit
@pytest.mark.asyncio
async def test_echo_present_drift_alone_is_evidence() -> None:
    dossier = Dossier(incident_id="inc-1", system="example", fragments=[_path_drift()])
    decision = await EchoReasoner().decide(dossier)
    assert [p.id for p in decision.possibilities] == ["drift-0"]
    assert decision.confidence == 0.45
    assert decision.noul is False
    assert decision.primitive is Primitive.CHOICE


HTTP_ERRORS_DATA: dict[str, Any] = {
    "route": {"service": "acme-api", "operation": "POST /v1/orders/checkout"},
    "status_min": 500,
    "threshold": 5,
    "window_s": 60.0,
    "errors": 5,
    "statuses": {"503": 2, "502": 3},
    "first_error": "2026-10-11T16:40:00Z",
    "last_error": "2026-10-11T16:40:20Z",
    "trace_ids": ["4bf92f3577b34da6a3ce929d0e0e4736"],
}
HTTP_ERRORS_LABEL = (
    "HTTP errors on POST /v1/orders/checkout: 5 responses >= 500 within 60s (502 x3, 503 x2)"
)


def _http_errors(data: dict[str, Any] = HTTP_ERRORS_DATA) -> EvidenceFragment:
    return EvidenceFragment(
        collector="http_errors",
        quality=EvidenceQuality.PRESENT,
        summary="HTTP errors summary",
        data=data,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_echo_builds_one_burst_possibility_per_http_errors_fragment_after_drift() -> None:
    dossier = Dossier(
        incident_id="inc-1",
        system="example",
        process_path=ProcessPath(hops=[ProcessHop(ref="api", operation="POST /orders")]),
        fragments=[
            _http_errors(),
            _path_drift(),
            EvidenceFragment(
                collector="http_errors", quality=EvidenceQuality.MISSING, summary="Not counted"
            ),
        ],
    )
    decision = await EchoReasoner().decide(dossier)
    assert [(p.id, p.label, p.rationale_refs) for p in decision.possibilities] == [
        ("hop-0", "Failure near api/POST /orders", ["process_path:api/POST /orders"]),
        ("drift-0", PATH_DRIFT_LABEL, ["path_drift"]),
        ("burst-0", HTTP_ERRORS_LABEL, ["http_errors"]),
        ("insufficient-evidence", "Insufficient evidence (MISSING fragments)", ["http_errors"]),
    ]


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "data",
    [
        {key: value for key, value in HTTP_ERRORS_DATA.items() if key != "route"},
        {**HTTP_ERRORS_DATA, "statuses": ["503"]},
        {**HTTP_ERRORS_DATA, "route": {"service": "acme-api"}},
    ],
    ids=["no-route", "statuses-not-a-mapping", "route-no-operation"],
)
async def test_echo_malformed_http_errors_data_falls_back_to_summary(data: dict[str, Any]) -> None:
    dossier = Dossier(incident_id="inc-1", system="example", fragments=[_http_errors(data)])
    decision = await EchoReasoner().decide(dossier)
    [possibility] = decision.possibilities
    assert (possibility.id, possibility.label) == ("burst-0", "HTTP errors summary")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_echo_present_burst_alone_is_evidence() -> None:
    dossier = Dossier(incident_id="inc-1", system="example", fragments=[_http_errors()])
    decision = await EchoReasoner().decide(dossier)
    assert [p.id for p in decision.possibilities] == ["burst-0"]
    assert decision.confidence == 0.45
    assert decision.noul is False
    assert decision.primitive is Primitive.CHOICE


DLQ_STREAM = "event:internal:order.charged:payload"
DLQ_DATA: dict[str, Any] = {
    "source": "order-domain-events",
    "stream": DLQ_STREAM,
    "schema_hash": "3f1c",
    "observed": 4,
    "abandoned": 3,
    "rate": 0.75,
}
DLQ_LABEL = f"Abandoned work on {DLQ_STREAM}: 3 of 4 work units with fingerprint 3f1c (75%)"


def _dlq(data: dict[str, Any] = DLQ_DATA) -> EvidenceFragment:
    return EvidenceFragment(
        collector="dlq_attribution",
        quality=EvidenceQuality.PRESENT,
        summary="DLQ summary",
        data=data,
    )


@pytest.mark.unit
async def test_echo_builds_one_dlq_possibility_per_attribution_after_bursts() -> None:
    dossier = Dossier(
        incident_id="inc-1",
        system="example",
        fragments=[
            _dlq(),
            _http_errors(),
            EvidenceFragment(
                collector="process_path", quality=EvidenceQuality.MISSING, summary="No flow"
            ),
            EvidenceFragment(
                collector="dlq_attribution", quality=EvidenceQuality.MISSING, summary="Not one"
            ),
        ],
    )
    decision = await EchoReasoner().decide(dossier)
    assert [(p.id, p.label, p.rationale_refs) for p in decision.possibilities] == [
        ("burst-0", HTTP_ERRORS_LABEL, ["http_errors"]),
        ("dlq-0", DLQ_LABEL, ["dlq_attribution"]),
        (
            "insufficient-evidence",
            "Insufficient evidence (MISSING fragments)",
            ["process_path", "dlq_attribution"],
        ),
    ]
    assert decision.noul is True


@pytest.mark.unit
@pytest.mark.parametrize(
    "data",
    [
        {key: value for key, value in DLQ_DATA.items() if key != "stream"},
        {**DLQ_DATA, "observed": 0},
        {**DLQ_DATA, "abandoned": "three"},
    ],
    ids=["no-stream", "nothing-observed", "abandoned-not-a-count"],
)
async def test_echo_malformed_dlq_data_falls_back_to_summary(data: dict[str, Any]) -> None:
    decision = await EchoReasoner().decide(
        Dossier(incident_id="inc-1", system="example", fragments=[_dlq(data)])
    )
    [possibility] = decision.possibilities
    assert (possibility.id, possibility.label) == ("dlq-0", "DLQ summary")


@pytest.mark.unit
async def test_echo_present_dlq_attribution_alone_is_evidence() -> None:
    decision = await EchoReasoner().decide(
        Dossier(incident_id="inc-1", system="example", fragments=[_dlq()])
    )
    assert [p.id for p in decision.possibilities] == ["dlq-0"]
    assert (decision.confidence, decision.noul, decision.primitive) == (
        0.45,
        False,
        Primitive.CHOICE,
    )


def _incident_io() -> EvidenceFragment:
    return EvidenceFragment(
        collector="incident_io",
        quality=EvidenceQuality.PRESENT,
        summary="incident.io incident INC-123: status=triage severity_rank=1",
        data={"incident_id": "01FDAG4SAP5TYPT98WGR2N7W91", "reference": "INC-123"},
    )


@pytest.mark.unit
async def test_an_incident_io_fragment_adds_no_possibility_and_is_no_evidence() -> None:
    missing = [
        EvidenceFragment(collector=collector, quality=EvidenceQuality.MISSING, summary="none")
        for collector in ("process_path", "shape")
    ]
    decision = await EchoReasoner().decide(
        Dossier(incident_id="inc-1", system="acme", fragments=[_incident_io(), *missing])
    )
    [possibility] = decision.possibilities
    assert (possibility.id, possibility.probability) == ("insufficient-evidence", 1.0)
    assert possibility.rationale_refs == ["process_path", "shape"]
    assert (decision.confidence, decision.noul, decision.primitive) == (0.2, True, Primitive.NOUL)

    alone = await EchoReasoner().decide(
        Dossier(incident_id="inc-2", system="acme", fragments=[_incident_io()])
    )
    assert [possibility.id for possibility in alone.possibilities] == ["unknown"]
    assert (alone.confidence, alone.noul) == (0.2, True)
