"""Origin and by-reference ProcessHop contracts."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from fire.core.domain.dossier import HopVia, ProcessHop
from fire.core.domain.origin import Origin, OriginKind


@pytest.mark.unit
def test_origin_has_no_form_fields() -> None:
    origin = Origin(
        origin_id="orig-http-orders",
        kind=OriginKind.HTTP,
        endpoint_ref="orders.create",
    )
    dumped = origin.model_dump()
    assert set(dumped) == {"origin_id", "kind", "endpoint_ref"}
    assert "fields" not in dumped
    assert "shape" not in dumped


@pytest.mark.unit
def test_process_hop_by_ref_no_embedded_shape() -> None:
    hop = ProcessHop(from_ref="orig-a", to_ref="orig-b", operation="POST /x")
    assert hop.display_label() == "orig-a->orig-b/POST /x"
    assert not hasattr(hop, "shape")
    assert "shape" not in hop.model_dump()


@pytest.mark.unit
def test_process_hop_identity_fields_are_ref_from_ref_to_ref() -> None:
    assert set(ProcessHop.model_fields) == {
        "ref",
        "from_ref",
        "to_ref",
        "operation",
        "span_kind",
        "status",
        "via",
    }


@pytest.mark.unit
@pytest.mark.parametrize(
    ("fields", "label"),
    [
        ({"ref": "api", "operation": "GET /y"}, "api/GET /y"),
        ({"ref": "orig-a"}, "orig-a"),
        ({"from_ref": "orig-a", "to_ref": "orig-b"}, "orig-a->orig-b"),
        ({"from_ref": "orig-a", "operation": "POST /x"}, "orig-a/POST /x"),
        ({"to_ref": "orig-b"}, "orig-b"),
    ],
    ids=["node-with-operation", "node", "edge", "edge-from-only", "edge-to-only"],
)
def test_process_hop_display_label(fields: dict[str, str], label: str) -> None:
    assert ProcessHop(**fields).display_label() == label


@pytest.mark.unit
@pytest.mark.parametrize(
    "fields",
    [
        {"operation": "alone"},
        {"ref": "", "operation": "empty node"},
        {"from_ref": "", "to_ref": ""},
    ],
    ids=["no-ref", "empty-ref", "empty-edge-refs"],
)
def test_process_hop_requires_a_ref(fields: dict[str, str]) -> None:
    with pytest.raises(ValidationError):
        ProcessHop(**fields)


@pytest.mark.unit
@pytest.mark.parametrize(
    "fields",
    [
        {"ref": "api", "from_ref": "orig-a"},
        {"ref": "api", "to_ref": "orig-b"},
        {"ref": "api", "from_ref": "orig-a", "to_ref": "orig-b"},
    ],
    ids=["ref-and-from", "ref-and-to", "ref-and-edge"],
)
def test_process_hop_cannot_be_node_and_edge(fields: dict[str, str]) -> None:
    with pytest.raises(ValidationError, match="node"):
        ProcessHop(**fields)


@pytest.mark.unit
@pytest.mark.parametrize(
    "fields",
    [
        {"from_ref": "orig-a", "to_ref": "orig-b", "via": "child"},
        {"from_ref": "orig-a", "to_ref": "orig-b", "via": "link"},
        {"from_ref": "orig-a", "via": "link"},
        {"to_ref": "orig-b", "via": "child"},
    ],
    ids=["edge-child", "edge-link", "edge-from-only-link", "edge-to-only-child"],
)
def test_process_hop_via_is_accepted_on_edge_hops(fields: dict[str, str]) -> None:
    assert ProcessHop(**fields).via is HopVia(fields["via"])


@pytest.mark.unit
@pytest.mark.parametrize("via", ["child", "link"])
def test_process_hop_via_is_rejected_on_a_node_hop(via: str) -> None:
    with pytest.raises(ValidationError, match="via"):
        ProcessHop(ref="api", via=via)


@pytest.mark.unit
def test_process_hop_via_rejects_an_unknown_value() -> None:
    with pytest.raises(ValidationError, match="via"):
        ProcessHop(from_ref="orig-a", to_ref="orig-b", via="sibling")


@pytest.mark.unit
def test_process_hop_json_round_trip_keeps_via() -> None:
    hop = ProcessHop(from_ref="orig-a", to_ref="orig-b", operation="POST /x", via=HopVia.LINK)
    dumped = hop.model_dump_json()
    assert '"via":"link"' in dumped
    assert ProcessHop.model_validate_json(dumped) == hop


@pytest.mark.unit
@pytest.mark.asyncio
async def test_echo_uses_ref_labels_without_new_prob_logic() -> None:
    from fire.adapters.reasoners.echo import EchoReasoner
    from fire.core.domain.dossier import Dossier, ProcessPath, ShapeFingerprint

    dossier = Dossier(
        incident_id="inc-a",
        system="proj-a",
        origin=Origin(origin_id="orig-a", kind=OriginKind.HTTP, endpoint_ref="ep-a"),
        fingerprint=ShapeFingerprint(schema_hash="h1"),
        process_path=ProcessPath(hops=[ProcessHop(from_ref="orig-a", to_ref="orig-b")]),
    )
    decision = await EchoReasoner().decide(dossier)
    assert decision.possibilities[0].label == "Failure near orig-a->orig-b"
    # still uniform 1/n — demo stub, not preset catalog
    assert decision.possibilities[0].probability == 1.0
