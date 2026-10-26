"""The System One wire request: options, criteria, and a shape-only state."""

from __future__ import annotations

import pytest

from fire.adapters.reasoners.systemone import build_options, build_request
from fire.core.domain.dossier import (
    Dossier,
    EvidenceFragment,
    EvidenceQuality,
    ProcessHop,
    ProcessPath,
    ShapeFingerprint,
)


def _dossier() -> Dossier:
    return Dossier(
        incident_id="inc-1",
        system="example",
        fingerprint=ShapeFingerprint(schema_hash="abc"),
        process_path=ProcessPath(
            hops=[ProcessHop(ref="api", operation="POST /orders"), ProcessHop(ref="db")]
        ),
        fragments=[
            EvidenceFragment(
                collector="origin", quality=EvidenceQuality.MISSING, summary="No Origin provided"
            )
        ],
    )


@pytest.mark.unit
def test_options_split_evenly_with_stable_ids() -> None:
    options = build_options(_dossier())
    assert [p.id for p in options] == ["hop-0", "hop-1", "insufficient-evidence"]
    assert [p.probability for p in options] == [1 / 3, 1 / 3, 1 / 3]


@pytest.mark.unit
def test_empty_dossier_has_only_unknown() -> None:
    options = build_options(Dossier(incident_id="inc-2", system="example"))
    assert [(p.id, p.probability) for p in options] == [("unknown", 1.0)]


@pytest.mark.unit
def test_request_shape() -> None:
    dossier = _dossier()
    request = build_request(dossier)
    assert set(request) == {"state", "questions"}
    assert "incident_id" not in request["state"]
    assert request["state"]["system"] == "example"
    cause = request["questions"]["cause"]
    assert cause["type"] == "choice"
    assert cause["instructions"] == "Which hypothesis best explains the failure?"
    assert cause["criteria"] == {p.id: p.label for p in build_options(dossier)}
    assert request["questions"]["sufficient"] == {
        "type": "noul",
        "instructions": "Is the evidence sufficient to name a cause?",
    }


@pytest.mark.unit
def test_request_is_deterministic() -> None:
    assert build_request(_dossier()) == build_request(_dossier())
