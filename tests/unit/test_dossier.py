"""Dossier holds each fact in exactly one field."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from fire.core.domain.dossier import Dossier, EvidenceFragment, EvidenceQuality


@pytest.mark.unit
def test_dossier_has_no_shape_record() -> None:
    assert "shape_record" not in Dossier.model_fields


@pytest.mark.unit
def test_dossier_names_its_system_and_rejects_project_id() -> None:
    assert Dossier(incident_id="inc-1", system="proj-a").system == "proj-a"
    assert "project_id" not in Dossier.model_fields
    with pytest.raises(ValidationError, match="project_id"):
        Dossier(incident_id="inc-1", system="proj-a", project_id="proj-a")


@pytest.mark.unit
def test_dossier_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError, match="shape_record"):
        Dossier(incident_id="inc-1", system="proj-a", shape_record=None)


@pytest.mark.unit
def test_dossier_missing_is_derived_from_fragments() -> None:
    no_origin = EvidenceFragment(
        collector="origin", quality=EvidenceQuality.MISSING, summary="No Origin provided"
    )
    shape = EvidenceFragment(
        collector="shape", quality=EvidenceQuality.PRESENT, summary="Shape provided"
    )
    no_fingerprint = EvidenceFragment(
        collector="fingerprint",
        quality=EvidenceQuality.MISSING,
        summary="Shape present without a fingerprint",
    )
    dossier = Dossier(
        incident_id="inc-1",
        system="proj-a",
        fragments=[no_origin, shape, no_fingerprint],
    )
    assert dossier.missing == [no_origin, no_fingerprint]


@pytest.mark.unit
def test_dossier_missing_is_not_a_field() -> None:
    dossier = Dossier(incident_id="inc-1", system="proj-a")
    assert "missing" not in Dossier.model_fields
    assert "missing" not in dossier.model_dump()
    with pytest.raises(ValidationError, match="missing"):
        Dossier(incident_id="inc-1", system="proj-a", missing=[])
