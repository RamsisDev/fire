"""ShapeField cardinality vocabulary, enforced by the model."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from fire.core.domain.dossier import ShapeField


@pytest.mark.unit
@pytest.mark.parametrize("cardinality", [None, "void", "n", "0", "1", "42"])
def test_cardinality_accepts_the_vocabulary(cardinality: str | None) -> None:
    assert ShapeField(path="a", cardinality=cardinality).cardinality == cardinality


@pytest.mark.unit
@pytest.mark.parametrize(
    "cardinality",
    ["many", "one", "optional", "N", "-1", "2.5", "1e3", "01", "00", "", " 1", "1\n", "void "],
)
def test_cardinality_rejects_other_strings(cardinality: str) -> None:
    with pytest.raises(ValidationError):
        ShapeField(path="a", cardinality=cardinality)


@pytest.mark.unit
@pytest.mark.parametrize("cardinality", [0, 2, 2.0, True])
def test_cardinality_rejects_numbers(cardinality: object) -> None:
    with pytest.raises(ValidationError):
        ShapeField.model_validate({"path": "a", "cardinality": cardinality})


@pytest.mark.unit
def test_cardinality_rejects_a_json_int() -> None:
    with pytest.raises(ValidationError):
        ShapeField.model_validate_json('{"path": "a", "cardinality": 2}')
