"""Decision contracts: typed primitive and probability distribution."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from fire.core.domain.decision import Decision, Possibility, Primitive


def _possibilities(*probabilities: float) -> list[Possibility]:
    return [
        Possibility(id=f"option-{index}", label=f"Option {index}", probability=probability)
        for index, probability in enumerate(probabilities)
    ]


@pytest.mark.unit
def test_decision_primitive_defaults_to_choice() -> None:
    assert Decision(incident_id="inc-1").primitive is Primitive.CHOICE


@pytest.mark.unit
@pytest.mark.parametrize(
    ("value", "primitive"),
    [("choice", Primitive.CHOICE), ("score", Primitive.SCORE), ("noul", Primitive.NOUL)],
)
def test_decision_primitive_parses_wire_value(value: str, primitive: Primitive) -> None:
    assert Decision(incident_id="inc-1", primitive=value).primitive is primitive


@pytest.mark.unit
def test_decision_rejects_unknown_primitive() -> None:
    with pytest.raises(ValidationError, match="primitive"):
        Decision(incident_id="inc-1", primitive="vote")


@pytest.mark.unit
@pytest.mark.parametrize(
    "probabilities",
    [(1 / 3, 1 / 3, 1 / 3), (0.5, 0.4999999)],
    ids=["three-way-split", "within-tolerance"],
)
def test_decision_accepts_probabilities_summing_to_one(probabilities: tuple[float, ...]) -> None:
    decision = Decision(incident_id="inc-1", possibilities=_possibilities(*probabilities))
    assert [p.probability for p in decision.possibilities] == list(probabilities)


@pytest.mark.unit
def test_decision_accepts_no_possibilities() -> None:
    assert Decision(incident_id="inc-1").possibilities == []


@pytest.mark.unit
@pytest.mark.parametrize(
    "probabilities",
    [(0.25, 0.25), (0.3333, 0.3333, 0.3333)],
    ids=["sum-0.5", "rounded-thirds"],
)
def test_decision_rejects_probabilities_not_summing_to_one(
    probabilities: tuple[float, ...],
) -> None:
    with pytest.raises(ValidationError, match="sum to 1"):
        Decision(incident_id="inc-1", possibilities=_possibilities(*probabilities))
