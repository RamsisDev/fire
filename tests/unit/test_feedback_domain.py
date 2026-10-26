"""Feedback domain: a verdict names a possibility unless it rejects them all, and a decision
record round-trips through JSON.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from fire.core.domain.decision import Decision, Possibility
from fire.core.domain.dossier import Dossier
from fire.core.domain.feedback import DecisionRecord, Feedback, TrainingStatus, Verdict

T0 = datetime(2026, 10, 26, 9, 0, tzinfo=UTC)


def _feedback(verdict: Verdict, possibility_id: str | None, **overrides: object) -> Feedback:
    data: dict[str, object] = {
        "id": "fb-1",
        "incident_id": "inc-1",
        "verdict": verdict,
        "possibility_id": possibility_id,
        "reviewer": "ana",
        "created_at": T0,
    }
    return Feedback.model_validate(data | overrides)


@pytest.mark.unit
def test_enum_values_are_the_wire_names() -> None:
    assert [v.value for v in Verdict] == ["confirmed", "corrected", "rejected"]
    assert [s.value for s in TrainingStatus] == ["pending", "exported", "trained", "discarded"]


@pytest.mark.unit
def test_new_feedback_is_pending_without_a_batch() -> None:
    feedback = _feedback(Verdict.CORRECTED, "drift-0", note="schema change")
    assert feedback.status is TrainingStatus.PENDING
    assert feedback.batch_id is None
    assert feedback.note == "schema change"


@pytest.mark.unit
@pytest.mark.parametrize("verdict", [Verdict.CONFIRMED, Verdict.CORRECTED])
def test_confirmed_and_corrected_name_a_possibility(verdict: Verdict) -> None:
    assert _feedback(verdict, "hop-0").possibility_id == "hop-0"
    with pytest.raises(ValidationError, match="names a possibility"):
        _feedback(verdict, None)


@pytest.mark.unit
def test_rejected_names_no_possibility() -> None:
    assert _feedback(Verdict.REJECTED, None).possibility_id is None
    with pytest.raises(ValidationError, match="names no possibility"):
        _feedback(Verdict.REJECTED, "hop-0")


@pytest.mark.unit
@pytest.mark.parametrize(
    "overrides",
    [{"reviewer": ""}, {"created_at": datetime(2026, 10, 26, 9, 0)}, {"status": "queued"}],
)
def test_feedback_rejects_bad_fields(overrides: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        _feedback(Verdict.CONFIRMED, "hop-0", **overrides)


@pytest.mark.unit
def test_decision_record_round_trips_through_json() -> None:
    record = DecisionRecord(
        incident_id="inc-1",
        system="acme",
        reasoner="laya",
        model="fire",
        dossier=Dossier(incident_id="inc-1", system="acme", service="api"),
        decision=Decision(
            incident_id="inc-1",
            possibilities=[Possibility(id="hop-0", label="api", probability=1.0)],
            confidence=0.8,
        ),
        decided_at=T0,
    )
    assert DecisionRecord.model_validate_json(record.model_dump_json()) == record


@pytest.mark.unit
def test_decision_record_model_defaults_to_none() -> None:
    record = DecisionRecord(
        incident_id="inc-1",
        system="acme",
        reasoner="echo",
        dossier=Dossier(incident_id="inc-1", system="acme"),
        decision=Decision(incident_id="inc-1"),
        decided_at=T0,
    )
    assert record.model is None
