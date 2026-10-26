"""FeedbackService: decisions recorded on the clock, verdicts checked against them, and a new
verdict superseding the incident's pending ones.
"""

from __future__ import annotations

import itertools
from datetime import UTC, datetime, timedelta

import pytest

from fire.adapters.memory.decision_store import MemoryDecisionStore
from fire.core.domain.decision import Decision, Possibility
from fire.core.domain.dossier import Dossier
from fire.core.domain.feedback import TrainingStatus, Verdict
from fire.core.services.feedback import FeedbackService, InvalidFeedback, UnknownIncident

T0 = datetime(2026, 10, 26, 9, 0, tzinfo=UTC)


class Clock:
    def __init__(self) -> None:
        self.now = T0

    def __call__(self) -> datetime:
        return self.now


def _decision(incident_id: str = "inc-1") -> Decision:
    return Decision(
        incident_id=incident_id,
        possibilities=[
            Possibility(id="hop-0", label="api", probability=0.2),
            Possibility(id="drift-0", label="orders drift", probability=0.7),
            Possibility(id="unknown", label="Unknown", probability=0.1),
        ],
        confidence=0.7,
    )


@pytest.fixture
def store() -> MemoryDecisionStore:
    return MemoryDecisionStore()


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def service(store: MemoryDecisionStore, clock: Clock) -> FeedbackService:
    ids = (f"fb-{n}" for n in itertools.count(1))
    service = FeedbackService(store, clock=clock, new_id=lambda: next(ids))
    service.record_decision(
        "acme", Dossier(incident_id="inc-1", system="acme"), _decision(), "laya", "fire"
    )
    return service


@pytest.mark.unit
def test_record_decision_saves_it_on_the_clock(
    store: MemoryDecisionStore, clock: Clock, service: FeedbackService
) -> None:
    record = store.get_decision("inc-1")
    assert record is not None
    assert (record.system, record.reasoner, record.model) == ("acme", "laya", "fire")
    assert record.decided_at == T0
    assert record.decision == _decision()


@pytest.mark.unit
def test_confirmed_names_the_top_possibility(
    store: MemoryDecisionStore, service: FeedbackService
) -> None:
    feedback = service.submit("inc-1", Verdict.CONFIRMED, None, "ana", "right")
    assert feedback.id == "fb-1"
    assert feedback.possibility_id == "drift-0"
    assert (feedback.reviewer, feedback.note, feedback.created_at) == ("ana", "right", T0)
    assert feedback.status is TrainingStatus.PENDING
    assert store.list_feedback() == [feedback]
    assert service.submit("inc-1", Verdict.CONFIRMED, "drift-0", "ana").possibility_id == (
        "drift-0"
    )


@pytest.mark.unit
def test_corrected_names_another_possibility(service: FeedbackService) -> None:
    feedback = service.submit("inc-1", Verdict.CORRECTED, "hop-0", reviewer="ana")
    assert feedback.verdict is Verdict.CORRECTED
    assert feedback.possibility_id == "hop-0"


@pytest.mark.unit
def test_rejected_names_none(service: FeedbackService) -> None:
    feedback = service.submit("inc-1", "rejected", None, "ana")  # type: ignore[arg-type]
    assert feedback.verdict is Verdict.REJECTED
    assert feedback.possibility_id is None


@pytest.mark.unit
@pytest.mark.parametrize(
    ("verdict", "possibility_id", "message"),
    [
        (Verdict.CONFIRMED, "hop-0", "use corrected"),
        (Verdict.CORRECTED, None, "names a possibility"),
        (Verdict.CORRECTED, "drift-0", "use confirmed"),
        (Verdict.CORRECTED, "burst-0", "not a possibility"),
        (Verdict.REJECTED, "hop-0", "names no possibility"),
        ("maybe", None, "unknown verdict"),
    ],
)
def test_a_verdict_that_does_not_fit_is_invalid(
    store: MemoryDecisionStore,
    service: FeedbackService,
    verdict: Verdict,
    possibility_id: str | None,
    message: str,
) -> None:
    with pytest.raises(InvalidFeedback, match=message):
        service.submit("inc-1", verdict, possibility_id, "ana")
    assert store.list_feedback() == []


@pytest.mark.unit
def test_an_empty_reviewer_is_invalid(service: FeedbackService) -> None:
    with pytest.raises(InvalidFeedback):
        service.submit("inc-1", Verdict.CONFIRMED, None, "")


@pytest.mark.unit
def test_a_decision_without_possibilities_takes_only_a_rejection(
    store: MemoryDecisionStore, service: FeedbackService
) -> None:
    service.record_decision(
        "acme", Dossier(incident_id="inc-2", system="acme"), Decision(incident_id="inc-2"), "echo"
    )
    with pytest.raises(InvalidFeedback, match="no possibility"):
        service.submit("inc-2", Verdict.CONFIRMED, None, "ana")
    assert service.submit("inc-2", Verdict.REJECTED, None, "ana").possibility_id is None


@pytest.mark.unit
def test_an_unknown_incident_is_refused(service: FeedbackService) -> None:
    with pytest.raises(UnknownIncident):
        service.submit("inc-9", Verdict.REJECTED, None, "ana")


@pytest.mark.unit
def test_a_new_verdict_discards_the_pending_ones(
    store: MemoryDecisionStore, clock: Clock, service: FeedbackService
) -> None:
    service.record_decision(
        "acme", Dossier(incident_id="inc-2", system="acme"), _decision("inc-2"), "echo"
    )
    service.submit("inc-1", Verdict.CONFIRMED, None, "ana")  # fb-1
    service.submit("inc-2", Verdict.CONFIRMED, None, "ana")  # fb-2
    store.mark(["fb-1"], TrainingStatus.EXPORTED, "batch-1")
    service.submit("inc-1", Verdict.REJECTED, None, "bo")  # fb-3
    clock.now = T0 + timedelta(minutes=1)
    latest = service.submit("inc-1", Verdict.CORRECTED, "hop-0", "bo")  # fb-4
    assert latest.created_at == T0 + timedelta(minutes=1)
    rows = {row.id: row.status for row in store.list_feedback()}
    assert rows == {
        "fb-1": TrainingStatus.EXPORTED,
        "fb-2": TrainingStatus.PENDING,
        "fb-3": TrainingStatus.DISCARDED,
        "fb-4": TrainingStatus.PENDING,
    }
