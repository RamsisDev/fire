"""Feedback service: records each decision and takes a reviewer's verdict on it, queued as
pending retraining.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from datetime import UTC, datetime

from pydantic import ValidationError

from fire.core.domain.decision import Decision
from fire.core.domain.dossier import Dossier
from fire.core.domain.feedback import (
    DecisionRecord,
    Feedback,
    InvalidFeedback,
    TrainingStatus,
    UnknownIncident,
    Verdict,
)
from fire.core.ports.decision_store import DecisionStore

__all__ = ["FeedbackService", "InvalidFeedback", "UnknownIncident"]


def _uuid4() -> str:
    return str(uuid.uuid4())


def _utc_now() -> datetime:
    return datetime.now(UTC)


class FeedbackService:
    """Keeps decisions and verdicts in a `DecisionStore`, on the body's `clock`.

    `submit` checks the verdict against the incident's recorded decision: `confirmed` names
    the top possibility (filled in when `possibility_id` is None), `corrected` another of its
    possibilities, and `rejected` none. The new row is `pending`; the incident's earlier
    `pending` rows become `discarded`, so only the latest verdict per incident is trained on.
    """

    def __init__(
        self,
        store: DecisionStore,
        clock: Callable[[], datetime] | None = None,
        new_id: Callable[[], str] = _uuid4,
    ) -> None:
        self._store = store
        self._clock = clock or _utc_now
        self._new_id = new_id
        self._lock = threading.Lock()

    def record_decision(
        self,
        system: str,
        dossier: Dossier,
        decision: Decision,
        reasoner: str,
        model: str | None = None,
    ) -> DecisionRecord:
        record = DecisionRecord(
            incident_id=decision.incident_id,
            system=system,
            reasoner=reasoner,
            model=model,
            dossier=dossier,
            decision=decision,
            decided_at=self._clock(),
        )
        self._store.save_decision(record)
        return record

    def submit(
        self,
        incident_id: str,
        verdict: Verdict,
        possibility_id: str | None,
        reviewer: str,
        note: str | None = None,
    ) -> Feedback:
        """Raises `UnknownIncident` when no decision is recorded for the incident, and
        `InvalidFeedback` when the verdict does not fit it.
        """
        record = self._store.get_decision(incident_id)
        if record is None:
            raise UnknownIncident(incident_id)
        try:
            verdict = Verdict(verdict)
        except ValueError as error:
            raise InvalidFeedback(f"unknown verdict {verdict!r}") from error
        possibility_id = _checked(record.decision, verdict, possibility_id)
        try:
            feedback = Feedback(
                id=self._new_id(),
                incident_id=incident_id,
                verdict=verdict,
                possibility_id=possibility_id,
                reviewer=reviewer,
                note=note,
                created_at=self._clock(),
            )
        except ValidationError as error:
            raise InvalidFeedback(str(error)) from error
        with self._lock:
            earlier = self._store.list_feedback(TrainingStatus.PENDING, incident_id=incident_id)
            self._store.mark([row.id for row in earlier], TrainingStatus.DISCARDED)
            self._store.add_feedback(feedback)
        return feedback


def _checked(decision: Decision, verdict: Verdict, possibility_id: str | None) -> str | None:
    """The possibility the verdict names, once it fits the decision."""
    if verdict is Verdict.REJECTED:
        if possibility_id is not None:
            raise InvalidFeedback("a rejected verdict names no possibility")
        return None
    if not decision.possibilities:
        raise InvalidFeedback(f"the decision has no possibility to be {verdict.value}")
    top = max(decision.possibilities, key=lambda p: p.probability).id
    if verdict is Verdict.CONFIRMED:
        if possibility_id not in (None, top):
            raise InvalidFeedback(f"confirmed names the top possibility {top!r}; use corrected")
        return top
    if possibility_id is None:
        raise InvalidFeedback("a corrected verdict names a possibility")
    if possibility_id not in {p.id for p in decision.possibilities}:
        raise InvalidFeedback(f"{possibility_id!r} is not a possibility of the decision")
    if possibility_id == top:
        raise InvalidFeedback(f"{top!r} is the top possibility; use confirmed")
    return possibility_id
