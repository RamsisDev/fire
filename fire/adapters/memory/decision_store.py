"""DecisionStore in process memory; a restart loses every decision and its feedback."""

from __future__ import annotations

import threading
from collections.abc import Collection

from fire.core.domain.feedback import DecisionRecord, Feedback, TrainingStatus


class MemoryDecisionStore:
    """Thread-safe: every operation runs under one lock. Rows are copies, so a caller's changes
    to what it passed in or got back do not reach the store.
    """

    def __init__(self) -> None:
        self._decisions: dict[str, DecisionRecord] = {}
        self._feedback: dict[str, Feedback] = {}
        self._lock = threading.Lock()

    def save_decision(self, record: DecisionRecord) -> None:
        with self._lock:
            self._decisions[record.incident_id] = record.model_copy(deep=True)

    def get_decision(self, incident_id: str) -> DecisionRecord | None:
        with self._lock:
            record = self._decisions.get(incident_id)
            return None if record is None else record.model_copy(deep=True)

    def add_feedback(self, feedback: Feedback) -> None:
        with self._lock:
            if feedback.id in self._feedback:
                raise ValueError(f"feedback {feedback.id!r} is already stored")
            self._feedback[feedback.id] = feedback.model_copy()

    def list_feedback(
        self,
        status: TrainingStatus | None = None,
        system: str | None = None,
        limit: int | None = None,
        *,
        incident_id: str | None = None,
    ) -> list[Feedback]:
        with self._lock:
            rows: list[Feedback] = []
            for feedback in self._feedback.values():
                if limit is not None and len(rows) >= limit:
                    break
                if status is not None and feedback.status != status:
                    continue
                if incident_id is not None and feedback.incident_id != incident_id:
                    continue
                if system is not None:
                    record = self._decisions.get(feedback.incident_id)
                    if record is None or record.system != system:
                        continue
                rows.append(feedback.model_copy())
            return rows

    def mark(
        self, ids: Collection[str], status: TrainingStatus, batch_id: str | None = None
    ) -> None:
        changes: dict[str, object] = {"status": TrainingStatus(status)}
        if batch_id is not None:
            changes["batch_id"] = batch_id
        with self._lock:
            for feedback_id in ids:
                feedback = self._feedback.get(feedback_id)
                if feedback is not None:
                    self._feedback[feedback_id] = feedback.model_copy(update=changes)

    def close(self) -> None:
        return None
