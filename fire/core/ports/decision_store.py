"""Port: where decisions and the feedback on them are kept until retraining reads them."""

from __future__ import annotations

from collections.abc import Collection
from typing import Protocol, runtime_checkable

from fire.core.domain.feedback import DecisionRecord, Feedback, TrainingStatus


@runtime_checkable
class DecisionStore(Protocol):
    """One decision per incident, and every feedback row given on it.

    `save_decision` replaces the incident's earlier record, if any. `add_feedback` raises
    `ValueError` for an id already stored. `list_feedback` returns the rows matching every filter
    given, in the order added, at most `limit` of them; `system` is the system of the incident's
    decision. `mark` sets the status of the rows with those ids, and their `batch_id` when one is
    given; unknown ids are ignored.
    """

    def save_decision(self, record: DecisionRecord) -> None: ...

    def get_decision(self, incident_id: str) -> DecisionRecord | None: ...

    def add_feedback(self, feedback: Feedback) -> None: ...

    def list_feedback(
        self,
        status: TrainingStatus | None = None,
        system: str | None = None,
        limit: int | None = None,
        *,
        incident_id: str | None = None,
    ) -> list[Feedback]: ...

    def mark(
        self, ids: Collection[str], status: TrainingStatus, batch_id: str | None = None
    ) -> None: ...

    def close(self) -> None: ...
