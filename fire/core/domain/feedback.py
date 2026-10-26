"""Human feedback on System One decisions: each decision as recorded, and a reviewer's verdict on
it, queued for retraining.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import AwareDatetime, BaseModel, Field, model_validator

from fire.core.domain.decision import Decision
from fire.core.domain.dossier import Dossier


class Verdict(StrEnum):
    """What a reviewer said of a decision's top possibility."""

    CONFIRMED = "confirmed"  # the top possibility is the cause
    CORRECTED = "corrected"  # another possibility of the decision is
    REJECTED = "rejected"  # none of the possibilities is


class TrainingStatus(StrEnum):
    """Where a feedback row stands in retraining."""

    PENDING = "pending"
    EXPORTED = "exported"
    TRAINED = "trained"
    DISCARDED = "discarded"  # superseded by a later verdict on the same incident


class UnknownIncident(LookupError):
    """No decision is recorded for the incident."""


class InvalidFeedback(ValueError):
    """The verdict does not fit the recorded decision."""


class DecisionRecord(BaseModel):
    """One decision as the reasoner made it, with the dossier it read.

    `reasoner` is the provider name and `model` its checkpoint, None when it has none.
    """

    incident_id: str
    system: str
    reasoner: str
    model: str | None = None
    dossier: Dossier
    decision: Decision
    decided_at: AwareDatetime


class Feedback(BaseModel):
    """A reviewer's verdict on the decision of one incident.

    `possibility_id` names the cause the reviewer settled on: the top possibility when
    `confirmed`, another one when `corrected`, and None when `rejected`. `batch_id` is the
    retraining batch that exported it, None until then.
    """

    id: str
    incident_id: str
    verdict: Verdict
    possibility_id: str | None = None
    reviewer: str = Field(min_length=1)
    note: str | None = None
    created_at: AwareDatetime
    status: TrainingStatus = TrainingStatus.PENDING
    batch_id: str | None = None

    @model_validator(mode="after")
    def _possibility_fits_verdict(self) -> Feedback:
        if self.verdict is Verdict.REJECTED:
            if self.possibility_id is not None:
                raise ValueError("a rejected verdict names no possibility")
        elif self.possibility_id is None:
            raise ValueError(f"a {self.verdict.value} verdict names a possibility")
        return self
