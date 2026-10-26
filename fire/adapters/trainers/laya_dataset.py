"""Laya training rows: `{state, questions, expected}`, one per reviewed decision.

The `state` and `questions` are what `build_request` sends to `POST /v1/systemone`, so the
model trains on exactly what it is asked at serving time. Laya reads only those three keys;
traceability lives in the batch manifest.
"""

from __future__ import annotations

from typing import Any

from fire.adapters.reasoners.systemone import build_request
from fire.core.domain.feedback import DecisionRecord, Feedback, Verdict


def training_row(record: DecisionRecord | None, feedback: Feedback) -> dict[str, Any] | None:
    """The row for one verdict, or None when it cannot train: the decision record is gone, or
    the verdict names a cause that is not among the request's options.

    A confirmed or corrected verdict labels the cause and says the evidence sufficed; a
    rejected one only says the evidence did not (Laya skips the unlabelled `cause`).
    """
    if record is None:
        return None
    request = build_request(record.dossier)
    if feedback.verdict is Verdict.REJECTED:
        expected: dict[str, Any] = {"sufficient": False}
    else:
        if feedback.possibility_id not in request["questions"]["cause"]["criteria"]:
            return None
        expected = {"cause": feedback.possibility_id, "sufficient": True}
    return {"state": request["state"], "questions": request["questions"], "expected": expected}
