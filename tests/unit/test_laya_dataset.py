"""Laya training rows: built from the same request the reasoner sends, labelled by the verdict."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fire.adapters.reasoners.systemone import build_request
from fire.adapters.trainers.laya_dataset import training_row
from fire.core.domain.decision import Decision, Possibility
from fire.core.domain.dossier import Dossier, ProcessHop, ProcessPath
from fire.core.domain.feedback import DecisionRecord, Feedback, Verdict

T0 = datetime(2026, 10, 26, 9, 0, tzinfo=UTC)

DOSSIER = Dossier(
    incident_id="inc-1",
    system="acme",
    process_path=ProcessPath(hops=[ProcessHop(ref="api")]),
)


def _record() -> DecisionRecord:
    return DecisionRecord(
        incident_id="inc-1",
        system="acme",
        reasoner="laya",
        dossier=DOSSIER,
        decision=Decision(
            incident_id="inc-1",
            possibilities=[Possibility(id="hop-0", label="api", probability=1.0)],
            confidence=1.0,
        ),
        decided_at=T0,
    )


def _feedback(verdict: Verdict, possibility_id: str | None) -> Feedback:
    return Feedback(
        id="fb-1",
        incident_id="inc-1",
        verdict=verdict,
        possibility_id=possibility_id,
        reviewer="ana",
        created_at=T0,
    )


def test_confirmed_labels_cause_and_sufficient() -> None:
    row = training_row(_record(), _feedback(Verdict.CONFIRMED, "hop-0"))
    request = build_request(DOSSIER)
    assert row == {
        "state": request["state"],
        "questions": request["questions"],
        "expected": {"cause": "hop-0", "sufficient": True},
    }


def test_corrected_names_the_reviewers_possibility() -> None:
    record = _record()
    record.dossier = Dossier(
        incident_id="inc-1",
        system="acme",
        process_path=ProcessPath(hops=[ProcessHop(ref="api"), ProcessHop(ref="db")]),
    )
    row = training_row(record, _feedback(Verdict.CORRECTED, "hop-1"))
    assert row is not None
    assert row["expected"] == {"cause": "hop-1", "sufficient": True}


def test_rejected_leaves_cause_out() -> None:
    row = training_row(_record(), _feedback(Verdict.REJECTED, None))
    assert row is not None
    assert row["expected"] == {"sufficient": False}


def test_missing_record_is_skipped() -> None:
    assert training_row(None, _feedback(Verdict.CONFIRMED, "hop-0")) is None


def test_cause_outside_the_options_is_skipped() -> None:
    assert training_row(_record(), _feedback(Verdict.CORRECTED, "hop-99")) is None


def test_row_matches_what_laya_accepts() -> None:
    """Laya's `items_from_rows`: a choice target is a criteria key, a noul target a bool."""
    row = training_row(_record(), _feedback(Verdict.CONFIRMED, "hop-0"))
    assert row is not None
    assert set(row) == {"state", "questions", "expected"}
    expected: dict[str, Any] = row["expected"]
    assert set(expected) <= set(row["questions"])
    assert expected["cause"] in row["questions"]["cause"]["criteria"]
    assert isinstance(expected["sufficient"], bool)
    assert row["state"]
