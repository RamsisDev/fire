"""DecisionStore contract, run against the memory and the SQLite adapter: one decision per
incident, feedback filtered and marked. The SQLite adapter's restart cases close the file and
open it again.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from contextlib import closing
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from fire.adapters.memory.decision_store import MemoryDecisionStore
from fire.adapters.sqlite.decision_store import SCHEMA_VERSION, SqliteDecisionStore
from fire.core.domain.decision import Decision, Possibility
from fire.core.domain.dossier import Dossier, ProcessHop, ProcessPath
from fire.core.domain.feedback import DecisionRecord, Feedback, TrainingStatus, Verdict
from fire.core.ports.decision_store import DecisionStore

T0 = datetime(2026, 10, 26, 9, 0, tzinfo=UTC)

Make = Callable[[], DecisionStore]


@pytest.fixture(params=["memory", "sqlite"])
def make_store(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Make]:
    """Opens a store of the parametrized adapter, each SQLite one in its own file."""
    opened: list[DecisionStore] = []

    def make() -> DecisionStore:
        store: DecisionStore
        if request.param == "memory":
            store = MemoryDecisionStore()
        else:
            store = SqliteDecisionStore(tmp_path / f"decisions-{len(opened)}.sqlite3")
        opened.append(store)
        return store

    yield make
    for store in opened:
        store.close()


def _record(incident_id: str = "inc-1", system: str = "acme", **overrides: Any) -> DecisionRecord:
    data: dict[str, Any] = {
        "incident_id": incident_id,
        "system": system,
        "reasoner": "echo",
        "dossier": Dossier(
            incident_id=incident_id,
            system=system,
            process_path=ProcessPath(hops=[ProcessHop(ref="api")]),
        ),
        "decision": Decision(
            incident_id=incident_id,
            possibilities=[
                Possibility(id="hop-0", label="api", probability=0.75),
                Possibility(id="unknown", label="Unknown", probability=0.25),
            ],
            confidence=0.75,
        ),
        "decided_at": T0,
    }
    return DecisionRecord.model_validate(data | overrides)


def _feedback(feedback_id: str, incident_id: str = "inc-1", **overrides: Any) -> Feedback:
    data: dict[str, Any] = {
        "id": feedback_id,
        "incident_id": incident_id,
        "verdict": Verdict.CONFIRMED,
        "possibility_id": "hop-0",
        "reviewer": "ana",
        "created_at": T0,
    }
    return Feedback.model_validate(data | overrides)


@pytest.mark.unit
def test_adapters_satisfy_the_port(make_store: Make) -> None:
    assert isinstance(make_store(), DecisionStore)


@pytest.mark.unit
def test_saved_decision_reads_back(make_store: Make) -> None:
    store = make_store()
    record = _record(model="fire")
    store.save_decision(record)
    assert store.get_decision("inc-1") == record
    assert store.get_decision("inc-2") is None


@pytest.mark.unit
def test_a_later_decision_replaces_the_earlier(make_store: Make) -> None:
    store = make_store()
    store.save_decision(_record())
    later = _record(system="zeta", reasoner="laya", decided_at=T0 + timedelta(minutes=5))
    store.save_decision(later)
    assert store.get_decision("inc-1") == later
    store.add_feedback(_feedback("fb-1"))
    assert [row.id for row in store.list_feedback(system="zeta")] == ["fb-1"]
    assert store.list_feedback(system="acme") == []


@pytest.mark.unit
def test_feedback_lists_in_the_order_added(make_store: Make) -> None:
    store = make_store()
    rows = [
        _feedback("fb-b", note="first"),
        _feedback("fb-a", verdict=Verdict.REJECTED, possibility_id=None),
        _feedback("fb-c", verdict=Verdict.CORRECTED, possibility_id="unknown"),
    ]
    for row in rows:
        store.add_feedback(row)
    assert store.list_feedback() == rows
    assert store.list_feedback(limit=2) == rows[:2]
    assert store.list_feedback(limit=0) == []


@pytest.mark.unit
def test_a_known_feedback_id_is_refused(make_store: Make) -> None:
    store = make_store()
    store.add_feedback(_feedback("fb-1"))
    with pytest.raises(ValueError, match="already stored"):
        store.add_feedback(_feedback("fb-1", reviewer="bo"))
    assert [row.reviewer for row in store.list_feedback()] == ["ana"]


@pytest.mark.unit
def test_feedback_filters_by_status_system_and_incident(make_store: Make) -> None:
    store = make_store()
    store.save_decision(_record("inc-1", "acme"))
    store.save_decision(_record("inc-2", "zeta"))
    store.add_feedback(_feedback("fb-1", "inc-1"))
    store.add_feedback(_feedback("fb-2", "inc-2"))
    store.add_feedback(_feedback("fb-3", "inc-1", status=TrainingStatus.EXPORTED))
    store.add_feedback(_feedback("fb-4", "inc-9"))  # no decision: in no system

    def ids(**filters: Any) -> list[str]:
        return [row.id for row in store.list_feedback(**filters)]

    assert ids(status=TrainingStatus.PENDING) == ["fb-1", "fb-2", "fb-4"]
    assert ids(system="acme") == ["fb-1", "fb-3"]
    assert ids(status=TrainingStatus.PENDING, system="acme") == ["fb-1"]
    assert ids(incident_id="inc-1") == ["fb-1", "fb-3"]
    assert ids(status=TrainingStatus.EXPORTED, incident_id="inc-1") == ["fb-3"]
    assert ids(system="acme", limit=1) == ["fb-1"]
    assert ids(system="none") == []


@pytest.mark.unit
def test_mark_sets_status_and_keeps_batch_unless_given(make_store: Make) -> None:
    store = make_store()
    for feedback_id in ("fb-1", "fb-2", "fb-3"):
        store.add_feedback(_feedback(feedback_id))
    store.mark(["fb-1", "fb-2", "fb-missing"], TrainingStatus.EXPORTED, "batch-1")
    store.mark(["fb-1"], TrainingStatus.TRAINED)
    store.mark([], TrainingStatus.DISCARDED)
    rows = {row.id: (row.status, row.batch_id) for row in store.list_feedback()}
    assert rows == {
        "fb-1": (TrainingStatus.TRAINED, "batch-1"),
        "fb-2": (TrainingStatus.EXPORTED, "batch-1"),
        "fb-3": (TrainingStatus.PENDING, None),
    }


@pytest.mark.unit
def test_rows_read_back_are_copies(make_store: Make) -> None:
    store = make_store()
    record = _record()
    store.save_decision(record)
    record.decision.possibilities.clear()
    got = store.get_decision("inc-1")
    assert got is not None
    got.decision.possibilities.clear()
    store.add_feedback(_feedback("fb-1"))
    store.list_feedback()[0].status = TrainingStatus.TRAINED
    again = store.get_decision("inc-1")
    assert again is not None
    assert len(again.decision.possibilities) == 2
    assert store.list_feedback()[0].status is TrainingStatus.PENDING


@pytest.mark.unit
def test_sqlite_keeps_decisions_and_feedback_across_a_restart(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "decisions.sqlite3"
    store = SqliteDecisionStore(path)
    record = _record()
    plus_two = timezone(timedelta(hours=2))
    feedback = _feedback("fb-1", note="ok", created_at=T0.astimezone(plus_two))
    store.save_decision(record)
    store.add_feedback(feedback)
    store.mark(["fb-1"], TrainingStatus.EXPORTED, "batch-1")
    store.close()

    store = SqliteDecisionStore(path)
    try:
        assert store.get_decision("inc-1") == record
        assert store.list_feedback(system="acme") == [
            feedback.model_copy(update={"status": TrainingStatus.EXPORTED, "batch_id": "batch-1"})
        ]
    finally:
        store.close()


@pytest.mark.unit
def test_sqlite_refuses_another_schema_version(tmp_path: Path) -> None:
    path = tmp_path / "decisions.sqlite3"
    with closing(sqlite3.connect(path)) as db:
        db.execute(f"PRAGMA user_version = {SCHEMA_VERSION + 1}")
    with pytest.raises(ValueError, match="move the file aside"):
        SqliteDecisionStore(path)
