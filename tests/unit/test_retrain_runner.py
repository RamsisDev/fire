"""Retrain commands against a SQLite store in tmp_path and a fake trainer."""

from __future__ import annotations

import io
import json
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import pytest

from fire.adapters.reasoners.systemone import build_request
from fire.adapters.sqlite.decision_store import SqliteDecisionStore
from fire.apps.retrain.runner import run_export, run_status, run_train
from fire.core.domain.decision import Decision, Possibility
from fire.core.domain.dossier import Dossier, ProcessHop, ProcessPath
from fire.core.domain.feedback import DecisionRecord, Feedback, TrainingStatus, Verdict

T0 = datetime(2026, 10, 26, 9, 0, tzinfo=UTC)


def _config(tmp_path: Path, provider: str = "sqlite", min_feedback: int = 2) -> Path:
    path = tmp_path / "fire.yml"
    path.write_text(
        f"feedback:\n  provider: {provider}\nretraining:\n  min_feedback: {min_feedback}\n"
        "  epochs: 3\n"
    )
    return path


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("FIRE_FEEDBACK_PATH", str(tmp_path / "fb.sqlite3"))
    monkeypatch.setenv("FIRE_LAYA_RETRAIN_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("FIRE_LAYA_BASE_CHECKPOINT", "base-ckpt")
    return tmp_path


def _seed(tmp_path: Path, count: int = 2, system: str = "acme", orphan: bool = False) -> None:
    store = SqliteDecisionStore(tmp_path / "fb.sqlite3")
    for n in range(count):
        incident = f"{system}-{n}"
        store.save_decision(
            DecisionRecord(
                incident_id=incident,
                system=system,
                reasoner="laya",
                dossier=Dossier(
                    incident_id=incident,
                    system=system,
                    process_path=ProcessPath(hops=[ProcessHop(ref="api")]),
                ),
                decision=Decision(
                    incident_id=incident,
                    possibilities=[Possibility(id="hop-0", label="api", probability=1.0)],
                    confidence=1.0,
                ),
                decided_at=T0,
            )
        )
        store.add_feedback(
            Feedback(
                id=f"fb-{incident}",
                incident_id=incident,
                verdict=Verdict.CONFIRMED,
                possibility_id="hop-0",
                reviewer="ana",
                created_at=T0,
            )
        )
    if orphan:
        store.add_feedback(
            Feedback(
                id="fb-orphan",
                incident_id="gone",
                verdict=Verdict.REJECTED,
                reviewer="ana",
                created_at=T0,
            )
        )
    store.close()


def _statuses(tmp_path: Path) -> set[TrainingStatus]:
    store = SqliteDecisionStore(tmp_path / "fb.sqlite3")
    try:
        return {f.status for f in store.list_feedback()}
    finally:
        store.close()


def _batch_dir(tmp_path: Path) -> Path:
    (batch,) = (tmp_path / "out").glob("batch-*")
    return batch


class Trainer:
    def __init__(self, code: int = 0) -> None:
        self.code = code
        self.calls: list[list[str]] = []

    def __call__(self, argv: Sequence[str]) -> int:
        self.calls.append(list(argv))
        return self.code


def test_export_writes_rows_manifest_and_marks(env: Path) -> None:
    _seed(env, orphan=True)
    out = io.StringIO()
    assert run_export(_config(env), out=out) == 0
    batch = _batch_dir(env)
    rows = [json.loads(line) for line in (batch / "train.jsonl").read_text().splitlines()]
    assert len(rows) == 2
    request = build_request(
        Dossier(
            incident_id="acme-0",
            system="acme",
            process_path=ProcessPath(hops=[ProcessHop(ref="api")]),
        )
    )
    assert rows[0] == {
        "state": request["state"],
        "questions": request["questions"],
        "expected": {"cause": "hop-0", "sufficient": True},
    }
    manifest = json.loads((batch / "manifest.json").read_text())
    assert manifest["batch_id"] == batch.name
    assert batch.name.startswith("batch-") and batch.name.endswith("Z")
    assert manifest["rows"] == 2
    assert manifest["skipped"] == 1
    assert manifest["feedback_ids"] == ["fb-acme-0", "fb-acme-1"]
    assert manifest["base_checkpoint"] == "base-ckpt"
    assert manifest["system"] is None
    store = SqliteDecisionStore(env / "fb.sqlite3")
    exported = store.list_feedback(TrainingStatus.EXPORTED)
    assert {f.batch_id for f in exported} == {batch.name}
    assert len(exported) == 2
    store.close()


def test_export_filters_by_system(env: Path) -> None:
    _seed(env, system="acme")
    _seed(env, system="other")
    assert run_export(_config(env), system="other", out=io.StringIO()) == 0
    manifest = json.loads((_batch_dir(env) / "manifest.json").read_text())
    assert manifest["system"] == "other"
    assert manifest["feedback_ids"] == ["fb-other-0", "fb-other-1"]


def test_export_with_nothing_pending_writes_no_batch(env: Path) -> None:
    _seed(env, count=0)
    out = io.StringIO()
    assert run_export(_config(env), out=out) == 0
    assert "nothing pending" in out.getvalue()
    assert not (env / "out").exists()


def test_memory_provider_is_refused(env: Path) -> None:
    out = io.StringIO()
    assert run_export(_config(env, provider="memory"), out=out) == 2
    assert "sqlite" in out.getvalue()
    assert run_train(_config(env, provider="memory"), out=out) == 2
    assert run_status(_config(env, provider="memory"), out=out) == 2


def test_train_runs_laya_train_and_marks_trained(env: Path) -> None:
    _seed(env)
    run_export(_config(env), out=io.StringIO())
    trainer = Trainer()
    out = io.StringIO()
    assert run_train(_config(env), runner=trainer, out=out) == 0
    batch = _batch_dir(env)
    assert trainer.calls == [
        [
            "laya-train",
            "--data",
            str(batch / "train.jsonl"),
            "--base",
            "base-ckpt",
            "--out",
            str(batch / "checkpoint"),
            "--epochs",
            "3",
        ]
    ]
    assert _statuses(env) == {TrainingStatus.TRAINED}
    assert "LAYA_EXTRA_MODELS=" in out.getvalue()
    assert "laya-serve" in out.getvalue()


def test_dry_run_passes_flag_and_marks_nothing(env: Path) -> None:
    _seed(env)
    run_export(_config(env), out=io.StringIO())
    trainer = Trainer()
    assert run_train(_config(env), dry_run=True, runner=trainer, out=io.StringIO()) == 0
    assert trainer.calls[0][-1] == "--dry-run"
    assert _statuses(env) == {TrainingStatus.EXPORTED}


def test_failed_training_keeps_exported_and_propagates_code(env: Path) -> None:
    _seed(env)
    run_export(_config(env), out=io.StringIO())
    assert run_train(_config(env), runner=Trainer(code=3), out=io.StringIO()) == 3
    assert _statuses(env) == {TrainingStatus.EXPORTED}


def test_train_refuses_below_min_feedback(env: Path) -> None:
    _seed(env)
    run_export(_config(env), out=io.StringIO())
    trainer = Trainer()
    out = io.StringIO()
    assert run_train(_config(env, min_feedback=5), runner=trainer, out=out) == 2
    assert "min_feedback" in out.getvalue()
    assert trainer.calls == []


def test_train_refuses_without_base_checkpoint(env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed(env)
    run_export(_config(env), out=io.StringIO())
    monkeypatch.delenv("FIRE_LAYA_BASE_CHECKPOINT")
    out = io.StringIO()
    assert run_train(_config(env), runner=Trainer(), out=out) == 2
    assert "FIRE_LAYA_BASE_CHECKPOINT" in out.getvalue()


def test_train_without_a_batch_is_refused(env: Path) -> None:
    _seed(env)
    assert run_train(_config(env), runner=Trainer(), out=io.StringIO()) == 2


def test_missing_trainer_command_mentions_pip_install(
    env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(env)
    run_export(_config(env), out=io.StringIO())
    monkeypatch.setattr("fire.apps.retrain.runner.shutil.which", lambda _name: None)
    out = io.StringIO()
    assert run_train(_config(env), out=out) == 2
    assert "pip install laya" in out.getvalue()


def test_status_counts_and_lists_batches(env: Path) -> None:
    _seed(env)
    run_export(_config(env), out=io.StringIO())
    out = io.StringIO()
    assert run_status(_config(env), out=out) == 0
    text = out.getvalue()
    assert "pending: 0" in text
    assert "exported: 2" in text
    assert _batch_dir(env).name in text
