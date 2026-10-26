"""The retrain commands. Exit codes: 0 done (or nothing to do), 2 refused, else the trainer's."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import sys
from collections import Counter
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO

from fire.adapters.trainers.laya_dataset import training_row
from fire.apps.config import FireConfig, load_fire_config
from fire.apps.incident_engine.wiring import build_decision_store
from fire.core.domain.feedback import TrainingStatus
from fire.core.ports.decision_store import DecisionStore

DEFAULT_OUTPUT_DIR = "var/retrain"

# Runs the trainer's argv and returns its exit code.
CommandRunner = Callable[[Sequence[str]], int]


def _run_command(argv: Sequence[str]) -> int:
    # The resolved path, not the bare name: on Windows only `which` honours PATHEXT (.cmd, .bat).
    command, *args = argv
    return subprocess.run([shutil.which(command) or command, *args], check=False).returncode


def _output_dir(config: FireConfig) -> Path:
    return Path(os.environ.get(config.retraining.output_dir_env) or DEFAULT_OUTPUT_DIR)


def _batches(root: Path) -> list[str]:
    return sorted(path.parent.name for path in root.glob("batch-*/manifest.json"))


def _open_store(config: FireConfig, out: TextIO) -> DecisionStore | None:
    if config.feedback.provider == "memory":
        print(
            "feedback.provider is memory: a memory store is empty in this process. "
            "Retraining needs feedback.provider: sqlite (and FIRE_FEEDBACK_PATH).",
            file=out,
        )
        return None
    return build_decision_store(config.feedback)


def run_export(config_path: Path, *, system: str | None = None, out: TextIO = sys.stdout) -> int:
    config = load_fire_config(config_path)
    store = _open_store(config, out)
    if store is None:
        return 2
    try:
        pending = store.list_feedback(TrainingStatus.PENDING, system=system)
        if not pending:
            print("nothing pending: no batch written", file=out)
            return 0
        now = datetime.now(UTC)
        batch_id = f"batch-{now:%Y%m%dT%H%M%SZ}"
        batch_dir = _output_dir(config) / batch_id
        rows: list[str] = []
        used: list[str] = []
        skipped = 0
        for feedback in pending:
            row = training_row(store.get_decision(feedback.incident_id), feedback)
            if row is None:
                skipped += 1
                continue
            rows.append(json.dumps(row, ensure_ascii=False, sort_keys=True))
            used.append(feedback.id)
        if not rows:
            print(f"{skipped} pending rows, none usable: no batch written", file=out)
            return 0
        batch_dir.mkdir(parents=True, exist_ok=True)
        (batch_dir / "train.jsonl").write_text("\n".join(rows) + "\n", encoding="utf-8")
        manifest = {
            "batch_id": batch_id,
            "created_at": now.isoformat(),
            "system": system,
            "rows": len(rows),
            "skipped": skipped,
            "feedback_ids": used,
            "base_checkpoint": os.environ.get(config.retraining.base_checkpoint_env) or None,
        }
        (batch_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )
        # Marked after the files exist, so a crash leaves rows pending, never lost.
        store.mark(used, TrainingStatus.EXPORTED, batch_id)
        print(f"{batch_id}: {len(rows)} rows, {skipped} skipped -> {batch_dir}", file=out)
        return 0
    finally:
        store.close()


def _latest_untrained(store: DecisionStore, root: Path) -> str | None:
    """The newest batch on disk that still has exported feedback."""
    exported = {feedback.batch_id for feedback in store.list_feedback(TrainingStatus.EXPORTED)}
    return next((name for name in reversed(_batches(root)) if name in exported), None)


def run_train(
    config_path: Path,
    *,
    batch: str | None = None,
    dry_run: bool = False,
    runner: CommandRunner = _run_command,
    out: TextIO = sys.stdout,
) -> int:
    config = load_fire_config(config_path)
    retraining = config.retraining
    store = _open_store(config, out)
    if store is None:
        return 2
    try:
        root = _output_dir(config)
        batch_id = batch or _latest_untrained(store, root)
        if batch_id is None:
            print("no exported batch: run `export` first", file=out)
            return 2
        batch_dir = root / batch_id
        try:
            manifest = json.loads((batch_dir / "manifest.json").read_text(encoding="utf-8"))
        except OSError:
            print(f"no manifest for batch {batch_id} in {root}", file=out)
            return 2
        if manifest["rows"] < retraining.min_feedback:
            print(
                f"{batch_id} has {manifest['rows']} rows; retraining.min_feedback is "
                f"{retraining.min_feedback}. Collect more feedback first.",
                file=out,
            )
            return 2
        base = os.environ.get(retraining.base_checkpoint_env)
        if not base:
            print(
                f"set {retraining.base_checkpoint_env} to the checkpoint to start from", file=out
            )
            return 2
        if runner is _run_command and shutil.which(retraining.train_command) is None:
            print(
                f"{retraining.train_command!r} not found on PATH. Laya is not a FIRE "
                "dependency: `pip install laya`, or set retraining.train_command.",
                file=out,
            )
            return 2
        checkpoint = batch_dir / "checkpoint"
        argv = [
            retraining.train_command,
            "--data",
            str(batch_dir / "train.jsonl"),
            "--base",
            base,
            "--out",
            str(checkpoint),
            "--epochs",
            str(retraining.epochs),
        ]
        if dry_run:
            argv.append("--dry-run")
        print("running: " + shlex.join(argv), file=out)
        code = runner(argv)
        if code != 0:
            print(f"trainer exited {code}; feedback stays exported", file=out)
            return code
        if dry_run:
            print("dry run: nothing marked", file=out)
            return 0
        store.mark(manifest["feedback_ids"], TrainingStatus.TRAINED, batch_id)
        models = json.dumps({"fire": str(checkpoint)})
        print(f"trained {batch_id} -> {checkpoint}", file=out)
        print("serve it with:", file=out)
        print(f"  LAYA_EXTRA_MODELS='{models}' laya-serve", file=out)
        print("then set `reasoner.model: fire` in config/fire.yml; for the next round:", file=out)
        print(f"  {retraining.base_checkpoint_env}={checkpoint}", file=out)
        return 0
    finally:
        store.close()


def run_status(config_path: Path, *, out: TextIO = sys.stdout) -> int:
    config = load_fire_config(config_path)
    store = _open_store(config, out)
    if store is None:
        return 2
    try:
        counts = Counter(feedback.status for feedback in store.list_feedback())
        for status in TrainingStatus:
            print(f"{status.value}: {counts[status]}", file=out)
        root = _output_dir(config)
        names = _batches(root)
        print(f"batches in {root}: {', '.join(names) if names else 'none'}", file=out)
        return 0
    finally:
        store.close()
