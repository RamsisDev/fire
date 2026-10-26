"""python -m fire.apps.retrain export|train|status"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from pathlib import Path

from fire.apps.env import load_fire_env
from fire.apps.incident_engine.wiring import DEFAULT_CONFIG
from fire.apps.retrain.runner import run_export, run_status, run_train


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m fire.apps.retrain",
        description="Export pending feedback as Laya training data and retrain on it.",
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="fire.yml to load")
    commands = parser.add_subparsers(dest="command", required=True)
    export = commands.add_parser("export", help="write pending feedback to a new batch")
    export.add_argument("--system", help="export only this system's feedback")
    train = commands.add_parser("train", help="run the trainer on a batch")
    train.add_argument("--batch", help="batch id; default the latest exported batch")
    train.add_argument("--dry-run", action="store_true", help="pass --dry-run; marks nothing")
    commands.add_parser("status", help="feedback counts and batches on disk")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    load_fire_env()
    if args.command == "export":
        return run_export(args.config, system=args.system)
    if args.command == "train":
        return run_train(args.config, batch=args.batch, dry_run=args.dry_run)
    return run_status(args.config)


if __name__ == "__main__":
    sys.exit(main())
