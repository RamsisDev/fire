"""python -m fire.apps.tentacle --system <system> [--source <id>] [--start latest|oldest]"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence

from fire.apps.env import load_fire_env
from fire.apps.tentacle.runner import run_tentacle
from fire.core.ports.record_source import StartPosition


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m fire.apps.tentacle",
        description="Read a watched system's sources and send their shapes to the body.",
    )
    parser.add_argument("--system", required=True, help="config/systems/<system>.yml to load")
    parser.add_argument("--source", help="run only this source id")
    parser.add_argument(
        "--start",
        choices=[position.value for position in StartPosition],
        default=StartPosition.LATEST.value,
        help="where live sources start (default: latest)",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    # One line per request would drown the counters.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    load_fire_env()
    return run_tentacle(args.system, source=args.source, start=StartPosition(args.start))


if __name__ == "__main__":
    sys.exit(main())
