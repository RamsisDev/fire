"""python -m fire.apps.incident_engine"""

from __future__ import annotations

import os

import uvicorn

from fire.apps.env import load_fire_env


def main() -> None:
    load_fire_env()
    uvicorn.run(
        "fire.apps.incident_engine.app:app",
        host=os.environ.get("FIRE_HOST", "127.0.0.1"),
        port=int(os.environ.get("FIRE_PORT", "8000")),
        reload=False,
    )


if __name__ == "__main__":
    main()
