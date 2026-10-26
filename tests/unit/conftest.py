"""Fixtures shared by unit tests."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

import fire.apps.incident_engine.wiring as wiring

CONFIG = Path(__file__).resolve().parents[2] / "config"


@pytest.fixture
def acme_fire_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The repo's `fire.yml` beside a `systems/` that holds the template as the system `acme`,
    and the default config of `build_container`, so the app's lifespan loads it too.
    """
    systems = tmp_path / "systems"
    systems.mkdir()
    shutil.copyfile(CONFIG / "systems" / "_template.yml", systems / "acme.yml")
    config = Path(shutil.copyfile(CONFIG / "fire.yml", tmp_path / "fire.yml"))
    monkeypatch.setattr(wiring, "DEFAULT_CONFIG", config)
    return config
