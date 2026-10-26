"""Prove process env wins over .env; file fills gaps when unset."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from fire.apps.env import load_fire_env


@pytest.mark.unit
def test_process_env_wins_over_dotenv_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text("FIRE_API_TOKEN=from-file\n", encoding="utf-8")
    monkeypatch.setenv("FIRE_API_TOKEN", "from-env")

    loaded = load_fire_env(dotenv_path=dotenv)

    assert loaded == dotenv
    assert os.environ["FIRE_API_TOKEN"] == "from-env"


@pytest.mark.unit
def test_dotenv_file_used_when_env_unset(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text("FIRE_API_TOKEN=from-file\n", encoding="utf-8")
    monkeypatch.delenv("FIRE_API_TOKEN", raising=False)

    loaded = load_fire_env(dotenv_path=dotenv)

    assert loaded == dotenv
    assert os.environ["FIRE_API_TOKEN"] == "from-file"
