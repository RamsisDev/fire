"""Load ``.env`` into ``os.environ`` with process-env precedence.

Search order (first existing file wins):
1. Repo root (parent of ``fire/``, same level as ``config/`` and ``pyproject.toml``)
2. Current working directory

Real environment variables always win over file values
(``load_dotenv(..., override=False)``). Call early (app lifespan / ``__main__``)
so auth and wiring that read ``os.environ`` see the values.
"""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

# fire/apps/env.py -> parents[2] == repo root
_REPO_ROOT = Path(__file__).resolve().parents[2]


def load_fire_env(*, dotenv_path: Path | None = None) -> Path | None:
    """Load a ``.env`` file if present. Returns the path loaded, or ``None``.

    When ``dotenv_path`` is given (e.g. tests), only that path is considered.
    Otherwise prefers repo-root ``.env``, then cwd ``.env``.
    """
    if dotenv_path is not None:
        path = Path(dotenv_path)
        if path.is_file():
            load_dotenv(path, override=False)
            return path
        return None

    candidates = [_REPO_ROOT / ".env", Path.cwd() / ".env"]
    seen: set[Path] = set()
    for path in candidates:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if path.is_file():
            load_dotenv(path, override=False)
            return path
    return None
