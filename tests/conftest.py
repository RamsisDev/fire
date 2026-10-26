"""Fixtures shared by every test."""

from __future__ import annotations

import logging
from collections.abc import Iterator

import pytest
from canary import Canary


@pytest.fixture
def canary() -> Canary:
    """Unique values to plant in records, and the checks that find leaks (tests/canary.py)."""
    return Canary()


@pytest.fixture
def quiet_httpx() -> Iterator[None]:
    """Holds the `httpx` logger at WARNING, as the tentacle's CLI does, so a test's own client
    logs no URL. `caplog.set_level(..., logger="httpx")` would also raise caplog's handler and
    hide every DEBUG and INFO line from the canary checks.
    """
    logger = logging.getLogger("httpx")
    level = logger.level
    logger.setLevel(logging.WARNING)
    yield
    logger.setLevel(level)
