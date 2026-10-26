"""Incident correlator: decides which incident each trigger event belongs to, and hands out each
incident's merged dossier for diagnosis once per new set of triggers.

Contract: docs/specs/ingest-drift-and-feedback.md#correlation.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from fire.core.domain.correlation import AcceptedTrigger, CorrelationPolicy, correlated_incident
from fire.core.domain.dossier import Dossier
from fire.core.domain.incident import Incident, Trigger
from fire.core.domain.trigger_event import TriggerEvent


def _uuid4() -> str:
    return str(uuid.uuid4())


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class Correlation:
    """Where one trigger event went: its incident, whether it opened it, and how many triggers
    the incident holds with it.
    """

    incident_id: str
    system: str
    trigger: Trigger
    opened: bool
    triggers: int


@dataclass
class _Window:
    incident_id: str
    system: str
    ends_at: datetime
    triggers: list[AcceptedTrigger] = field(default_factory=list)
    diagnosed: int = 0


class IncidentCorrelator:
    """Opens one incident per system and window, on the body's `clock`.

    A system's first trigger event opens an incident and its window, `window_s` of its
    `triggers.correlation` from acceptance; its later events accepted before the window ends join
    that incident, and one accepted at its end or later opens the next. A system without a
    policy has no window: each event opens its own incident. Windows live in process memory.

    `take` returns an incident with every trigger it holds, for the caller to diagnose; `close`
    ends the windows that are over and returns, once each, the incidents that gained triggers
    since they were last taken. So every trigger is first diagnosed in exactly one snapshot, and
    no snapshot repeats the last one handed out for its incident.
    """

    def __init__(
        self,
        policies: Mapping[str, CorrelationPolicy],
        clock: Callable[[], datetime] | None = None,
        new_id: Callable[[], str] = _uuid4,
    ) -> None:
        self._windows = {
            system: timedelta(seconds=policy.window_s) for system, policy in policies.items()
        }
        self._clock = clock or _utc_now
        self._new_id = new_id
        self._incidents: dict[str, _Window] = {}
        self._open: dict[str, _Window] = {}
        self._lock = threading.Lock()

    def add(self, event: TriggerEvent) -> Correlation:
        with self._lock:
            now = self._clock()
            accepted = AcceptedTrigger(event=event, accepted_at=now)
            current = self._open.get(event.system)
            if current is not None and now < current.ends_at:
                current.triggers.append(accepted)
                return Correlation(
                    current.incident_id, event.system, event.trigger, False, len(current.triggers)
                )
            window = self._windows.get(event.system)
            opened = _Window(
                incident_id=self._new_id(),
                system=event.system,
                ends_at=now if window is None else now + window,
                triggers=[accepted],
            )
            self._incidents[opened.incident_id] = opened
            if window is not None:
                self._open[event.system] = opened
            return Correlation(opened.incident_id, event.system, event.trigger, True, 1)

    def take(self, incident_id: str) -> tuple[Incident, Dossier] | None:
        """The incident with all its triggers so far, or `None` once nothing is left to
        diagnose.
        """
        with self._lock:
            window = self._incidents.get(incident_id)
            if window is None or window.diagnosed == len(window.triggers):
                return None
            run = self._snapshot(window)
            if self._clock() >= window.ends_at:
                self._forget(window)
            return run

    def close(self) -> list[tuple[Incident, Dossier]]:
        """Ends every window that is over; returns the incidents among them with triggers not
        yet taken, in the order they opened.
        """
        with self._lock:
            now = self._clock()
            over = [window for window in self._incidents.values() if now >= window.ends_at]
            runs: list[tuple[Incident, Dossier]] = []
            for window in over:
                if window.diagnosed < len(window.triggers):
                    runs.append(self._snapshot(window))
                self._forget(window)
            return runs

    def _snapshot(self, window: _Window) -> tuple[Incident, Dossier]:
        window.diagnosed = len(window.triggers)
        return correlated_incident(window.incident_id, window.triggers)

    def _forget(self, window: _Window) -> None:
        del self._incidents[window.incident_id]
        if self._open.get(window.system) is window:
            del self._open[window.system]
