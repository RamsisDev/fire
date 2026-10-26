"""Port: Notifier — delivers narration to a channel (console/Slack)."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from fire.core.domain.incident import Incident
from fire.core.domain.narration import Narration


@runtime_checkable
class Notifier(Protocol):
    async def publish(self, incident: Incident, narration: Narration) -> None: ...
