"""Console notifier for local demos."""

from __future__ import annotations

import sys
from typing import TextIO

from fire.core.domain.incident import Incident
from fire.core.domain.narration import Narration


class ConsoleNotifier:
    """Prints each notification to `stream`, else to the `sys.stdout` current at publish. The
    header names the opening trigger, and the trigger count of a correlated incident.
    """

    def __init__(self, stream: TextIO | None = None) -> None:
        self._stream = stream

    async def publish(self, incident: Incident, narration: Narration) -> None:
        count = f" triggers={len(incident.triggers)}" if len(incident.triggers) > 1 else ""
        print(
            f"[fire] notify incident={incident.id} system={incident.system} "
            f"trigger={incident.trigger}{count}\n{narration.channel_text}\n",
            file=self._stream or sys.stdout,
        )
