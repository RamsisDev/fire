"""Port: Narrator — formats Decision + safe fragments into channel prose."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from fire.core.domain.decision import Decision
from fire.core.domain.dossier import Dossier
from fire.core.domain.narration import Narration


@runtime_checkable
class Narrator(Protocol):
    """Must not invent cause, raise confidence, or read raw payload values."""

    async def narrate(self, dossier: Dossier, decision: Decision) -> Narration: ...
