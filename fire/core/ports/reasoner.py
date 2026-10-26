"""Port: System One reasoner — decides possibilities; does not write Slack prose."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from fire.core.domain.decision import Decision
from fire.core.domain.dossier import Dossier


@runtime_checkable
class Reasoner(Protocol):
    """System One: the echo stub today, a System One reasoner adapter over HTTP next. Happy
    path is not a generative LLM.
    """

    async def decide(self, dossier: Dossier) -> Decision: ...
