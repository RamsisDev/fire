"""Diagnosis pipeline: dossier → Reasoner → Narrator → Notifier."""

from __future__ import annotations

from fire.core.domain.decision import Decision
from fire.core.domain.dossier import Dossier
from fire.core.domain.incident import Incident
from fire.core.domain.narration import Narration
from fire.core.ports.narrator import Narrator
from fire.core.ports.notifier import Notifier
from fire.core.ports.reasoner import Reasoner


class DiagnosisService:
    def __init__(
        self,
        reasoner: Reasoner,
        narrator: Narrator,
        notifier: Notifier,
    ) -> None:
        self._reasoner = reasoner
        self._narrator = narrator
        self._notifier = notifier

    async def run(self, incident: Incident, dossier: Dossier) -> tuple[Decision, Narration]:
        decision = await self._reasoner.decide(dossier)
        narration = await self._narrator.narrate(dossier, decision)
        await self._notifier.publish(incident, narration)
        return decision, narration
