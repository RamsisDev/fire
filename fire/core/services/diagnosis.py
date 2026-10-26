"""Diagnosis pipeline: dossier → Reasoner → Narrator → Notifier, with each decision recorded for
feedback when a `FeedbackService` is given.
"""

from __future__ import annotations

import logging

from fire.core.domain.decision import Decision
from fire.core.domain.dossier import Dossier
from fire.core.domain.incident import Incident
from fire.core.domain.narration import Narration
from fire.core.ports.narrator import Narrator
from fire.core.ports.notifier import Notifier
from fire.core.ports.reasoner import Reasoner
from fire.core.services.feedback import FeedbackService

logger = logging.getLogger(__name__)


class DiagnosisService:
    """Records the decision under `provider` (the reasoner's class name by default) and `model`
    before narrating it. A decision that fails to record is logged and still narrated.
    """

    def __init__(
        self,
        reasoner: Reasoner,
        narrator: Narrator,
        notifier: Notifier,
        *,
        feedback: FeedbackService | None = None,
        provider: str | None = None,
        model: str | None = None,
    ) -> None:
        self._reasoner = reasoner
        self._narrator = narrator
        self._notifier = notifier
        self._feedback = feedback
        self._provider = provider or type(reasoner).__name__
        self._model = model

    async def run(self, incident: Incident, dossier: Dossier) -> tuple[Decision, Narration]:
        decision = await self._reasoner.decide(dossier)
        if self._feedback is not None:
            try:
                self._feedback.record_decision(
                    incident.system, dossier, decision, self._provider, self._model
                )
            except Exception:
                logger.warning("decision for incident %s not recorded", incident.id, exc_info=True)
        narration = await self._narrator.narrate(dossier, decision)
        await self._notifier.publish(incident, narration)
        return decision, narration
