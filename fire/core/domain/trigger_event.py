"""Trigger events: what a trigger raised, with its evidence, before an incident is opened for it.

Contract: docs/specs/dossier-and-decision.md#trigger-events.
"""

from __future__ import annotations

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from fire.core.domain.dossier import EvidenceFragment, ProcessPath, Shape, ShapeFingerprint
from fire.core.domain.incident import Trigger


class TriggerEvent(BaseModel):
    """One trigger firing for one system: the incident fields it names and the dossier evidence
    it gathered. No payload fields.
    """

    model_config = ConfigDict(extra="forbid")

    system: str
    trigger: Trigger
    summary: str
    started_at: AwareDatetime
    service: str | None = None
    shape: Shape | None = None
    fingerprint: ShapeFingerprint | None = None
    process_path: ProcessPath = Field(default_factory=ProcessPath)
    fragments: list[EvidenceFragment] = Field(default_factory=list)
