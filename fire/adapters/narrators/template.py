"""Template Narrator — formats Decision without calling an LLM."""

from __future__ import annotations

from fire.core.domain.decision import Decision
from fire.core.domain.dossier import Dossier
from fire.core.domain.narration import Narration


class TemplateNarrator:
    async def narrate(self, dossier: Dossier, decision: Decision) -> Narration:
        scope = dossier.system + (f"/{dossier.service}" if dossier.service else "")
        lines = [
            f"*FIRE ENGINE* incident `{decision.incident_id}` (`{scope}`)",
            f"Confidence: {decision.confidence:.0%}"
            + (" — `noul` (do not treat as settled cause)" if decision.noul else ""),
            "Possibilities:",
        ]
        for possibility in decision.possibilities:
            refs = ", ".join(possibility.rationale_refs) or "—"
            lines.append(f"• ({possibility.probability:.0%}) {possibility.label} _[{refs}]_")
        if len(dossier.triggers) > 1:
            lines.append(f"Triggers ({len(dossier.triggers)}):")
            lines.extend(
                f"• `{trigger.trigger}` {trigger.summary}" for trigger in dossier.triggers
            )
        if dossier.origin is not None:
            lines.append(
                f"Origin: `{dossier.origin.origin_id}` "
                f"({dossier.origin.kind}/{dossier.origin.endpoint_ref})"
            )
        path = " → ".join(hop.display_label() for hop in dossier.process_path.hops)
        if path:
            lines.append(f"Process path: `{path}`")
        if dossier.fingerprint is not None:
            lines.append(f"Shape hash: `{dossier.fingerprint.schema_hash}`")
        elif dossier.shape is not None:
            paths = ", ".join(f.path for f in dossier.shape.fields) or "—"
            lines.append(f"Shape fields: `{paths}` (no fingerprint yet)")
        lines.append("_Narrator: template stub (swap OpenAI-compat via FIRE_NARRATOR_*)._")
        return Narration(
            incident_id=decision.incident_id,
            channel_text="\n".join(lines),
            used_possibility_ids=[p.id for p in decision.possibilities],
        )
