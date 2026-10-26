"""DiagnosisService wires Reasoner → Narrator → Notifier."""

from __future__ import annotations

import pytest

from fire.adapters.narrators.template import TemplateNarrator
from fire.adapters.notifiers.console import ConsoleNotifier
from fire.adapters.reasoners.echo import EchoReasoner
from fire.core.domain.dossier import (
    Dossier,
    EvidenceFragment,
    EvidenceQuality,
    ProcessHop,
    ProcessPath,
    ShapeFingerprint,
)
from fire.core.domain.incident import Incident, Trigger
from fire.core.services.diagnosis import DiagnosisService


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pipeline_runs(capsys: pytest.CaptureFixture[str]) -> None:
    service = DiagnosisService(
        reasoner=EchoReasoner(),
        narrator=TemplateNarrator(),
        notifier=ConsoleNotifier(),
    )
    incident = Incident(id="inc-2", system="example", trigger=Trigger.MANUAL, service="api")
    dossier = Dossier(
        incident_id="inc-2",
        system="example",
        service="api",
        fingerprint=ShapeFingerprint(schema_hash="xyz"),
        process_path=ProcessPath(hops=[ProcessHop(ref="api")]),
    )
    decision, narration = await service.run(incident, dossier)
    assert decision.incident_id == "inc-2"
    assert "inc-2" in narration.channel_text
    captured = capsys.readouterr()
    assert "inc-2" in captured.out


@pytest.mark.unit
@pytest.mark.asyncio
async def test_shape_drift_dossier_prints_stream_and_changed_paths(
    capsys: pytest.CaptureFixture[str],
) -> None:
    stream = "http:inbound:POST /v1/orders/checkout:request"
    service = DiagnosisService(
        reasoner=EchoReasoner(),
        narrator=TemplateNarrator(),
        notifier=ConsoleNotifier(),
    )
    incident = Incident(id="inc-3", system="example", trigger=Trigger.SHAPE_DRIFT)
    dossier = Dossier(
        incident_id="inc-3",
        system="example",
        fragments=[
            EvidenceFragment(
                collector="shape_drift",
                quality=EvidenceQuality.PRESENT,
                summary=f"Shape drift on {stream}: added=0 type_changed=1 removed=1",
                data={
                    "source": "api-proxy",
                    "stream": stream,
                    "schema_hash": "c47d",
                    "diff": {
                        "added": [],
                        "type_changed": [
                            {"path": "referenceOrderId", "old": ["string"], "new": ["integer"]}
                        ],
                        "removed": [{"path": "timezone", "value_type": "string"}],
                    },
                    "occurrences": 1,
                    "first_seen": "2026-10-11T16:30:21Z",
                },
            ),
            EvidenceFragment(
                collector="process_path",
                quality=EvidenceQuality.MISSING,
                summary="No complete flow",
            ),
        ],
    )
    decision, _ = await service.run(incident, dossier)
    assert [p.id for p in decision.possibilities] == ["drift-0", "insufficient-evidence"]
    out = capsys.readouterr().out
    assert out.startswith("[fire] notify incident=inc-3 system=example trigger=shape_drift\n")
    assert stream in out
    assert "referenceOrderId changed string -> integer" in out
    assert "timezone (string) removed" in out
