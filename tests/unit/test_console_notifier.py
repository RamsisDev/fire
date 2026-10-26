"""Console notifier prints a header naming the incident, then the narration."""

from __future__ import annotations

import io

import pytest

from fire.adapters.notifiers.console import ConsoleNotifier
from fire.core.domain.incident import Incident, Trigger
from fire.core.domain.narration import Narration

INCIDENT = Incident(id="inc-7", system="example", trigger=Trigger.SHAPE_DRIFT)
NARRATION = Narration(incident_id="inc-7", channel_text="first line\nsecond line")
OUTPUT = (
    "[fire] notify incident=inc-7 system=example trigger=shape_drift\nfirst line\nsecond line\n\n"
)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_header_names_incident_system_and_trigger() -> None:
    stream = io.StringIO()
    await ConsoleNotifier(stream).publish(INCIDENT, NARRATION)
    assert stream.getvalue().splitlines()[0] == (
        "[fire] notify incident=inc-7 system=example trigger=shape_drift"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_header_of_a_correlated_incident_adds_its_trigger_count() -> None:
    incident = INCIDENT.model_copy(
        update={"triggers": [Trigger.SHAPE_DRIFT, Trigger.HTTP_ERRORS, Trigger.DLQ]}
    )
    stream = io.StringIO()
    await ConsoleNotifier(stream).publish(incident, NARRATION)
    assert stream.getvalue().splitlines()[0] == (
        "[fire] notify incident=inc-7 system=example trigger=shape_drift triggers=3"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_without_a_stream_writes_to_stdout_current_at_publish(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with capsys.disabled():
        notifier = ConsoleNotifier()
    await notifier.publish(INCIDENT, NARRATION)
    assert capsys.readouterr().out == OUTPUT


@pytest.mark.unit
@pytest.mark.asyncio
async def test_explicit_stream_receives_the_output(capsys: pytest.CaptureFixture[str]) -> None:
    stream = io.StringIO()
    await ConsoleNotifier(stream).publish(INCIDENT, NARRATION)
    assert stream.getvalue() == OUTPUT
    assert capsys.readouterr().out == ""
