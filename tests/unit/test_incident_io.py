"""incident.io incidents: mapping rules, reading webhook bodies, and raising trigger events.

The bodies are incident.io's documented samples (tests/fixtures/incident_io/), with every free
text replaced by a `fixture-` placeholder.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from fire.core.domain.dossier import EvidenceQuality
from fire.core.domain.incident import Trigger
from fire.core.domain.incident_io import (
    IncidentIoIncident,
    IncidentIoMatch,
    IncidentIoOutcome,
    IncidentIoRule,
    IncidentIoTrigger,
    match_incident,
    read_event,
)
from fire.core.services.incident_io import IncidentIoService

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "incident_io"
CREATED = "public_incident.incident_created_v2"
UPDATED = "public_incident.incident_updated_v2"
STATUS_UPDATED = "public_incident.incident_status_updated_v2"
# The ids of the documented samples.
INCIDENT_ID = "01FDAG4SAP5TYPT98WGR2N7W91"
FIELD_ID = "01FCNDV6P870EA6S7TK1DSYDG0"
EXTERNAL_ID = "761722cd-d1d7-477b-ac7e-90f9e079dc33"
TEAM_ID = "01JPQA75EPNEES4479P16P4XAB"
TYPE_ID = "01FCNDV6P870EA6S7TK1DSYDG0"
OTHER = "01ZZZZZZZZZZZZZZZZZZZZZZZZ"
NOW = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)


def _sample(event_type: str) -> dict[str, Any]:
    return json.loads((FIXTURES / f"{event_type}.json").read_text(encoding="utf-8"))


def _body(document: dict[str, Any]) -> bytes:
    return json.dumps(document).encode()


def _incident(document: dict[str, Any]) -> dict[str, Any]:
    event_type = document["event_type"]
    data = document[event_type]
    return data["incident"] if event_type == STATUS_UPDATED else data


def _with_field_value(
    *, option: str | None = None, entry: str | None = None, external: str | None = None
) -> IncidentIoIncident:
    """The created sample, with one value in its custom field holding these ids."""
    document = _sample(CREATED)
    [field_entry] = _incident(document)["custom_field_entries"]
    [value] = field_entry["values"]
    value["value_option"] = {"id": option, "custom_field_id": FIELD_ID} if option else None
    value["value_catalog_entry"] = (
        {"id": entry, "external_id": external, "name": "fixture-name"} if entry else None
    )
    return IncidentIoIncident.model_validate(_incident(document))


def _trigger(*rules: dict[str, Any]) -> IncidentIoTrigger:
    return IncidentIoTrigger.model_validate({"match": list(rules)})


def _service(triggers: dict[str, IncidentIoTrigger]) -> IncidentIoService:
    return IncidentIoService(triggers, clock=lambda: NOW)


BY_TEAM = _trigger({"team_id": TEAM_ID})


@pytest.mark.unit
@pytest.mark.parametrize(
    "rule",
    [
        {},
        {"team_id": TEAM_ID, "incident_type_id": TYPE_ID},
        {"custom_field_id": FIELD_ID, "values": [OTHER], "team_id": TEAM_ID},
        {"team_id": TEAM_ID, "values": [OTHER]},
        {"custom_field_id": FIELD_ID},
        {"custom_field_id": FIELD_ID, "values": []},
        {"custom_field_id": FIELD_ID, "values": [OTHER, OTHER]},
        {"team_id": "two words"},
        {"team_id": ""},
        {"service_id": OTHER},
    ],
    ids=[
        "no-selector",
        "two-selectors",
        "field-and-team",
        "values-without-field",
        "field-without-values",
        "field-with-empty-values",
        "repeated-value",
        "whitespace",
        "empty",
        "unknown-key",
    ],
)
def test_a_rule_sets_one_selector(rule: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        IncidentIoRule.model_validate(rule)


@pytest.mark.unit
def test_a_trigger_needs_a_rule_and_rejects_unknown_keys() -> None:
    with pytest.raises(ValidationError):
        IncidentIoTrigger.model_validate({"match": []})
    with pytest.raises(ValidationError):
        IncidentIoTrigger.model_validate({"match": [{"team_id": TEAM_ID}], "events": []})


@pytest.mark.unit
@pytest.mark.parametrize(
    ("ids", "value"),
    [
        ({"option": "01OPTION00000000000000000A"}, "01OPTION00000000000000000A"),
        ({"entry": "01ENTRY000000000000000000A"}, "01ENTRY000000000000000000A"),
        ({"entry": "01ENTRY000000000000000000A", "external": "acme"}, "acme"),
    ],
    ids=["option", "catalog-entry", "external-id"],
)
def test_a_field_rule_matches_an_option_a_catalog_entry_or_its_external_id(
    ids: dict[str, str], value: str
) -> None:
    trigger = _trigger({"custom_field_id": FIELD_ID, "values": [OTHER, value]})
    assert match_incident(trigger, _with_field_value(**ids)) == IncidentIoMatch(
        0, "custom_field", value, FIELD_ID
    )


@pytest.mark.unit
def test_a_field_rule_reads_only_its_own_field() -> None:
    trigger = _trigger({"custom_field_id": OTHER, "values": [EXTERNAL_ID]})
    assert match_incident(trigger, _with_field_value(entry=FIELD_ID, external=EXTERNAL_ID)) is None


@pytest.mark.unit
def test_team_and_incident_type_rules_match() -> None:
    incident = IncidentIoIncident.model_validate(_incident(_sample(CREATED)))
    assert match_incident(BY_TEAM, incident) == IncidentIoMatch(0, "team", TEAM_ID)
    assert match_incident(_trigger({"incident_type_id": TYPE_ID}), incident) == IncidentIoMatch(
        0, "incident_type", TYPE_ID
    )
    assert match_incident(_trigger({"incident_type_id": OTHER}), incident) is None


@pytest.mark.unit
def test_the_first_rule_that_matches_decides() -> None:
    incident = IncidentIoIncident.model_validate(_incident(_sample(CREATED)))
    trigger = _trigger(
        {"team_id": OTHER},
        {"custom_field_id": FIELD_ID, "values": [EXTERNAL_ID]},
        {"team_id": TEAM_ID},
    )
    assert match_incident(trigger, incident) == IncidentIoMatch(
        1, "custom_field", EXTERNAL_ID, FIELD_ID
    )


@pytest.mark.unit
@pytest.mark.parametrize("event_type", [CREATED, UPDATED, STATUS_UPDATED])
def test_each_incident_event_reads_the_incident_and_no_free_text(event_type: str) -> None:
    event = read_event(_body(_sample(event_type)))
    assert not isinstance(event, IncidentIoOutcome)
    assert event.event_type == event_type
    incident = event.incident
    assert (incident.id, incident.reference, incident.visibility) == (
        INCIDENT_ID,
        "INC-123",
        "public",
    )
    assert incident.team_ids == [TEAM_ID]
    assert incident.field_value_ids(FIELD_ID) == {FIELD_ID, EXTERNAL_ID}
    assert "fixture-" not in incident.model_dump_json()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("body", "outcome"),
    [
        (_body(_sample("private_incident.incident_created_v2")), IncidentIoOutcome.PRIVATE),
        (_body(_sample("private_incident.incident_updated_v2")), IncidentIoOutcome.PRIVATE),
        (
            _body(_sample("public_incident.follow_up_created_v2")),
            IncidentIoOutcome.IGNORED_EVENT_TYPE,
        ),
        (b'{"event_type": "ping"}', IncidentIoOutcome.IGNORED_EVENT_TYPE),
        (b"not json", IncidentIoOutcome.MALFORMED),
        (b"\xff", IncidentIoOutcome.MALFORMED),
        (b"[" * 100_000, IncidentIoOutcome.MALFORMED),
        (b"[]", IncidentIoOutcome.MALFORMED),
        (b'{"event_type": 1}', IncidentIoOutcome.MALFORMED),
        (_body({"event_type": CREATED}), IncidentIoOutcome.MALFORMED),
        (_body({"event_type": STATUS_UPDATED, STATUS_UPDATED: {}}), IncidentIoOutcome.MALFORMED),
    ],
    ids=[
        "private-created",
        "private-updated",
        "follow-up",
        "unknown-type",
        "not-json",
        "not-utf8",
        "nested-too-deeply",
        "not-an-object",
        "event-type-not-a-string",
        "no-incident",
        "status-update-without-incident",
    ],
)
def test_bodies_without_a_public_incident_give_their_outcome(
    body: bytes, outcome: IncidentIoOutcome
) -> None:
    assert read_event(body) is outcome


@pytest.mark.unit
def test_an_incident_without_its_id_or_with_a_sentence_for_an_id_is_malformed() -> None:
    document = _sample(CREATED)
    del _incident(document)["id"]
    assert read_event(_body(document)) is IncidentIoOutcome.MALFORMED
    document = _sample(CREATED)
    _incident(document)["reference"] = "fixture reference"
    assert read_event(_body(document)) is IncidentIoOutcome.MALFORMED


@pytest.mark.unit
def test_a_private_incident_in_a_public_event_is_private() -> None:
    document = _sample(UPDATED)
    _incident(document)["visibility"] = "private"
    assert read_event(_body(document)) is IncidentIoOutcome.PRIVATE


@pytest.mark.unit
def test_a_mapped_incident_raises_one_incident_io_event() -> None:
    service = _service({"acme": _trigger({"custom_field_id": FIELD_ID, "values": [EXTERNAL_ID]})})
    receipt = service.receive(_body(_sample(CREATED)))
    assert receipt.outcome is IncidentIoOutcome.OPENED
    assert (receipt.event_type, receipt.incident_id) == (CREATED, INCIDENT_ID)
    [event] = receipt.events
    assert event.system == "acme"
    assert event.trigger is Trigger.INCIDENT_IO
    assert event.summary == "incident.io incident INC-123"
    assert event.service is None
    assert event.started_at == datetime(2021, 8, 17, 13, 28, 57, 801578, tzinfo=UTC)
    assert (event.shape, event.fingerprint) == (None, None)
    assert event.process_path.hops == []
    fragment, process_path, shape = event.fragments
    assert (fragment.collector, fragment.quality) == ("incident_io", EvidenceQuality.PRESENT)
    assert fragment.summary == "incident.io incident INC-123: status=triage severity_rank=1"
    assert fragment.data == {
        "event_type": CREATED,
        "incident_id": INCIDENT_ID,
        "reference": "INC-123",
        "mode": "standard",
        "created_at": "2021-08-17T13:28:57.801578+00:00",
        "status_id": "01FCNDV6P870EA6S7TK1DSYD5H",
        "status_category": "triage",
        "severity_id": "01FCNDV6P870EA6S7TK1DSYDG0",
        "severity_rank": 1,
        "incident_type_id": TYPE_ID,
        "team_ids": [TEAM_ID],
        "matched": {
            "rule": 0,
            "by": "custom_field",
            "value": EXTERNAL_ID,
            "custom_field_id": FIELD_ID,
        },
    }
    assert [(f.collector, f.quality, f.summary) for f in (process_path, shape)] == [
        ("process_path", EvidenceQuality.MISSING, "No flow linked to the incident.io incident"),
        ("shape", EvidenceQuality.MISSING, "No shape linked to the incident.io incident"),
    ]
    assert "fixture-" not in event.model_dump_json()


@pytest.mark.unit
def test_one_incident_io_incident_raises_one_event_per_system() -> None:
    service = _service({"acme": BY_TEAM})
    assert service.receive(_body(_sample(CREATED))).outcome is IncidentIoOutcome.OPENED
    for event_type in (CREATED, UPDATED, STATUS_UPDATED):
        receipt = service.receive(_body(_sample(event_type)))
        assert (receipt.outcome, receipt.events) == (IncidentIoOutcome.DUPLICATE, [])
    assert service.counts == {IncidentIoOutcome.OPENED: 1, IncidentIoOutcome.DUPLICATE: 3}


@pytest.mark.unit
def test_an_update_that_first_maps_the_incident_raises_it() -> None:
    service = _service({"acme": _trigger({"incident_type_id": TYPE_ID})})
    created = _sample(CREATED)
    _incident(created)["incident_type"]["id"] = OTHER
    assert service.receive(_body(created)).outcome is IncidentIoOutcome.UNMAPPED
    receipt = service.receive(_body(_sample(STATUS_UPDATED)))
    assert receipt.outcome is IncidentIoOutcome.OPENED
    [event] = receipt.events
    assert event.fragments[0].data["event_type"] == STATUS_UPDATED
    assert event.fragments[0].data["matched"] == {
        "rule": 0,
        "by": "incident_type",
        "value": TYPE_ID,
    }


@pytest.mark.unit
def test_an_incident_of_two_systems_raises_one_for_each_and_dedupes_each() -> None:
    by_field = _trigger({"custom_field_id": FIELD_ID, "values": [EXTERNAL_ID]})
    both = _service({"acme": BY_TEAM, "billing": by_field})
    receipt = both.receive(_body(_sample(CREATED)))
    assert receipt.outcome is IncidentIoOutcome.OPENED
    assert [event.system for event in receipt.events] == ["acme", "billing"]
    assert both.receive(_body(_sample(UPDATED))).outcome is IncidentIoOutcome.DUPLICATE

    service = _service({"acme": BY_TEAM, "billing": by_field})
    created = _sample(CREATED)
    _incident(created)["custom_field_entries"] = []
    assert [event.system for event in service.receive(_body(created)).events] == ["acme"]
    receipt = service.receive(_body(_sample(UPDATED)))
    assert receipt.outcome is IncidentIoOutcome.OPENED
    assert [event.system for event in receipt.events] == ["billing"]


@pytest.mark.unit
def test_an_unmapped_incident_and_other_bodies_are_counted_and_raise_nothing() -> None:
    service = _service({"acme": _trigger({"team_id": OTHER})})
    bodies = [
        _body(_sample(CREATED)),
        _body(_sample("private_incident.incident_created_v2")),
        _body(_sample("public_incident.follow_up_created_v2")),
        b"not json",
    ]
    receipts = [service.receive(body) for body in bodies]
    assert [receipt.events for receipt in receipts] == [[], [], [], []]
    assert receipts[0].incident_id == INCIDENT_ID
    assert receipts[1].incident_id is None
    assert service.counts == {
        IncidentIoOutcome.UNMAPPED: 1,
        IncidentIoOutcome.PRIVATE: 1,
        IncidentIoOutcome.IGNORED_EVENT_TYPE: 1,
        IncidentIoOutcome.MALFORMED: 1,
    }


@pytest.mark.unit
def test_an_incident_without_created_at_starts_on_the_clock() -> None:
    document = _sample(CREATED)
    del _incident(document)["created_at"]
    [event] = _service({"acme": BY_TEAM}).receive(_body(document)).events
    assert event.started_at == NOW
    assert event.fragments[0].data["created_at"] is None
