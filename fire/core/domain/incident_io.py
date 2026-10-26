"""incident.io incidents: the ids FIRE reads from a webhook, and the rules that map one to a
system. Free text (names, summaries, text and link field values) is never read.

Contract: docs/specs/ingest-drift-and-feedback.md#incidentio-webhook.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

# Ids and enum values carried into a dossier: no whitespace, so no sentence fits.
Token = Annotated[str, Field(pattern=r"^\S{1,128}$")]

CREATED = "public_incident.incident_created_v2"
UPDATED = "public_incident.incident_updated_v2"
STATUS_UPDATED = "public_incident.incident_status_updated_v2"
# Where each handled event type holds the incident, under the key named after the event type.
INCIDENT_EVENTS: dict[str, tuple[str, ...]] = {
    CREATED: (),
    UPDATED: (),
    STATUS_UPDATED: ("incident",),
}
# A private incident's payload carries its id alone.
PRIVATE_EVENTS = frozenset(
    {"private_incident.incident_created_v2", "private_incident.incident_updated_v2"}
)


class IncidentIoOutcome(StrEnum):
    """What a verified webhook did."""

    OPENED = "opened"
    DUPLICATE = "duplicate"
    UNMAPPED = "unmapped"
    PRIVATE = "private"
    IGNORED_EVENT_TYPE = "ignored_event_type"
    MALFORMED = "malformed"


class IncidentIoRule(BaseModel):
    """One way an incident.io incident names a system: exactly one of a custom field with its
    `values`, a team, or an incident type.
    """

    model_config = ConfigDict(extra="forbid")

    # A select or catalog field, such as Affected services. A value is an option id, a catalog
    # entry id, or a catalog entry's external id.
    custom_field_id: Token | None = None
    values: list[Token] = Field(default_factory=list)
    team_id: Token | None = None
    incident_type_id: Token | None = None

    @model_validator(mode="after")
    def _one_selector(self) -> IncidentIoRule:
        selectors = [self.custom_field_id, self.team_id, self.incident_type_id]
        if sum(selector is not None for selector in selectors) != 1:
            raise ValueError(
                "a rule sets exactly one of custom_field_id, team_id, incident_type_id"
            )
        if (self.custom_field_id is None) != (not self.values):
            raise ValueError("values goes with custom_field_id, and custom_field_id needs values")
        repeated = sorted({value for value in self.values if self.values.count(value) > 1})
        if repeated:
            raise ValueError(f"values listed twice: {', '.join(repeated)}")
        return self


class IncidentIoTrigger(BaseModel):
    """The system YAML's `triggers.incident_io`: an incident belongs to the system when any
    rule matches.
    """

    model_config = ConfigDict(extra="forbid")

    match: list[IncidentIoRule] = Field(min_length=1)


class _Ref(BaseModel):
    id: Token


class _Severity(_Ref):
    rank: int | None = None


class _Status(_Ref):
    category: Token


class _CatalogEntry(BaseModel):
    id: str
    external_id: str | None = None


class _Option(BaseModel):
    id: str


class _FieldValue(BaseModel):
    value_option: _Option | None = None
    value_catalog_entry: _CatalogEntry | None = None

    def ids(self) -> list[str]:
        ids = []
        if self.value_option is not None:
            ids.append(self.value_option.id)
        if self.value_catalog_entry is not None:
            ids.append(self.value_catalog_entry.id)
            if self.value_catalog_entry.external_id is not None:
                ids.append(self.value_catalog_entry.external_id)
        return ids


class _FieldEntry(BaseModel):
    custom_field: _Ref
    values: list[_FieldValue] = Field(default_factory=list)


class IncidentIoIncident(BaseModel):
    """What FIRE reads of an incident.io `IncidentV2`; every other key is dropped."""

    id: Token
    reference: Token | None = None
    visibility: Literal["public", "private"]
    mode: Token | None = None
    created_at: datetime | None = None
    incident_status: _Status | None = None
    severity: _Severity | None = None
    incident_type: _Ref | None = None
    team_ids: list[Token] = Field(default_factory=list)
    custom_field_entries: list[_FieldEntry] = Field(default_factory=list)

    def field_value_ids(self, custom_field_id: str) -> set[str]:
        return {
            value_id
            for entry in self.custom_field_entries
            if entry.custom_field.id == custom_field_id
            for value in entry.values
            for value_id in value.ids()
        }


@dataclass(frozen=True)
class IncidentIoMatch:
    """The rule that mapped an incident to a system: its index in `match`, what it matched on,
    and the configured id that matched.
    """

    rule: int
    by: Literal["custom_field", "team", "incident_type"]
    value: str
    custom_field_id: str | None = None


def match_incident(
    trigger: IncidentIoTrigger, incident: IncidentIoIncident
) -> IncidentIoMatch | None:
    """The first rule, in config order, that the incident matches."""
    for index, rule in enumerate(trigger.match):
        if rule.custom_field_id is not None:
            found = incident.field_value_ids(rule.custom_field_id)
            value = next((value for value in rule.values if value in found), None)
            if value is not None:
                return IncidentIoMatch(index, "custom_field", value, rule.custom_field_id)
        elif rule.team_id is not None:
            if rule.team_id in incident.team_ids:
                return IncidentIoMatch(index, "team", rule.team_id)
        elif (
            incident.incident_type is not None
            and rule.incident_type_id == incident.incident_type.id
        ):
            return IncidentIoMatch(index, "incident_type", rule.incident_type_id)
    return None


@dataclass(frozen=True)
class IncidentIoEvent:
    event_type: str
    incident: IncidentIoIncident


def read_event(body: bytes) -> IncidentIoEvent | IncidentIoOutcome:
    """The incident of a webhook body, or the outcome of a body that carries none."""
    try:
        document: Any = json.loads(body)
    except (UnicodeDecodeError, ValueError, RecursionError):
        return IncidentIoOutcome.MALFORMED
    if not isinstance(document, dict) or not isinstance(document.get("event_type"), str):
        return IncidentIoOutcome.MALFORMED
    event_type: str = document["event_type"]
    if event_type in PRIVATE_EVENTS:
        return IncidentIoOutcome.PRIVATE
    if event_type not in INCIDENT_EVENTS:
        return IncidentIoOutcome.IGNORED_EVENT_TYPE
    data = document.get(event_type)
    for key in INCIDENT_EVENTS[event_type]:
        data = data.get(key) if isinstance(data, dict) else None
    try:
        incident = IncidentIoIncident.model_validate(data)
    except ValidationError:
        return IncidentIoOutcome.MALFORMED
    if incident.visibility == "private":
        return IncidentIoOutcome.PRIVATE
    return IncidentIoEvent(event_type, incident)
