"""What a watched system declares about itself: events, processors, ingress. Never values.

Discovered from the system's own manifest, never written by hand in config.
Contract: docs/specs/system-config-and-discovery.md.
"""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator

Name = Annotated[str, Field(min_length=1)]


class DeclaredField(BaseModel):
    """One field of a declared payload type: its name and the type the system names."""

    name: Name
    type: str


class DeclaredEvent(BaseModel):
    name: Name
    payload_type: str | None = None
    payload_fields: list[DeclaredField] = Field(default_factory=list)
    consumers: list[str] = Field(default_factory=list)


class DeclaredProcessor(BaseModel):
    name: Name
    region_policy: str | None = None


class DeclaredIngress(BaseModel):
    """An entry point from outside the system and the events it emits first."""

    id: Name
    label: str | None = None
    transport: str | None = None
    first_events: list[str] = Field(default_factory=list)


class SystemInventory(BaseModel):
    """Everything one manifest declares. Names are unique within each list."""

    model_config = ConfigDict(extra="forbid")

    system: Name
    manifest_kind: Name
    events: list[DeclaredEvent] = Field(default_factory=list)
    processors: list[DeclaredProcessor] = Field(default_factory=list)
    ingress: list[DeclaredIngress] = Field(default_factory=list)

    @model_validator(mode="after")
    def _require_unique_names(self) -> SystemInventory:
        for label, names in (
            ("event", [event.name for event in self.events]),
            ("processor", [processor.name for processor in self.processors]),
            ("ingress", [ingress.id for ingress in self.ingress]),
        ):
            duplicates = sorted({name for name in names if names.count(name) > 1})
            if duplicates:
                raise ValueError(f"duplicate {label} names: {', '.join(duplicates)}")
        return self

    def event_names(self) -> set[str]:
        return {event.name for event in self.events}


class CoverageReport(BaseModel):
    """Declared versus observed events, and config references to neither. Lists are sorted."""

    model_config = ConfigDict(extra="forbid")

    system: str
    declared_unseen: list[str] = Field(default_factory=list)
    seen_undeclared: list[str] = Field(default_factory=list)
    unknown_references: list[str] = Field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return not (self.declared_unseen or self.seen_undeclared or self.unknown_references)


class MissingCoverage(BaseModel):
    """A part of a coverage report with nothing declared to compare against, and why."""

    model_config = ConfigDict(extra="forbid")

    missing_reason: Name


class RouteCoverage(BaseModel):
    """One inbound source's declared routes, each `METHOD template`, versus the routes observed
    on it. Lists are sorted, and None exactly when `missing_reason` says why nothing is declared.
    """

    model_config = ConfigDict(extra="forbid")

    source: Name
    missing_reason: Name | None = None
    declared_unseen: list[str] | None = None
    seen_undeclared: list[str] | None = None

    @model_validator(mode="after")
    def _lists_or_reason(self) -> RouteCoverage:
        lists = (self.declared_unseen, self.seen_undeclared)
        if self.missing_reason is None and None in lists:
            raise ValueError("a route coverage without missing_reason needs both lists")
        if self.missing_reason is not None and lists != (None, None):
            raise ValueError("a route coverage with missing_reason has no lists")
        return self


class OutboundCoverage(BaseModel):
    """The outbound operations observed on one source, sorted."""

    model_config = ConfigDict(extra="forbid")

    source: Name
    operations: list[str]


class SystemCoverage(BaseModel):
    """`GET /v0/systems/{system}/coverage`: events, inbound routes, and outbound operations."""

    model_config = ConfigDict(extra="forbid")

    system: Name
    events: CoverageReport | MissingCoverage
    inbound_routes: list[RouteCoverage] = Field(default_factory=list)
    outbound_operations: list[OutboundCoverage] = Field(default_factory=list)
