"""Composition root: provider selection from config (reasoner echo or laya, template
narrator, console or slack notifier, memory or sqlite catalog and feedback store).
"""

from __future__ import annotations

import logging
import os
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import httpx

from fire.adapters.manifests.event_chains_graph import EventChainsGraphParser
from fire.adapters.memory.decision_store import MemoryDecisionStore
from fire.adapters.memory.observation_catalog import MemoryObservationCatalog
from fire.adapters.narrators.template import TemplateNarrator
from fire.adapters.notifiers.console import ConsoleNotifier
from fire.adapters.notifiers.slack import SlackNotifier
from fire.adapters.reasoners.echo import EchoReasoner
from fire.adapters.reasoners.laya import LayaReasoner
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.adapters.sqlite.decision_store import SqliteDecisionStore
from fire.adapters.sqlite.observation_catalog import SqliteObservationCatalog
from fire.apps.config import (
    CatalogConfig,
    FeedbackConfig,
    FireConfig,
    NotifierConfig,
    ReasonerConfig,
    SystemConfig,
    load_fire_config,
    load_system_configs,
)
from fire.apps.discovery import Discovery, check_manifest_kinds, discover, discover_routes
from fire.core.domain.baseline import DriftPolicy
from fire.core.domain.dlq import FingerprintOutcomes
from fire.core.domain.drift import DriftSignal
from fire.core.domain.flow import PathPolicy
from fire.core.domain.http_errors import HttpErrorBurst
from fire.core.domain.incident import Incident
from fire.core.domain.observation import ShapeObservation
from fire.core.domain.path import PathDrift, PathDriftRule
from fire.core.ports.decision_store import DecisionStore
from fire.core.ports.manifest_parser import ManifestParser
from fire.core.ports.notifier import Notifier
from fire.core.ports.observation_catalog import ObservationCatalog
from fire.core.ports.reasoner import Reasoner
from fire.core.ports.shape_fingerprinter import ShapeFingerprinter
from fire.core.services.coverage import CoverageScope
from fire.core.services.diagnosis import DiagnosisService
from fire.core.services.dlq import DlqService
from fire.core.services.drift_incident import DriftIncidentService
from fire.core.services.feedback import FeedbackService
from fire.core.services.http_errors import HttpErrorsService
from fire.core.services.incident_correlator import Correlation, IncidentCorrelator
from fire.core.services.incident_io import IncidentIoReceipt, IncidentIoService
from fire.core.services.ingest import IngestService

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = Path(__file__).resolve().parents[3] / "config" / "fire.yml"


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass
class Container:
    config: FireConfig
    diagnosis: DiagnosisService
    feedback: FeedbackService
    decisions: DecisionStore
    reasoner: Reasoner
    fingerprinter: ShapeFingerprinter
    catalog: ObservationCatalog
    ingest: IngestService
    drift_incidents: DriftIncidentService
    http_errors: HttpErrorsService
    dlq: DlqService
    incidents: IncidentCorrelator
    systems: dict[str, SystemConfig] = field(default_factory=dict)
    discoveries: dict[str, Discovery] = field(default_factory=dict)
    coverage: dict[str, CoverageScope] = field(default_factory=dict)
    incident_io: IncidentIoService = field(default_factory=lambda: IncidentIoService({}))
    clock: Callable[[], datetime] = _utc_now


def manifest_parsers() -> dict[str, ManifestParser]:
    parser = EventChainsGraphParser()
    return {parser.kind: parser}


def log_drift(observation: ShapeObservation, signal: DriftSignal) -> None:
    """One INFO line per new drift signal, with names and counts only: a path may hold data."""
    logger.info(
        "shape drift raised: system=%s source=%s stream=%s fingerprint=%s "
        "added=%d type_changed=%d removed=%d",
        observation.system,
        observation.source,
        observation.channel.key,
        signal.schema_hash,
        len(signal.diff.added),
        len(signal.diff.type_changed),
        len(signal.diff.removed),
    )


def log_path_drift(drift: PathDrift) -> None:
    """One INFO line per flow that fires a new path drift signal, with its entry and counts."""
    rules = Counter(firing.signal.rule for firing in drift.firings)
    logger.info(
        "path drift raised: system=%s service=%s entry=%s "
        "hop_added=%d hop_missing=%d hop_failing=%d new=%d",
        drift.system,
        drift.entry.service,
        drift.entry.operation,
        rules[PathDriftRule.HOP_ADDED],
        rules[PathDriftRule.HOP_MISSING],
        rules[PathDriftRule.HOP_FAILING],
        sum(firing.new for firing in drift.firings),
    )


def log_http_errors(burst: HttpErrorBurst) -> None:
    """One INFO line per burst, with its route and counts."""
    logger.info(
        "http errors raised: system=%s service=%s route=%s errors=%d status_min=%d window_s=%g",
        burst.system,
        burst.route.service,
        burst.route.operation,
        burst.errors,
        burst.status_min,
        burst.window_s,
    )


def log_dlq(system: str, fingerprint: FingerprintOutcomes) -> None:
    """One INFO line per fingerprint that crosses its system's `triggers.dlq`."""
    logger.info(
        "dlq attribution raised: system=%s source=%s stream=%s fingerprint=%s "
        "abandoned=%d observed=%d rate=%.3f",
        system,
        fingerprint.source,
        fingerprint.stream,
        fingerprint.schema_hash,
        fingerprint.abandoned,
        fingerprint.observed,
        fingerprint.rate,
    )


def log_dlq_unattributed(observation: ShapeObservation) -> None:
    """One INFO line per abandoned work unit that no observation of another source carried."""
    logger.info(
        "dlq unattributed: system=%s source=%s outcome=%s work_id=%s",
        observation.system,
        observation.source,
        observation.channel.key,
        observation.work_id,
    )


def log_incident_io(receipt: IncidentIoReceipt) -> None:
    """One INFO line per verified incident.io webhook, with ids and counts only."""
    logger.info(
        "incident.io webhook: event_type=%s outcome=%s incident=%s raised=%d",
        receipt.event_type or "-",
        receipt.outcome,
        receipt.incident_id or "-",
        len(receipt.events),
    )


def log_joined(correlation: Correlation) -> None:
    """One INFO line per trigger event that joins an open incident."""
    logger.info(
        "incident joined: system=%s incident=%s trigger=%s triggers=%d",
        correlation.system,
        correlation.incident_id,
        correlation.trigger,
        correlation.triggers,
    )


def log_updated(incident: Incident) -> None:
    """One INFO line per incident whose window closed with triggers not yet diagnosed."""
    logger.info(
        "incident updated: system=%s incident=%s triggers=%d",
        incident.system,
        incident.id,
        len(incident.triggers),
    )


def build_catalog(
    config: CatalogConfig,
    drift_policies: Mapping[str, DriftPolicy],
    path_policies: Mapping[str, PathPolicy],
    clock: Callable[[], datetime] | None = None,
) -> ObservationCatalog:
    """The `catalog.provider` of `config/fire.yml`; `sqlite` needs `catalog.path_env` set."""
    if config.provider == "memory":
        return MemoryObservationCatalog(
            drift_policies,
            path_policies=path_policies,
            clock=clock,
            retention_s=config.retention_s,
        )
    path = os.environ.get(config.path_env)
    if not path:
        raise ValueError(f"catalog provider sqlite needs {config.path_env} set to a file path")
    return SqliteObservationCatalog(
        path,
        drift_policies,
        path_policies=path_policies,
        clock=clock,
        retention_s=config.retention_s,
    )


def build_decision_store(config: FeedbackConfig) -> DecisionStore:
    """The `feedback.provider` of `config/fire.yml`; `sqlite` needs `feedback.path_env` set."""
    if config.provider == "memory":
        return MemoryDecisionStore()
    path = os.environ.get(config.path_env)
    if not path:
        raise ValueError(f"feedback provider sqlite needs {config.path_env} set to a file path")
    return SqliteDecisionStore(path)


def build_reasoner(
    config: ReasonerConfig, transport: httpx.AsyncBaseTransport | None = None
) -> Reasoner:
    """The reasoner `config.provider` names. `laya` asks a laya-serve `/v1/systemone` and fails
    here, at startup, when its URL env var is unset.
    """
    if config.provider == "echo":
        return EchoReasoner()
    if config.provider == "laya":
        url = os.environ.get(config.base_url_env)
        if not url:
            raise ValueError(f"reasoner provider 'laya' needs {config.base_url_env} set")
        return LayaReasoner(
            url,
            api_key=os.environ.get(config.api_key_env) or None,
            model=config.model,
            timeout_s=config.timeout_s,
            min_sufficient=config.min_sufficient,
            transport=transport,
        )
    raise ValueError(f"reasoner provider {config.provider!r} not wired (use echo or laya)")


def build_notifier(
    config: NotifierConfig,
    systems: Mapping[str, SystemConfig],
    transport: httpx.AsyncBaseTransport | None = None,
) -> Notifier:
    """The notifier `config.provider` names. `slack` posts to each system's
    `notify.slack.channel_id` and fails here, at startup, when its bot token env var is unset.
    """
    if config.provider == "console":
        return ConsoleNotifier()
    if config.provider == "slack":
        token = os.environ.get(config.bot_token_env)
        if not token:
            raise ValueError(f"notifier provider 'slack' needs {config.bot_token_env} set")
        channels = {name: system.notify.slack.channel_id for name, system in systems.items()}
        return SlackNotifier(token, channels, timeout_s=config.timeout_s, transport=transport)
    raise ValueError(f"notifier provider {config.provider!r} not wired (use console or slack)")


def build_container(
    config_path: Path | None = None, clock: Callable[[], datetime] | None = None
) -> Container:
    """`clock` is the body's clock for flows, bursts, correlation windows, and webhook
    timestamps, the current UTC time unless a test injects one.
    """
    config_path = config_path or DEFAULT_CONFIG
    config = load_fire_config(config_path)

    if config.narrator.provider != "template":
        raise ValueError(
            f"narrator provider {config.narrator.provider!r} not wired in v0 "
            "(use template; openai_compat comes next)"
        )

    systems = load_system_configs(config_path.parent / "systems")
    parsers = manifest_parsers()
    check_manifest_kinds(systems, parsers)

    reasoner = build_reasoner(config.reasoner)
    decisions = build_decision_store(config.feedback)
    feedback = FeedbackService(decisions, clock=clock)
    diagnosis = DiagnosisService(
        reasoner=reasoner,
        narrator=TemplateNarrator(),
        notifier=build_notifier(config.notifier, systems),
        feedback=feedback,
        provider=config.reasoner.provider,
        model=config.reasoner.model,
    )
    fingerprinter = Sha256ShapeFingerprinter()
    drift_policies = {name: system.drift for name, system in systems.items()}
    path_policies = {name: system.paths for name, system in systems.items()}
    catalog = build_catalog(config.catalog, drift_policies, path_policies, clock)
    http_errors = {
        name: system.triggers.http_errors
        for name, system in systems.items()
        if system.triggers.http_errors is not None
    }
    sources = {name: system.source_ids() for name, system in systems.items()}
    discoveries = {name: discover(system, parsers) for name, system in systems.items()}
    scopes = {
        name: CoverageScope(
            inventory=discoveries[name].inventory,
            missing_reason=discoveries[name].missing_reason,
            critical=tuple(system.policy.critical),
            ignore=tuple(system.policy.ignore),
            routes=discover_routes(system),
        )
        for name, system in systems.items()
    }
    incident_io_triggers = {
        name: system.triggers.incident_io
        for name, system in systems.items()
        if system.triggers.incident_io is not None
    }
    now = clock or _utc_now
    correlation = {
        name: system.triggers.correlation
        for name, system in systems.items()
        if system.triggers.correlation is not None
    }
    return Container(
        config=config,
        diagnosis=diagnosis,
        feedback=feedback,
        decisions=decisions,
        reasoner=reasoner,
        fingerprinter=fingerprinter,
        catalog=catalog,
        ingest=IngestService(catalog, fingerprinter, sources),
        drift_incidents=DriftIncidentService(catalog, scopes),
        http_errors=HttpErrorsService(catalog, http_errors, clock=clock),
        incidents=IncidentCorrelator(correlation, clock=now),
        systems=systems,
        discoveries=discoveries,
        coverage=scopes,
        incident_io=IncidentIoService(incident_io_triggers, clock=now),
        dlq=DlqService(
            catalog,
            fingerprinter,
            {name: system.outcome_sources() for name, system in systems.items()},
            {
                name: system.triggers.dlq
                for name, system in systems.items()
                if system.triggers.dlq is not None
            },
            window_s=config.catalog.retention_s,
        ),
        clock=now,
    )
