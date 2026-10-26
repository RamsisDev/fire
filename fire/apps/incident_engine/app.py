"""FastAPI edge: every /v0 route requires X-FIRE-Token, except webhooks, which verify their
signature; ingest, diagnose, and webhooks answer 202.

While the app runs, settled flows complete every second, and before `/paths` answers; every
second, correlation windows that are over close too.
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import os
import uuid
from collections.abc import Iterable
from contextlib import asynccontextmanager, suppress
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from fire.adapters.webhooks.signing import SignatureRejected, decode_secret, verify
from fire.apps.env import load_fire_env
from fire.apps.incident_engine.wiring import (
    Container,
    build_container,
    log_dlq,
    log_dlq_unattributed,
    log_drift,
    log_http_errors,
    log_incident_io,
    log_joined,
    log_path_drift,
    log_updated,
)
from fire.core.domain.dossier import (
    Dossier,
    EvidenceFragment,
    EvidenceQuality,
    ProcessHop,
    ProcessPath,
    Shape,
    ShapeFingerprint,
)
from fire.core.domain.drift import DriftSignal
from fire.core.domain.feedback import InvalidFeedback, TrainingStatus, UnknownIncident, Verdict
from fire.core.domain.incident import Incident, Trigger
from fire.core.domain.observation import ShapeObservation, SpanObservation
from fire.core.domain.origin import Origin
from fire.core.domain.trigger_event import TriggerEvent
from fire.core.services.coverage import report_coverage
from fire.core.services.incident_correlator import Correlation

logger = logging.getLogger(__name__)

# How often the running app completes settled flows.
COMPLETE_EVERY_S = 1.0

_container: Container | None = None


def get_container() -> Container:
    if _container is None:
        raise RuntimeError("container not initialized")
    return _container


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global _container
    # Process env wins over .env (override=False). Must run before auth/wiring
    # read os.environ.
    load_fire_env()
    _container = build_container()
    completing = asyncio.create_task(_complete_flows_forever())
    try:
        yield
    finally:
        completing.cancel()
        with suppress(asyncio.CancelledError):
            await completing
        _container.catalog.close()
        _container.decisions.close()
        aclose = getattr(_container.reasoner, "aclose", None)
        if aclose is not None:
            await aclose()
        _container = None


app = FastAPI(title="FIRE ENGINE", version="0.1.0", lifespan=lifespan)


@app.exception_handler(RequestValidationError)
async def _validation_error(_request: Request, exc: RequestValidationError) -> JSONResponse:
    """A 422 lists each error's location and type, never the input that failed."""
    detail = [{"loc": list(error["loc"]), "type": error["type"]} for error in exc.errors()]
    return JSONResponse(status_code=422, content={"detail": detail})


class ObservationBatch(BaseModel):
    """Body of `POST /v0/observations`. Items are checked one by one by the ingest service."""

    model_config = ConfigDict(extra="forbid")

    shapes: list[Any] = Field(default_factory=list)
    spans: list[Any] = Field(default_factory=list)


class DiagnoseRequest(BaseModel):
    """Edge ingest — system-scoped; Origin + nested shape; hops by reference."""

    system: str
    service: str | None = None
    summary: str | None = None
    origin: Origin | None = None
    shape: Shape | None = None
    fingerprint: ShapeFingerprint | None = None
    process_hops: list[ProcessHop] = Field(default_factory=list)


class FeedbackRequest(BaseModel):
    """Body of `POST /v0/incidents/{incident_id}/feedback`."""

    model_config = ConfigDict(extra="forbid")

    verdict: Verdict
    possibility_id: str | None = None
    reviewer: str
    note: str | None = None


def _require_token(x_fire_token: str | None, token_env: str) -> None:
    expected = os.environ.get(token_env)
    if not expected:
        raise HTTPException(
            status_code=503,
            detail=f"server misconfigured: {token_env} is not set",
        )
    # Compare bytes: compare_digest raises TypeError on non-ASCII str inputs.
    provided = (x_fire_token or "").encode()
    if not hmac.compare_digest(provided, expected.encode()):
        raise HTTPException(status_code=401, detail="invalid or missing X-FIRE-Token")


def _signing_key(secret_env: str) -> bytes:
    secret = os.environ.get(secret_env)
    if not secret:
        raise HTTPException(
            status_code=503,
            detail=f"server misconfigured: {secret_env} is not set",
        )
    try:
        return decode_secret(secret)
    except ValueError:
        raise HTTPException(
            status_code=503,
            detail=f"server misconfigured: {secret_env} is not a signing secret",
        ) from None


async def _run_pipeline(incident: Incident, dossier: Dossier) -> None:
    await get_container().diagnosis.run(incident, dossier)


def _correlate(container: Container, event: TriggerEvent) -> Correlation:
    """Hands one trigger event to the correlator; a join is logged."""
    correlation = container.incidents.add(event)
    if not correlation.opened:
        log_joined(correlation)
    return correlation


def _take(container: Container, opened: Iterable[str]) -> list[tuple[Incident, Dossier]]:
    """The incidents opened by one batch or one completion, each once with every trigger it
    holds by now; the caller runs their diagnoses.
    """
    runs: list[tuple[Incident, Dossier]] = []
    for incident_id in dict.fromkeys(opened):
        run = container.incidents.take(incident_id)
        if run is not None:
            runs.append(run)
    return runs


def _complete_flows(container: Container) -> list[tuple[Incident, Dossier]]:
    """Completes settled flows and correlates one trigger event per flow that fired a new path
    drift signal; returns the incidents they opened, whose diagnoses the caller runs.
    """
    opened: list[str] = []
    for drift in container.catalog.complete_flows():
        log_path_drift(drift)
        correlation = _correlate(container, container.drift_incidents.path_drift_event(drift))
        if correlation.opened:
            opened.append(correlation.incident_id)
    return _take(container, opened)


def _close_windows(container: Container) -> list[tuple[Incident, Dossier]]:
    """Closes the correlation windows that are over; returns the incidents that gained triggers
    since their last diagnosis, whose update the caller diagnoses.
    """
    runs = container.incidents.close()
    for incident, _dossier in runs:
        log_updated(incident)
    return runs


def _attribute_abandoned(
    container: Container, system: str, abandoned: list[ShapeObservation]
) -> list[TriggerEvent]:
    """Joins one batch's abandoned work units of a system to its work index, logs those no
    observation carried, and returns one trigger event per fingerprint that crossed
    `triggers.dlq`.
    """
    evaluation = container.dlq.evaluate(system, abandoned)
    for observation in evaluation.unattributed:
        log_dlq_unattributed(observation)
    events: list[TriggerEvent] = []
    for fingerprint in evaluation.raised:
        log_dlq(system, fingerprint)
        events.append(container.dlq.trigger_event(system, fingerprint, evaluation.outcomes))
    return events


async def _complete_flows_forever() -> None:
    """Every `COMPLETE_EVERY_S`, completes settled flows, closes correlation windows that are
    over, and diagnoses their incidents in turn.

    A failure is logged with its exception type, never its message, and the loop goes on.
    """
    while True:
        await asyncio.sleep(COMPLETE_EVERY_S)
        container = get_container()
        runs: list[tuple[Incident, Dossier]] = []
        try:
            runs += _complete_flows(container)
        except Exception as error:
            logger.error("flow completion failed: error=%s", type(error).__name__)
        try:
            runs += _close_windows(container)
        except Exception as error:
            logger.error("correlation close failed: error=%s", type(error).__name__)
        for incident, dossier in runs:
            try:
                await _run_pipeline(incident, dossier)
            except Exception as error:
                logger.error(
                    "diagnosis failed: incident=%s error=%s", incident.id, type(error).__name__
                )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/v0/observations",
    status_code=202,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": ObservationBatch.model_json_schema()}},
        }
    },
)
async def ingest_observations(
    request: Request,
    background_tasks: BackgroundTasks,
    x_fire_token: str | None = Header(default=None, alias="X-FIRE-Token"),
) -> dict[str, Any]:
    container = get_container()
    _require_token(x_fire_token, container.config.api.token_env)
    # Parsed here rather than declared as a parameter: FastAPI validates parameters before the
    # handler runs, and the token must be checked before the body.
    try:
        batch = ObservationBatch.model_validate_json(await request.body())
    except ValidationError as error:
        raise RequestValidationError(
            [
                {"loc": ("body", *detail["loc"]), "type": detail["type"]}
                for detail in error.errors(include_input=False, include_context=False)
            ]
        ) from None

    opened: list[str] = []

    def correlate(event: TriggerEvent) -> None:
        correlation = _correlate(container, event)
        if correlation.opened:
            opened.append(correlation.incident_id)

    def open_drift_incident(observation: ShapeObservation, signal: DriftSignal) -> None:
        log_drift(observation, signal)
        correlate(container.drift_incidents.shape_drift_event(observation, signal))

    def watch_http_errors(span: SpanObservation) -> None:
        burst = container.http_errors.observe(span)
        if burst is None:
            return
        log_http_errors(burst)
        correlate(container.http_errors.trigger_event(burst))

    abandoned: list[ShapeObservation] = []

    def watch_outcomes(observation: ShapeObservation) -> None:
        if container.dlq.abandoned(observation):
            abandoned.append(observation)

    report = container.ingest.ingest(
        batch.shapes,
        batch.spans,
        on_drift=open_drift_incident,
        on_span=watch_http_errors,
        on_shape=watch_outcomes,
    )
    for system in dict.fromkeys(observation.system for observation in abandoned):
        try:
            events = _attribute_abandoned(
                container, system, [o for o in abandoned if o.system == system]
            )
        except Exception as error:
            logger.error(
                "dlq attribution failed: system=%s error=%s", system, type(error).__name__
            )
            continue
        for event in events:
            correlate(event)
    for incident, dossier in _take(container, opened):
        background_tasks.add_task(_run_pipeline, incident, dossier)
    logger.info(
        "observations ingested: shapes accepted=%d duplicate=%d rejected=%d, "
        "spans accepted=%d duplicate=%d rejected=%d",
        report.shapes.accepted,
        report.shapes.duplicate,
        len(report.shapes.rejected),
        report.spans.accepted,
        report.spans.duplicate,
        len(report.spans.rejected),
    )
    return report.model_dump(mode="json", exclude_none=True)


@app.get("/v0/systems/{system}/streams")
async def streams(
    system: str,
    x_fire_token: str | None = Header(default=None, alias="X-FIRE-Token"),
) -> dict[str, Any]:
    container = get_container()
    _require_token(x_fire_token, container.config.api.token_env)
    if system not in container.systems:
        raise HTTPException(status_code=404, detail="unknown system")
    return {
        "system": system,
        "streams": [
            {
                "source": stream.source,
                "key": stream.key,
                "status": stream.status,
                "samples": stream.samples,
                "observations": stream.observations,
                "fingerprints": [
                    fingerprint.model_dump(mode="json") for fingerprint in stream.fingerprints
                ],
                "drift": [signal.model_dump(mode="json") for signal in stream.drift],
            }
            for stream in container.catalog.streams(system)
        ],
    }


@app.get("/v0/systems/{system}/coverage")
async def coverage(
    system: str,
    x_fire_token: str | None = Header(default=None, alias="X-FIRE-Token"),
) -> dict[str, Any]:
    container = get_container()
    _require_token(x_fire_token, container.config.api.token_env)
    if system not in container.systems:
        raise HTTPException(status_code=404, detail="unknown system")
    report = report_coverage(system, container.coverage[system], container.catalog.streams(system))
    return report.model_dump(mode="json", exclude_none=True)


@app.get("/v0/systems/{system}/dlq")
async def dlq(
    system: str,
    x_fire_token: str | None = Header(default=None, alias="X-FIRE-Token"),
) -> dict[str, Any]:
    container = get_container()
    _require_token(x_fire_token, container.config.api.token_env)
    if system not in container.systems:
        raise HTTPException(status_code=404, detail="unknown system")
    report = container.dlq.report(system)
    if report.missing_reason is not None:
        return {"system": system, "missing_reason": report.missing_reason}
    return report.model_dump(mode="json", exclude={"missing_reason"})


@app.get("/v0/systems/{system}/paths")
async def paths(
    system: str,
    background_tasks: BackgroundTasks,
    x_fire_token: str | None = Header(default=None, alias="X-FIRE-Token"),
) -> dict[str, Any]:
    container = get_container()
    _require_token(x_fire_token, container.config.api.token_env)
    if system not in container.systems:
        raise HTTPException(status_code=404, detail="unknown system")
    for incident, dossier in _complete_flows(container):
        background_tasks.add_task(_run_pipeline, incident, dossier)
    return container.catalog.paths(system).model_dump(mode="json", by_alias=True)


@app.post("/v0/diagnose", status_code=202)
async def diagnose(
    body: DiagnoseRequest,
    background_tasks: BackgroundTasks,
    x_fire_token: str | None = Header(default=None, alias="X-FIRE-Token"),
) -> dict[str, Any]:
    container = get_container()
    _require_token(x_fire_token, container.config.api.token_env)
    if body.system not in container.systems:
        raise RequestValidationError([{"loc": ("body", "system"), "type": "unknown_system"}])
    shape = body.shape
    fingerprint = body.fingerprint
    if shape is not None:
        computed = container.fingerprinter.fingerprint(shape)
        if fingerprint is not None and fingerprint != computed:
            raise RequestValidationError(
                [{"loc": ("body", "fingerprint"), "type": "fingerprint_mismatch"}]
            )
        fingerprint = computed

    # Never correlated: the caller gets an incident holding exactly what it posted.
    incident_id = str(uuid.uuid4())
    incident = Incident(
        id=incident_id,
        system=body.system,
        trigger=Trigger.MANUAL,
        service=body.service,
        summary=body.summary,
    )
    origin = body.origin
    fragments: list[EvidenceFragment] = []
    if origin is None:
        fragments.append(
            EvidenceFragment(
                collector="origin",
                quality=EvidenceQuality.MISSING,
                summary="No Origin provided",
            )
        )
    if shape is None and fingerprint is None:
        fragments.append(
            EvidenceFragment(
                collector="shape",
                quality=EvidenceQuality.MISSING,
                summary="No shape or fingerprint provided",
            )
        )
    dossier = Dossier(
        incident_id=incident_id,
        system=body.system,
        service=body.service,
        origin=origin,
        shape=shape,
        fingerprint=fingerprint,
        process_path=ProcessPath(hops=list(body.process_hops)),
        fragments=fragments,
    )
    background_tasks.add_task(_run_pipeline, incident, dossier)
    return {
        "status": "accepted",
        "incident_id": incident_id,
        "system": body.system,
        "service": body.service,
        "origin_id": origin.origin_id if origin else None,
    }


@app.get("/v0/incidents/{incident_id}/decision")
async def get_decision(
    incident_id: str,
    x_fire_token: str | None = Header(default=None, alias="X-FIRE-Token"),
) -> dict[str, Any]:
    container = get_container()
    _require_token(x_fire_token, container.config.api.token_env)
    record = container.decisions.get_decision(incident_id)
    if record is None:
        raise HTTPException(status_code=404, detail="unknown incident")
    rows = container.decisions.list_feedback(incident_id=incident_id)
    # The dossier stays in the store: it is what the retraining export reads.
    return {
        "incident_id": record.incident_id,
        "system": record.system,
        "reasoner": record.reasoner,
        "model": record.model,
        "decided_at": record.decided_at.isoformat(),
        "decision": record.decision.model_dump(mode="json"),
        "feedback": rows[-1].model_dump(mode="json") if rows else None,
    }


@app.post("/v0/incidents/{incident_id}/feedback", status_code=201)
async def submit_feedback(
    incident_id: str,
    request: Request,
    x_fire_token: str | None = Header(default=None, alias="X-FIRE-Token"),
) -> dict[str, Any]:
    container = get_container()
    _require_token(x_fire_token, container.config.api.token_env)
    # Parsed here: the token is checked before the body.
    try:
        body = FeedbackRequest.model_validate_json(await request.body())
    except ValidationError as error:
        raise RequestValidationError(
            [
                {"loc": ("body", *detail["loc"]), "type": detail["type"]}
                for detail in error.errors(include_input=False, include_context=False)
            ]
        ) from None
    try:
        feedback = container.feedback.submit(
            incident_id, body.verdict, body.possibility_id, body.reviewer, body.note
        )
    except UnknownIncident:
        raise HTTPException(status_code=404, detail="unknown incident") from None
    except InvalidFeedback as error:
        raise HTTPException(status_code=422, detail=str(error)) from None
    return feedback.model_dump(mode="json")


@app.get("/v0/feedback")
async def list_feedback(
    status: TrainingStatus | None = None,
    system: str | None = None,
    limit: int | None = Query(default=None, ge=1),
    x_fire_token: str | None = Header(default=None, alias="X-FIRE-Token"),
) -> dict[str, Any]:
    container = get_container()
    _require_token(x_fire_token, container.config.api.token_env)
    items = container.decisions.list_feedback(status, system, limit)
    everything = container.decisions.list_feedback(system=system)
    return {
        "items": [row.model_dump(mode="json") for row in items],
        "counts": {s.value: sum(row.status is s for row in everything) for s in TrainingStatus},
    }


@app.post("/v0/webhooks/incident-io", status_code=202)
async def incident_io_webhook(
    request: Request, background_tasks: BackgroundTasks
) -> dict[str, Any]:
    """Verified with the signing secret instead of X-FIRE-Token. Every verified body is `202`,
    so incident.io never retries one FIRE ignores.
    """
    container = get_container()
    webhook = container.config.webhooks.incident_io
    key = _signing_key(webhook.signing_secret_env)
    body = await request.body()
    try:
        verify(key, request.headers, body, container.clock(), webhook.tolerance_s)
    except SignatureRejected as rejected:
        logger.warning("incident.io webhook rejected: reason=%s", rejected.reason)
        raise HTTPException(
            status_code=401, detail="invalid or missing webhook signature"
        ) from None
    receipt = container.incident_io.receive(body)
    log_incident_io(receipt)
    correlations = [_correlate(container, event) for event in receipt.events]
    opened = [c.incident_id for c in correlations if c.opened]
    for incident, dossier in _take(container, opened):
        background_tasks.add_task(_run_pipeline, incident, dossier)
    return {
        "status": "accepted",
        "outcome": receipt.outcome,
        "incident_ids": list(dict.fromkeys(c.incident_id for c in correlations)),
    }
