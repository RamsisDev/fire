"""Ingest: check each observation of a batch, store those that pass, and count every outcome.

Contract: docs/specs/observations.md#ingest-endpoint.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Collection, Mapping, Sequence

from pydantic import BaseModel, ValidationError

from fire.core.domain.drift import Admission, DriftSignal
from fire.core.domain.ingest import IngestCounts, IngestReport, Rejection, RejectionReason
from fire.core.domain.observation import ShapeObservation, SpanObservation
from fire.core.ports.observation_catalog import ObservationCatalog
from fire.core.ports.shape_fingerprinter import ShapeFingerprinter

logger = logging.getLogger(__name__)

# Called once per new drift signal with the observation that raised it.
DriftHook = Callable[[ShapeObservation, DriftSignal], None]
# Called once per stored shape.
ShapeHook = Callable[[ShapeObservation], None]
# Called once per stored span.
SpanHook = Callable[[SpanObservation], None]


class IngestService:
    """Checks items one by one, shapes first, each list in order; the first failing check decides.

    The checks: the item validates as its model, its system is loaded, its source is one of
    that system's sources, and for a shape the recomputed fingerprint equals the one sent. An
    item that passes is offered to the catalog, which counts it `accepted` or `duplicate`. A
    rejected item is not stored and holds no dedupe key.
    """

    def __init__(
        self,
        catalog: ObservationCatalog,
        fingerprinter: ShapeFingerprinter,
        sources: Mapping[str, Collection[str]],
    ) -> None:
        """`sources` maps each loaded system to its source ids."""
        self._catalog = catalog
        self._fingerprinter = fingerprinter
        self._sources = sources

    def ingest(
        self,
        shapes: Sequence[object],
        spans: Sequence[object],
        on_drift: DriftHook | None = None,
        on_span: SpanHook | None = None,
        on_shape: ShapeHook | None = None,
    ) -> IngestReport:
        """Checks, stores, and counts one batch.

        After the whole batch is counted, `on_drift` is called once for each drift signal the
        batch raised, in batch order. A later occurrence of a signal and a rejected item call
        nothing. A call that raises is logged with names only, and the next signals still run:
        the batch is stored, so a resend would count as duplicates and never raise them again.
        Then `on_shape` is called once for each shape the batch stored, and `on_span` once for
        each span it stored, in batch order, by the same rules; a duplicate calls nothing.
        """
        raised: list[tuple[ShapeObservation, DriftSignal]] = []
        kept: list[ShapeObservation] = []
        stored: list[SpanObservation] = []

        def add_shape(observation: ShapeObservation) -> bool:
            admission = self._catalog.add_shape(observation)
            if admission.outcome is Admission.DRIFT_RAISED and admission.signal is not None:
                raised.append((observation, admission.signal))
            if admission.stored:
                kept.append(observation)
            return admission.stored

        def add_span(observation: SpanObservation) -> bool:
            added = self._catalog.add_span(observation)
            if added:
                stored.append(observation)
            return added

        report = IngestReport(
            shapes=_count(shapes, ShapeObservation, self._check_shape, add_shape),
            spans=_count(spans, SpanObservation, self._check_source, add_span),
        )
        if on_drift is not None:
            for observation, signal in raised:
                try:
                    on_drift(observation, signal)
                except Exception as error:
                    # The error's message is not logged: it may quote the shape.
                    logger.error(
                        "drift hook failed: system=%s source=%s stream=%s fingerprint=%s error=%s",
                        observation.system,
                        observation.source,
                        observation.channel.key,
                        signal.schema_hash,
                        type(error).__name__,
                    )
        if on_shape is not None:
            for observation in kept:
                try:
                    on_shape(observation)
                except Exception as error:
                    logger.error(
                        "shape hook failed: system=%s source=%s stream=%s error=%s",
                        observation.system,
                        observation.source,
                        observation.channel.key,
                        type(error).__name__,
                    )
        if on_span is not None:
            for span in stored:
                try:
                    on_span(span)
                except Exception as error:
                    logger.error(
                        "span hook failed: system=%s source=%s service=%s operation=%s error=%s",
                        span.system,
                        span.source,
                        span.service,
                        span.operation,
                        type(error).__name__,
                    )
        return report

    def _check_source(
        self, observation: ShapeObservation | SpanObservation
    ) -> RejectionReason | None:
        sources = self._sources.get(observation.system)
        if sources is None:
            return RejectionReason.UNKNOWN_SYSTEM
        if observation.source not in sources:
            return RejectionReason.UNKNOWN_SOURCE
        return None

    def _check_shape(self, observation: ShapeObservation) -> RejectionReason | None:
        reason = self._check_source(observation)
        if reason is not None:
            return reason
        if self._fingerprinter.fingerprint(observation.shape) != observation.fingerprint:
            return RejectionReason.FINGERPRINT_MISMATCH
        return None


def _count[M: BaseModel](
    items: Sequence[object],
    model: type[M],
    check: Callable[[M], RejectionReason | None],
    add: Callable[[M], bool],
) -> IngestCounts:
    accepted = duplicate = 0
    rejected: list[Rejection] = []
    for index, item in enumerate(items):
        try:
            observation = model.model_validate(item)
        except ValidationError as error:
            rejected.append(
                Rejection(index=index, reason=RejectionReason.INVALID, fields=_locations(error))
            )
            continue
        reason = check(observation)
        if reason is not None:
            rejected.append(Rejection(index=index, reason=reason))
        elif add(observation):
            accepted += 1
        else:
            duplicate += 1
    return IngestCounts(accepted=accepted, duplicate=duplicate, rejected=rejected)


def _locations(error: ValidationError) -> list[str]:
    """Dotted error locations, each once, in order; never the input that failed."""
    dotted = (
        ".".join(str(part) for part in detail["loc"])
        for detail in error.errors(include_input=False, include_context=False)
    )
    return list(dict.fromkeys(location for location in dotted if location))
