"""Capture pipeline: one capture pair to outbound request and response
ShapeObservations.

Only a document's `body` and a response's `status_code` are read. Nothing here keeps a key or a
document, or puts one in an exception. Contract: docs/specs/tentacle.md#kinds (`s3_capture`).
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from fire.core.domain.capture import (
    CaptureIndex,
    CaptureLayout,
    SpanRef,
    checked_action,
    read_capture_key,
)
from fire.core.domain.observation import (
    Channel,
    ChannelDirection,
    ChannelKind,
    ChannelPart,
    ShapeObservation,
    StatusClass,
)
from fire.core.ports.capture_source import ObjectState, RawCapture
from fire.core.ports.shape_fingerprinter import ShapeFingerprinter
from fire.core.ports.shape_generalizer import ShapeGeneralizer
from fire.core.services.record_pipeline import LOCAL_DEDUPE_WINDOW, _RecentIds, sampled_in


class CaptureRejectReason(StrEnum):
    """Why a whole pair gives nothing."""

    NO_LAYOUT = "no_layout"
    MISSING_ACTION = "missing_action"
    BAD_ACTION = "bad_action"


class CaptureSkipReason(StrEnum):
    """`sampled_out` and `local_duplicate` once per pair; the others once per part."""

    SAMPLED_OUT = "sampled_out"
    LOCAL_DUPLICATE = "local_duplicate"
    OBJECT_MISSING = "object_missing"
    OBJECT_UNDECODABLE = "object_undecodable"
    OBJECT_TOO_LARGE = "object_too_large"
    NO_BODY = "no_body"
    BODY_NOT_JSON = "body_not_json"
    BODY_NOT_CONTAINER = "body_not_container"
    STATUS_UNCLASSIFIED = "status_unclassified"
    SHAPING_FAILED = "shaping_failed"


class CaptureNote(StrEnum):
    UNJOINED = "unjoined"


_OBJECT_SKIPS = {
    ObjectState.MISSING: CaptureSkipReason.OBJECT_MISSING,
    ObjectState.UNDECODABLE: CaptureSkipReason.OBJECT_UNDECODABLE,
    ObjectState.TOO_LARGE: CaptureSkipReason.OBJECT_TOO_LARGE,
}


@dataclass(frozen=True)
class CaptureOutcome:
    """What one pair gave: why it was rejected or skipped whole, else the shapes of its parts,
    why a part gave none, in part order, and what was counted without dropping anything.
    """

    shapes: tuple[ShapeObservation, ...] = ()
    rejected: CaptureRejectReason | None = None
    skipped: tuple[CaptureSkipReason, ...] = ()
    noted: tuple[CaptureNote, ...] = ()


@dataclass(frozen=True)
class _Pair:
    """What a pair's key and metadata give; never the key itself."""

    pair_id: str
    action: str
    work_id: str | None
    occurred_at: datetime


class _Reject(Exception):
    """Carries a reason code only, never the value that failed."""

    def __init__(self, reason: CaptureRejectReason) -> None:
        super().__init__(reason.value)
        self.reason = reason


class _Skip(Exception):
    """Carries a reason code only, never the document that failed."""

    def __init__(self, reason: CaptureSkipReason) -> None:
        super().__init__(reason.value)
        self.reason = reason


class CapturePipeline:
    """Turns one source's capture pairs into ShapeObservations; the first failing pair check
    decides.

    The pair checks: the key fits its rule's layout, the action is present and a name, the
    pair id `{label}:{work_id or -}:{action}:{stamp}` is sampled in, and it is not among the
    source's last `dedupe_window` pair ids. A pair then gives its request part, then its
    response part. A part is shaped when its object was found and its `body` is a JSON object
    or array, as decoded or as JSON text. A response needs a `status_code` from 200 to 599;
    one with no `status_code` is the empty `none` shape. The channel is
    `http:outbound:{label}:{action}:{part}`, `occurred_at` is the key's time, `work_id` is the
    key's work segment when it fits `work_id_pattern`, and both shapes carry the ids of the
    span the index holds for the response key, else for the request key.
    """

    def __init__(
        self,
        *,
        system: str,
        source: str,
        sample_rate: float,
        work_id_pattern: re.Pattern[str] | None,
        index: CaptureIndex | None,
        generalizer: ShapeGeneralizer,
        fingerprinter: ShapeFingerprinter,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        dedupe_window: int = LOCAL_DEDUPE_WINDOW,
    ) -> None:
        self._system = system
        self._source = source
        self._sample_rate = sample_rate
        self._work_id_pattern = work_id_pattern
        self._index = index
        self._generalizer = generalizer
        self._fingerprinter = fingerprinter
        self._clock = clock
        self._recent = _RecentIds(dedupe_window)

    def process(self, raw: RawCapture) -> CaptureOutcome:
        """Never raises; sampling hashes the pair id, so both parts are kept or dropped."""
        try:
            pair = self._pair(raw)
        except _Reject as reject:
            return CaptureOutcome(rejected=reject.reason)
        if not sampled_in(pair.pair_id, self._sample_rate):
            return CaptureOutcome(skipped=(CaptureSkipReason.SAMPLED_OUT,))
        if self._recent.seen(pair.pair_id):
            return CaptureOutcome(skipped=(CaptureSkipReason.LOCAL_DUPLICATE,))
        span = self._join(raw)
        shapes: list[ShapeObservation] = []
        skipped: list[CaptureSkipReason] = []
        for part in (ChannelPart.REQUEST, ChannelPart.RESPONSE):
            outcome = self._part(raw, pair, span, part)
            if isinstance(outcome, ShapeObservation):
                shapes.append(outcome)
            else:
                skipped.append(outcome)
        noted = (CaptureNote.UNJOINED,) if shapes and span is None else ()
        return CaptureOutcome(shapes=tuple(shapes), skipped=tuple(skipped), noted=noted)

    def _pair(self, raw: RawCapture) -> _Pair:
        """Checks in order: the key's layout, then the action."""
        key = read_capture_key(raw.name, raw.rule.layout)
        if key is None:
            raise _Reject(CaptureRejectReason.NO_LAYOUT)
        if raw.rule.layout is CaptureLayout.WORK_UNIT_ACTION:
            action = key.action
        else:
            action = raw.metadata_action
        if action is None:
            raise _Reject(CaptureRejectReason.MISSING_ACTION)
        if checked_action(action) is None:
            raise _Reject(CaptureRejectReason.BAD_ACTION)
        pattern = self._work_id_pattern
        work_id = key.work if pattern is None or pattern.fullmatch(key.work) else None
        return _Pair(
            pair_id=f"{raw.rule.label}:{work_id or '-'}:{action}:{key.stamp}",
            action=action,
            work_id=work_id,
            occurred_at=key.at,
        )

    def _join(self, raw: RawCapture) -> SpanRef | None:
        if self._index is None:
            return None
        now = self._clock()
        return self._index.get(raw.response_key, now) or self._index.get(raw.request_key, now)

    def _part(
        self, raw: RawCapture, pair: _Pair, span: SpanRef | None, part: ChannelPart
    ) -> ShapeObservation | CaptureSkipReason:
        try:
            if part is ChannelPart.REQUEST:
                status_class = None
                value = _json_body(_document(raw.request_state, raw.request))
            else:
                status_class, value = _response(_document(raw.response_state, raw.response))
            shape = self._generalizer.generalize(value)
            return ShapeObservation(
                system=self._system,
                source=self._source,
                channel=Channel(
                    kind=ChannelKind.HTTP,
                    direction=ChannelDirection.OUTBOUND,
                    name=f"{raw.rule.label}:{pair.action}",
                    part=part,
                    status_class=status_class,
                ),
                observation_id=f"{pair.pair_id}:{part.value}",
                occurred_at=pair.occurred_at,
                observed_at=self._clock(),
                trace_id=None if span is None else span.trace_id,
                span_id=None if span is None else span.span_id,
                work_id=pair.work_id,
                shape=shape,
                fingerprint=self._fingerprinter.fingerprint(shape),
            )
        except _Skip as skip:
            return skip.reason
        except Exception:
            # The text may quote the body, so it is dropped here, unread.
            return CaptureSkipReason.SHAPING_FAILED


def _document(state: ObjectState, document: Mapping[str, Any] | None) -> Mapping[str, Any]:
    """The decoded object of a half its source found."""
    if state is not ObjectState.FOUND:
        raise _Skip(_OBJECT_SKIPS[state])
    if document is None:
        raise _Skip(CaptureSkipReason.SHAPING_FAILED)
    return document


def _response(document: Mapping[str, Any]) -> tuple[StatusClass, Any]:
    """The status class and the value to shape. With no `status_code` that is `none` and an
    empty object, and `body` is not read.
    """
    status_code = document.get("status_code")
    if status_code is None:
        return StatusClass.NO_RESPONSE, {}
    if not isinstance(status_code, int) or isinstance(status_code, bool):
        raise _Skip(CaptureSkipReason.STATUS_UNCLASSIFIED)
    status_class = StatusClass.of(status_code)
    if status_class is None:
        raise _Skip(CaptureSkipReason.STATUS_UNCLASSIFIED)
    return status_class, _json_body(document)


def _json_body(document: Mapping[str, Any]) -> Any:
    """The document's `body` as a JSON object or array.

    Checks in order: present, parsed when it is text, a container.
    """
    body = document.get("body")
    if body is None:
        raise _Skip(CaptureSkipReason.NO_BODY)
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except (ValueError, RecursionError):
            raise _Skip(CaptureSkipReason.BODY_NOT_JSON) from None
    if not isinstance(body, dict | list):
        raise _Skip(CaptureSkipReason.BODY_NOT_CONTAINER)
    return body
