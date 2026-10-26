"""Record pipeline: one raw record to a ShapeObservation, or the reason it was not built.

Nothing here keeps a record or puts one in an exception. Contract:
docs/specs/tentacle.md#record-pipeline.
"""

from __future__ import annotations

import hashlib
import re
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

from fire.core.domain.envelope import Envelope, EpochUnit, TimestampPath, ValuePath, lookup
from fire.core.domain.observation import (
    Channel,
    ChannelDirection,
    ChannelKind,
    ChannelPart,
    ShapeObservation,
)
from fire.core.ports.record_source import RawRecord, RecordSkipReason
from fire.core.ports.shape_fingerprinter import ShapeFingerprinter
from fire.core.ports.shape_generalizer import ShapeGeneralizer

EVENT_TYPE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}")
LOCAL_DEDUPE_WINDOW = 100_000

_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_EPOCH_UNITS = {
    EpochUnit.MILLISECONDS: timedelta(milliseconds=1),
    EpochUnit.SECONDS: timedelta(seconds=1),
}


class RejectReason(StrEnum):
    UNDECODABLE = "undecodable"
    MISSING_EVENT_ID = "missing_event_id"
    MISSING_EVENT_TYPE = "missing_event_type"
    MISSING_OCCURRED_AT = "missing_occurred_at"
    MISSING_PAYLOAD = "missing_payload"
    MISSING_WORK_ID = "missing_work_id"
    BAD_ID = "bad_id"
    BAD_NAME = "bad_name"
    BAD_TIMESTAMP = "bad_timestamp"
    INVALID = "invalid"


class SkipReason(StrEnum):
    SAMPLED_OUT = "sampled_out"
    LOCAL_DUPLICATE = "local_duplicate"


@dataclass(frozen=True)
class Rejected:
    reason: RejectReason


@dataclass(frozen=True)
class Skipped:
    reason: SkipReason | RecordSkipReason
    # The id a `local_duplicate` repeats.
    observation_id: str | None = field(default=None, compare=False)


type Outcome = ShapeObservation | Rejected | Skipped


def sampled_in(observation_id: str, sample_rate: float) -> bool:
    """The first 8 bytes of SHA-256 of the id, as an integer over 2^64, fall below the rate.

    Every region and source makes the same choice for one id. Compared as integers, so a rate
    of 1 keeps every id.
    """
    digest = hashlib.sha256(observation_id.encode(errors="surrogatepass")).digest()
    return int.from_bytes(digest[:8], "big") < sample_rate * 2**64


class _Reject(Exception):
    """Carries a reason code only, never the value that failed."""

    def __init__(self, reason: RejectReason) -> None:
        super().__init__(reason.value)
        self.reason = reason


@dataclass(frozen=True)
class _EnvelopeValues:
    event_id: str
    event_type: str
    occurred_at: datetime
    payload: Any
    work_id: str | None
    correlation_id: str | None


class _RecentIds:
    """The last `size` distinct ids, oldest dropped first."""

    def __init__(self, size: int) -> None:
        self._size = size
        self._ids: OrderedDict[str, None] = OrderedDict()

    def seen(self, observation_id: str) -> bool:
        """Whether the id is among the recent ones; remembers it when it is not."""
        if observation_id in self._ids:
            return True
        self._ids[observation_id] = None
        if len(self._ids) > self._size:
            self._ids.popitem(last=False)
        return False

    def forget(self, observation_id: str) -> None:
        self._ids.pop(observation_id, None)


class RecordPipeline:
    """Turns one source's raw records into ShapeObservations; the first failing step decides.

    The steps: the source did not skip the record; the record decoded to an object; its
    envelope values are present and well formed; its id is sampled in; the id is not among the
    source's last `dedupe_window` ids, whatever region it was read from; and the observation
    built from its shape validates. The channel is `{kind}:{direction}:{event_type}:payload`
    and `observed_at` is the clock's time. An `outcome` record without a `work_id` names no
    work unit, so it is rejected.
    """

    def __init__(
        self,
        *,
        system: str,
        source: str,
        direction: ChannelDirection,
        envelope: Envelope,
        sample_rate: float,
        generalizer: ShapeGeneralizer,
        fingerprinter: ShapeFingerprinter,
        kind: ChannelKind = ChannelKind.EVENT,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        dedupe_window: int = LOCAL_DEDUPE_WINDOW,
    ) -> None:
        self._system = system
        self._source = source
        self._kind = kind
        self._direction = direction
        self._envelope = envelope
        self._sample_rate = sample_rate
        self._generalizer = generalizer
        self._fingerprinter = fingerprinter
        self._clock = clock
        self._recent = _RecentIds(dedupe_window)

    def process(self, raw: RawRecord) -> Outcome:
        if raw.skipped is not None:
            return Skipped(raw.skipped)
        if raw.record is None:
            return Rejected(RejectReason.UNDECODABLE)
        try:
            values = _read_envelope(self._envelope, raw.record)
        except _Reject as reject:
            return Rejected(reject.reason)
        if self._kind is ChannelKind.OUTCOME and values.work_id is None:
            return Rejected(RejectReason.MISSING_WORK_ID)
        if not sampled_in(values.event_id, self._sample_rate):
            return Skipped(SkipReason.SAMPLED_OUT)
        if self._recent.seen(values.event_id):
            return Skipped(SkipReason.LOCAL_DUPLICATE, values.event_id)
        try:
            return self._build(raw, values)
        except (ValueError, RecursionError):
            # A pydantic ValidationError quotes its input, so it is dropped here, unread.
            return Rejected(RejectReason.INVALID)

    def forget(self, observation_id: str) -> None:
        """Drops an id from the recent ones, so its next copy is built again: the body never
        accepted the observation built from it.
        """
        self._recent.forget(observation_id)

    def _build(self, raw: RawRecord, values: _EnvelopeValues) -> ShapeObservation:
        shape = self._generalizer.generalize(values.payload)
        return ShapeObservation(
            system=self._system,
            source=self._source,
            channel=Channel(
                kind=self._kind,
                direction=self._direction,
                name=values.event_type,
                part=ChannelPart.PAYLOAD,
            ),
            observation_id=values.event_id,
            occurred_at=values.occurred_at,
            observed_at=self._clock(),
            region=raw.region,
            work_id=values.work_id,
            correlation_id=values.correlation_id,
            shape=shape,
            fingerprint=self._fingerprinter.fingerprint(shape),
        )


def _read(record: Mapping[str, Any], spec: ValuePath) -> Any:
    """The value at `spec.path`, prefix stripped; None when absent, null, or unprefixed."""
    value = lookup(record, spec.path)
    if spec.strip_prefix is None or value is None:
        return value
    if not isinstance(value, str) or not value.startswith(spec.strip_prefix):
        return None
    return value.removeprefix(spec.strip_prefix)


def _checked_id(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise _Reject(RejectReason.BAD_ID)
    return value


def _required_id(record: Mapping[str, Any], spec: ValuePath, missing: RejectReason) -> str:
    value = _read(record, spec)
    if value is None:
        raise _Reject(missing)
    return _checked_id(value)


def _optional_id(record: Mapping[str, Any], spec: ValuePath | None) -> str | None:
    value = None if spec is None else _read(record, spec)
    return None if value is None else _checked_id(value)


def _read_event_type(record: Mapping[str, Any], spec: ValuePath) -> str:
    value = _read(record, spec)
    if value is None:
        raise _Reject(RejectReason.MISSING_EVENT_TYPE)
    if not isinstance(value, str) or EVENT_TYPE.fullmatch(value) is None:
        raise _Reject(RejectReason.BAD_NAME)
    return value


def _read_timestamp(record: Mapping[str, Any], spec: TimestampPath) -> datetime:
    """ISO 8601, read as UTC when it has no offset; or an integer in `spec.epoch` units."""
    value = _read(record, spec)
    if value is None:
        raise _Reject(RejectReason.MISSING_OCCURRED_AT)
    if spec.epoch is not None:
        if not isinstance(value, int) or isinstance(value, bool):
            raise _Reject(RejectReason.BAD_TIMESTAMP)
        try:
            return _UNIX_EPOCH + value * _EPOCH_UNITS[spec.epoch]
        except OverflowError:
            raise _Reject(RejectReason.BAD_TIMESTAMP) from None
    if not isinstance(value, str):
        raise _Reject(RejectReason.BAD_TIMESTAMP)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise _Reject(RejectReason.BAD_TIMESTAMP) from None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _read_envelope(envelope: Envelope, record: Mapping[str, Any]) -> _EnvelopeValues:
    """Checks in order: event_id, event_type, occurred_at, payload, work_id, correlation_id."""
    event_id = _required_id(record, envelope.event_id, RejectReason.MISSING_EVENT_ID)
    event_type = _read_event_type(record, envelope.event_type)
    occurred_at = _read_timestamp(record, envelope.occurred_at)
    payload = lookup(record, envelope.payload.path)
    if payload is None:
        raise _Reject(RejectReason.MISSING_PAYLOAD)
    return _EnvelopeValues(
        event_id=event_id,
        event_type=event_type,
        occurred_at=occurred_at,
        payload=payload,
        work_id=_optional_id(record, envelope.work_id),
        correlation_id=_optional_id(record, envelope.correlation_id),
    )
