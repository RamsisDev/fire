"""RecordPipeline: envelope paths, rejections, sampling, local dedupe, and the observation."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest

from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.core.domain.envelope import Envelope
from fire.core.domain.observation import ChannelDirection, ChannelKind, ShapeObservation
from fire.core.ports.record_source import RawRecord, RecordSkipReason
from fire.core.services.record_pipeline import (
    RecordPipeline,
    Rejected,
    RejectReason,
    Skipped,
    SkipReason,
    sampled_in,
)

NOW = datetime(2026, 10, 11, 16, 30, 1, tzinfo=UTC)
WORK_UNIT_ENVELOPE = {
    "event_id": "work_unit_id",
    "event_type": "event_type",
    "occurred_at": "metadata.timestamp",
    "work_id": "work_unit_id",
    "correlation_id": "metadata.correlation_id",
    "payload": "payload",
}


def _record(**overrides: Any) -> dict[str, Any]:
    record: dict[str, Any] = {
        "work_unit_id": "wu-1",
        "event_type": "fulfillment.allocated",
        "payload": {"order_id": "o-1", "items": [{"sku": "s-1", "quantity": 1}]},
        "metadata": {"timestamp": "2026-10-11T16:30:00+00:00", "correlation_id": "c-1"},
    }
    record.update(overrides)
    return record


def _pipeline(
    envelope: dict[str, Any] | None = None,
    *,
    sample_rate: float = 1.0,
    direction: ChannelDirection = ChannelDirection.INTERNAL,
    dedupe_window: int = 100_000,
) -> RecordPipeline:
    return RecordPipeline(
        system="shop",
        source="work-units",
        direction=direction,
        envelope=Envelope.model_validate(envelope or WORK_UNIT_ENVELOPE),
        sample_rate=sample_rate,
        generalizer=FlatStarShapeGeneralizer(),
        fingerprinter=Sha256ShapeFingerprinter(),
        clock=lambda: NOW,
        dedupe_window=dedupe_window,
    )


def _process(record: dict[str, Any] | None, pipeline: RecordPipeline | None = None, **kw: Any):
    raw = RawRecord(source="work-units", region=kw.get("region"), record=record)
    return (pipeline or _pipeline()).process(raw)


def _built(record: dict[str, Any], pipeline: RecordPipeline | None = None) -> ShapeObservation:
    outcome = _process(record, pipeline)
    assert isinstance(outcome, ShapeObservation), outcome
    return outcome


@pytest.mark.unit
def test_builds_an_event_observation_from_the_envelope() -> None:
    record = _record()
    observation = _process(record, region="us-west-2")

    assert isinstance(observation, ShapeObservation)
    assert observation.system == "shop"
    assert observation.source == "work-units"
    assert observation.channel.key == "event:internal:fulfillment.allocated:payload"
    assert observation.observation_id == "wu-1"
    assert observation.work_id == "wu-1"
    assert observation.correlation_id == "c-1"
    assert observation.region == "us-west-2"
    assert observation.occurred_at == datetime(2026, 10, 11, 16, 30, tzinfo=UTC)
    assert observation.observed_at == NOW
    assert observation.trace_id is None and observation.span_id is None
    shape = FlatStarShapeGeneralizer().generalize(record["payload"])
    assert observation.shape == shape
    assert observation.fingerprint == Sha256ShapeFingerprinter().fingerprint(shape)


@pytest.mark.unit
def test_channel_direction_comes_from_the_source() -> None:
    observation = _built(_record(), _pipeline(direction=ChannelDirection.INBOUND))
    assert observation.channel.key == "event:inbound:fulfillment.allocated:payload"


@pytest.mark.unit
def test_an_outcome_source_builds_outcome_channels() -> None:
    pipeline = RecordPipeline(
        system="shop",
        source="task-outcomes",
        direction=ChannelDirection.INTERNAL,
        envelope=Envelope.model_validate(WORK_UNIT_ENVELOPE),
        sample_rate=1.0,
        generalizer=FlatStarShapeGeneralizer(),
        fingerprinter=Sha256ShapeFingerprinter(),
        kind=ChannelKind.OUTCOME,
        clock=lambda: NOW,
    )

    observation = _built(_record(event_type="billing-processor"), pipeline)

    assert observation.channel.key == "outcome:internal:billing-processor:payload"
    assert observation.work_id == "wu-1"
    assert _process(_record(work_unit_id="wu-2", metadata={}), pipeline) == Rejected(
        RejectReason.MISSING_OCCURRED_AT
    )
    no_work = {key: value for key, value in WORK_UNIT_ENVELOPE.items() if key != "work_id"}
    pipeline = RecordPipeline(
        system="shop",
        source="task-outcomes",
        direction=ChannelDirection.INTERNAL,
        envelope=Envelope.model_validate({**no_work, "work_id": "metadata.work"}),
        sample_rate=1.0,
        generalizer=FlatStarShapeGeneralizer(),
        fingerprinter=Sha256ShapeFingerprinter(),
        kind=ChannelKind.OUTCOME,
    )
    assert _process(_record(), pipeline) == Rejected(RejectReason.MISSING_WORK_ID)


@pytest.mark.unit
def test_optional_ids_may_be_absent() -> None:
    observation = _built(_record(metadata={"timestamp": "2026-10-11T16:30:00Z"}))
    assert observation.correlation_id is None
    envelope = {key: value for key, value in WORK_UNIT_ENVELOPE.items() if key != "work_id"}
    assert _built(_record(), _pipeline(envelope)).work_id is None


@pytest.mark.unit
def test_undecodable_record() -> None:
    assert _process(None) == Rejected(RejectReason.UNDECODABLE)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("record", "reason"),
    [
        ({"work_unit_id": None}, RejectReason.MISSING_EVENT_ID),
        ({"event_type": None}, RejectReason.MISSING_EVENT_TYPE),
        ({"metadata": {}}, RejectReason.MISSING_OCCURRED_AT),
        ({"metadata": "2026-10-11"}, RejectReason.MISSING_OCCURRED_AT),
        ({"metadata": {"timestamp": None}}, RejectReason.MISSING_OCCURRED_AT),
        ({"payload": None}, RejectReason.MISSING_PAYLOAD),
    ],
    ids=["event-id", "event-type", "occurred-at", "parent-not-object", "null-ts", "payload"],
)
def test_null_or_absent_required_value_is_missing(
    record: dict[str, Any], reason: RejectReason
) -> None:
    assert _process(_record(**record)) == Rejected(reason)
    [key] = record
    absent = _record()
    del absent[key]
    assert _process(absent) == Rejected(reason)


@pytest.mark.unit
def test_first_failing_check_decides() -> None:
    record = _record(work_unit_id=None, event_type="bad name", payload=None)
    assert _process(record) == Rejected(RejectReason.MISSING_EVENT_ID)
    record = _record(event_type="bad name", metadata={"timestamp": "never"})
    assert _process(record) == Rejected(RejectReason.BAD_NAME)


@pytest.mark.unit
@pytest.mark.parametrize(
    "overrides",
    [{"work_unit_id": 42}, {"work_unit_id": ""}, {"work_unit_id": ["wu-1"]}],
    ids=["integer", "empty", "list"],
)
def test_ids_are_non_empty_strings(overrides: dict[str, Any]) -> None:
    assert _process(_record(**overrides)) == Rejected(RejectReason.BAD_ID)


@pytest.mark.unit
def test_optional_id_that_is_not_a_string_is_bad() -> None:
    record = _record(metadata={"timestamp": "2026-10-11T16:30:00Z", "correlation_id": 7})
    assert _process(record) == Rejected(RejectReason.BAD_ID)


@pytest.mark.unit
def test_strip_prefix() -> None:
    envelope = {
        **WORK_UNIT_ENVELOPE,
        "event_id": {"path": "SK", "strip_prefix": "EVENT#"},
        "work_id": {"path": "SK", "strip_prefix": "EVENT#"},
    }
    pipeline = _pipeline(envelope)
    observation = _built(_record(SK="EVENT#01J9Z3K4N8"), pipeline)
    assert (observation.observation_id, observation.work_id) == ("01J9Z3K4N8", "01J9Z3K4N8")

    for value in ["ORDER#1", "event#01J9", 42]:
        assert _process(_record(SK=value), pipeline) == Rejected(RejectReason.MISSING_EVENT_ID)
    assert _process(_record(SK="EVENT#"), pipeline) == Rejected(RejectReason.BAD_ID)


@pytest.mark.unit
@pytest.mark.parametrize(
    "event_type",
    ["fulfillment.allocated", "a", "Order:Created/v2_x-1", "a" * 128],
    ids=["dotted", "single", "symbols", "longest"],
)
def test_event_type_names(event_type: str) -> None:
    assert _built(_record(event_type=event_type)).channel.name == event_type


@pytest.mark.unit
@pytest.mark.parametrize(
    "event_type",
    ["has space", ".leading", "-leading", "a" * 129, "line\n", "", 42, {"name": "x"}, "café"],
    ids=["space", "dot", "dash", "long", "newline", "empty", "int", "object", "non-ascii"],
)
def test_bad_event_type_names(event_type: Any) -> None:
    assert _process(_record(event_type=event_type)) == Rejected(RejectReason.BAD_NAME)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("timestamp", "expected"),
    [
        ("2026-10-11T16:30:00Z", datetime(2026, 10, 11, 16, 30, tzinfo=UTC)),
        ("2026-10-11T16:30:00", datetime(2026, 10, 11, 16, 30, tzinfo=UTC)),
        (
            "2026-10-11T11:30:00-05:00",
            datetime(2026, 10, 11, 11, 30, tzinfo=timezone(timedelta(hours=-5))),
        ),
        ("2026-10-11", datetime(2026, 10, 11, tzinfo=UTC)),
    ],
    ids=["z", "naive-is-utc", "offset", "date"],
)
def test_iso_timestamps(timestamp: str, expected: datetime) -> None:
    occurred_at = _built(_record(metadata={"timestamp": timestamp})).occurred_at
    assert occurred_at == expected
    assert occurred_at.utcoffset() == expected.utcoffset()


@pytest.mark.unit
@pytest.mark.parametrize(
    "timestamp", ["never", "2026-13-01", 1760000000, True], ids=["text", "month", "int", "bool"]
)
def test_bad_iso_timestamps(timestamp: Any) -> None:
    record = _record(metadata={"timestamp": timestamp})
    assert _process(record) == Rejected(RejectReason.BAD_TIMESTAMP)


@pytest.mark.unit
@pytest.mark.parametrize(("unit", "value"), [("ms", 1760185800000), ("s", 1760185800)])
def test_epoch_timestamps(unit: str, value: int) -> None:
    envelope = {**WORK_UNIT_ENVELOPE, "occurred_at": {"path": "ts", "epoch": unit}}
    observation = _built(_record(ts=value), _pipeline(envelope))
    assert observation.occurred_at == datetime(2025, 10, 11, 12, 30, tzinfo=UTC)


@pytest.mark.unit
@pytest.mark.parametrize(
    "value",
    ["1760185800000", 1760185800000.5, True, 10**30],
    ids=["text", "float", "bool", "huge"],
)
def test_bad_epoch_timestamps(value: Any) -> None:
    envelope = {**WORK_UNIT_ENVELOPE, "occurred_at": {"path": "ts", "epoch": "ms"}}
    assert _process(_record(ts=value), _pipeline(envelope)) == Rejected(RejectReason.BAD_TIMESTAMP)


@pytest.mark.unit
@pytest.mark.parametrize("payload", [[1, 2], "text", 0, False, {}], ids=str)
def test_any_non_null_payload_is_generalized(payload: Any) -> None:
    observation = _built(_record(payload=payload))
    assert observation.shape == FlatStarShapeGeneralizer().generalize(payload)


@pytest.mark.unit
def test_observation_that_fails_validation_is_invalid() -> None:
    assert _process(_record(work_unit_id="w" * 257)) == Rejected(RejectReason.INVALID)


@pytest.mark.unit
def test_unencodable_strings_are_invalid() -> None:
    surrogate_id = json.loads('{"id": "wu-\\ud800"}')["id"]
    assert _process(_record(work_unit_id=surrogate_id)) == Rejected(RejectReason.INVALID)
    surrogate_key = json.loads('{"k\\udc00": 1}')
    assert _process(_record(payload=surrogate_key)) == Rejected(RejectReason.INVALID)


@pytest.mark.unit
def test_payload_too_deep_to_generalize_is_invalid() -> None:
    payload: Any = 1
    for _ in range(5000):
        payload = {"a": payload}
    assert _process(_record(payload=payload)) == Rejected(RejectReason.INVALID)


def _hash_fraction(observation_id: str) -> float:
    """The first 16 hex digits of the SHA-256 hex digest, over 2^64."""
    return int(hashlib.sha256(observation_id.encode()).hexdigest()[:16], 16) / 2**64


@pytest.mark.unit
def test_sampling_follows_the_first_eight_bytes_of_sha256() -> None:
    ids = [f"wu-{index}" for index in range(200)]
    for observation_id in ids:
        assert sampled_in(observation_id, 1.0)
        for rate in (0.1, 0.25, 0.5, 0.9):
            assert sampled_in(observation_id, rate) == (_hash_fraction(observation_id) < rate)
    kept = sum(sampled_in(observation_id, 0.25) for observation_id in ids)
    assert 20 < kept < 80


@pytest.mark.unit
def test_sampled_out_record_is_skipped_the_same_way_everywhere() -> None:
    observation_id = next(f"wu-{n}" for n in range(1000) if _hash_fraction(f"wu-{n}") > 0.5)
    pipeline = _pipeline(sample_rate=0.5)
    for region in ["us-east-1", "us-west-2", None]:
        outcome = _process(_record(work_unit_id=observation_id), pipeline, region=region)
        assert outcome == Skipped(SkipReason.SAMPLED_OUT)


@pytest.mark.unit
def test_local_dedupe_skips_a_recent_repeat() -> None:
    pipeline = _pipeline()
    _built(_record(), pipeline)
    assert _process(_record(), pipeline, region="us-west-2") == Skipped(SkipReason.LOCAL_DUPLICATE)


@pytest.mark.unit
def test_one_event_from_two_regions_of_a_source_is_one_observation() -> None:
    pipeline = _pipeline()
    built = _built(_record(), pipeline)
    again = _process(_record(), pipeline, region="us-west-2")

    assert built.region is None
    assert again == Skipped(SkipReason.LOCAL_DUPLICATE)


@pytest.mark.unit
def test_one_event_from_two_sources_is_two_observations() -> None:
    first = _built(_record())
    second = RecordPipeline(
        system="shop",
        source="work-units-stream",
        direction=ChannelDirection.INTERNAL,
        envelope=Envelope.model_validate(WORK_UNIT_ENVELOPE),
        sample_rate=1.0,
        generalizer=FlatStarShapeGeneralizer(),
        fingerprinter=Sha256ShapeFingerprinter(),
    ).process(RawRecord(source="work-units-stream", region="us-east-1", record=_record()))

    assert isinstance(second, ShapeObservation)
    assert (first.source, second.source) == ("work-units", "work-units-stream")
    assert first.observation_id == second.observation_id
    assert first.channel == second.channel


@pytest.mark.unit
@pytest.mark.parametrize("skip", list(RecordSkipReason))
def test_a_record_the_source_skipped_is_skipped_for_its_reason_first(
    skip: RecordSkipReason,
) -> None:
    pipeline = _pipeline()
    raw = RawRecord(source="work-units", region="us-east-1", record=None, skipped=skip)

    assert pipeline.process(raw) == Skipped(skip)
    _built(_record(), pipeline)


@pytest.mark.unit
def test_a_skipped_record_carries_no_object() -> None:
    with pytest.raises(ValueError) as caught:
        RawRecord(
            source="work-units",
            region=None,
            record={"secret": "planted-value"},
            skipped=RecordSkipReason.FILTERED,
        )
    assert "planted-value" not in str(caught.value)


@pytest.mark.unit
def test_a_forgotten_id_is_built_again() -> None:
    pipeline = _pipeline()
    _built(_record(), pipeline)
    duplicate = _process(_record(), pipeline)
    assert duplicate == Skipped(SkipReason.LOCAL_DUPLICATE)
    assert isinstance(duplicate, Skipped) and duplicate.observation_id == "wu-1"

    pipeline.forget("wu-1")

    _built(_record(), pipeline)


@pytest.mark.unit
def test_local_dedupe_window_drops_the_oldest_id() -> None:
    pipeline = _pipeline(dedupe_window=2)
    for observation_id in ["a", "b", "c"]:
        _built(_record(work_unit_id=observation_id), pipeline)
    assert _process(_record(work_unit_id="c"), pipeline) == Skipped(SkipReason.LOCAL_DUPLICATE)
    _built(_record(work_unit_id="a"), pipeline)


@pytest.mark.unit
def test_raw_record_repr_omits_the_record() -> None:
    raw = RawRecord(source="work-units", region="us-east-1", record={"secret": "planted-value"})
    assert "planted-value" not in repr(raw)
    assert "secret" not in repr(raw)
