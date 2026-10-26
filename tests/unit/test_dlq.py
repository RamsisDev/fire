"""DLQ attribution: the join of abandoned work units to fingerprints, the rule, re-firing, the
unattributed work units, and the trigger event a fingerprint opens.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pydantic import ValidationError
from spans import Clock

from fire.adapters.memory.observation_catalog import MemoryObservationCatalog
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.apps.incident_engine.wiring import log_dlq, log_dlq_unattributed
from fire.core.domain.baseline import DriftPolicy
from fire.core.domain.dlq import (
    DlqTrigger,
    FingerprintOutcomes,
    StreamOutcomes,
    WorkOutcomes,
    WorkRef,
    tally_outcomes,
)
from fire.core.domain.dossier import EvidenceQuality, Shape, ShapeField, ValueType
from fire.core.domain.incident import Trigger
from fire.core.domain.observation import (
    Channel,
    ChannelDirection,
    ChannelKind,
    ChannelPart,
    ShapeObservation,
)
from fire.core.services.dlq import DlqService

T0 = datetime(2026, 10, 11, 16, 30, tzinfo=UTC)
SYSTEM = "shop"
EVENTS = "order-events"
OUTCOMES = "task-outcomes"
CHARGED = "event:internal:order.charged:payload"
SHIPPED = "event:internal:order.shipped:payload"
BILLING = "outcome:internal:billing-processor:payload"
NOTIFIER = "outcome:internal:notifier:payload"
FINGERPRINTER = Sha256ShapeFingerprinter()
WINDOW_S = 3600.0


def _ref(source: str, key: str, schema_hash: str, work_id: str, minute: int = 0) -> WorkRef:
    return WorkRef(source, key, schema_hash, work_id, T0 + timedelta(minutes=minute))


# The tally


@pytest.mark.unit
def test_tally_joins_abandoned_work_units_to_the_fingerprints_that_carried_them() -> None:
    refs = [
        *(_ref(EVENTS, CHARGED, "h-a", f"w-{n}") for n in range(4)),
        _ref(EVENTS, CHARGED, "h-b", "w-4"),
        _ref(EVENTS, SHIPPED, "h-c", "w-0"),
        _ref(OUTCOMES, BILLING, "o-1", "w-0", minute=5),
        _ref(OUTCOMES, BILLING, "o-1", "w-1", minute=2),
        _ref(OUTCOMES, NOTIFIER, "o-2", "w-1", minute=9),
        _ref(OUTCOMES, BILLING, "o-1", "w-9", minute=1),
    ]

    outcomes = tally_outcomes(refs, {OUTCOMES})

    assert outcomes == WorkOutcomes(
        abandoned=3,
        unattributed=["w-9"],
        fingerprints=[
            FingerprintOutcomes(
                source=EVENTS,
                stream=CHARGED,
                schema_hash="h-a",
                observed=4,
                abandoned=2,
                abandoned_by={BILLING: 2, NOTIFIER: 1},
                first_abandoned=T0 + timedelta(minutes=2),
                last_abandoned=T0 + timedelta(minutes=5),
            ),
            FingerprintOutcomes(
                source=EVENTS,
                stream=SHIPPED,
                schema_hash="h-c",
                observed=1,
                abandoned=1,
                abandoned_by={BILLING: 1},
                first_abandoned=T0 + timedelta(minutes=5),
                last_abandoned=T0 + timedelta(minutes=5),
            ),
        ],
        streams=[
            StreamOutcomes(source=EVENTS, stream=CHARGED, observed=5, abandoned=2),
            StreamOutcomes(source=EVENTS, stream=SHIPPED, observed=1, abandoned=1),
        ],
    )
    assert [f.rate for f in outcomes.fingerprints] == [0.5, 1.0]


@pytest.mark.unit
def test_tally_counts_work_units_once_and_sources_apart() -> None:
    refs = [
        _ref(EVENTS, CHARGED, "h-a", "w-1"),
        _ref(EVENTS, CHARGED, "h-a", "w-1"),
        _ref("kinesis-events", CHARGED, "h-a", "w-1"),
        _ref(OUTCOMES, BILLING, "o-1", "w-1", minute=3),
        _ref("other-outcomes", BILLING, "o-1", "w-1", minute=1),
    ]

    outcomes = tally_outcomes(refs, {OUTCOMES, "other-outcomes"})

    assert outcomes.abandoned == 1
    assert [(f.source, f.observed, f.abandoned) for f in outcomes.fingerprints] == [
        ("kinesis-events", 1, 1),
        (EVENTS, 1, 1),
    ]
    assert outcomes.fingerprints[0].first_abandoned == T0 + timedelta(minutes=1)


@pytest.mark.unit
def test_tally_without_outcomes_lists_nothing() -> None:
    assert tally_outcomes([_ref(EVENTS, CHARGED, "h-a", "w-1")], {OUTCOMES}) == WorkOutcomes()
    assert tally_outcomes([], set()) == WorkOutcomes()


@pytest.mark.unit
def test_an_outcome_source_never_counts_as_observed() -> None:
    outcomes = tally_outcomes([_ref(OUTCOMES, BILLING, "o-1", "w-1")], {OUTCOMES})
    assert (outcomes.abandoned, outcomes.unattributed, outcomes.fingerprints) == (1, ["w-1"], [])


# The rule


def _fingerprint(observed: int, abandoned: int) -> FingerprintOutcomes:
    return FingerprintOutcomes(
        source=EVENTS,
        stream=CHARGED,
        schema_hash="h-a",
        observed=observed,
        abandoned=abandoned,
        abandoned_by={BILLING: abandoned},
        first_abandoned=T0,
        last_abandoned=T0,
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("observed", "abandoned", "crosses"),
    [(15, 3, True), (16, 3, False), (2, 2, False), (3, 3, True), (100, 50, True)],
    ids=["at-both", "rate-just-below", "abandoned-below", "all-abandoned", "above-both"],
)
def test_the_rule_needs_both_min_abandoned_and_min_rate(
    observed: int, abandoned: int, crosses: bool
) -> None:
    assert DlqTrigger().crosses(_fingerprint(observed, abandoned)) is crosses


@pytest.mark.unit
def test_trigger_defaults_and_models_reject_unknown_fields() -> None:
    assert DlqTrigger().model_dump() == {"min_abandoned": 3, "min_rate": 0.2}
    for model, data in (
        (DlqTrigger, {"window_s": 1}),
        (
            StreamOutcomes,
            {"source": EVENTS, "stream": CHARGED, "observed": 1, "abandoned": 0, "x": 1},
        ),
        (WorkOutcomes, {"values": []}),
    ):
        with pytest.raises(ValidationError):
            model.model_validate(data)
    dumped = _fingerprint(4, 1).model_dump(mode="json")
    assert dumped["rate"] == 0.25
    with pytest.raises(ValidationError):
        FingerprintOutcomes.model_validate({**dumped, "payload": {}})


# The service


def _channel(key_kind: ChannelKind, name: str) -> Channel:
    return Channel(
        kind=key_kind,
        direction=ChannelDirection.INTERNAL,
        name=name,
        part=ChannelPart.PAYLOAD,
    )


def _shape(paths: tuple[str, ...]) -> Shape:
    return Shape(fields=[ShapeField(path=path, value_type=ValueType.STRING) for path in paths])


def _event(
    work_id: str, paths: tuple[str, ...] = ("order_id",), **overrides: Any
) -> ShapeObservation:
    shape = _shape(paths)
    data: dict[str, Any] = {
        "system": SYSTEM,
        "source": EVENTS,
        "channel": _channel(ChannelKind.EVENT, "order.charged"),
        "observation_id": work_id,
        "work_id": work_id,
        "occurred_at": T0,
        "observed_at": T0,
        "shape": shape,
        "fingerprint": FINGERPRINTER.fingerprint(shape),
    }
    data.update(overrides)
    return ShapeObservation(**data)


def _abandoned(
    work_id: str, processor: str = "billing-processor", **overrides: Any
) -> ShapeObservation:
    return _event(
        work_id,
        ("source_queue_arn",),
        source=OUTCOMES,
        channel=_channel(ChannelKind.OUTCOME, processor),
        occurred_at=T0 + timedelta(hours=1),
        **overrides,
    )


class _Body:
    """A memory catalog and the DLQ service on it, on a hand-moved clock."""

    def __init__(
        self,
        trigger: DlqTrigger | None = None,
        *,
        policy: DriftPolicy | None = None,
        retention_s: float = WINDOW_S,
    ) -> None:
        self.clock = Clock(T0)
        self.catalog = MemoryObservationCatalog(
            {SYSTEM: policy or DriftPolicy()}, clock=self.clock, retention_s=retention_s
        )
        self.service = DlqService(
            self.catalog,
            FINGERPRINTER,
            {SYSTEM: {OUTCOMES}, "quiet": set()},
            {} if trigger is None else {SYSTEM: trigger},
            window_s=retention_s,
        )

    def add(self, *observations: ShapeObservation) -> list[ShapeObservation]:
        stored = [o for o in observations if self.catalog.add_shape(o).stored]
        return [o for o in stored if self.service.abandoned(o)]

    def abandon(self, *work_ids: str) -> list[FingerprintOutcomes]:
        abandoned = self.add(*(_abandoned(work_id) for work_id in work_ids))
        return self.service.evaluate(SYSTEM, abandoned).raised


TRIGGER = DlqTrigger(min_abandoned=2, min_rate=0.5)


@pytest.mark.unit
def test_abandoned_are_the_shapes_of_outcome_sources_that_name_a_work_unit() -> None:
    service = _Body().service
    assert service.abandoned(_abandoned("w-1"))
    assert not service.abandoned(_abandoned("w-1").model_copy(update={"work_id": None}))
    assert not service.abandoned(_event("w-1"))
    assert not service.abandoned(_abandoned("w-1", system="quiet"))
    assert not service.abandoned(_abandoned("w-1", system="unknown"))


@pytest.mark.unit
def test_a_fingerprint_raises_once_when_it_crosses_the_rule() -> None:
    body = _Body(TRIGGER)
    body.add(*(_event(f"w-{n}") for n in range(4)))

    assert body.abandon("w-0") == []
    [raised] = body.abandon("w-1")
    assert (raised.stream, raised.observed, raised.abandoned) == (CHARGED, 4, 2)
    assert raised.schema_hash == FINGERPRINTER.fingerprint(_shape(("order_id",))).schema_hash
    assert body.abandon("w-2") == []
    assert body.abandon("w-1") == []


@pytest.mark.unit
def test_a_fingerprint_raises_again_only_after_an_evaluation_finds_it_below_the_rule() -> None:
    body = _Body(TRIGGER)
    body.add(*(_event(f"w-{n}") for n in range(4)))
    assert len(body.abandon("w-0", "w-1")) == 1

    body.add(*(_event(f"w-{n}") for n in range(4, 10)))
    assert body.abandon("w-other") == []
    body.add(*(_event(f"w-{n}") for n in range(10, 20)))
    assert body.abandon(*(f"w-{n}" for n in range(2, 9))) == []
    assert len(body.abandon(*(f"w-{n}" for n in range(9, 12)))) == 1


@pytest.mark.unit
def test_two_fingerprints_raise_apart_and_a_system_without_the_trigger_raises_none() -> None:
    body = _Body(DlqTrigger(min_abandoned=1, min_rate=0.5))
    body.add(_event("w-1"), _event("w-2", ("order_id", "coupon")))
    raised = body.abandon("w-1", "w-2")
    assert [f.observed for f in raised] == [1, 1]
    assert len({f.schema_hash for f in raised}) == 2

    quiet = _Body()
    quiet.add(_event("w-1"))
    assert quiet.abandon("w-1") == []
    assert [f.abandoned for f in quiet.service.report(SYSTEM).fingerprints] == [1]


@pytest.mark.unit
def test_evaluate_returns_the_batch_work_units_no_observation_carried() -> None:
    body = _Body(TRIGGER)
    body.add(_event("w-1"))
    abandoned = body.add(_abandoned("w-1"), _abandoned("w-9"), _abandoned("w-8"))

    evaluation = body.service.evaluate(SYSTEM, abandoned)

    assert [o.work_id for o in evaluation.unattributed] == ["w-9", "w-8"]
    assert evaluation.outcomes.unattributed == ["w-8", "w-9"]
    assert evaluation.outcomes.abandoned == 3


@pytest.mark.unit
def test_counts_keep_the_retention_window() -> None:
    body = _Body(DlqTrigger(min_abandoned=1, min_rate=0.5), retention_s=60)
    body.add(_event("w-1"))
    body.clock.advance(59)
    assert len(body.abandon("w-1")) == 1
    body.clock.advance(1)
    report = body.service.report(SYSTEM)
    assert (report.abandoned, report.unattributed, report.fingerprints) == (1, ["w-1"], [])


@pytest.mark.unit
def test_report_names_the_sources_window_rule_and_what_is_firing() -> None:
    body = _Body(TRIGGER)
    body.add(*(_event(f"w-{n}") for n in range(3)), _event("w-3", ("coupon",)))
    body.abandon("w-0", "w-1", "w-3", "w-9")

    report = body.service.report(SYSTEM)

    assert (report.sources, report.window_s, report.rule) == ([OUTCOMES], WINDOW_S, TRIGGER)
    assert (report.abandoned, report.unattributed) == (4, ["w-9"])
    hashes = {
        paths: FINGERPRINTER.fingerprint(_shape(paths)).schema_hash
        for paths in (("order_id",), ("coupon",))
    }
    assert {f.schema_hash: (f.observed, f.abandoned, f.firing) for f in report.fingerprints} == {
        hashes[("order_id",)]: (3, 2, True),
        hashes[("coupon",)]: (1, 1, False),
    }
    assert [f.schema_hash for f in report.fingerprints] == sorted(hashes.values())
    assert report.streams == [
        StreamOutcomes(source=EVENTS, stream=CHARGED, observed=4, abandoned=3)
    ]
    assert report.missing_reason is None


@pytest.mark.unit
def test_report_of_a_system_without_an_outcome_source_says_so() -> None:
    service = _Body().service
    for system in ("quiet", "unknown"):
        report = service.report(system)
        assert report.missing_reason == "No source declares outcome: abandoned"
        assert report.sources == []


# The trigger event


def _raise(body: _Body, *work_ids: str) -> tuple[FingerprintOutcomes, WorkOutcomes]:
    abandoned = body.add(*(_abandoned(work_id) for work_id in work_ids))
    evaluation = body.service.evaluate(SYSTEM, abandoned)
    [raised] = evaluation.raised
    return raised, evaluation.outcomes


@pytest.mark.unit
def test_the_trigger_event_holds_the_attribution_the_shape_and_a_missing_process_path() -> None:
    body = _Body(TRIGGER)
    body.add(*(_event(f"w-{n}") for n in range(3)), _event("w-3", ("coupon",)))
    raised, outcomes = _raise(body, "w-0", "w-1")

    event = body.service.trigger_event(SYSTEM, raised, outcomes)

    shape = _shape(("order_id",))
    assert (event.system, event.trigger, event.service) == (SYSTEM, Trigger.DLQ, None)
    assert event.summary == f"Abandoned work units on {CHARGED}"
    assert event.started_at == T0 + timedelta(hours=1)
    assert event.shape == shape
    assert event.fingerprint == FINGERPRINTER.fingerprint(shape)
    assert event.process_path.hops == []
    attribution, path = event.fragments
    assert attribution.collector == "dlq_attribution"
    assert attribution.quality is EvidenceQuality.PRESENT
    schema_hash = raised.schema_hash
    assert attribution.summary == (
        f"2 of 3 work units with fingerprint {schema_hash} on {CHARGED} abandoned (67%)"
    )
    assert attribution.data == {
        "source": EVENTS,
        "stream": CHARGED,
        "schema_hash": schema_hash,
        "observed": 3,
        "abandoned": 2,
        "rate": 2 / 3,
        "abandoned_by": {BILLING: 2},
        "first_abandoned": "2026-10-11T17:30:00Z",
        "last_abandoned": "2026-10-11T17:30:00Z",
        "drifted": False,
        "stream_observed": 4,
        "stream_abandoned": 2,
        "min_abandoned": 2,
        "min_rate": 0.5,
        "window_s": WINDOW_S,
    }
    assert (path.collector, path.quality, path.summary) == (
        "process_path",
        EvidenceQuality.MISSING,
        "No flow of the abandoned work units",
    )


@pytest.mark.unit
def test_the_trigger_event_declares_unattributed_work_units_missing() -> None:
    body = _Body(TRIGGER)
    body.add(_event("w-0"), _event("w-1"))
    raised, outcomes = _raise(body, "w-0", "w-1", "w-8", "w-9")

    event = body.service.trigger_event(SYSTEM, raised, outcomes)

    assert [f.collector for f in event.fragments] == [
        "dlq_attribution",
        "dlq_unattributed",
        "process_path",
    ]
    unattributed = event.fragments[1]
    assert unattributed.quality is EvidenceQuality.MISSING
    assert unattributed.summary == (
        "2 of 4 abandoned work units carry no observation: no source read their event"
    )
    assert unattributed.data == {"abandoned": 4, "unattributed": 2}


@pytest.mark.unit
def test_the_attribution_says_whether_the_fingerprint_drifted() -> None:
    body = _Body(TRIGGER, policy=DriftPolicy(min_samples=2))
    body.add(_event("w-0"), _event("w-1"), _event("w-2", ("coupon",)), _event("w-3", ("coupon",)))
    raised, outcomes = _raise(body, "w-2", "w-3")

    event = body.service.trigger_event(SYSTEM, raised, outcomes)

    assert event.fragments[0].data["drifted"] is True
    assert event.shape == _shape(("coupon",))


# Log lines


@pytest.mark.unit
def test_log_lines_name_ids_and_counts(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    log_dlq(SYSTEM, _fingerprint(4, 3))
    log_dlq_unattributed(_abandoned("w-9"))
    assert [message for _, _, message in caplog.record_tuples] == [
        f"dlq attribution raised: system=shop source={EVENTS} stream={CHARGED} fingerprint=h-a "
        "abandoned=3 observed=4 rate=0.750",
        f"dlq unattributed: system=shop source={OUTCOMES} outcome={BILLING} work_id=w-9",
    ]
