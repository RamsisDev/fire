"""Shape drift: the three rules, one signal per drifted fingerprint, and what folds silently."""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any, get_args, get_origin

import pytest
from pydantic import BaseModel, ValidationError

from fire.adapters.memory.observation_catalog import MemoryObservationCatalog
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.apps.incident_engine.wiring import log_drift
from fire.core.domain.baseline import Baseline, BaselineStatus, DriftPolicy
from fire.core.domain.dossier import Shape, ShapeField, ValueType
from fire.core.domain.drift import (
    Admission,
    DriftSignal,
    FieldRef,
    ShapeAdmission,
    ShapeDiff,
    TypeChange,
)
from fire.core.domain.observation import (
    Channel,
    ChannelDirection,
    ChannelKind,
    ChannelPart,
    ShapeObservation,
    StatusClass,
)

T0 = datetime(2026, 10, 11, 16, 30, tzinfo=UTC)
FINGERPRINTER = Sha256ShapeFingerprinter()
POLICY = DriftPolicy(min_samples=3)

ORDER_ID = ("order_id", ValueType.STRING)
ORDER_ID_INTEGER = ("order_id", ValueType.INTEGER)
TOTAL = ("total", ValueType.NUMBER)
NOTE = ("note", ValueType.STRING)
COUPON = ("coupon", ValueType.STRING)
GIFT = ("gift", ValueType.BOOLEAN)

ORDER_ID_TO_INTEGER = TypeChange(path="order_id", old=[ValueType.STRING], new=[ValueType.INTEGER])

EVENT_CHANNEL = Channel(
    kind=ChannelKind.EVENT,
    direction=ChannelDirection.INTERNAL,
    name="order.created",
    part=ChannelPart.PAYLOAD,
)
REQUEST_CHANNEL = Channel(
    kind=ChannelKind.HTTP,
    direction=ChannelDirection.INBOUND,
    name="POST /v1/orders",
    part=ChannelPart.REQUEST,
)
RESPONSE_CHANNEL = Channel(
    kind=ChannelKind.HTTP,
    direction=ChannelDirection.INBOUND,
    name="POST /v1/orders",
    part=ChannelPart.RESPONSE,
    status_class=StatusClass.SUCCESS,
)


def _shape(*pairs: tuple[str, ValueType]) -> Shape:
    return Shape(
        fields=[ShapeField(path=path, value_type=value_type) for path, value_type in pairs]
    )


def _ref(pair: tuple[str, ValueType]) -> FieldRef:
    path, value_type = pair
    return FieldRef(path=path, value_type=value_type)


def _hash(shape: Shape) -> str:
    return FINGERPRINTER.fingerprint(shape).schema_hash


def _baseline(*shapes: Shape) -> Baseline:
    baseline = Baseline()
    for shape in shapes:
        baseline.fold(_hash(shape), shape)
    return baseline


def _warm() -> Baseline:
    return _baseline(*[_shape(ORDER_ID, TOTAL)] * POLICY.min_samples)


def _state(baseline: Baseline) -> tuple[int, Counter[tuple[str, ValueType]], set[str]]:
    return baseline.samples, Counter(baseline.presence), set(baseline.fingerprints)


def _admit(
    baseline: Baseline,
    shape: Shape,
    policy: DriftPolicy = POLICY,
    observed_at: datetime = T0,
) -> ShapeAdmission:
    return baseline.admit(_hash(shape), shape, observed_at, policy)


def _raised(shape: Shape, diff: ShapeDiff, observed_at: datetime = T0) -> ShapeAdmission:
    return ShapeAdmission(
        outcome=Admission.DRIFT_RAISED,
        signal=DriftSignal(
            schema_hash=_hash(shape),
            diff=diff,
            occurrences=1,
            first_seen=observed_at,
            last_seen=observed_at,
        ),
    )


def _signal(admission: ShapeAdmission) -> DriftSignal:
    assert admission.signal is not None
    return admission.signal


def _observation(
    observation_id: str,
    shape: Shape,
    channel: Channel = EVENT_CHANNEL,
    observed_at: datetime = T0,
) -> ShapeObservation:
    return ShapeObservation(
        system="shop",
        source="orders",
        channel=channel,
        observation_id=observation_id,
        occurred_at=T0,
        observed_at=observed_at,
        shape=shape,
        fingerprint=FINGERPRINTER.fingerprint(shape),
    )


def _errors(model: type[BaseModel], data: dict[str, Any]) -> list[tuple[tuple[Any, ...], str]]:
    with pytest.raises(ValidationError) as caught:
        model.model_validate(data)
    return [(error["loc"], error["type"]) for error in caught.value.errors()]


def _dig(data: Any, where: tuple[Any, ...]) -> Any:
    for step in where:
        data = data[step]
    return data


def _leaf_types(annotation: Any) -> Iterator[Any]:
    yield annotation
    if get_origin(annotation) is not None:
        yield get_origin(annotation)
    for arg in get_args(annotation):
        yield from _leaf_types(arg)


# Rules


@pytest.mark.unit
@pytest.mark.parametrize(
    ("shape", "diff"),
    [
        (_shape(ORDER_ID, TOTAL, COUPON), ShapeDiff(added=[_ref(COUPON)])),
        (_shape(ORDER_ID_INTEGER, TOTAL), ShapeDiff(type_changed=[ORDER_ID_TO_INTEGER])),
        (_shape(ORDER_ID), ShapeDiff(removed=[_ref(TOTAL)])),
    ],
    ids=["added", "type-changed", "removed"],
)
def test_each_rule_fires_alone(shape: Shape, diff: ShapeDiff) -> None:
    baseline = _warm()
    assert baseline.diff(shape, POLICY) == diff
    assert _admit(baseline, shape) == _raised(shape, diff)


@pytest.mark.unit
def test_type_changed_has_every_seen_type_only_unseen_new_ones_and_no_removal() -> None:
    string = ("id", ValueType.STRING)
    baseline = _baseline(_shape(string, ("id", ValueType.UUID)), _shape(string), _shape(string))
    shape = _shape(("id", ValueType.NUMBER), ("id", ValueType.UUID), ("id", ValueType.INTEGER))

    assert baseline.diff(shape, POLICY) == ShapeDiff(
        type_changed=[
            TypeChange(
                path="id",
                old=[ValueType.STRING, ValueType.UUID],
                new=[ValueType.INTEGER, ValueType.NUMBER],
            )
        ]
    )


@pytest.mark.unit
def test_all_rules_fire_at_once_each_sorted_by_path_then_value_type() -> None:
    sku = ("items[*].sku", ValueType.STRING)
    status = ("status", ValueType.STRING)
    base = _shape(ORDER_ID, TOTAL, sku, status)
    baseline = _baseline(base, base, base)
    coupon_null = ("coupon", ValueType.NULL)
    shape = _shape(GIFT, ("status", ValueType.INTEGER), COUPON, ORDER_ID_INTEGER, coupon_null)

    assert baseline.diff(shape, POLICY) == ShapeDiff(
        added=[_ref(coupon_null), _ref(COUPON), _ref(GIFT)],
        type_changed=[
            ORDER_ID_TO_INTEGER,
            TypeChange(path="status", old=[ValueType.STRING], new=[ValueType.INTEGER]),
        ],
        removed=[_ref(sku), _ref(TOTAL)],
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("carried_by", "policy", "required"),
    [
        (8, DriftPolicy(min_samples=10, required_ratio=0.8), True),
        (7, DriftPolicy(min_samples=10, required_ratio=0.8), False),
        (10, DriftPolicy(min_samples=10), True),
        (9, DriftPolicy(min_samples=10), False),
    ],
    ids=["ratio-0.8-carried-by-8", "ratio-0.8-carried-by-7", "default-by-all", "default-by-9"],
)
def test_removed_fires_for_pairs_carried_by_at_least_required_ratio(
    carried_by: int, policy: DriftPolicy, required: bool
) -> None:
    baseline = _baseline(
        *[_shape(ORDER_ID, TOTAL)] * carried_by, *[_shape(ORDER_ID)] * (10 - carried_by)
    )
    probe = Shape(fields=[ShapeField(path="order_id", value_type=ValueType.STRING, optional=True)])
    expected = ShapeDiff(removed=[_ref(TOTAL)] if required else [])

    assert baseline.diff(probe, policy) == expected
    outcome = Admission.DRIFT_RAISED if required else Admission.FOLDED
    assert _admit(baseline, probe, policy).outcome is outcome


@pytest.mark.unit
def test_optional_key_variation_folds_silently() -> None:
    baseline = _baseline(
        _shape(ORDER_ID, NOTE),
        _shape(ORDER_ID, COUPON),
        _shape(ORDER_ID, COUPON, NOTE),
    )
    unseen_combination = _shape(ORDER_ID)
    assert baseline.compares(_hash(unseen_combination), POLICY) is True

    assert _admit(baseline, unseen_combination) == ShapeAdmission(outcome=Admission.FOLDED)
    assert baseline.samples == 4
    assert baseline.presence == Counter({ORDER_ID: 4, NOTE: 2, COUPON: 2})
    assert _hash(unseen_combination) in baseline.fingerprints
    assert baseline.signals == {}


@pytest.mark.unit
@pytest.mark.parametrize(
    "order_id",
    [
        ShapeField(path="order_id", value_type=ValueType.STRING, optional=True),
        ShapeField(path="order_id", value_type=ValueType.STRING, cardinality="n"),
        ShapeField(path="order_id", value_type=ValueType.STRING, cardinality=None),
    ],
    ids=["optional", "cardinality-n", "cardinality-null"],
)
def test_cardinality_and_optional_do_not_drift(order_id: ShapeField) -> None:
    baseline = _warm()
    shape = Shape(fields=[order_id, ShapeField(path="total", value_type=ValueType.NUMBER)])
    assert baseline.compares(_hash(shape), POLICY) is True

    assert _admit(baseline, shape) == ShapeAdmission(outcome=Admission.FOLDED)
    assert baseline.signals == {}


@pytest.mark.unit
def test_removed_fires_when_the_path_is_carried_only_with_another_seen_type() -> None:
    integer = ("xs[*]", ValueType.INTEGER)
    string = ("xs[*]", ValueType.STRING)
    baseline = _baseline(_shape(integer, string), _shape(integer), _shape(integer))
    shape = _shape(string)
    expected = ShapeDiff(removed=[_ref(integer)])

    assert baseline.diff(shape, POLICY) == expected
    assert _admit(baseline, shape) == _raised(shape, expected)


# Admission


@pytest.mark.unit
def test_rebuilding_baseline_folds_a_shape_that_would_fire_every_rule() -> None:
    first = _shape(ORDER_ID, TOTAL)
    drifting = _shape(ORDER_ID_INTEGER, COUPON)
    baseline = _baseline(first)
    diff = baseline.diff(drifting, POLICY)
    assert diff.added and diff.type_changed and diff.removed

    assert _admit(baseline, drifting) == ShapeAdmission(outcome=Admission.FOLDED)
    assert _admit(baseline, first).outcome is Admission.FOLDED
    assert baseline.status(POLICY) is BaselineStatus.WARM
    assert baseline.presence == Counter({ORDER_ID: 2, TOTAL: 2, ORDER_ID_INTEGER: 1, COUPON: 1})
    assert baseline.signals == {}

    absorbed = _shape(ORDER_ID_INTEGER, COUPON, TOTAL)
    assert baseline.compares(_hash(absorbed), POLICY) is True
    assert _admit(baseline, absorbed) == ShapeAdmission(outcome=Admission.FOLDED)


@pytest.mark.unit
def test_comparison_starts_after_the_observation_that_reaches_min_samples() -> None:
    baseline = _baseline(_shape(ORDER_ID, TOTAL), _shape(ORDER_ID, TOTAL))
    reaching = _shape(ORDER_ID, TOTAL, COUPON)
    assert not baseline.diff(reaching, POLICY).is_empty

    assert _admit(baseline, reaching) == ShapeAdmission(outcome=Admission.FOLDED)
    assert baseline.status(POLICY) is BaselineStatus.WARM

    after = _shape(ORDER_ID, TOTAL, GIFT)
    assert _admit(baseline, after) == _raised(after, ShapeDiff(added=[_ref(GIFT)]))


@pytest.mark.unit
def test_a_fingerprint_folded_before_folds_again_without_comparison() -> None:
    policy = DriftPolicy(min_samples=3, required_ratio=0.5)
    early = _shape(ORDER_ID)
    later = _shape(ORDER_ID, NOTE)
    baseline = Baseline()
    for shape in (early, later, later, later):
        assert _admit(baseline, shape, policy).outcome is Admission.FOLDED
    assert baseline.diff(early, policy) == ShapeDiff(removed=[_ref(NOTE)])
    assert baseline.compares(_hash(early), policy) is False

    assert _admit(baseline, early, policy) == ShapeAdmission(outcome=Admission.FOLDED)
    assert baseline.samples == 5
    assert baseline.signals == {}


@pytest.mark.unit
def test_one_drifted_fingerprint_seen_100_times_is_one_signal_of_100() -> None:
    baseline = _warm()
    before = _state(baseline)
    shape = _shape(ORDER_ID, TOTAL, COUPON)
    minutes = [(index * 37 + 50) % 100 for index in range(100)]

    admissions = [
        _admit(baseline, shape, observed_at=T0 + timedelta(minutes=minute)) for minute in minutes
    ]

    assert [admission.outcome for admission in admissions] == [
        Admission.DRIFT_RAISED,
        *[Admission.DRIFT_REPEATED] * 99,
    ]
    assert [_signal(admission).occurrences for admission in admissions] == list(range(1, 101))
    assert baseline.signals == {
        _hash(shape): DriftSignal(
            schema_hash=_hash(shape),
            diff=ShapeDiff(added=[_ref(COUPON)]),
            occurrences=100,
            first_seen=T0,
            last_seen=T0 + timedelta(minutes=99),
        )
    }
    assert _state(baseline) == before


@pytest.mark.unit
def test_a_drifted_fingerprint_is_not_folded() -> None:
    baseline = _warm()
    before = _state(baseline)
    assert _admit(baseline, _shape(ORDER_ID, TOTAL, COUPON)).outcome is Admission.DRIFT_RAISED
    assert _state(baseline) == before

    later = _shape(ORDER_ID, TOTAL, COUPON, GIFT)
    assert _admit(baseline, later) == _raised(later, ShapeDiff(added=[_ref(COUPON), _ref(GIFT)]))
    assert _state(baseline) == before


@pytest.mark.unit
def test_two_drifted_fingerprints_are_two_signals() -> None:
    baseline = _warm()
    added = _shape(ORDER_ID, TOTAL, COUPON)
    removed = _shape(ORDER_ID)
    for shape in (added, removed, added, removed, removed):
        _admit(baseline, shape)

    assert {
        schema_hash: (signal.diff, signal.occurrences)
        for schema_hash, signal in baseline.signals.items()
    } == {
        _hash(added): (ShapeDiff(added=[_ref(COUPON)]), 2),
        _hash(removed): (ShapeDiff(removed=[_ref(TOTAL)]), 3),
    }


@pytest.mark.unit
def test_admit_returns_copies_of_the_signal() -> None:
    baseline = _warm()
    shape = _shape(ORDER_ID, TOTAL, COUPON)
    raised = _signal(_admit(baseline, shape))
    repeated = _signal(_admit(baseline, shape, observed_at=T0 + timedelta(minutes=1)))
    repeated.occurrences = 50
    repeated.diff.added.clear()

    kept = baseline.signals[_hash(shape)]
    assert (raised.occurrences, raised.last_seen) == (1, T0)
    assert (kept.occurrences, kept.last_seen) == (2, T0 + timedelta(minutes=1))
    assert kept.diff == raised.diff == ShapeDiff(added=[_ref(COUPON)])


# Catalog


@pytest.mark.unit
@pytest.mark.parametrize(
    "channel",
    [EVENT_CHANNEL, REQUEST_CHANNEL, RESPONSE_CHANNEL],
    ids=["event", "request", "response"],
)
def test_rules_behave_the_same_on_every_channel(channel: Channel) -> None:
    catalog = MemoryObservationCatalog({"shop": POLICY})
    shapes = [
        _shape(ORDER_ID, TOTAL, NOTE),
        _shape(ORDER_ID, TOTAL, GIFT),
        _shape(ORDER_ID, TOTAL, NOTE, GIFT),
        _shape(ORDER_ID, TOTAL),
        _shape(ORDER_ID, TOTAL, COUPON),
        _shape(ORDER_ID, TOTAL, COUPON),
        _shape(ORDER_ID_INTEGER, TOTAL),
        _shape(ORDER_ID),
    ]

    admissions = [
        catalog.add_shape(
            _observation(f"obs-{index}", shape, channel, T0 + timedelta(minutes=index))
        )
        for index, shape in enumerate(shapes)
    ]

    added = ShapeDiff(added=[_ref(COUPON)])
    assert [
        (admission.outcome, admission.signal.diff if admission.signal else None)
        for admission in admissions
    ] == [
        *[(Admission.FOLDED, None)] * 4,
        (Admission.DRIFT_RAISED, added),
        (Admission.DRIFT_REPEATED, added),
        (Admission.DRIFT_RAISED, ShapeDiff(type_changed=[ORDER_ID_TO_INTEGER])),
        (Admission.DRIFT_RAISED, ShapeDiff(removed=[_ref(TOTAL)])),
    ]
    [stream] = catalog.streams("shop")
    assert stream.channel == channel
    assert (stream.status, stream.samples, stream.observations) == (BaselineStatus.WARM, 4, 8)
    assert [(signal.schema_hash, signal.occurrences) for signal in stream.drift] == [
        (_hash(shapes[4]), 2),
        (_hash(shapes[6]), 1),
        (_hash(shapes[7]), 1),
    ]


@pytest.mark.unit
def test_catalog_returns_signal_snapshots() -> None:
    catalog = MemoryObservationCatalog({"shop": POLICY})
    for index in range(POLICY.min_samples):
        catalog.add_shape(_observation(f"warm-{index}", _shape(ORDER_ID, TOTAL)))
    drifted = _shape(ORDER_ID, TOTAL, COUPON)

    raised = _signal(catalog.add_shape(_observation("drift-0", drifted)))
    repeated = _signal(
        catalog.add_shape(_observation("drift-1", drifted, observed_at=T0 + timedelta(minutes=1)))
    )
    repeated.occurrences = 50
    repeated.diff.added.clear()
    listed = catalog.streams("shop")[0].drift[0]
    listed.occurrences = 70
    listed.diff.added.clear()
    latest = _signal(
        catalog.add_shape(_observation("drift-2", drifted, observed_at=T0 + timedelta(minutes=2)))
    )

    assert (raised.occurrences, raised.last_seen) == (1, T0)
    assert (latest.occurrences, latest.last_seen) == (3, T0 + timedelta(minutes=2))
    assert latest.diff == raised.diff == ShapeDiff(added=[_ref(COUPON)])
    assert catalog.streams("shop")[0].drift == [latest]


# Models


SIGNAL = DriftSignal(
    schema_hash="3f1c",
    diff=ShapeDiff(added=[_ref(COUPON)]),
    occurrences=1,
    first_seen=T0,
    last_seen=T0,
)


def _admission_data() -> dict[str, Any]:
    return {
        "outcome": "drift_raised",
        "signal": {
            "schema_hash": "3f1c",
            "diff": {
                "added": [{"path": "coupon", "value_type": "string"}],
                "type_changed": [{"path": "order_id", "old": ["string"], "new": ["integer"]}],
                "removed": [{"path": "total", "value_type": "number"}],
            },
            "occurrences": 1,
            "first_seen": "2026-10-11T16:30:00Z",
            "last_seen": "2026-10-11T16:30:00Z",
        },
    }


@pytest.mark.unit
def test_admission_values() -> None:
    assert [admission.value for admission in Admission] == [
        "duplicate",
        "folded",
        "drift_raised",
        "drift_repeated",
    ]


@pytest.mark.unit
def test_admission_json_round_trip() -> None:
    admission = ShapeAdmission.model_validate(_admission_data())
    assert ShapeAdmission.model_validate_json(admission.model_dump_json()) == admission


@pytest.mark.unit
@pytest.mark.parametrize(
    "outcome", [Admission.DRIFT_RAISED, Admission.DRIFT_REPEATED], ids=["raised", "repeated"]
)
def test_a_drift_outcome_needs_its_signal(outcome: Admission) -> None:
    with pytest.raises(ValidationError, match="a drift outcome carries its signal"):
        ShapeAdmission(outcome=outcome)


@pytest.mark.unit
@pytest.mark.parametrize(
    "outcome", [Admission.DUPLICATE, Admission.FOLDED], ids=["duplicate", "folded"]
)
def test_no_other_outcome_carries_a_signal(outcome: Admission) -> None:
    with pytest.raises(ValidationError, match="no other outcome does"):
        ShapeAdmission(outcome=outcome, signal=SIGNAL)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("admission", "stored"),
    [
        (ShapeAdmission(outcome=Admission.DUPLICATE), False),
        (ShapeAdmission(outcome=Admission.FOLDED), True),
        (ShapeAdmission(outcome=Admission.DRIFT_RAISED, signal=SIGNAL), True),
        (ShapeAdmission(outcome=Admission.DRIFT_REPEATED, signal=SIGNAL), True),
    ],
    ids=["duplicate", "folded", "drift-raised", "drift-repeated"],
)
def test_stored_is_false_only_for_a_duplicate(admission: ShapeAdmission, stored: bool) -> None:
    assert admission.stored is stored


@pytest.mark.unit
@pytest.mark.parametrize("key", ["value", "payload"])
@pytest.mark.parametrize(
    "where",
    [
        (),
        ("signal",),
        ("signal", "diff"),
        ("signal", "diff", "added", 0),
        ("signal", "diff", "type_changed", 0),
        ("signal", "diff", "removed", 0),
    ],
    ids=["admission", "signal", "diff", "added", "type-changed", "removed"],
)
def test_drift_models_reject_unknown_keys_at_every_depth(key: str, where: tuple[Any, ...]) -> None:
    data = _admission_data()
    _dig(data, where)[key] = "planted-value"
    assert _errors(ShapeAdmission, data) == [((*where, key), "extra_forbidden")]


@pytest.mark.unit
@pytest.mark.parametrize("model", [FieldRef, TypeChange, ShapeDiff, DriftSignal, ShapeAdmission])
def test_drift_models_have_no_open_types(model: type[BaseModel]) -> None:
    for name, field in model.model_fields.items():
        leaves = list(_leaf_types(field.annotation))
        assert Any not in leaves, name
        assert dict not in leaves, name


@pytest.mark.unit
@pytest.mark.parametrize("field", ["old", "new"])
def test_type_change_needs_a_type_on_each_side(field: str) -> None:
    data = {"path": "order_id", "old": ["string"], "new": ["integer"], field: []}
    assert _errors(TypeChange, data) == [((field,), "too_short")]


@pytest.mark.unit
@pytest.mark.parametrize("occurrences", [0, -1], ids=["zero", "negative"])
def test_drift_signal_needs_an_occurrence(occurrences: int) -> None:
    data = {**_admission_data()["signal"], "occurrences": occurrences}
    assert _errors(DriftSignal, data) == [(("occurrences",), "greater_than_equal")]


@pytest.mark.unit
@pytest.mark.parametrize("field", ["first_seen", "last_seen"])
def test_drift_signal_rejects_naive_datetimes(field: str) -> None:
    data = {**_admission_data()["signal"], field: "2026-10-11T16:30:00"}
    assert _errors(DriftSignal, data) == [((field,), "timezone_aware")]


@pytest.mark.unit
def test_empty_diff_is_empty() -> None:
    assert ShapeDiff().is_empty
    assert ShapeDiff() == ShapeDiff(added=[], type_changed=[], removed=[])


@pytest.mark.unit
@pytest.mark.parametrize(
    "diff",
    [
        ShapeDiff(added=[_ref(COUPON)]),
        ShapeDiff(type_changed=[ORDER_ID_TO_INTEGER]),
        ShapeDiff(removed=[_ref(TOTAL)]),
    ],
    ids=["added", "type-changed", "removed"],
)
def test_a_diff_with_any_rule_is_not_empty(diff: ShapeDiff) -> None:
    assert not diff.is_empty


# Log line


@pytest.mark.unit
def test_log_drift_writes_one_info_line_with_counts_and_no_paths(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    email = ("customer.email", ValueType.EMAIL)
    shape = _shape(email, COUPON, ORDER_ID_INTEGER)
    signal = DriftSignal(
        schema_hash=_hash(shape),
        diff=ShapeDiff(
            added=[_ref(COUPON), _ref(email)],
            type_changed=[ORDER_ID_TO_INTEGER],
            removed=[_ref(TOTAL)],
        ),
        occurrences=1,
        first_seen=T0,
        last_seen=T0,
    )

    log_drift(_observation("drift-0", shape), signal)

    assert caplog.record_tuples == [
        (
            "fire.apps.incident_engine.wiring",
            logging.INFO,
            "shape drift raised: system=shop source=orders "
            f"stream=event:internal:order.created:payload fingerprint={_hash(shape)} "
            "added=2 type_changed=1 removed=1",
        )
    ]
    for path in ("customer.email", "coupon", "order_id", "total"):
        assert path not in caplog.text
