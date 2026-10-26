"""DriftPolicy, BaselineStatus, and Baseline: what a stream key folded, and when it is warm."""

from __future__ import annotations

from collections import Counter

import pytest
from pydantic import ValidationError

from fire.core.domain.baseline import Baseline, BaselineStatus, DriftPolicy
from fire.core.domain.dossier import Shape, ShapeField, ValueType

ORDER_ID = ("order_id", ValueType.STRING)
TOTAL = ("total", ValueType.NUMBER)


def _shape(*pairs: tuple[str, ValueType]) -> Shape:
    return Shape(
        fields=[ShapeField(path=path, value_type=value_type) for path, value_type in pairs]
    )


def _folded(samples: int, schema_hash: str = "known") -> Baseline:
    baseline = Baseline()
    for _ in range(samples):
        baseline.fold(schema_hash, _shape(ORDER_ID))
    return baseline


@pytest.mark.unit
def test_drift_policy_defaults() -> None:
    policy = DriftPolicy()
    assert (policy.min_samples, policy.required_ratio) == (20, 1.0)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("key", "value"),
    [("min_samples", 1), ("required_ratio", 1.0), ("required_ratio", 0.01)],
    ids=["min-samples-one", "ratio-one", "ratio-small"],
)
def test_drift_policy_accepts_edge_values(key: str, value: float) -> None:
    assert getattr(DriftPolicy.model_validate({key: value}), key) == value


@pytest.mark.unit
@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("min_samples", 0),
        ("min_samples", -1),
        ("required_ratio", 0),
        ("required_ratio", -0.5),
        ("required_ratio", 1.5),
        ("required_ratio", float("nan")),
        ("required_ratio", float("inf")),
    ],
    ids=[
        "min-samples-zero",
        "min-samples-negative",
        "ratio-zero",
        "ratio-negative",
        "ratio-above-one",
        "ratio-nan",
        "ratio-inf",
    ],
)
def test_drift_policy_rejects_out_of_range_values(key: str, value: float) -> None:
    with pytest.raises(ValidationError) as caught:
        DriftPolicy.model_validate({key: value})
    assert [error["loc"] for error in caught.value.errors()] == [(key,)]


@pytest.mark.unit
def test_drift_policy_rejects_unknown_keys() -> None:
    with pytest.raises(ValidationError) as caught:
        DriftPolicy.model_validate({"min_sample": 5})
    assert [error["loc"] for error in caught.value.errors()] == [("min_sample",)]


@pytest.mark.unit
def test_baseline_status_values() -> None:
    assert [status.value for status in BaselineStatus] == ["baseline rebuilding", "warm"]


@pytest.mark.unit
def test_empty_baseline_is_rebuilding() -> None:
    baseline = Baseline()
    assert (baseline.samples, baseline.presence, baseline.fingerprints) == (0, Counter(), set())
    assert baseline.status(DriftPolicy(min_samples=1)) is BaselineStatus.REBUILDING


@pytest.mark.unit
def test_fold_counts_a_sample_and_each_pair_it_carried() -> None:
    baseline = Baseline()
    baseline.fold("both", _shape(ORDER_ID, TOTAL))
    baseline.fold("order-only", _shape(ORDER_ID))

    assert baseline.samples == 2
    assert baseline.presence == Counter({ORDER_ID: 2, TOTAL: 1})
    assert baseline.fingerprints == {"both", "order-only"}


@pytest.mark.unit
def test_fold_counts_a_pair_listed_twice_once() -> None:
    shape = Shape(
        fields=[
            ShapeField(path="order_id", value_type=ValueType.STRING),
            ShapeField(path="order_id", value_type=ValueType.STRING),
            ShapeField(
                path="order_id", value_type=ValueType.STRING, cardinality="n", optional=True
            ),
        ]
    )
    baseline = Baseline()
    baseline.fold("repeated", shape)

    assert baseline.samples == 1
    assert baseline.presence == Counter({ORDER_ID: 1})


@pytest.mark.unit
def test_fold_counts_each_value_type_at_one_path_separately() -> None:
    integer = ("total", ValueType.INTEGER)
    baseline = Baseline()
    baseline.fold("mixed", _shape(integer, TOTAL))
    baseline.fold("integer", _shape(integer))

    assert baseline.presence == Counter({integer: 2, TOTAL: 1})


@pytest.mark.unit
def test_fold_of_an_empty_shape_counts_a_sample() -> None:
    baseline = Baseline()
    baseline.fold("empty", Shape())

    assert baseline.samples == 1
    assert baseline.presence == Counter()
    assert baseline.fingerprints == {"empty"}


@pytest.mark.unit
def test_folding_a_known_fingerprint_counts_again() -> None:
    baseline = _folded(2)

    assert baseline.samples == 2
    assert baseline.presence == Counter({ORDER_ID: 2})
    assert baseline.fingerprints == {"known"}


@pytest.mark.unit
@pytest.mark.parametrize("min_samples", [1, 2, 20])
def test_status_turns_warm_at_min_samples(min_samples: int) -> None:
    policy = DriftPolicy(min_samples=min_samples)
    baseline = _folded(min_samples - 1)
    assert baseline.status(policy) is BaselineStatus.REBUILDING

    baseline.fold("known", _shape(ORDER_ID))
    assert baseline.status(policy) is BaselineStatus.WARM

    baseline.fold("known", _shape(ORDER_ID))
    assert baseline.status(policy) is BaselineStatus.WARM


@pytest.mark.unit
def test_status_follows_the_policy_it_is_given() -> None:
    baseline = _folded(5)
    assert baseline.status(DriftPolicy(min_samples=5)) is BaselineStatus.WARM
    assert baseline.status(DriftPolicy()) is BaselineStatus.REBUILDING


@pytest.mark.unit
def test_rebuilding_baseline_compares_nothing() -> None:
    policy = DriftPolicy(min_samples=3)
    baseline = _folded(2)

    assert baseline.compares("new", policy) is False
    assert baseline.compares("known", policy) is False


@pytest.mark.unit
def test_warm_baseline_compares_only_new_fingerprints() -> None:
    policy = DriftPolicy(min_samples=3)
    baseline = _folded(3)

    assert baseline.compares("known", policy) is False
    assert baseline.compares("new", policy) is True
    assert baseline == _folded(3)

    baseline.fold("new", _shape(ORDER_ID, TOTAL))
    assert baseline.compares("new", policy) is False
