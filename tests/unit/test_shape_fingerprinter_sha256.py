"""Sha256ShapeFingerprinter: stable, order-independent id of a Shape."""

from __future__ import annotations

import pytest

from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.core.domain.dossier import Shape, ShapeField, ValueType
from fire.core.ports.shape_fingerprinter import ShapeFingerprinter

FIELDS = [
    ShapeField(path="order.id", value_type=ValueType.STRING),
    ShapeField(path="order.lines", value_type=ValueType.ARRAY, cardinality="n"),
    ShapeField(path="order.note", value_type=ValueType.NULL, cardinality=None, optional=True),
    ShapeField(path="order.note", value_type=ValueType.STRING, optional=True),
]


def _hash(fields: list[ShapeField]) -> str:
    return Sha256ShapeFingerprinter().fingerprint(Shape(fields=fields)).schema_hash


@pytest.mark.unit
def test_satisfies_port() -> None:
    assert isinstance(Sha256ShapeFingerprinter(), ShapeFingerprinter)


@pytest.mark.unit
def test_hash_is_hex_sha256_and_deterministic() -> None:
    first = _hash(FIELDS)
    assert len(first) == 64
    assert int(first, 16) >= 0
    assert _hash(FIELDS) == first


@pytest.mark.unit
def test_hash_ignores_field_order() -> None:
    assert _hash(list(reversed(FIELDS))) == _hash(FIELDS)


@pytest.mark.unit
@pytest.mark.parametrize(
    "changed",
    [
        ShapeField(path="order.ref", value_type=ValueType.STRING),
        ShapeField(path="order.id", value_type=ValueType.UUID),
        ShapeField(path="order.id", value_type=ValueType.STRING, cardinality="n"),
        ShapeField(path="order.id", value_type=ValueType.STRING, optional=True),
    ],
    ids=["path", "value_type", "cardinality", "optional"],
)
def test_hash_changes_with_any_field_attribute(changed: ShapeField) -> None:
    assert _hash([changed, *FIELDS[1:]]) != _hash(FIELDS)


@pytest.mark.unit
def test_empty_shape_has_a_fingerprint() -> None:
    fingerprint = Sha256ShapeFingerprinter().fingerprint(Shape())
    assert len(fingerprint.schema_hash) == 64
    assert fingerprint.normalized_paths == []


@pytest.mark.unit
def test_normalized_paths_are_sorted_and_unique() -> None:
    fingerprint = Sha256ShapeFingerprinter().fingerprint(Shape(fields=list(reversed(FIELDS))))
    assert fingerprint.normalized_paths == ["order.id", "order.lines", "order.note"]


@pytest.mark.unit
def test_duplicate_fields_with_null_and_digit_cardinality_sort() -> None:
    fields = [
        ShapeField(path="a", value_type=ValueType.STRING, cardinality="1"),
        ShapeField(path="a", value_type=ValueType.STRING, cardinality=None),
    ]
    assert _hash(fields) == _hash(list(reversed(fields)))


@pytest.mark.unit
def test_payloads_of_one_form_share_a_fingerprint() -> None:
    generalizer = FlatStarShapeGeneralizer()
    fingerprinter = Sha256ShapeFingerprinter()
    first = {"order_id": "A-1", "lines": [{"sku": "X", "qty": 1}]}
    second = {"lines": [{"qty": 7, "sku": "Y"}, {"sku": "Z", "qty": 2}], "order_id": "B-22"}
    other = {"order_id": "A-1", "lines": [{"sku": "X", "qty": 1.5}]}
    hashes = [
        fingerprinter.fingerprint(generalizer.generalize(payload)).schema_hash
        for payload in (first, second, other)
    ]
    assert hashes[0] == hashes[1]
    assert hashes[0] != hashes[2]
