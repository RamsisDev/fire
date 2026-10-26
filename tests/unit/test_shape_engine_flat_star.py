"""FlatStarShapeGeneralizer against docs/specs/shape-generalizer.md."""

from __future__ import annotations

from collections.abc import Iterator
from itertools import permutations, product

import pytest

from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.core.domain.dossier import Shape, ValueType
from fire.core.ports.shape_generalizer import ShapeGeneralizer

type Row = tuple[str, str, str | None, bool]


def _paths(shape: Shape) -> dict[str, tuple[str, object]]:
    return {field.path: (field.value_type.value, field.cardinality) for field in shape.fields}


def _rows(shape: Shape) -> list[Row]:
    return [
        (field.path, field.value_type.value, field.cardinality, field.optional)
        for field in shape.fields
    ]


def _leaf_type(engine: FlatStarShapeGeneralizer, value: object) -> str:
    [field] = engine.generalize({"x": value}).fields
    return field.value_type.value


def _reorderings(value: object) -> Iterator[object]:
    """Every arrangement of `value` with array elements and keys permuted at every depth."""
    if isinstance(value, dict):
        for items in permutations(value.items()):
            keys = [key for key, _ in items]
            for children in product(*(_reorderings(child) for _, child in items)):
                yield dict(zip(keys, children, strict=True))
    elif isinstance(value, list):
        for elements in permutations(value):
            for children in product(*(_reorderings(element) for element in elements)):
                yield list(children)
    else:
        yield value


@pytest.fixture
def engine() -> FlatStarShapeGeneralizer:
    return FlatStarShapeGeneralizer()


@pytest.mark.unit
def test_port_protocol(engine: FlatStarShapeGeneralizer) -> None:
    assert isinstance(engine, ShapeGeneralizer)


@pytest.mark.unit
def test_empty_object_root(engine: FlatStarShapeGeneralizer) -> None:
    assert engine.generalize({}).fields == []


@pytest.mark.unit
def test_empty_array_root(engine: FlatStarShapeGeneralizer) -> None:
    assert _rows(engine.generalize([])) == [("", "array", "0", False)]


@pytest.mark.unit
def test_root_array_of_objects(engine: FlatStarShapeGeneralizer) -> None:
    assert _rows(engine.generalize([{"a": 1}, {"a": 2}])) == [
        ("", "array", "n", False),
        ("[*]", "object", "1", False),
        ("[*].a", "integer", "1", False),
    ]


@pytest.mark.unit
def test_root_scalar(engine: FlatStarShapeGeneralizer) -> None:
    assert _rows(engine.generalize("x")) == [("", "string", "1", False)]


@pytest.mark.unit
def test_null_leaf(engine: FlatStarShapeGeneralizer) -> None:
    assert _rows(engine.generalize({"a": None})) == [("a", "null", None, False)]


@pytest.mark.unit
def test_homogeneous_order_lines(engine: FlatStarShapeGeneralizer) -> None:
    payload = {
        "order_id": "x",
        "lines": [{"sku": "a", "qty": 1}, {"sku": "b", "qty": 2}],
    }
    assert _rows(engine.generalize(payload)) == [
        ("lines", "array", "n", False),
        ("lines[*]", "object", "1", False),
        ("lines[*].qty", "integer", "1", False),
        ("lines[*].sku", "string", "1", False),
        ("order_id", "string", "1", False),
    ]


@pytest.mark.unit
def test_key_missing_from_one_element_is_optional(engine: FlatStarShapeGeneralizer) -> None:
    payload = {"lines": [{"sku": "a"}, {"sku": "b", "gift": True}]}
    assert _rows(engine.generalize(payload)) == [
        ("lines", "array", "n", False),
        ("lines[*]", "object", "1", False),
        ("lines[*].gift", "boolean", "1", True),
        ("lines[*].sku", "string", "1", False),
    ]


@pytest.mark.unit
def test_element_order_does_not_change_the_shape(engine: FlatStarShapeGeneralizer) -> None:
    first = engine.generalize({"lines": [{"sku": "a"}, {"sku": "b", "gift": True}]})
    swapped = engine.generalize({"lines": [{"sku": "b", "gift": True}, {"sku": "a"}]})
    assert swapped == first


@pytest.mark.unit
def test_array_length_does_not_change_the_shape(engine: FlatStarShapeGeneralizer) -> None:
    def order(line_count: int) -> dict[str, object]:
        lines = [{"sku": f"sku-{index}", "qty": index} for index in range(line_count)]
        return {"order_id": "x", "lines": lines}

    shapes = [engine.generalize(order(line_count)) for line_count in (1, 2, 3, 10)]
    assert all(shape == shapes[0] for shape in shapes)


@pytest.mark.unit
def test_array_cardinality_is_zero_only_when_every_sample_is_empty(
    engine: FlatStarShapeGeneralizer,
) -> None:
    assert _paths(engine.generalize({"xs": []})) == {"xs": ("array", "0")}
    assert _paths(engine.generalize({"xs": [[], []]})) == {
        "xs": ("array", "n"),
        "xs[*]": ("array", "0"),
    }


@pytest.mark.unit
def test_mixed_scalars_and_object(engine: FlatStarShapeGeneralizer) -> None:
    assert _rows(engine.generalize({"events": [1, "a", {}]})) == [
        ("events", "array", "n", False),
        ("events[*]", "integer", "1", False),
        ("events[*]", "object", "void", False),
        ("events[*]", "string", "1", False),
    ]


@pytest.mark.unit
def test_mixed_object_variants(engine: FlatStarShapeGeneralizer) -> None:
    payload = {"events": [{"type": "n", "n": 1}, {"type": "m", "msg": "x"}]}
    assert _rows(engine.generalize(payload)) == [
        ("events", "array", "n", False),
        ("events[*]", "object", "1", False),
        ("events[*].msg", "string", "1", True),
        ("events[*].n", "integer", "1", True),
        ("events[*].type", "string", "1", False),
    ]


@pytest.mark.unit
def test_null_and_string_under_one_key(engine: FlatStarShapeGeneralizer) -> None:
    assert _rows(engine.generalize([{"a": None}, {"a": "x"}, {}])) == [
        ("", "array", "n", False),
        ("[*]", "object", "1", False),
        ("[*].a", "null", None, True),
        ("[*].a", "string", "1", True),
    ]


@pytest.mark.unit
def test_object_cardinality_is_void_only_when_every_sample_is_empty(
    engine: FlatStarShapeGeneralizer,
) -> None:
    assert _rows(engine.generalize({"xs": [{}, {}]})) == [
        ("xs", "array", "n", False),
        ("xs[*]", "object", "void", False),
    ]
    assert _rows(engine.generalize({"xs": [{}, {"a": 1}]})) == [
        ("xs", "array", "n", False),
        ("xs[*]", "object", "1", False),
        ("xs[*].a", "integer", "1", True),
    ]


@pytest.mark.unit
def test_optional_is_relative_to_the_parent(engine: FlatStarShapeGeneralizer) -> None:
    payload = {"xs": [{"addr": {"zip": "10001"}}, {}]}
    assert _rows(engine.generalize(payload)) == [
        ("xs", "array", "n", False),
        ("xs[*]", "object", "1", False),
        ("xs[*].addr", "object", "1", True),
        ("xs[*].addr.zip", "string", "1", False),
    ]


@pytest.mark.unit
def test_array_of_arrays_homogeneous(engine: FlatStarShapeGeneralizer) -> None:
    assert _rows(engine.generalize({"matrix": [[1, 2], [3, 4]]})) == [
        ("matrix", "array", "n", False),
        ("matrix[*]", "array", "n", False),
        ("matrix[*][*]", "integer", "1", False),
    ]


@pytest.mark.unit
def test_nested_arrays_pool_their_elements(engine: FlatStarShapeGeneralizer) -> None:
    payload = {"matrix": [[1], [], ["a", {"k": 1}], [{"k": 2, "z": None}]]}
    assert _rows(engine.generalize(payload)) == [
        ("matrix", "array", "n", False),
        ("matrix[*]", "array", "n", False),
        ("matrix[*][*]", "integer", "1", False),
        ("matrix[*][*]", "object", "1", False),
        ("matrix[*][*]", "string", "1", False),
        ("matrix[*][*].k", "integer", "1", False),
        ("matrix[*][*].z", "null", None, True),
    ]


@pytest.mark.unit
def test_structural_equality_ignores_key_order(engine: FlatStarShapeGeneralizer) -> None:
    first = engine.generalize({"lines": [{"qty": 1, "sku": "a"}, {"sku": "b", "qty": 2}]})
    second = engine.generalize({"lines": [{"sku": "a", "qty": 1}, {"qty": 2, "sku": "b"}]})
    assert second == first
    assert "lines[*].sku" in _paths(first)


@pytest.mark.unit
@pytest.mark.parametrize(
    "payload",
    [
        {"lines": [{"sku": "a"}, {"sku": "b", "gift": True}, {"qty": 1, "sku": "c"}]},
        {"xs": [1, "a", {}, None, [2]]},
        [{"a": None}, {"a": "x"}, {"a": {"b": 1}}, {}],
        {"m": [[1], [], ["a", {"k": 1, "z": None}]], "id": "x"},
    ],
)
def test_shape_is_independent_of_element_and_key_order(
    engine: FlatStarShapeGeneralizer, payload: object
) -> None:
    shapes = [engine.generalize(variant) for variant in _reorderings(payload)]
    assert len(shapes) > 1
    assert all(shape == shapes[0] for shape in shapes)


@pytest.mark.unit
def test_fields_are_sorted_by_path_and_type_without_duplicates(
    engine: FlatStarShapeGeneralizer,
) -> None:
    payload = {"z": [{"b": 1}, {"b": "x"}, 3], "a": {"c": None}, "m": [[{}], ["s"]]}
    keys = [(field.path, field.value_type) for field in engine.generalize(payload).fields]
    assert keys == sorted(set(keys))


@pytest.mark.unit
def test_colliding_paths_merge_into_one_field(engine: FlatStarShapeGeneralizer) -> None:
    assert _rows(engine.generalize({"a.b": {}, "a": {"b": {"c": 1}}})) == [
        ("a", "object", "1", False),
        ("a.b", "object", "1", False),
        ("a.b.c", "integer", "1", False),
    ]
    payload = {"xs": [{"a.b": 1, "a": {"b": 2}}, {"a": {"b": 3}}]}
    assert ("xs[*].a.b", "integer", "1", False) in _rows(engine.generalize(payload))


@pytest.mark.unit
def test_no_values_in_shape(engine: FlatStarShapeGeneralizer) -> None:
    shape = engine.generalize({"n": 99, "items": [1, 1, 1, 1]})
    assert "99" not in str(shape.model_dump())
    assert _paths(shape)["items"] == ("array", "n")


@pytest.mark.unit
def test_homogeneous_rows_different_lengths(engine: FlatStarShapeGeneralizer) -> None:
    assert _rows(engine.generalize({"matrix": [[1], [1, 2, 3]]})) == [
        ("matrix", "array", "n", False),
        ("matrix[*]", "array", "n", False),
        ("matrix[*][*]", "integer", "1", False),
    ]


@pytest.mark.unit
def test_integer_vs_number(engine: FlatStarShapeGeneralizer) -> None:
    paths = _paths(engine.generalize({"n": 1, "x": 1.5}))
    assert paths["n"] == ("integer", "1")
    assert paths["x"] == ("number", "1")


@pytest.mark.unit
def test_leaf_types(engine: FlatStarShapeGeneralizer) -> None:
    assert _leaf_type(engine, None) == "null"
    assert _leaf_type(engine, True) == "boolean"
    assert _leaf_type(engine, 0) == "integer"
    assert _leaf_type(engine, 10**30) == "integer"
    assert _leaf_type(engine, 1.0) == "number"
    assert _leaf_type(engine, "hello") == "string"
    assert _leaf_type(engine, {1, 2}) == "unknown"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("550e8400-e29b-41d4-a716-446655440000", "uuid"),
        ("550E8400-E29B-41D4-A716-446655440000", "uuid"),
        ("user@example.com", "email"),
        ("User.Name+tag@Example.COM", "email"),
        ("https://example.com/x", "uri"),
        ("http://localhost:8080/health", "uri"),
        ("file:///etc/hosts", "uri"),
        ("2024-01-15", "date"),
        ("0999-12-31", "date"),
        ("2024-01-15T12:00", "datetime"),
        ("2024-01-15 12:00", "datetime"),
        ("2024-01-15T12:00:00", "datetime"),
        ("2024-01-15T12:00:00Z", "datetime"),
        ("2024-01-15T12:00Z", "datetime"),
        ("2024-01-15T12:00:00.5", "datetime"),
        ("2024-01-15T12:00:00.123456789Z", "datetime"),
        ("2024-01-15T12:00:00+05:30", "datetime"),
        ("2024-01-15 12:00-08:00", "datetime"),
    ],
)
def test_strict_string_forms(engine: FlatStarShapeGeneralizer, value: str, expected: str) -> None:
    assert _leaf_type(engine, value) == expected


@pytest.mark.unit
@pytest.mark.parametrize(
    "value",
    [
        # numbers and words that lax parsers read as dates or datetimes
        "12345",
        "1700000000",
        "2024",
        "1.5",
        "0",
        # SKUs and hex whose length is a multiple of 4
        "WP1234567890ABCD",
        "deadbeefdeadbeef",
        "WP1234567890ABC",
        # scheme-like text without "://"
        "orders:create",
        "localhost:8080",
        "foo:bar",
        "mailto:a@b.com",
        "urn:isbn:0451450523",
        # URLs with whitespace or text around them
        " https://example.com",
        "https://example.com/a b",
        "see https://example.com",
        # non-canonical uuids
        "550e8400e29b41d4a716446655440000",
        "{550e8400-e29b-41d4-a716-446655440000}",
        "urn:uuid:550e8400-e29b-41d4-a716-446655440000",
        "+50e8400-e29b-41d4-a716-446655440000",
        "5_0e8400-e29b-41d4-a716-446655440000",
        "550e8400-e29b41d4-a716-4466554400-00",
        # email with a display name, brackets, or padding
        "Name <addr@x.com>",
        "<addr@x.com>",
        " user@example.com",
        "user@example.com ",
        # dates outside the extended YYYY-MM-DD form
        "20240115",
        "2024-W03-1",
        "2024-015",
        "2024-1-15",
        "2024-02-30",
        # datetimes outside the extended form
        "20240115T120000",
        "2024-01-15T12",
        "2024-01-15T1200",
        "2024-01-15x12:00",
        "2024-01-15t12:00",
        "2024-01-15T12:00z",
        "2024-01-15T12:00:00,5",
        "2024-01-15T12:00.5",
        "2024-01-15T12:00:00+0530",
        "2024-01-15T12:00:00+05",
        "2024-01-15T12:00:00+05:30:00",
        "2024-01-15T12:00:00.5+0530",
        "2024-01-15T23:59:60",
        "",
    ],
)
def test_lookalike_strings_are_strings(engine: FlatStarShapeGeneralizer, value: str) -> None:
    assert _leaf_type(engine, value) == "string"


@pytest.mark.unit
def test_numeric_ids_and_skus_are_strings_at_every_length(
    engine: FlatStarShapeGeneralizer,
) -> None:
    digits = "1234567890" * 4
    sku = "WP1234567890ABCDEFGHJKLMNPQRSTUV"
    samples = [digits[:length] for length in range(1, 41)] + [
        sku[:length] for length in range(1, len(sku) + 1)
    ]
    shapes = [engine.generalize({"id": value}) for value in samples]
    assert all(shape == shapes[0] for shape in shapes)
    assert _paths(shapes[0]) == {"id": ("string", "1")}


@pytest.mark.unit
def test_base64_text_is_a_string(engine: FlatStarShapeGeneralizer) -> None:
    assert _leaf_type(engine, "SGVsbG8gV29ybGQhISE=") == "string"
    assert _leaf_type(engine, "not!!valid!!base64!!") == "string"


@pytest.mark.unit
def test_value_type_is_formal_enum() -> None:
    values = {member.value for member in ValueType}
    assert ValueType.INTEGER.value == "integer"
    assert "image" not in values
    assert "binary" not in values


@pytest.mark.unit
def test_cardinality_vocabulary(engine: FlatStarShapeGeneralizer) -> None:
    paths = _paths(engine.generalize({"a": None, "empty": {}, "n": 1, "none": [], "xs": [1, 2]}))
    assert paths["a"] == ("null", None)
    assert paths["empty"] == ("object", "void")
    assert paths["n"] == ("integer", "1")
    assert paths["none"] == ("array", "0")
    assert paths["xs"] == ("array", "n")
    dumped = engine.generalize({"xs": [1, 2]}).model_dump()
    assert dumped["fields"][0]["cardinality"] == "n"
