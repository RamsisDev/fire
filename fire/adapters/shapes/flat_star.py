"""Flat-star ShapeGeneralizer: JSON → flat ShapeField paths, with `[*]` for array elements.

Every value seen at a path is a sample of it. All elements of all arrays at `p` are samples
of `p[*]`, and the object samples at a path merge into one template whose keys are the union
of theirs; a key that some object sample lacks is optional. A path carries one field per
distinct value type. Array cardinality is "0" when every sample is empty and "n" otherwise,
never a length; object cardinality is "void" when every sample is empty and "1" otherwise.
The result holds no index paths, values, or fingerprints, and does not depend on element or
key order.

A string gets a refined type only in one exact form: a structural check (length, separator
positions) followed by a stdlib or Pydantic parser. Any other string is `string`, so a field
keeps its type whatever its value. Contract: docs/specs/shape-generalizer.md.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
from typing import Any
from uuid import UUID

from pydantic import AnyUrl, EmailStr, TypeAdapter

from fire.core.domain.dossier import (
    Shape,
    ShapeField,
    ValueType,
)

_EMAIL = TypeAdapter(EmailStr)
_URL = TypeAdapter(AnyUrl)


def _parses(parse: Callable[[str], object], value: str) -> bool:
    try:
        parse(value)
    except ValueError:
        return False
    return True


def _has_whitespace(value: str) -> bool:
    return any(char.isspace() for char in value)


def _is_uuid(value: str) -> bool:
    """Canonical 8-4-4-4-12 hex in either case.

    `UUID` alone also takes braces, `urn:uuid:`, bare hex, misplaced hyphens, and `+` or `_`.
    """
    try:
        return str(UUID(value)) == value.lower()
    except ValueError:
        return False


def _is_email(value: str) -> bool:
    """A plain address. `EmailStr` alone also takes `Name <addr>` and strips padding."""
    if "<" in value or _has_whitespace(value):
        return False
    return _parses(_EMAIL.validate_python, value)


def _is_uri(value: str) -> bool:
    """`scheme://…`. `AnyUrl` alone also takes `scheme:path` and drops or encodes whitespace."""
    if "://" not in value or _has_whitespace(value):
        return False
    return _parses(_URL.validate_python, value)


def _is_date(value: str) -> bool:
    """`YYYY-MM-DD`. `date.fromisoformat` alone also takes `YYYYMMDD` and week dates."""
    return len(value) == 10 and value[4] == value[7] == "-" and _parses(date.fromisoformat, value)


def _is_datetime(value: str) -> bool:
    """`YYYY-MM-DD`, `T` or a space, a clock, then an optional `Z`, `+HH:MM`, or `-HH:MM`.

    `datetime.fromisoformat` alone also takes any separator, hour-only and basic (`HHMMSS`,
    `+HHMM`) forms, and comma fractions.
    """
    if len(value) < 16 or value[10] not in "T " or not _is_date(value[:10]):
        return False
    return _is_clock(_without_offset(value[11:])) and _parses(datetime.fromisoformat, value)


def _without_offset(time: str) -> str:
    if time.endswith("Z"):
        return time[:-1]
    if len(time) > 6 and time[-6] in "+-" and time[-3] == ":":
        return time[:-6]
    return time


def _is_clock(clock: str) -> bool:
    """`HH:MM`, `HH:MM:SS`, or `HH:MM:SS.` plus digits; the parser checks the ranges."""
    if len(clock) < 5 or clock[2] != ":":
        return False
    if len(clock) == 5:
        return True
    if len(clock) < 8 or clock[5] != ":":
        return False
    return len(clock) == 8 or (clock[8] == "." and clock[9:].isdigit())


def _classify_string(value: str) -> ValueType:
    if _is_uuid(value):
        return ValueType.UUID
    if _is_email(value):
        return ValueType.EMAIL
    if _is_uri(value):
        return ValueType.URI
    if _is_date(value):
        return ValueType.DATE
    if _is_datetime(value):
        return ValueType.DATETIME
    return ValueType.STRING


def _classify_leaf(value: Any) -> ValueType:
    if value is None:
        return ValueType.NULL
    if isinstance(value, bool):
        return ValueType.BOOLEAN
    if isinstance(value, int):
        return ValueType.INTEGER
    if isinstance(value, float):
        return ValueType.NUMBER
    if isinstance(value, str):
        return _classify_string(value)
    return ValueType.UNKNOWN


def _child_path(prefix: str, key: str) -> str:
    return f"{prefix}.{key}" if prefix else key


def _star_path(prefix: str) -> str:
    return f"{prefix}[*]" if prefix else "[*]"


type _Fields = dict[tuple[str, ValueType], ShapeField]

_EMPTY_CARDINALITIES = frozenset({"void", "0"})


def _add(
    fields: _Fields,
    path: str,
    value_type: ValueType,
    *,
    cardinality: str | None,
    optional: bool,
) -> None:
    """Keep one field per (path, value_type).

    Keys are not escaped, so a key holding `.` or `[*]` can land on a nested key's path. The
    merged field is optional only if both are, and populated if either is.
    """
    earlier = fields.get((path, value_type))
    if earlier is not None:
        optional = optional and earlier.optional
        if cardinality in _EMPTY_CARDINALITIES:
            cardinality = earlier.cardinality
    fields[path, value_type] = ShapeField(
        path=path, value_type=value_type, cardinality=cardinality, optional=optional
    )


def _walk(samples: list[Any], path: str, optional: bool, fields: _Fields) -> None:
    """Emit one field per value type among `samples`, every value seen at `path`."""
    objects = [sample for sample in samples if isinstance(sample, dict)]
    arrays = [sample for sample in samples if isinstance(sample, list)]
    leaves = {_classify_leaf(sample) for sample in samples if not isinstance(sample, dict | list)}
    for leaf in leaves:
        cardinality = None if leaf is ValueType.NULL else "1"
        _add(fields, path, leaf, cardinality=cardinality, optional=optional)
    if objects:
        _walk_objects(objects, path, optional, fields)
    if arrays:
        _walk_arrays(arrays, path, optional, fields)


def _walk_objects(
    objects: list[dict[str, Any]], path: str, optional: bool, fields: _Fields
) -> None:
    """The root object emits no field of its own; its keys start the paths."""
    if path:
        cardinality = "1" if any(objects) else "void"
        _add(fields, path, ValueType.OBJECT, cardinality=cardinality, optional=optional)
    for key in set().union(*objects):
        present = [obj[key] for obj in objects if key in obj]
        _walk(present, _child_path(path, key), len(present) < len(objects), fields)


def _walk_arrays(arrays: list[list[Any]], path: str, optional: bool, fields: _Fields) -> None:
    cardinality = "n" if any(arrays) else "0"
    _add(fields, path, ValueType.ARRAY, cardinality=cardinality, optional=optional)
    elements = [element for array in arrays for element in array]
    if elements:
        _walk(elements, _star_path(path), False, fields)


class FlatStarShapeGeneralizer:
    """Pure adapter: merge every value seen at a path into flat, sorted Shape fields."""

    def generalize(self, payload: Any) -> Shape:
        fields: _Fields = {}
        _walk([payload], "", False, fields)
        return Shape(fields=[fields[key] for key in sorted(fields)])
