"""Channel, ShapeObservation, and SpanObservation contracts."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, get_args, get_origin

import pytest
from pydantic import BaseModel, ValidationError

from fire.core.domain.observation import (
    Channel,
    ChannelDirection,
    ChannelKind,
    ChannelPart,
    FingerprintSummary,
    ShapeObservation,
    SpanLink,
    SpanObservation,
    StatusClass,
    StreamSummary,
)

TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
SPAN_ID = "00f067aa0ba902b7"
OTHER_SPAN_ID = "a3ce929d0e0e4736"

EVENT_CHANNEL = {
    "kind": "event",
    "direction": "internal",
    "name": "order.created",
    "part": "payload",
}
NO_RESPONSE_CHANNEL = {
    "kind": "http",
    "direction": "outbound",
    "name": "GET partner/api/{version}/orders/{order_id}",
    "part": "response",
    "status_class": "none",
}


def _shape_observation(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "system": "shop",
        "source": "order-events",
        "channel": dict(EVENT_CHANNEL),
        "observation_id": "01J9Z3K4N8Q2R5T7V9X1Y3Z5A7",
        "occurred_at": "2026-10-11T16:30:00Z",
        "observed_at": "2026-10-11T16:30:01Z",
        "shape": {
            "fields": [
                {"path": "order_id", "value_type": "string", "cardinality": "1", "optional": False}
            ]
        },
        "fingerprint": {"schema_hash": "3f1c", "normalized_paths": ["order_id"]},
    }
    data.update(overrides)
    return data


def _span_observation(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "system": "shop",
        "source": "traces",
        "trace_id": TRACE_ID,
        "span_id": SPAN_ID,
        "parent_span_id": OTHER_SPAN_ID,
        "links": [{"trace_id": TRACE_ID, "span_id": OTHER_SPAN_ID}],
        "service": "orders-api",
        "kind": "server",
        "operation": "POST /v1/orders",
        "status_code": 201,
        "error": False,
        "start": "2026-10-11T16:30:00.123Z",
        "duration_ms": 41.5,
    }
    data.update(overrides)
    return data


def _errors(model: type[BaseModel], data: dict[str, Any]) -> list[tuple[tuple[Any, ...], str]]:
    with pytest.raises(ValidationError) as caught:
        model.model_validate(data)
    return [(error["loc"], error["type"]) for error in caught.value.errors()]


def _locs(model: type[BaseModel], data: dict[str, Any]) -> list[tuple[Any, ...]]:
    return [loc for loc, _type in _errors(model, data)]


def _dig(data: Any, where: tuple[Any, ...]) -> Any:
    for step in where:
        data = data[step]
    return data


def _path_id(where: tuple[Any, ...]) -> str:
    return ".".join(str(step) for step in where)


# Channel


@pytest.mark.unit
@pytest.mark.parametrize(
    ("channel", "key"),
    [
        (
            {
                "kind": "http",
                "direction": "inbound",
                "name": "POST /v1/orders/checkout",
                "part": "response",
                "status_class": "2xx",
            },
            "http:inbound:POST /v1/orders/checkout:response:2xx",
        ),
        (
            NO_RESPONSE_CHANNEL,
            "http:outbound:GET partner/api/{version}/orders/{order_id}:response:none",
        ),
        (
            {
                "kind": "event",
                "direction": "internal",
                "name": "fulfillment.allocated",
                "part": "payload",
            },
            "event:internal:fulfillment.allocated:payload",
        ),
        (
            {"kind": "http", "direction": "inbound", "name": "GET /health", "part": "request"},
            "http:inbound:GET /health:request",
        ),
        (
            {
                "kind": "outcome",
                "direction": "internal",
                "name": "sales-order-processor",
                "part": "payload",
            },
            "outcome:internal:sales-order-processor:payload",
        ),
    ],
    ids=[
        "inbound-response",
        "outbound-no-response",
        "internal-event",
        "inbound-request",
        "internal-outcome",
    ],
)
def test_channel_renders_its_key(channel: dict[str, str], key: str) -> None:
    assert Channel.model_validate(channel).key == key


@pytest.mark.unit
def test_channel_key_is_not_serialized() -> None:
    channel = Channel.model_validate(EVENT_CHANNEL)
    assert "key" not in channel.model_dump()
    assert Channel.model_validate_json(channel.model_dump_json()) == channel


@pytest.mark.unit
@pytest.mark.parametrize(
    ("channel", "loc"),
    [
        ({"kind": "http", "part": "payload"}, "part"),
        ({"kind": "event", "part": "request"}, "part"),
        ({"kind": "event", "part": "response", "status_class": "2xx"}, "part"),
        ({"kind": "http", "part": "response"}, "status_class"),
        ({"kind": "http", "part": "response", "status_class": None}, "status_class"),
        ({"kind": "http", "part": "request", "status_class": "2xx"}, "status_class"),
        ({"kind": "http", "part": "request", "status_class": "none"}, "status_class"),
        ({"kind": "event", "part": "payload", "status_class": "5xx"}, "status_class"),
        ({"kind": "http", "part": "response", "status_class": "1xx"}, "status_class"),
        ({"kind": "grpc", "part": "request"}, "kind"),
        ({"kind": "http", "part": "header"}, "part"),
        ({"kind": "outcome", "part": "request"}, "part"),
        ({"kind": "outcome", "part": "response", "status_class": "2xx"}, "part"),
    ],
    ids=[
        "http-payload",
        "event-request",
        "event-response",
        "response-without-status-class",
        "response-with-null-status-class",
        "request-with-status-class",
        "request-with-none-status-class",
        "payload-with-status-class",
        "unknown-status-class",
        "unknown-kind",
        "unknown-part",
        "outcome-request",
        "outcome-response",
    ],
)
def test_channel_rejects_invalid_part_and_status_class(channel: dict[str, Any], loc: str) -> None:
    data = {"direction": "inbound", "name": "x", **channel}
    assert _locs(Channel, data) == [(loc,)]


@pytest.mark.unit
def test_channel_rejects_unknown_direction() -> None:
    assert _locs(Channel, {**EVENT_CHANNEL, "direction": "sideways"}) == [("direction",)]


@pytest.mark.unit
def test_channel_names_with_colons_never_collide() -> None:
    names = [
        "a",
        ":",
        "a:",
        ":a",
        "a:request",
        "a:payload",
        "a:response",
        "a:response:2xx",
        "a:response:none",
        "http:inbound:a",
        "event:internal:a:payload",
    ]
    forms = [
        (ChannelKind.HTTP, ChannelPart.REQUEST, None),
        *((ChannelKind.HTTP, ChannelPart.RESPONSE, status) for status in StatusClass),
        (ChannelKind.EVENT, ChannelPart.PAYLOAD, None),
        (ChannelKind.OUTCOME, ChannelPart.PAYLOAD, None),
    ]
    channels = [
        Channel(kind=kind, direction=direction, name=name, part=part, status_class=status)
        for name in names
        for direction in ChannelDirection
        for kind, part, status in forms
    ]
    assert len({channel.key for channel in channels}) == len(channels)


# Unknown fields


UNKNOWN_KEYS = ["payload", "body", "value", "attributes"]


@pytest.mark.unit
@pytest.mark.parametrize("key", UNKNOWN_KEYS)
@pytest.mark.parametrize(
    "where",
    [(), ("channel",), ("shape",), ("shape", "fields", 0), ("fingerprint",)],
    ids=["top", "channel", "shape", "shape-field", "fingerprint"],
)
def test_shape_observation_rejects_unknown_keys_at_every_depth(
    key: str, where: tuple[Any, ...]
) -> None:
    data = _shape_observation()
    _dig(data, where)[key] = "planted-value"
    assert _errors(ShapeObservation, data) == [((*where, key), "extra_forbidden")]


@pytest.mark.unit
@pytest.mark.parametrize("key", UNKNOWN_KEYS)
@pytest.mark.parametrize("where", [(), ("links", 0)], ids=["top", "link"])
def test_span_observation_rejects_unknown_keys_at_every_depth(
    key: str, where: tuple[Any, ...]
) -> None:
    data = _span_observation()
    _dig(data, where)[key] = "planted-value"
    assert _errors(SpanObservation, data) == [((*where, key), "extra_forbidden")]


def _leaf_types(annotation: Any) -> Iterator[Any]:
    yield annotation
    if get_origin(annotation) is not None:
        yield get_origin(annotation)
    for arg in get_args(annotation):
        yield from _leaf_types(arg)


@pytest.mark.unit
@pytest.mark.parametrize(
    "model",
    [Channel, ShapeObservation, SpanLink, SpanObservation, FingerprintSummary, StreamSummary],
)
def test_observation_models_have_no_open_types(model: type[BaseModel]) -> None:
    for name, field in model.model_fields.items():
        leaves = list(_leaf_types(field.annotation))
        assert Any not in leaves, name
        assert dict not in leaves, name


# ShapeObservation


@pytest.mark.unit
def test_shape_observation_minimal_has_null_optionals() -> None:
    observation = ShapeObservation.model_validate(_shape_observation())
    assert observation.region is None
    assert observation.trace_id is None
    assert observation.span_id is None
    assert observation.work_id is None
    assert observation.correlation_id is None


@pytest.mark.unit
def test_shape_observation_keys() -> None:
    observation = ShapeObservation.model_validate(_shape_observation())
    assert observation.stream_key == (
        "shop",
        "order-events",
        "event:internal:order.created:payload",
    )
    assert observation.dedupe_key == ("shop", "order-events", "01J9Z3K4N8Q2R5T7V9X1Y3Z5A7")


@pytest.mark.unit
def test_shape_observation_json_round_trip() -> None:
    observation = ShapeObservation.model_validate(
        _shape_observation(
            channel=dict(NO_RESPONSE_CHANNEL),
            shape={"fields": []},
            region="us-east-1",
            trace_id=TRACE_ID,
            span_id=SPAN_ID,
            work_id="01J9Z3K4N8Q2R5T7V9X1Y3Z5A7",
            correlation_id="corr-1",
        )
    )
    assert ShapeObservation.model_validate_json(observation.model_dump_json()) == observation


@pytest.mark.unit
def test_shape_observation_span_id_requires_trace_id() -> None:
    assert _locs(ShapeObservation, _shape_observation(span_id=SPAN_ID)) == [("span_id",)]
    assert _locs(ShapeObservation, _shape_observation(trace_id=None, span_id=SPAN_ID)) == [
        ("span_id",)
    ]


@pytest.mark.unit
def test_shape_observation_trace_id_alone_is_valid() -> None:
    observation = ShapeObservation.model_validate(_shape_observation(trace_id=TRACE_ID))
    assert observation.trace_id == TRACE_ID
    assert observation.span_id is None


@pytest.mark.unit
def test_shape_observation_on_a_no_response_channel_has_an_empty_shape() -> None:
    data = _shape_observation(channel=dict(NO_RESPONSE_CHANNEL))
    assert _locs(ShapeObservation, data) == [("shape",)]
    data["shape"] = {"fields": []}
    assert ShapeObservation.model_validate(data).shape.fields == []


@pytest.mark.unit
def test_shape_observation_with_an_invalid_channel_reports_only_the_channel() -> None:
    channel = {**EVENT_CHANNEL, "part": "response", "status_class": "none"}
    assert _locs(ShapeObservation, _shape_observation(channel=channel)) == [("channel", "part")]


@pytest.mark.unit
@pytest.mark.parametrize("field", ["occurred_at", "observed_at"])
def test_shape_observation_rejects_naive_datetimes(field: str) -> None:
    data = _shape_observation(**{field: "2026-10-11T16:30:00"})
    assert _errors(ShapeObservation, data) == [((field,), "timezone_aware")]


BOUNDED_SHAPE_FIELDS = [
    ("system",),
    ("source",),
    ("observation_id",),
    ("region",),
    ("work_id",),
    ("correlation_id",),
    ("channel", "name"),
]


@pytest.mark.unit
@pytest.mark.parametrize("where", BOUNDED_SHAPE_FIELDS, ids=_path_id)
@pytest.mark.parametrize(
    ("value", "error_type"),
    [("", "string_too_short"), ("x" * 257, "string_too_long")],
    ids=["empty", "too-long"],
)
def test_shape_observation_bounds_strings(
    where: tuple[str, ...], value: str, error_type: str
) -> None:
    data = _shape_observation()
    _dig(data, where[:-1])[where[-1]] = value
    assert _errors(ShapeObservation, data) == [(where, error_type)]


@pytest.mark.unit
@pytest.mark.parametrize("where", BOUNDED_SHAPE_FIELDS, ids=_path_id)
def test_shape_observation_accepts_256_characters(where: tuple[str, ...]) -> None:
    data = _shape_observation()
    _dig(data, where[:-1])[where[-1]] = "x" * 256
    ShapeObservation.model_validate(data)


# Ids


BAD_TRACE_IDS = [
    TRACE_ID.upper(),
    TRACE_ID[:-1],
    TRACE_ID + "0",
    "g" + TRACE_ID[1:],
    "0" * 32,
    "",
]
BAD_SPAN_IDS = [
    SPAN_ID.upper(),
    SPAN_ID[:-1],
    SPAN_ID + "0",
    "g" + SPAN_ID[1:],
    "0" * 16,
    "",
]
BAD_ID_LABELS = ["uppercase", "short", "long", "not-hex", "all-zeros", "empty"]


@pytest.mark.unit
@pytest.mark.parametrize("bad", BAD_TRACE_IDS, ids=BAD_ID_LABELS)
@pytest.mark.parametrize("where", [("trace_id",), ("links", 0, "trace_id")], ids=_path_id)
def test_span_observation_rejects_bad_trace_ids(bad: str, where: tuple[Any, ...]) -> None:
    data = _span_observation()
    _dig(data, where[:-1])[where[-1]] = bad
    assert _locs(SpanObservation, data) == [where]


@pytest.mark.unit
@pytest.mark.parametrize("bad", BAD_SPAN_IDS, ids=BAD_ID_LABELS)
@pytest.mark.parametrize(
    "where",
    [("span_id",), ("parent_span_id",), ("links", 0, "span_id")],
    ids=_path_id,
)
def test_span_observation_rejects_bad_span_ids(bad: str, where: tuple[Any, ...]) -> None:
    data = _span_observation()
    _dig(data, where[:-1])[where[-1]] = bad
    assert _locs(SpanObservation, data) == [where]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("field", "bad"),
    [*(("trace_id", bad) for bad in BAD_TRACE_IDS), *(("span_id", bad) for bad in BAD_SPAN_IDS)],
    ids=[f"{field}-{label}" for field in ("trace_id", "span_id") for label in BAD_ID_LABELS],
)
def test_shape_observation_rejects_bad_ids(field: str, bad: str) -> None:
    data = _shape_observation(**{"trace_id": TRACE_ID, "span_id": SPAN_ID, field: bad})
    assert _locs(ShapeObservation, data) == [(field,)]


# SpanObservation


@pytest.mark.unit
def test_span_observation_root_span_defaults() -> None:
    data = _span_observation()
    for field in ("parent_span_id", "links", "status_code"):
        del data[field]
    span = SpanObservation.model_validate(data)
    assert span.parent_span_id is None
    assert span.links == []
    assert span.status_code is None


@pytest.mark.unit
@pytest.mark.parametrize(
    "field",
    [
        "system",
        "source",
        "trace_id",
        "span_id",
        "service",
        "kind",
        "operation",
        "error",
        "start",
        "duration_ms",
    ],
)
def test_span_observation_requires_field(field: str) -> None:
    data = _span_observation()
    del data[field]
    assert _errors(SpanObservation, data) == [((field,), "missing")]


@pytest.mark.unit
def test_span_observation_dedupe_key() -> None:
    span = SpanObservation.model_validate(_span_observation())
    assert span.dedupe_key == ("shop", TRACE_ID, SPAN_ID)


@pytest.mark.unit
def test_span_observation_json_round_trip() -> None:
    span = SpanObservation.model_validate(_span_observation())
    assert SpanObservation.model_validate_json(span.model_dump_json()) == span


@pytest.mark.unit
@pytest.mark.parametrize("kind", ["server", "client", "producer", "consumer", "internal"])
def test_span_observation_accepts_every_kind(kind: str) -> None:
    assert SpanObservation.model_validate(_span_observation(kind=kind)).kind == kind


@pytest.mark.unit
def test_span_observation_rejects_unknown_kind() -> None:
    assert _locs(SpanObservation, _span_observation(kind="unspecified")) == [("kind",)]


@pytest.mark.unit
@pytest.mark.parametrize("status_code", [100, 599, None])
def test_span_observation_accepts_status_code(status_code: int | None) -> None:
    span = SpanObservation.model_validate(_span_observation(status_code=status_code))
    assert span.status_code == status_code


@pytest.mark.unit
@pytest.mark.parametrize("status_code", [99, 600, 0, -1])
def test_span_observation_rejects_status_code(status_code: int) -> None:
    assert _locs(SpanObservation, _span_observation(status_code=status_code)) == [("status_code",)]


@pytest.mark.unit
def test_span_observation_accepts_zero_duration() -> None:
    assert SpanObservation.model_validate(_span_observation(duration_ms=0)).duration_ms == 0


@pytest.mark.unit
@pytest.mark.parametrize(
    "duration_ms", [-0.001, float("nan"), float("inf")], ids=["negative", "nan", "inf"]
)
def test_span_observation_rejects_bad_duration(duration_ms: float) -> None:
    assert _locs(SpanObservation, _span_observation(duration_ms=duration_ms)) == [("duration_ms",)]


@pytest.mark.unit
def test_span_observation_rejects_naive_start() -> None:
    data = _span_observation(start="2026-10-11T16:30:00")
    assert _errors(SpanObservation, data) == [(("start",), "timezone_aware")]


@pytest.mark.unit
@pytest.mark.parametrize("field", ["system", "source", "service", "operation"])
@pytest.mark.parametrize(
    ("value", "error_type"),
    [("", "string_too_short"), ("x" * 257, "string_too_long")],
    ids=["empty", "too-long"],
)
def test_span_observation_bounds_strings(field: str, value: str, error_type: str) -> None:
    assert _errors(SpanObservation, _span_observation(**{field: value})) == [
        ((field,), error_type)
    ]
