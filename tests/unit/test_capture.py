"""Captures: keys per layout, action names, object keys, the capture index, and the
capture rule and event models.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from canary import Canary
from pydantic import ValidationError

from fire.core.domain.capture import (
    MAX_ACTION_LENGTH,
    CaptureEvent,
    CaptureIndex,
    CaptureKey,
    CaptureLayout,
    CaptureRule,
    SpanRef,
    checked_action,
    object_key,
    read_capture_key,
)

WORK = "01JABCDEFGHJKMNPQRSTVWXYZ0"
STAMP = "20261011_163001_123456"
AT = datetime(2026, 10, 11, 16, 30, 1, 123456, tzinfo=UTC)
T0 = datetime(2026, 10, 11, 16, 30, tzinfo=UTC)
SPAN = SpanRef("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7")
OTHER_SPAN = SpanRef("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331")
KEY = "captures/partner/responses/w/estimate_place_20261011_163001_123456.json"
LAYOUTS = list(CaptureLayout)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("layout", "name", "action"),
    [
        (CaptureLayout.WORK_UNIT_ACTION, f"{WORK}/estimate_place_{STAMP}.json", "estimate_place"),
        (CaptureLayout.WORK_UNIT_ACTION, f"{WORK}/email_{STAMP}.json", "email"),
        (CaptureLayout.WORK_UNIT_ENTITY_ACTION, f"{WORK}/SO-1_create_order_{STAMP}.json", None),
        (CaptureLayout.WORK_UNIT_ENTITY, f"{WORK}/SO-1_{STAMP}.json", None),
        (CaptureLayout.WORK_UNIT_ENTITY, f"{WORK}/SO_1_2_{STAMP}.json", None),
    ],
    ids=["action", "short-action", "entity-action", "entity", "entity-with-underscores"],
)
def test_a_key_is_read_by_its_layout(layout: CaptureLayout, name: str, action: str | None) -> None:
    assert read_capture_key(name, layout) == CaptureKey(
        work=WORK, action=action, stamp=STAMP, at=AT
    )


@pytest.mark.unit
def test_the_key_time_is_utc() -> None:
    key = read_capture_key(f"{WORK}/get_{STAMP}.json", CaptureLayout.WORK_UNIT_ACTION)
    assert key is not None and key.at.tzinfo is UTC


@pytest.mark.unit
@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize(
    "name",
    [
        f"get_{STAMP}.json",
        f"/get_{STAMP}.json",
        f"{WORK}/deeper/get_{STAMP}.json",
        f"{WORK}/get_{STAMP}/",
        f"{WORK}/",
        f"{WORK}/get_{STAMP}.xml",
        f"{WORK}/get_{STAMP}.json.gz",
        f"{WORK}/get_{STAMP}",
        f"{WORK}/get_{STAMP}.JSON",
        f"{WORK}/get.json",
        f"{WORK}/{STAMP}.json",
        f"{WORK}/_{STAMP}.json",
        f"{WORK}/get_20261011_163001_1234.json",
        f"{WORK}/get_20261011-163001-123456.json",
        f"{WORK}/get_{STAMP}_x.json",
        f"{WORK}/get_20261311_163001_123456.json",
        f"{WORK}/get_20260230_163001_123456.json",
        f"{WORK}/get_20261011_243001_123456.json",
        f"{WORK}/get_20261011_166001_123456.json",
        f"{WORK}/get_00001011_163001_123456.json",
        "",
    ],
    ids=[
        "no-work-segment",
        "empty-work-segment",
        "deeper-path",
        "trailing-slash",
        "no-file",
        "xml",
        "gz",
        "no-suffix",
        "upper-suffix",
        "no-timestamp",
        "timestamp-only",
        "empty-head",
        "short-microseconds",
        "dashed-timestamp",
        "text-after-timestamp",
        "month-13",
        "february-30",
        "hour-24",
        "minute-60",
        "year-0",
        "empty",
    ],
)
def test_a_key_of_another_form_is_none(layout: CaptureLayout, name: str) -> None:
    assert read_capture_key(name, layout) is None


@pytest.mark.unit
def test_entity_action_needs_an_underscore_between_entity_id_and_action() -> None:
    name = f"{WORK}/createorder_{STAMP}.json"
    assert read_capture_key(name, CaptureLayout.WORK_UNIT_ENTITY_ACTION) is None
    assert read_capture_key(name, CaptureLayout.WORK_UNIT_ENTITY) is not None
    action_key = read_capture_key(name, CaptureLayout.WORK_UNIT_ACTION)
    assert action_key is not None and action_key.action == "createorder"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("layout", "template"),
    [
        (CaptureLayout.WORK_UNIT_ENTITY_ACTION, "{entity}_create_order_{stamp}.json"),
        (CaptureLayout.WORK_UNIT_ENTITY, "{entity}_{stamp}.json"),
    ],
    ids=["entity-action", "entity"],
)
def test_an_entity_id_segment_never_reaches_the_key(
    layout: CaptureLayout, template: str, canary: Canary
) -> None:
    entity = canary.token()
    key = read_capture_key(f"{WORK}/{template.format(entity=entity, stamp=STAMP)}", layout)
    assert key is not None
    assert canary.leaks(repr(asdict(key))) == []
    assert canary.leaks(repr(key)) == []


@pytest.mark.unit
def test_a_key_repr_shows_only_its_time() -> None:
    key = read_capture_key(f"{WORK}/estimate_place_{STAMP}.json", CaptureLayout.WORK_UNIT_ACTION)
    assert repr(key) == f"CaptureKey(at={AT!r})"


@pytest.mark.unit
@pytest.mark.parametrize(
    "action",
    [
        "estimate_place",
        "create_or_update_order",
        "get_action_request",
        "rma_receipt_confirmation",
        "email",
        "get",
        "POST_unknown",
        "orders.v2.place",
        "cancel-order",
        "x" * MAX_ACTION_LENGTH,
    ],
)
def test_names_are_actions(action: str) -> None:
    assert checked_action(action) == action


@pytest.mark.unit
@pytest.mark.parametrize(
    "value",
    [
        "1234567",
        WORK,
        "123e4567-e89b-42d3-a456-426614174000",
        "abcdefab-cdef-abcd-efab-cdefabcdefab",
        "deadbeefdeadbeef",
        "0123456789abcdef0",
        "TPC4ZEG0A0",
        "SO-1234567",
        "place_SO-1234567",
        "pay order",
        "user@example.com",
        "orders/place",
        "_place",
        ".place",
        "place\n",
        "x" * (MAX_ACTION_LENGTH + 1),
        "",
        None,
        42,
        ["get"],
        b"get",
    ],
    ids=[
        "digits",
        "ulid",
        "uuid",
        "uuid-letters-only",
        "hex-16",
        "hex-17",
        "mixed",
        "order-number",
        "name-with-order-number",
        "space",
        "at",
        "slash",
        "leading-underscore",
        "leading-dot",
        "newline",
        "too-long",
        "empty",
        "none",
        "int",
        "list",
        "bytes",
    ],
)
def test_ids_and_values_are_not_actions(value: object) -> None:
    assert checked_action(value) is None


@pytest.mark.unit
@pytest.mark.parametrize(
    ("uri", "key"),
    [
        (f"s3://bucket/{KEY}", KEY),
        (f"s3://my.bucket-1/{KEY}", KEY),
        (KEY, KEY),
        (f"/{KEY}", KEY),
        ("s3://bucket", ""),
        ("s3://bucket/", ""),
    ],
    ids=["s3-uri", "dotted-bucket", "bare-key", "leading-slash", "bucket-only", "bucket-slash"],
)
def test_object_key(uri: str, key: str) -> None:
    assert object_key(uri) == key


@pytest.mark.unit
def test_the_index_maps_a_key_to_its_span() -> None:
    index = CaptureIndex()
    index.put(KEY, SPAN, 900, T0)
    assert index.get(KEY, T0) == SPAN
    assert index.get("captures/partner/responses/w/other.json", T0) is None
    assert len(index) == 1


@pytest.mark.unit
def test_an_entry_expires_exactly_at_its_ttl() -> None:
    index = CaptureIndex()
    index.put(KEY, SPAN, 10, T0)
    assert index.get(KEY, T0 + timedelta(seconds=10) - timedelta(microseconds=1)) == SPAN
    assert index.get(KEY, T0 + timedelta(seconds=10)) is None
    assert len(index) == 0
    assert index.get(KEY, T0) is None


@pytest.mark.unit
def test_each_entry_keeps_its_own_ttl() -> None:
    index = CaptureIndex()
    index.put("a", SPAN, 5, T0)
    index.put("b", OTHER_SPAN, 50, T0)
    later = T0 + timedelta(seconds=10)
    assert (index.get("a", later), index.get("b", later)) == (None, OTHER_SPAN)


@pytest.mark.unit
def test_a_second_put_refreshes_the_entry() -> None:
    index = CaptureIndex()
    index.put(KEY, SPAN, 10, T0)
    index.put(KEY, OTHER_SPAN, 10, T0 + timedelta(seconds=5))
    assert len(index) == 1
    assert index.get(KEY, T0 + timedelta(seconds=12)) == OTHER_SPAN
    assert index.get(KEY, T0 + timedelta(seconds=15)) is None


@pytest.mark.unit
def test_one_entry_past_max_entries_drops_the_oldest() -> None:
    index = CaptureIndex(max_entries=2)
    for key in ("a", "b", "c"):
        index.put(key, SPAN, 900, T0)
    assert len(index) == 2
    assert [index.get(key, T0) for key in ("a", "b", "c")] == [None, SPAN, SPAN]


@pytest.mark.unit
def test_a_refreshed_entry_is_no_longer_the_oldest() -> None:
    index = CaptureIndex(max_entries=2)
    index.put("a", SPAN, 900, T0)
    index.put("b", SPAN, 900, T0)
    index.put("a", OTHER_SPAN, 900, T0)
    index.put("c", SPAN, 900, T0)
    assert [index.get(key, T0) for key in ("a", "b", "c")] == [OTHER_SPAN, None, SPAN]


@pytest.mark.unit
def test_an_s3_uri_and_its_bare_key_are_one_entry() -> None:
    index = CaptureIndex()
    index.put(f"s3://bucket/{KEY}", SPAN, 900, T0)
    for uri in (KEY, f"/{KEY}", f"s3://bucket/{KEY}", f"s3://another-bucket/{KEY}"):
        assert index.get(uri, T0) == SPAN
    index.put(KEY, OTHER_SPAN, 900, T0)
    assert len(index) == 1
    assert index.get(f"s3://bucket/{KEY}", T0) == OTHER_SPAN


@pytest.mark.unit
def test_a_uri_with_no_key_is_not_indexed() -> None:
    index = CaptureIndex()
    index.put("s3://bucket", SPAN, 900, T0)
    index.put("", SPAN, 900, T0)
    assert len(index) == 0
    assert index.get("s3://bucket", T0) is None


@pytest.mark.unit
def test_the_index_repr_holds_no_key(canary: Canary) -> None:
    index = CaptureIndex()
    index.put(f"s3://bucket/captures/{canary.token()}.json", SPAN, 900, T0)
    assert repr(index) == "CaptureIndex(entries=1)"
    assert canary.leaks(repr(index)) == []


@pytest.mark.unit
@pytest.mark.parametrize("max_entries", [0, -1])
def test_the_index_holds_at_least_one_entry(max_entries: int) -> None:
    with pytest.raises(ValueError):
        CaptureIndex(max_entries=max_entries)


def _rule(**overrides: Any) -> dict[str, Any]:
    return {
        "prefix": "captures/partner/",
        "label": "partner",
        "layout": "work_unit_entity",
    } | overrides


@pytest.mark.unit
def test_a_capture_rule_reads_its_three_keys() -> None:
    rule = CaptureRule.model_validate(_rule())
    assert rule.prefix == "captures/partner/"
    assert rule.label == "partner"
    assert rule.layout is CaptureLayout.WORK_UNIT_ENTITY


@pytest.mark.unit
@pytest.mark.parametrize("prefix", ["p/", "a/b-c_d.e/", "x" * 199 + "/"])
def test_valid_prefixes(prefix: str) -> None:
    assert CaptureRule.model_validate(_rule(prefix=prefix)).prefix == prefix


@pytest.mark.unit
@pytest.mark.parametrize(
    "overrides",
    [
        {"prefix": "/captures/partner/"},
        {"prefix": "captures/partner"},
        {"prefix": "/"},
        {"prefix": ""},
        {"prefix": "captures/part ner/"},
        {"prefix": " captures/"},
        {"prefix": "x" * 200 + "/"},
        {"label": "Partner"},
        {"label": "-partner"},
        {"label": "_partner"},
        {"label": "part.ner"},
        {"label": "part:ner"},
        {"label": "part ner"},
        {"label": ""},
        {"layout": "work_unit"},
        {"bucket": "b"},
    ],
    ids=[
        "prefix-leading-slash",
        "prefix-no-trailing-slash",
        "prefix-slash-only",
        "prefix-empty",
        "prefix-space",
        "prefix-leading-space",
        "prefix-too-long",
        "label-upper",
        "label-leading-dash",
        "label-leading-underscore",
        "label-dot",
        "label-colon",
        "label-space",
        "label-empty",
        "layout-unknown",
        "unknown-key",
    ],
)
def test_invalid_capture_rules(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        CaptureRule.model_validate(_rule(**overrides))


def _event(**overrides: Any) -> dict[str, Any]:
    return {
        "name": "capture.written",
        "request_uri": "acme.capture.request_uri",
        "response_uri": "acme.capture.response_uri",
    } | overrides


@pytest.mark.unit
def test_a_capture_event_has_a_default_ttl() -> None:
    event = CaptureEvent.model_validate(_event())
    assert event.ttl_s == 900
    assert CaptureEvent.model_validate(_event(ttl_s=0.5)).ttl_s == 0.5


@pytest.mark.unit
@pytest.mark.parametrize(
    "overrides",
    [
        {"response_uri": "acme.capture.request_uri"},
        {"ttl_s": 0},
        {"ttl_s": -1},
        {"name": ""},
        {"request_uri": ""},
        {"response_uri": ""},
        {"status_code": "acme.capture.status_code"},
    ],
    ids=[
        "same-attribute",
        "ttl-zero",
        "ttl-negative",
        "name-empty",
        "request-empty",
        "response-empty",
        "unknown-key",
    ],
)
def test_invalid_capture_events(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        CaptureEvent.model_validate(_event(**overrides))
