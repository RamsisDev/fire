"""Envelope: dotted paths into a record, in a short and a long form."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from fire.core.domain.envelope import Envelope, EpochUnit

REQUIRED = {"event_id": "id", "event_type": "type", "occurred_at": "at", "payload": "data"}


def _locs(**overrides: Any) -> list[tuple[str | int, ...]]:
    with pytest.raises(ValidationError) as caught:
        Envelope.model_validate({**REQUIRED, **overrides})
    return [error["loc"] for error in caught.value.errors()]


@pytest.mark.unit
def test_short_form_is_a_path() -> None:
    envelope = Envelope.model_validate({**REQUIRED, "occurred_at": "metadata.timestamp"})
    assert envelope.event_id.path == "id"
    assert envelope.event_id.strip_prefix is None
    assert envelope.occurred_at.path == "metadata.timestamp"
    assert envelope.occurred_at.epoch is None
    assert envelope.work_id is None
    assert envelope.correlation_id is None


@pytest.mark.unit
def test_long_forms() -> None:
    envelope = Envelope.model_validate(
        {
            **REQUIRED,
            "event_id": {"path": "SK", "strip_prefix": "EVENT#"},
            "occurred_at": {"path": "ts", "epoch": "ms"},
            "payload": {"path": "data"},
            "work_id": {"path": "SK", "strip_prefix": "EVENT#"},
            "correlation_id": "meta.correlation",
        }
    )
    assert envelope.event_id.strip_prefix == "EVENT#"
    assert envelope.occurred_at.epoch is EpochUnit.MILLISECONDS
    assert envelope.payload.path == "data"
    assert envelope.work_id is not None and envelope.work_id.path == "SK"
    assert envelope.correlation_id is not None
    assert envelope.correlation_id.path == "meta.correlation"


@pytest.mark.unit
@pytest.mark.parametrize("key", sorted(REQUIRED))
def test_required_keys(key: str) -> None:
    raw = dict(REQUIRED)
    del raw[key]
    with pytest.raises(ValidationError) as caught:
        Envelope.model_validate(raw)
    assert [error["loc"] for error in caught.value.errors()] == [(key,)]


@pytest.mark.unit
@pytest.mark.parametrize(
    "path",
    ["", ".id", "id.", "a..b", "items[0]", "items.*", "items[*].id"],
    ids=["empty", "leading-dot", "trailing-dot", "empty-key", "index", "wildcard", "star"],
)
def test_paths_are_object_keys_joined_by_dots(path: str) -> None:
    assert _locs(event_id=path) == [("event_id", "path")]


@pytest.mark.unit
def test_unknown_keys_are_rejected() -> None:
    assert _locs(dedupe_key="id") == [("dedupe_key",)]
    assert _locs(event_id={"path": "id", "index": 0}) == [("event_id", "index")]


@pytest.mark.unit
@pytest.mark.parametrize("key", ["event_id", "event_type", "work_id", "correlation_id"])
def test_epoch_only_on_occurred_at(key: str) -> None:
    assert _locs(**{key: {"path": "ts", "epoch": "ms"}}) == [(key, "epoch")]


@pytest.mark.unit
def test_payload_has_no_prefix_to_strip() -> None:
    assert _locs(payload={"path": "data", "strip_prefix": "x"}) == [("payload", "strip_prefix")]


@pytest.mark.unit
def test_epoch_and_strip_prefix_exclude_each_other() -> None:
    assert _locs(occurred_at={"path": "ts", "epoch": "s", "strip_prefix": "T#"}) == [
        ("occurred_at",)
    ]


@pytest.mark.unit
@pytest.mark.parametrize("epoch", ["us", "ns", ""])
def test_epoch_units(epoch: str) -> None:
    assert _locs(occurred_at={"path": "ts", "epoch": epoch}) == [("occurred_at", "epoch")]


@pytest.mark.unit
def test_empty_strip_prefix_is_rejected() -> None:
    assert _locs(event_id={"path": "SK", "strip_prefix": ""}) == [("event_id", "strip_prefix")]
