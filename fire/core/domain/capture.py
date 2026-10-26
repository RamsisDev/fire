"""Captures: the request and response pairs a system's clients write to a bucket.

A capture rule names a client's key prefix, the label that starts its channel names, and the
layout of its keys. A key gives the pair's work unit, its time, and for some layouts its action;
an entity id segment is a value and is never read. The capture index joins a pair to the span
whose `capture_event` named its keys. Keys never leave the tentacle and are never logged.
Contract: docs/specs/tentacle.md#kinds (`s3_capture`) and
docs/specs/http-and-span-capture.md#capture-events.
"""

from __future__ import annotations

import re
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator

from fire.core.domain.operation import ID_RULES, is_id

REQUESTS = "requests"
RESPONSES = "responses"
JSON_SUFFIX = ".json"
STAMP_FORMAT = "%Y%m%d_%H%M%S_%f"
MAX_ACTION_LENGTH = 64
DEFAULT_CAPTURE_TTL_S = 900.0
MAX_INDEX_ENTRIES = 100_000

_STEM = re.compile(r"(?P<head>.+)_(?P<stamp>[0-9]{8}_[0-9]{6}_[0-9]{6})")
_ACTION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
_ACTION_TOKENS = re.compile(r"[._-]")

Label = Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9_-]*$")]
# Object key characters up to the client's directory; no leading `/`, ends with `/`.
Prefix = Annotated[str, Field(pattern=r"^[^/\s][^\s]*/$", max_length=200)]


class CaptureLayout(StrEnum):
    """Where a client puts the action in its keys, under `{requests,responses}/{work}/`."""

    # `{action}_{ts}.json`: the action from the key.
    WORK_UNIT_ACTION = "work_unit_action"
    # `{entity_id}_{action}_{ts}.json`: the action from object metadata `action`.
    WORK_UNIT_ENTITY_ACTION = "work_unit_entity_action"
    # `{entity_id}_{ts}.json`: the action from object metadata `action`.
    WORK_UNIT_ENTITY = "work_unit_entity"


class CaptureRule(BaseModel):
    """One client's captures: its key prefix, the label of its channels, and its layout."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    prefix: Prefix
    label: Label
    layout: CaptureLayout


class CaptureEvent(BaseModel):
    """The span event a client adds when it writes a capture, and its two key attributes.

    The `otlp` source's `capture_event`: the receiver maps each key to the span's ids for
    `ttl_s`.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1)
    request_uri: str = Field(min_length=1)
    response_uri: str = Field(min_length=1)
    ttl_s: float = Field(default=DEFAULT_CAPTURE_TTL_S, gt=0)

    @model_validator(mode="after")
    def _two_attributes(self) -> CaptureEvent:
        if self.request_uri == self.response_uri:
            raise ValueError("request_uri and response_uri must be different attributes")
        return self


@dataclass(frozen=True)
class CaptureKey:
    """A response key read by its layout.

    `work` is the work segment, `action` the key's action (None when the layout takes it from
    metadata), `stamp` the key's `%Y%m%d_%H%M%S_%f` text, and `at` that time in UTC. Only
    `at` appears in `repr`.
    """

    work: str = field(repr=False)
    action: str | None = field(repr=False)
    stamp: str = field(repr=False)
    at: datetime


def read_capture_key(name: str, layout: CaptureLayout) -> CaptureKey | None:
    """The key after `{prefix}responses/`, `{work}/{file}.json`, read by `layout`.

    None when it has another form: no work segment, a deeper path, another suffix, no
    timestamp, a timestamp that is no date, or for `work_unit_entity_action` no `_` between
    the entity id and the action. Nothing is taken from an entity id segment.
    """
    work, slash, file = name.partition("/")
    if not slash or not work or "/" in file or not file.endswith(JSON_SUFFIX):
        return None
    matched = _STEM.fullmatch(file.removesuffix(JSON_SUFFIX))
    if matched is None:
        return None
    head, stamp = matched["head"], matched["stamp"]
    try:
        at = datetime.strptime(stamp, STAMP_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        return None
    if layout is CaptureLayout.WORK_UNIT_ACTION:
        return CaptureKey(work=work, action=head, stamp=stamp, at=at)
    if layout is CaptureLayout.WORK_UNIT_ENTITY_ACTION and "_" not in head:
        return None
    return CaptureKey(work=work, action=None, stamp=stamp, at=at)


def checked_action(value: object) -> str | None:
    """The action when it is a name, else None.

    A name starts with a letter or digit, then letters, digits, `.`, `_`, or `-`, at most
    `MAX_ACTION_LENGTH` characters, is not a UUID, and none of its tokens between `.`, `_`, and
    `-` is id-like by the id rules of operation names.
    """
    if not isinstance(value, str) or len(value) > MAX_ACTION_LENGTH:
        return None
    if _ACTION.fullmatch(value) is None or ID_RULES["uuid"](value):
        return None
    if any(is_id(token) for token in _ACTION_TOKENS.split(value) if token):
        return None
    return value


def object_key(uri: str) -> str:
    """The object key of a capture URI: `s3://{bucket}/{key}` gives `key`; a bare key is kept."""
    if uri.startswith("s3://"):
        _, _, key = uri.removeprefix("s3://").partition("/")
        return key
    return uri.removeprefix("/")


@dataclass(frozen=True)
class SpanRef:
    """The span a capture event was on."""

    trace_id: str
    span_id: str


class CaptureIndex:
    """Capture object keys to the span whose capture event named them, each for its own ttl.

    In memory, on the clock its callers pass. At most `max_entries` keys; one more drops the
    oldest. Keys are never in `repr`.
    """

    def __init__(self, max_entries: int = MAX_INDEX_ENTRIES) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be at least 1")
        self._max_entries = max_entries
        self._entries: OrderedDict[str, tuple[SpanRef, datetime]] = OrderedDict()

    def put(self, uri: str, span: SpanRef, ttl_s: float, now: datetime) -> None:
        key = object_key(uri)
        if not key:
            return
        self._entries.pop(key, None)
        self._entries[key] = (span, now + timedelta(seconds=ttl_s))
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)

    def get(self, uri: str, now: datetime) -> SpanRef | None:
        key = object_key(uri)
        entry = self._entries.get(key)
        if entry is None:
            return None
        span, expires = entry
        if now >= expires:
            del self._entries[key]
            return None
        return span

    def __len__(self) -> int:
        return len(self._entries)

    def __repr__(self) -> str:
        return f"CaptureIndex(entries={len(self._entries)})"
