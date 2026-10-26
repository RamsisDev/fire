"""Port: the request and response pairs a capture source reads. Pairs never leave the tentacle."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from fire.core.domain.capture import REQUESTS, RESPONSES, CaptureRule


class ObjectState(StrEnum):
    """What a source found for one half of a pair."""

    FOUND = "found"
    MISSING = "missing"
    UNDECODABLE = "undecodable"
    TOO_LARGE = "too_large"


async def _nothing_to_ack() -> None:
    return None


@dataclass(frozen=True)
class RawCapture:
    """One request and response pair as its source read it.

    `rule` is the capture rule whose prefix holds it, and `name` the response key after
    `{prefix}responses/`; the request has the same name under `{prefix}requests/`. Each half
    is a decoded JSON object when its state is `FOUND`, else None. `metadata_action` is the
    object metadata `action` of the response, else of the request, None when neither has one.
    Only `source`, `rule`, and the states appear in `repr`.
    """

    source: str
    rule: CaptureRule
    name: str = field(repr=False)
    response_state: ObjectState
    response: Mapping[str, Any] | None = field(repr=False)
    request_state: ObjectState
    request: Mapping[str, Any] | None = field(repr=False)
    metadata_action: str | None = field(default=None, repr=False)
    ack: Callable[[], Awaitable[None]] = field(default=_nothing_to_ack, repr=False, compare=False)

    @property
    def response_key(self) -> str:
        return f"{self.rule.prefix}{RESPONSES}/{self.name}"

    @property
    def request_key(self) -> str:
        return f"{self.rule.prefix}{REQUESTS}/{self.name}"


@runtime_checkable
class CaptureSource(Protocol):
    """The capture pairs of one declared source, each read once, as their responses land.

    A live source runs until its task is cancelled. Adapters live in `fire/adapters/sources/`.
    Contract: docs/specs/tentacle.md#kinds.
    """

    def pairs(self) -> AsyncIterator[RawCapture]: ...
