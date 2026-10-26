"""Port: a system's self-description document → SystemInventory. Does not read files."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

from fire.core.domain.inventory import SystemInventory


@runtime_checkable
class ManifestParser(Protocol):
    """Parses one manifest format. Raises ValueError when the document is not that format."""

    def parse(self, system: str, document: Mapping[str, Any]) -> SystemInventory: ...
