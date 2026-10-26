"""Port: JSON payload → Shape (form only). No fingerprint, no AI."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from fire.core.domain.dossier import Shape


@runtime_checkable
class ShapeGeneralizer(Protocol):
    """Structural generalizer — keys, types, cardinality, and optionality; never values.

    The same form must give the same Shape whatever the values, array lengths, element
    order, or key order. Contract: docs/specs/shape-generalizer.md.
    """

    def generalize(self, payload: Any) -> Shape: ...
