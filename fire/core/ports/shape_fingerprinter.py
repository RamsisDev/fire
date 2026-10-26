"""Port: Shape → ShapeFingerprint (stable id of a form). Never sees payload values."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from fire.core.domain.dossier import Shape, ShapeFingerprint


@runtime_checkable
class ShapeFingerprinter(Protocol):
    """Stable id for a Shape.

    Shapes with the same fields give the same fingerprint whatever the order of
    `Shape.fields`; any change to a field's path, value type, cardinality, or optionality
    gives a different one.
    """

    def fingerprint(self, shape: Shape) -> ShapeFingerprint: ...
