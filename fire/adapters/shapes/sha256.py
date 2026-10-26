"""SHA-256 ShapeFingerprinter: hash of the Shape's fields in canonical order."""

from __future__ import annotations

import hashlib
import json

from fire.core.domain.dossier import Shape, ShapeFingerprint


class Sha256ShapeFingerprinter:
    """`schema_hash` is the hex SHA-256 of the fields sorted by every attribute, as JSON.

    `normalized_paths` is the sorted set of field paths.
    """

    def fingerprint(self, shape: Shape) -> ShapeFingerprint:
        rows = sorted(
            (
                (field.path, field.value_type.value, field.cardinality, field.optional)
                for field in shape.fields
            ),
            # "" is not a valid cardinality, so it stands in for None without colliding.
            key=lambda row: (row[0], row[1], row[2] or "", row[3]),
        )
        canonical = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
        return ShapeFingerprint(
            schema_hash=hashlib.sha256(canonical.encode()).hexdigest(),
            normalized_paths=sorted({field.path for field in shape.fields}),
        )
