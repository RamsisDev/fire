"""Wire request for a System One reasoner: the candidate options and the Jev question set.

Shared by `EchoReasoner` (which splits probability evenly over the options) and
`LayaReasoner` (which asks `POST /v1/systemone`). Pure and deterministic, so the retrain
exporter can rebuild the same request from a stored dossier.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, ValidationError, model_validator

from fire.core.domain.decision import Possibility
from fire.core.domain.dossier import Dossier, EvidenceFragment, EvidenceQuality
from fire.core.domain.drift import ShapeDiff


class _ShapeDriftData(BaseModel):
    stream: str
    diff: ShapeDiff

    @model_validator(mode="after")
    def _diff_names_a_change(self) -> _ShapeDriftData:
        if self.diff.is_empty:
            raise ValueError("a shape drift diff names at least one change")
        return self

    def label(self) -> str:
        changes = [
            *(f"{field.path} ({field.value_type}) added" for field in self.diff.added),
            *(
                f"{change.path} changed {'|'.join(change.old)} -> {'|'.join(change.new)}"
                for change in self.diff.type_changed
            ),
            *(f"{field.path} ({field.value_type}) removed" for field in self.diff.removed),
        ]
        return f"Shape drift on {self.stream}: " + "; ".join(changes)


class _Operation(BaseModel):
    operation: str


class _PathDriftData(BaseModel):
    rule: str
    entry: _Operation
    node: _Operation

    def label(self) -> str:
        return f"{self.rule}: {self.node.operation} on {self.entry.operation}"


_DRIFT_DATA: dict[str, type[_ShapeDriftData | _PathDriftData]] = {
    "shape_drift": _ShapeDriftData,
    "path_drift": _PathDriftData,
}


def _drift_label(fragment: EvidenceFragment) -> str:
    try:
        data = _DRIFT_DATA[fragment.collector].model_validate(fragment.data)
    except ValidationError:
        return fragment.summary
    return data.label()


class _HttpErrorsData(BaseModel):
    route: _Operation
    status_min: int
    window_s: float
    errors: int
    statuses: dict[str, int]

    def label(self) -> str:
        statuses = ", ".join(f"{code} x{count}" for code, count in sorted(self.statuses.items()))
        return (
            f"HTTP errors on {self.route.operation}: {self.errors} responses "
            f">= {self.status_min} within {self.window_s:g}s ({statuses})"
        )


def _http_errors_label(fragment: EvidenceFragment) -> str:
    try:
        return _HttpErrorsData.model_validate(fragment.data).label()
    except ValidationError:
        return fragment.summary


class _DlqData(BaseModel):
    stream: str
    schema_hash: str
    observed: int = Field(ge=1)
    abandoned: int = Field(ge=0)

    def label(self) -> str:
        return (
            f"Abandoned work on {self.stream}: {self.abandoned} of {self.observed} work units "
            f"with fingerprint {self.schema_hash} ({self.abandoned / self.observed:.0%})"
        )


def _dlq_label(fragment: EvidenceFragment) -> str:
    try:
        return _DlqData.model_validate(fragment.data).label()
    except ValidationError:
        return fragment.summary


def _present(dossier: Dossier, collector: str | set[str]) -> list[EvidenceFragment]:
    names = {collector} if isinstance(collector, str) else collector
    return [
        f
        for f in dossier.fragments
        if f.collector in names and f.quality is EvidenceQuality.PRESENT
    ]


def has_evidence(dossier: Dossier) -> bool:
    """True when the dossier has a shape, fingerprint, drift, burst, or DLQ possibility."""
    return bool(
        dossier.shape is not None
        or dossier.fingerprint is not None
        or _present(dossier, set(_DRIFT_DATA))
        or _present(dossier, "http_errors")
        or _present(dossier, "dlq_attribution")
    )


def build_options(dossier: Dossier) -> list[Possibility]:
    """Candidate causes, in order, with probability split evenly.

    One per hop; one per present drift fragment, then `http_errors`, then `dlq_attribution`;
    `insufficient-evidence` when any fragment is missing; `unknown` when there is none.
    """
    possibilities: list[Possibility] = []
    for index, hop in enumerate(dossier.process_path.hops):
        label = hop.display_label()
        possibilities.append(
            Possibility(
                id=f"hop-{index}",
                label=f"Failure near {label}",
                probability=0.0,
                rationale_refs=[f"process_path:{label}"],
            )
        )
    groups = (
        ("drift", set(_DRIFT_DATA), _drift_label),
        ("burst", "http_errors", _http_errors_label),
        ("dlq", "dlq_attribution", _dlq_label),
    )
    for prefix, collector, label_of in groups:
        for index, fragment in enumerate(_present(dossier, collector)):
            possibilities.append(
                Possibility(
                    id=f"{prefix}-{index}",
                    label=label_of(fragment),
                    probability=0.0,
                    rationale_refs=[fragment.collector],
                )
            )
    missing = dossier.missing
    if missing:
        possibilities.append(
            Possibility(
                id="insufficient-evidence",
                label="Insufficient evidence (MISSING fragments)",
                probability=0.0,
                rationale_refs=[f.collector for f in missing],
            )
        )
    if not possibilities:
        has_structure = dossier.shape is not None or dossier.fingerprint is not None
        possibilities.append(
            Possibility(
                id="unknown",
                label=(
                    "No process path or drift evidence yet"
                    if has_structure
                    else "No process path, shape, or drift evidence yet"
                ),
                probability=0.0,
                rationale_refs=[],
            )
        )
    probability = 1.0 / len(possibilities)
    for possibility in possibilities:
        possibility.probability = probability
    return possibilities


def build_request(dossier: Dossier) -> dict[str, Any]:
    """The `state` and `questions` of a `POST /v1/systemone` request (no model, no auth)."""
    options = build_options(dossier)
    return {
        "state": dossier.model_dump(mode="json", exclude={"incident_id"}),
        "questions": {
            "cause": {
                "type": "choice",
                "instructions": "Which hypothesis best explains the failure?",
                "criteria": {p.id: p.label for p in options},
            },
            "sufficient": {
                "type": "noul",
                "instructions": "Is the evidence sufficient to name a cause?",
            },
        },
    }
