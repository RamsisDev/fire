"""Deterministic local Reasoner for demos — not a generative LLM."""

from __future__ import annotations

from pydantic import BaseModel, Field, ValidationError, model_validator

from fire.core.domain.decision import Decision, Possibility, Primitive
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


class EchoReasoner:
    """Builds options from process-path hops, drift, burst, and DLQ fragments, and missing
    evidence.

    Possibilities, in order: one per hop; one per present `shape_drift` or `path_drift`
    fragment, then one per present `http_errors` fragment, then one per present
    `dlq_attribution` fragment, each labelled from its `data` (its `summary` when `data` is
    malformed); `insufficient-evidence` when any fragment is missing; `unknown` when there is
    none of these. Probabilities split evenly. The decision is `noul` when any fragment is
    missing or the dossier has no shape, fingerprint, drift, burst, or DLQ possibility. An
    `incident_io` fragment says what opened the incident, not what caused it: it adds no
    possibility and is no evidence.
    """

    async def decide(self, dossier: Dossier) -> Decision:
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
        drift = [
            f
            for f in dossier.fragments
            if f.collector in _DRIFT_DATA and f.quality is EvidenceQuality.PRESENT
        ]
        for index, fragment in enumerate(drift):
            possibilities.append(
                Possibility(
                    id=f"drift-{index}",
                    label=_drift_label(fragment),
                    probability=0.0,
                    rationale_refs=[fragment.collector],
                )
            )
        bursts = [
            f
            for f in dossier.fragments
            if f.collector == "http_errors" and f.quality is EvidenceQuality.PRESENT
        ]
        for index, fragment in enumerate(bursts):
            possibilities.append(
                Possibility(
                    id=f"burst-{index}",
                    label=_http_errors_label(fragment),
                    probability=0.0,
                    rationale_refs=[fragment.collector],
                )
            )
        abandons = [
            f
            for f in dossier.fragments
            if f.collector == "dlq_attribution" and f.quality is EvidenceQuality.PRESENT
        ]
        for index, fragment in enumerate(abandons):
            possibilities.append(
                Possibility(
                    id=f"dlq-{index}",
                    label=_dlq_label(fragment),
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
        has_structure = dossier.shape is not None or dossier.fingerprint is not None
        if not possibilities:
            possibilities.append(
                Possibility(
                    id="unknown",
                    label=(
                        "No process path or drift evidence yet"
                        if has_structure
                        else "No process path, shape, or drift evidence yet"
                    ),
                    probability=1.0,
                    rationale_refs=[],
                )
            )

        probability = 1.0 / len(possibilities)
        for possibility in possibilities:
            possibility.probability = probability

        has_evidence = has_structure or bool(drift) or bool(bursts) or bool(abandons)
        confidence = 0.2 if missing or not has_evidence else 0.45
        noul = confidence < 0.3 or not has_evidence

        return Decision(
            incident_id=dossier.incident_id,
            possibilities=possibilities,
            confidence=confidence,
            noul=noul,
            primitive=Primitive.NOUL if noul else Primitive.CHOICE,
        )
