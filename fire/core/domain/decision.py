"""System One decision: calibrated possibilities, not prose."""

from __future__ import annotations

import math
from enum import StrEnum

from pydantic import BaseModel, Field, model_validator

PROBABILITY_SUM_TOLERANCE = 1e-6


class Primitive(StrEnum):
    """System One primitive a reasoner used to reach the decision."""

    CHOICE = "choice"
    SCORE = "score"
    NOUL = "noul"


class Possibility(BaseModel):
    """One dynamic hypothesis option (usually derived from the process path)."""

    id: str
    label: str
    probability: float = Field(ge=0.0, le=1.0)
    rationale_refs: list[str] = Field(
        default_factory=list,
        description="Fragment/collector ids cited — not free-form invention.",
    )


class Decision(BaseModel):
    """Output of the Reasoner (System One). Narrator consumes this; does not invent it."""

    incident_id: str
    possibilities: list[Possibility] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)
    noul: bool = Field(
        default=False,
        description="Privacy/insufficient-evidence gate: do not narrate as settled cause.",
    )
    primitive: Primitive = Field(
        default=Primitive.CHOICE,
        description="System One primitive used.",
    )

    @model_validator(mode="after")
    def _possibilities_form_a_distribution(self) -> Decision:
        if not self.possibilities:
            return self
        total = math.fsum(p.probability for p in self.possibilities)
        if not math.isclose(total, 1.0, abs_tol=PROBABILITY_SUM_TOLERANCE):
            raise ValueError(f"possibility probabilities must sum to 1, got {total}")
        return self
