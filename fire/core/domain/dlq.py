"""DLQ attribution: abandoned work units joined to the fingerprints their observations carried.

A work unit is abandoned when a shape from one of the system's outcome sources carries its
`work_id`. Per fingerprint, `observed` counts the work units whose observations carried it and
`abandoned` how many of those were abandoned, both inside the catalog's retention window. Nothing
here holds a value: ids, channel keys, hashes, counts, and times.
Contract: docs/specs/ingest-drift-and-feedback.md#dlq-attribution.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Collection, Iterable
from dataclasses import dataclass
from datetime import datetime

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, computed_field


class DlqTrigger(BaseModel):
    """When a fingerprint's abandoned work units open an incident: the system YAML's
    `triggers.dlq`.
    """

    model_config = ConfigDict(extra="forbid")

    # Abandoned work units that carried the fingerprint.
    min_abandoned: int = Field(default=3, ge=1)
    # Abandoned over observed work units of the fingerprint.
    min_rate: float = Field(default=0.2, gt=0, le=1, allow_inf_nan=False)

    def crosses(self, fingerprint: FingerprintOutcomes) -> bool:
        return fingerprint.abandoned >= self.min_abandoned and fingerprint.rate >= self.min_rate


@dataclass(frozen=True)
class WorkRef:
    """One work index entry, as the tally reads it."""

    source: str
    key: str
    schema_hash: str
    work_id: str
    occurred_at: datetime


class FingerprintOutcomes(BaseModel):
    """One fingerprint of one stream with at least one abandoned work unit.

    `abandoned_by` counts its abandoned work units per outcome channel key that marked them;
    `first_abandoned` and `last_abandoned` are the earliest and latest `occurred_at` of their
    outcomes.
    """

    model_config = ConfigDict(extra="forbid")

    source: str
    stream: str
    schema_hash: str
    observed: int = Field(ge=1)
    abandoned: int = Field(ge=1)
    abandoned_by: dict[str, int]
    first_abandoned: AwareDatetime
    last_abandoned: AwareDatetime

    @computed_field  # type: ignore[prop-decorator]
    @property
    def rate(self) -> float:
        return self.abandoned / self.observed

    @property
    def ref(self) -> tuple[str, str, str]:
        return (self.source, self.stream, self.schema_hash)


class StreamOutcomes(BaseModel):
    """The work units of one stream, across its fingerprints, and how many were abandoned."""

    model_config = ConfigDict(extra="forbid")

    source: str
    stream: str
    observed: int = Field(ge=1)
    abandoned: int = Field(ge=0)


class WorkOutcomes(BaseModel):
    """A system's abandoned work units joined to its work index.

    `abandoned` counts the distinct abandoned work units; `unattributed` lists, sorted, those no
    observation of another source carried. `fingerprints` holds the fingerprints with at least
    one abandoned work unit, sorted by source, stream, and hash; `streams` the streams they are
    in, sorted by source and stream.
    """

    model_config = ConfigDict(extra="forbid")

    abandoned: int = Field(default=0, ge=0)
    unattributed: list[str] = Field(default_factory=list)
    fingerprints: list[FingerprintOutcomes] = Field(default_factory=list)
    streams: list[StreamOutcomes] = Field(default_factory=list)


def tally_outcomes(refs: Iterable[WorkRef], outcome_sources: Collection[str]) -> WorkOutcomes:
    """Joins the work index entries of `outcome_sources` to the entries of every other source."""
    abandoned_at: dict[str, datetime] = {}
    marked_by: defaultdict[str, set[str]] = defaultdict(set)
    carried: defaultdict[tuple[str, str, str], set[str]] = defaultdict(set)
    in_stream: defaultdict[tuple[str, str], set[str]] = defaultdict(set)
    for ref in refs:
        if ref.source in outcome_sources:
            at = abandoned_at.get(ref.work_id)
            abandoned_at[ref.work_id] = ref.occurred_at if at is None else min(at, ref.occurred_at)
            marked_by[ref.work_id].add(ref.key)
        else:
            carried[(ref.source, ref.key, ref.schema_hash)].add(ref.work_id)
            in_stream[(ref.source, ref.key)].add(ref.work_id)
    fingerprints: list[FingerprintOutcomes] = []
    for (source, key, schema_hash), work in sorted(carried.items()):
        hit = work & abandoned_at.keys()
        if not hit:
            continue
        times = [abandoned_at[work_id] for work_id in hit]
        by = Counter(outcome for work_id in hit for outcome in marked_by[work_id])
        fingerprints.append(
            FingerprintOutcomes(
                source=source,
                stream=key,
                schema_hash=schema_hash,
                observed=len(work),
                abandoned=len(hit),
                abandoned_by=dict(sorted(by.items())),
                first_abandoned=min(times),
                last_abandoned=max(times),
            )
        )
    listed = {(fingerprint.source, fingerprint.stream) for fingerprint in fingerprints}
    seen = set().union(*in_stream.values())
    return WorkOutcomes(
        abandoned=len(abandoned_at),
        unattributed=sorted(abandoned_at.keys() - seen),
        fingerprints=fingerprints,
        streams=[
            StreamOutcomes(
                source=source,
                stream=key,
                observed=len(work),
                abandoned=len(work & abandoned_at.keys()),
            )
            for (source, key), work in sorted(in_stream.items())
            if (source, key) in listed
        ],
    )
