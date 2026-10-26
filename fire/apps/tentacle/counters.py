"""Per-source counters, logged as one line of numbers and reason codes."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field


@dataclass
class SourceCounters:
    """`sent + unsent + pending = built` at all times; `pending` is what is not settled yet.

    `rejected` and `skipped` count the tentacle's reasons; `noted` counts events that build or
    drop nothing, such as an invalid `traceparent` the proxy forwarded; `body_duplicate` and
    `body_rejected` count what the body answered for observations it took.
    """

    source: str
    read: int = 0
    built: int = 0
    sent: int = 0
    unsent: int = 0
    rejected: Counter[str] = field(default_factory=Counter)
    skipped: Counter[str] = field(default_factory=Counter)
    noted: Counter[str] = field(default_factory=Counter)
    body_duplicate: int = 0
    body_rejected: Counter[str] = field(default_factory=Counter)

    @property
    def pending(self) -> int:
        return self.built - self.sent - self.unsent

    def line(self) -> str:
        return (
            f"source={self.source} read={self.read} built={self.built} sent={self.sent} "
            f"unsent={self.unsent} pending={self.pending} rejected={_reasons(self.rejected)} "
            f"skipped={_reasons(self.skipped)} noted={_reasons(self.noted)} "
            f"body_duplicate={self.body_duplicate} body_rejected={_reasons(self.body_rejected)}"
        )


def _reasons(counts: Counter[str]) -> str:
    """`reason:count` pairs sorted by reason, or `-` when there are none."""
    pairs = [f"{reason}:{count}" for reason, count in sorted(counts.items()) if count]
    return ",".join(pairs) or "-"
