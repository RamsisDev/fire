"""Paths: what the folded flows of one entry reached, and the path drift of the flows it did not.

A path keeps names, kinds, channel keys, and counts: no span, timestamp, duration, status code,
shape, or value. It is the baseline of path drift, warm from `drift.min_samples` folded flows.
Contract: docs/specs/flows-and-paths.md#paths and #path-drift.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from fire.core.domain.baseline import BaselineStatus, DriftPolicy
from fire.core.domain.dossier import HopVia, ProcessPath
from fire.core.domain.flow import (
    CompleteFlow,
    EdgeKey,
    EdgeReach,
    FlowCounters,
    Node,
    NodeReach,
)
from fire.core.domain.observation import SpanKind, TraceId


class PathDriftRule(StrEnum):
    HOP_ADDED = "hop_added"
    HOP_MISSING = "hop_missing"
    HOP_FAILING = "hop_failing"


_RULE_ORDER = list(PathDriftRule)


class PathDriftSignal(BaseModel):
    """One rule firing for one node of a path: how many flows fired it, and when.

    `first_seen` and `last_seen` are the earliest and latest entry start among those flows;
    `trace_ids` are the first such flow's, sorted.
    """

    model_config = ConfigDict(extra="forbid")

    rule: PathDriftRule
    node: Node
    occurrences: int = Field(ge=1)
    first_seen: AwareDatetime
    last_seen: AwareDatetime
    trace_ids: list[TraceId] = Field(min_length=1)


class PathDriftFiring(BaseModel):
    """A signal one drifted flow fired, as the flow left it, and the counts its rule read.

    `node_flows` and `node_errors` are the path's, `0` for `hop_added`; `flow_spans` and
    `flow_errors` are the flow's, `0` for `hop_missing`.
    """

    model_config = ConfigDict(extra="forbid")

    signal: PathDriftSignal
    new: bool
    path_flows: int = Field(ge=0)
    node_flows: int = Field(ge=0)
    node_errors: int = Field(ge=0)
    flow_spans: int = Field(ge=0)
    flow_errors: int = Field(ge=0)


class PathDrift(BaseModel):
    """A complete flow that fired at least one new path drift signal: what its incident needs.

    `firings` holds every signal the flow fired, new or not, in rule order, then by node.
    """

    model_config = ConfigDict(extra="forbid")

    system: str
    entry: Node
    started_at: AwareDatetime
    trace_ids: list[TraceId] = Field(min_length=1)
    process_path: ProcessPath
    firings: list[PathDriftFiring] = Field(min_length=1)


class FlowOutcome(StrEnum):
    """What a complete flow ended as; the first that applies, in this order."""

    ORPHAN = "orphan"
    TRUNCATED = "truncated"
    BROKEN = "broken"
    DRIFTED = "drifted"
    FOLDED = "folded"


class NodeSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service: str
    operation: str
    kinds: list[SpanKind]
    flows: int = Field(ge=0)
    spans: int = Field(ge=0)
    errors: int = Field(ge=0)
    channels: dict[str, int]


class EdgeSummary(BaseModel):
    """`from` and `to` are positions in the path's `nodes`."""

    model_config = ConfigDict(extra="forbid")

    from_: int = Field(alias="from", ge=0)
    to: int = Field(ge=0)
    via: HopVia
    flows: int = Field(ge=0)
    count: int = Field(ge=0)
    errors: int = Field(ge=0)


class PathSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entry: Node
    status: BaselineStatus
    flows: int = Field(ge=0)
    drifted_flows: int = Field(ge=0)
    broken_flows: int = Field(ge=0)
    truncated_flows: int = Field(ge=0)
    nodes: list[NodeSummary]
    edges: list[EdgeSummary]
    flow_channels: dict[str, int]
    drift: list[PathDriftSignal]


class PathsReport(BaseModel):
    """`GET /v0/systems/{system}/paths`: dumped with `by_alias=True` for the edges' `from`."""

    model_config = ConfigDict(extra="forbid")

    system: str
    open_flows: int = Field(ge=0)
    counters: FlowCounters
    paths: list[PathSummary]


@dataclass
class NodeStats:
    kinds: set[SpanKind] = field(default_factory=set)
    # Folded flows that reached the node.
    flows: int = 0
    spans: int = 0
    errors: int = 0
    # Per channel key, the folded flows whose shape on it attached to the node.
    channels: Counter[str] = field(default_factory=Counter)


@dataclass
class EdgeStats:
    flows: int = 0
    # Traversals, and the traversals whose `to` span erred.
    count: int = 0
    errors: int = 0


@dataclass
class Path:
    """One entry's path: what its folded flows reached, and its path drift signals.

    A drifted, broken, or truncated flow is counted and never folded.
    """

    entry: Node
    flows: int = 0
    drifted_flows: int = 0
    broken_flows: int = 0
    truncated_flows: int = 0
    nodes: dict[Node, NodeStats] = field(default_factory=dict)
    edges: dict[EdgeKey, EdgeStats] = field(default_factory=dict)
    # Per channel key, the folded flows its shapes attached to without a span.
    flow_channels: Counter[str] = field(default_factory=Counter)
    signals: dict[tuple[PathDriftRule, Node], PathDriftSignal] = field(default_factory=dict)

    def status(self, policy: DriftPolicy) -> BaselineStatus:
        if self.flows >= policy.min_samples:
            return BaselineStatus.WARM
        return BaselineStatus.REBUILDING

    def fires(
        self, reached: Mapping[Node, NodeReach], policy: DriftPolicy
    ) -> list[tuple[PathDriftRule, Node]]:
        """The rules that fire for a flow that reached `reached`, in rule order, then by node."""
        fired: list[tuple[PathDriftRule, Node]] = []
        for node in sorted(reached, key=lambda node: node.sort_key):
            stats = self.nodes.get(node)
            if stats is None:
                fired.append((PathDriftRule.HOP_ADDED, node))
            elif stats.errors == 0 and reached[node].errors > 0:
                fired.append((PathDriftRule.HOP_FAILING, node))
        for node in sorted(self.nodes, key=lambda node: node.sort_key):
            stats = self.nodes[node]
            required = self.flows > 0 and stats.flows / self.flows >= policy.required_ratio
            if required and node not in reached:
                fired.append((PathDriftRule.HOP_MISSING, node))
        return sorted(fired, key=lambda item: (_RULE_ORDER.index(item[0]), item[1].sort_key))

    def record(
        self,
        rule: PathDriftRule,
        node: Node,
        reached: Mapping[Node, NodeReach],
        started_at: datetime,
        trace_ids: list[str],
    ) -> PathDriftFiring:
        """Adds one occurrence of `(rule, node)` for a flow that reached `reached` and started at
        `started_at`, opening the signal with the flow's `trace_ids` if it is new.
        """
        signal = self.signals.get((rule, node))
        new = signal is None
        if signal is None:
            signal = PathDriftSignal(
                rule=rule,
                node=node,
                occurrences=1,
                first_seen=started_at,
                last_seen=started_at,
                trace_ids=trace_ids,
            )
            self.signals[(rule, node)] = signal
        else:
            signal.occurrences += 1
            signal.first_seen = min(signal.first_seen, started_at)
            signal.last_seen = max(signal.last_seen, started_at)
        stats = self.nodes.get(node)
        flow_reach = reached.get(node)
        return PathDriftFiring(
            signal=signal.model_copy(deep=True),
            new=new,
            path_flows=self.flows,
            node_flows=0 if stats is None else stats.flows,
            node_errors=0 if stats is None else stats.errors,
            flow_spans=0 if flow_reach is None else flow_reach.spans,
            flow_errors=0 if flow_reach is None else flow_reach.errors,
        )

    def fold(
        self,
        reached: Mapping[Node, NodeReach],
        edges: Mapping[EdgeKey, EdgeReach],
        node_channels: Mapping[Node, set[str]],
        flow_channels: set[str],
    ) -> None:
        self.flows += 1
        for node, reach in reached.items():
            stats = self.nodes.setdefault(node, NodeStats())
            stats.kinds |= reach.kinds
            stats.flows += 1
            stats.spans += reach.spans
            stats.errors += reach.errors
            stats.channels.update(node_channels.get(node, ()))
        for key, traversed in edges.items():
            edge = self.edges.setdefault(key, EdgeStats())
            edge.flows += 1
            edge.count += traversed.count
            edge.errors += traversed.errors
        self.flow_channels.update(flow_channels)

    def summary(self, policy: DriftPolicy) -> PathSummary:
        nodes = sorted(self.nodes, key=lambda node: node.sort_key)
        position = {node: index for index, node in enumerate(nodes)}
        edges = sorted(
            (
                EdgeSummary.model_validate(
                    {
                        "from": position[source],
                        "to": position[target],
                        "via": via,
                        "flows": stats.flows,
                        "count": stats.count,
                        "errors": stats.errors,
                    }
                )
                for (source, target, via), stats in self.edges.items()
            ),
            key=lambda edge: (edge.from_, edge.to, edge.via),
        )
        return PathSummary(
            entry=self.entry,
            status=self.status(policy),
            flows=self.flows,
            drifted_flows=self.drifted_flows,
            broken_flows=self.broken_flows,
            truncated_flows=self.truncated_flows,
            nodes=[
                NodeSummary(
                    service=node.service,
                    operation=node.operation,
                    kinds=sorted(self.nodes[node].kinds),
                    flows=self.nodes[node].flows,
                    spans=self.nodes[node].spans,
                    errors=self.nodes[node].errors,
                    channels=dict(sorted(self.nodes[node].channels.items())),
                )
                for node in nodes
            ],
            edges=edges,
            flow_channels=dict(sorted(self.flow_channels.items())),
            drift=[
                signal.model_copy(deep=True)
                for signal in sorted(
                    self.signals.values(),
                    key=lambda signal: (
                        signal.first_seen,
                        _RULE_ORDER.index(signal.rule),
                        signal.node.sort_key,
                    ),
                )
            ],
        )


def settle(
    flow: CompleteFlow,
    paths: MutableMapping[Node, Path],
    policy: DriftPolicy,
    counters: FlowCounters,
    system: str,
) -> tuple[FlowOutcome, PathDrift | None]:
    """Ends a complete flow in exactly one outcome, and returns its path drift when it fired a
    new signal.

    An orphan has no entry, and its shapes count as unattached. A truncated or broken flow is
    counted on its entry's path. On a warm path, a flow that fires any rule is drifted: its
    signals record it and it is not folded. Every other flow folds, comparison skipped while the
    path is rebuilding, and its shapes attach to it.
    """
    entry = flow.entry
    if entry is None:
        counters.orphan_flows += 1
        counters.unattached_shapes += len(flow.shapes)
        return FlowOutcome.ORPHAN, None
    node = Node.of(entry)
    path = paths.get(node)
    if path is None:
        path = paths[node] = Path(entry=node)
    if flow.truncated:
        path.truncated_flows += 1
        return FlowOutcome.TRUNCATED, None
    if flow.broken:
        path.broken_flows += 1
        return FlowOutcome.BROKEN, None
    reached, edges = flow.reach()
    if path.status(policy) is BaselineStatus.WARM:
        fired = path.fires(reached, policy)
        if fired:
            firings = [
                path.record(rule, hop, reached, entry.start, flow.trace_ids) for rule, hop in fired
            ]
            path.drifted_flows += 1
            if not any(firing.new for firing in firings):
                return FlowOutcome.DRIFTED, None
            return FlowOutcome.DRIFTED, PathDrift(
                system=system,
                entry=node,
                started_at=entry.start,
                trace_ids=flow.trace_ids,
                process_path=flow.process_path(),
                firings=firings,
            )
    node_channels, flow_channels, unattached = flow.attachments()
    path.fold(reached, edges, node_channels, flow_channels)
    counters.unattached_shapes += unattached
    return FlowOutcome.FOLDED, None
