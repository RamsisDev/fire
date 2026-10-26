"""Flows: accepted spans assembled into units of work, and what one complete flow reached.

A flow is a head trace plus every trace that links into it, directly or through another linked
trace. The body assembles flows per system and completes each once `settle_s` passes with no
span joining it; complete flows fold into paths (fire/core/domain/path.py). Nothing here holds a
value: spans carry names, ids, statuses, and times, and a shape is kept as its channel key.
Contract: docs/specs/flows-and-paths.md.
"""

from __future__ import annotations

from collections import OrderedDict, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from functools import cached_property

from pydantic import BaseModel, ConfigDict, Field

from fire.core.domain.dossier import HopVia, ProcessHop, ProcessPath
from fire.core.domain.observation import SpanKind, SpanObservation

# A complete flow's traces stay in the completed index for this many settle times.
COMPLETED_RETENTION = 10


class PathPolicy(BaseModel):
    """When an open flow completes, and how much the body holds open. Set per system."""

    model_config = ConfigDict(extra="forbid")

    # A flow completes after this long with no span joining it.
    settle_s: float = Field(default=60, ge=1, allow_inf_nan=False)
    # One more evicts the open flow joined least recently.
    max_open_flows: int = Field(default=10000, ge=1)
    # More are dropped and the flow is truncated.
    max_spans_per_flow: int = Field(default=1000, ge=1)


class Node(BaseModel):
    """One `(service, operation)`: the same operation in two services is two nodes."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    service: str
    operation: str

    @classmethod
    def of(cls, span: SpanObservation) -> Node:
        return cls(service=span.service, operation=span.operation)

    @property
    def sort_key(self) -> tuple[str, str]:
        return (self.service, self.operation)


class FlowCounters(BaseModel):
    """Per system, what the body dropped or could not place since it started."""

    model_config = ConfigDict(extra="forbid")

    orphan_flows: int = 0
    evicted_flows: int = 0
    late_spans: int = 0
    late_shapes: int = 0
    unattached_shapes: int = 0
    fan_in_links: int = 0


@dataclass(frozen=True)
class ShapeRef:
    """What flows keep of an accepted shape: its channel key and the ids that join it."""

    channel_key: str
    trace_id: str | None = None
    span_id: str | None = None
    work_id: str | None = None


@dataclass
class NodeReach:
    """What one flow did at one node: the kinds of its spans, how many, and how many erred."""

    kinds: set[SpanKind] = field(default_factory=set)
    spans: int = 0
    errors: int = 0


@dataclass
class EdgeReach:
    """How often one flow traversed one edge; `errors` counts traversals whose `to` span erred."""

    count: int = 0
    errors: int = 0


# `(from, to, via)`: an edge's identity.
EdgeKey = tuple[Node, Node, HopVia]


def _order(span: SpanObservation) -> tuple[datetime, str]:
    return (span.start, span.span_id)


def _starts_a_flow(root: SpanObservation) -> bool:
    """Whether a local root of the head trace may be its flow's entry.

    A `server` or `consumer` span takes work from outside its service, so its parent or link may
    be a span FIRE never receives. An `internal` root is an entry only with no parent and no link;
    a `client` or `producer` root means the caller's span is missing.
    """
    if root.kind in (SpanKind.SERVER, SpanKind.CONSUMER):
        return True
    return root.kind is SpanKind.INTERNAL and root.parent_span_id is None and not root.links


def _status(spans: list[SpanObservation]) -> str:
    """`error` when any span erred, else `ok`, then the highest status code among them."""
    state = "error" if any(span.error for span in spans) else "ok"
    codes = [span.status_code for span in spans if span.status_code is not None]
    return f"{state} {max(codes)}" if codes else state


@dataclass(frozen=True)
class CompleteFlow:
    """A flow that settled: its traces, head first, the spans it kept, and its shapes."""

    head: str
    traces: tuple[str, ...]
    spans: tuple[SpanObservation, ...]
    shapes: tuple[ShapeRef, ...] = ()
    truncated: bool = False

    @cached_property
    def _by_id(self) -> dict[tuple[str, str], SpanObservation]:
        return {(span.trace_id, span.span_id): span for span in self.spans}

    def _parent(self, span: SpanObservation) -> SpanObservation | None:
        if span.parent_span_id is None:
            return None
        return self._by_id.get((span.trace_id, span.parent_span_id))

    def _causes(self, span: SpanObservation) -> list[tuple[SpanObservation, HopVia]]:
        """The spans of this flow that `span` continues: its parent, then each span it links to."""
        causes: list[tuple[SpanObservation, HopVia]] = []
        parent = self._parent(span)
        if parent is not None:
            causes.append((parent, HopVia.CHILD))
        for link in span.links:
            linked = self._by_id.get((link.trace_id, link.span_id))
            if linked is not None:
                causes.append((linked, HopVia.LINK))
        return causes

    @cached_property
    def local_roots(self) -> dict[str, list[SpanObservation]]:
        """Per trace, its spans whose parent is not in the flow, earliest first."""
        roots: dict[str, list[SpanObservation]] = {trace: [] for trace in self.traces}
        for span in sorted(self.spans, key=_order):
            if self._parent(span) is None:
                roots[span.trace_id].append(span)
        return roots

    @cached_property
    def entry(self) -> SpanObservation | None:
        """The earliest local root of the head trace that may start a flow; None for an orphan."""
        return next((root for root in self.local_roots[self.head] if _starts_a_flow(root)), None)

    @property
    def broken(self) -> bool:
        """A trace has two local roots."""
        return any(len(roots) > 1 for roots in self.local_roots.values())

    @property
    def trace_ids(self) -> list[str]:
        return sorted(self.traces)

    def reach(self) -> tuple[dict[Node, NodeReach], dict[EdgeKey, EdgeReach]]:
        """The nodes and edges the flow reached, with its own counts."""
        nodes: defaultdict[Node, NodeReach] = defaultdict(NodeReach)
        edges: defaultdict[EdgeKey, EdgeReach] = defaultdict(EdgeReach)
        for span in self.spans:
            node = Node.of(span)
            reached = nodes[node]
            reached.kinds.add(span.kind)
            reached.spans += 1
            reached.errors += int(span.error)
            for cause, via in self._causes(span):
                edge = edges[(Node.of(cause), node, via)]
                edge.count += 1
                edge.errors += int(span.error)
        return dict(nodes), dict(edges)

    def attachments(self) -> tuple[dict[Node, set[str]], set[str], int]:
        """Where the flow's shapes attach: channel keys per node, the flow's own channel keys,
        and how many shapes attach nowhere.

        A shape with a span id attaches to that span's node or, when no span has the id, to the
        node of the `server` span whose parent it is: the inbound proxy's id. One with only a
        trace id attaches to the flow.
        """
        servers: dict[tuple[str, str], SpanObservation] = {}
        for span in sorted(self.spans, key=_order):
            if span.kind is SpanKind.SERVER and span.parent_span_id is not None:
                servers.setdefault((span.trace_id, span.parent_span_id), span)
        nodes: defaultdict[Node, set[str]] = defaultdict(set)
        flow: set[str] = set()
        unattached = 0
        for shape in self.shapes:
            if shape.trace_id is None or shape.span_id is None:
                flow.add(shape.channel_key)
                continue
            key = (shape.trace_id, shape.span_id)
            span = self._by_id.get(key) or servers.get(key)
            if span is None:
                unattached += 1
            else:
                nodes[Node.of(span)].add(shape.channel_key)
        return dict(nodes), flow, unattached

    def process_path(self) -> ProcessPath:
        """The flow as dossier hops: one per node, and one per edge that crosses services or a
        link, each edge just before its `to` node.

        Nodes sort by the earliest start of their spans, then service and operation; the edges
        into one node by the earliest start of their `to` spans, then `from` service and `via`.
        Edges that differ only in their `from` operation are one hop.
        """
        node_spans: defaultdict[Node, list[SpanObservation]] = defaultdict(list)
        edge_spans: defaultdict[tuple[str, Node, HopVia], list[SpanObservation]] = defaultdict(
            list
        )
        for span in sorted(self.spans, key=_order):
            node = Node.of(span)
            node_spans[node].append(span)
            for cause, via in self._causes(span):
                if via is HopVia.LINK or cause.service != span.service:
                    edge_spans[(cause.service, node, via)].append(span)
        incoming: defaultdict[Node, list[tuple[str, HopVia, list[SpanObservation]]]] = defaultdict(
            list
        )
        for (from_ref, node, via), traversed in edge_spans.items():
            incoming[node].append((from_ref, via, traversed))
        hops: list[ProcessHop] = []
        ordered = sorted(
            node_spans.items(), key=lambda item: (item[1][0].start, *item[0].sort_key)
        )
        for node, spans in ordered:
            kind = spans[0].kind.value
            for from_ref, via, traversed in sorted(
                incoming[node], key=lambda edge: (edge[2][0].start, edge[0], edge[1])
            ):
                hops.append(
                    ProcessHop(
                        from_ref=from_ref,
                        to_ref=node.service,
                        via=via,
                        operation=node.operation,
                        span_kind=kind,
                        status=_status(traversed),
                    )
                )
            hops.append(
                ProcessHop(
                    ref=node.service,
                    operation=node.operation,
                    span_kind=kind,
                    status=_status(spans),
                )
            )
        return ProcessPath(hops=hops)


@dataclass
class _OpenFlow:
    head: str
    last_joined: datetime
    # Head first, then in the order they joined.
    traces: list[str]
    spans: dict[tuple[str, str], SpanObservation] = field(default_factory=dict)
    shapes: list[ShapeRef] = field(default_factory=list)
    truncated: bool = False

    def keep(self, span: SpanObservation, limit: int) -> None:
        if len(self.spans) >= limit:
            self.truncated = True
        else:
            self.spans[(span.trace_id, span.span_id)] = span


class FlowAssembler:
    """One system's open flows, and the indexes that join spans and shapes to them.

    Every time is the body's clock at acceptance, passed in by the caller, which also holds the
    lock that makes each call one step. A duplicate span must never reach `add_span`.
    """

    def __init__(self, policy: PathPolicy) -> None:
        self._policy = policy
        self._settle = timedelta(seconds=policy.settle_s)
        # Least recently joined first.
        self._open: OrderedDict[int, _OpenFlow] = OrderedDict()
        self._next_id = 0
        # Trace id to the open flow holding it.
        self._flow_of: dict[str, int] = {}
        # Trace id to when its flow completed, was evicted, or was dropped as late.
        self._completed: dict[str, datetime] = {}
        # Trace id not seen yet to the traces whose links wait for its first span.
        self._waiting: defaultdict[str, list[str]] = defaultdict(list)
        # Trace id not seen yet to the shapes that wait for its first span.
        self._pending: defaultdict[str, list[tuple[ShapeRef, datetime]]] = defaultdict(list)
        # Work id to the trace of the first shape that carried both.
        self._work: dict[str, str] = {}
        self._pending_work: defaultdict[str, list[tuple[ShapeRef, datetime]]] = defaultdict(list)
        self.counters = FlowCounters()

    @property
    def open_flows(self) -> int:
        return len(self._open)

    def add_span(self, span: SpanObservation, now: datetime) -> None:
        """Joins an accepted span to its flow, or counts it late."""
        trace = span.trace_id
        if trace in self._completed:
            self.counters.late_spans += 1
            return
        new_trace = trace not in self._flow_of
        if new_trace:
            target = self._first_known(span)
            if target is not None and target in self._completed:
                self._completed[trace] = now
                self.counters.late_spans += 1
                self.counters.late_shapes += len(self._pending.pop(trace, []))
                self._resolve_waiting(trace, now)
                return
            if target is None:
                self._start(trace, now)
            else:
                self._join(trace, self._flow_of[target])
            adopted = self._pending.pop(trace, [])
            self._open[self._flow_of[trace]].shapes.extend(shape for shape, _ in adopted)
        if not self._link(span, now):
            self.counters.late_spans += 1
            return
        flow_id = self._flow_of[trace]
        flow = self._open[flow_id]
        flow.keep(span, self._policy.max_spans_per_flow)
        flow.last_joined = now
        self._open.move_to_end(flow_id)
        if new_trace:
            self._resolve_waiting(trace, now)

    def add_shape(self, shape: ShapeRef, now: datetime) -> None:
        """Joins an accepted shape to its flow; a shape never extends a flow's settle time."""
        if shape.trace_id is not None:
            if shape.work_id is not None and shape.work_id not in self._work:
                self._work[shape.work_id] = shape.trace_id
                for waiting, at in self._pending_work.pop(shape.work_id, []):
                    self._route(replace(waiting, trace_id=shape.trace_id), at)
            self._route(shape, now)
        elif shape.work_id is not None:
            trace = self._work.get(shape.work_id)
            if trace is None:
                self._pending_work[shape.work_id].append((shape, now))
            else:
                self._route(replace(shape, trace_id=trace), now)

    def complete(self, now: datetime) -> list[CompleteFlow]:
        """Completes every flow no span joined for `settle_s`, least recently joined first.

        Shapes that waited `settle_s` for a flow count as unattached, and the completed index
        forgets traces after `COMPLETED_RETENTION` settle times.
        """
        settled = [
            flow_id
            for flow_id, flow in self._open.items()
            if now - flow.last_joined >= self._settle
        ]
        complete: list[CompleteFlow] = []
        for flow_id in settled:
            flow = self._close(flow_id, now)
            complete.append(
                CompleteFlow(
                    head=flow.head,
                    traces=tuple(flow.traces),
                    spans=tuple(flow.spans.values()),
                    shapes=tuple(flow.shapes),
                    truncated=flow.truncated,
                )
            )
        self._expire(now)
        return complete

    def snapshot(self, trace_ids: Iterable[str]) -> list[CompleteFlow]:
        """The open flows holding any of `trace_ids`, each once, as assembled so far; a trace no
        open flow holds gives nothing. Nothing completes.
        """
        flow_ids = dict.fromkeys(
            self._flow_of[trace] for trace in trace_ids if trace in self._flow_of
        )
        return [
            CompleteFlow(
                head=flow.head,
                traces=tuple(flow.traces),
                spans=tuple(flow.spans.values()),
                shapes=tuple(flow.shapes),
                truncated=flow.truncated,
            )
            for flow in (self._open[flow_id] for flow_id in flow_ids)
        ]

    def _first_known(self, span: SpanObservation) -> str | None:
        """The trace of the span's first link to another trace, open or completed."""
        for link in span.links:
            target = link.trace_id
            if target != span.trace_id and (target in self._flow_of or target in self._completed):
                return target
        return None

    def _start(self, trace: str, now: datetime) -> None:
        if len(self._open) >= self._policy.max_open_flows:
            self._close(next(iter(self._open)), now)
            self.counters.evicted_flows += 1
        flow_id = self._next_id
        self._next_id += 1
        self._open[flow_id] = _OpenFlow(head=trace, last_joined=now, traces=[trace])
        self._flow_of[trace] = flow_id

    def _join(self, trace: str, flow_id: int) -> None:
        self._open[flow_id].traces.append(trace)
        self._flow_of[trace] = flow_id

    def _link(self, span: SpanObservation, now: datetime) -> bool:
        """Applies the span's links; False when one drops the span's flow as late."""
        for link in span.links:
            target = link.trace_id
            if target == span.trace_id:
                continue
            if target in self._flow_of or target in self._completed:
                if not self._resolve(span.trace_id, target, now):
                    return False
            else:
                self._waiting[target].append(span.trace_id)
        return True

    def _resolve(self, trace: str, target: str, now: datetime) -> bool:
        """Applies one link from open `trace` to known `target`; False when it drops `trace`'s
        flow as late.

        A head trace joins through its first link to another flow, so the two flows merge, and a
        head whose first such link names a completed trace makes its whole flow late. Any other
        trace has joined already: a link from it to another flow is counted, nothing else.
        """
        flow_id = self._flow_of[trace]
        is_head = self._open[flow_id].head == trace
        if target in self._completed:
            if is_head:
                self._drop_late(flow_id, now)
                return False
            self.counters.fan_in_links += 1
            return True
        target_flow = self._flow_of[target]
        if target_flow == flow_id:
            return True
        if is_head:
            self._merge(flow_id, target_flow)
        else:
            self.counters.fan_in_links += 1
        return True

    def _resolve_waiting(self, target: str, now: datetime) -> None:
        """Applies the links that waited for `target`'s first span."""
        for trace in self._waiting.pop(target, []):
            if trace in self._flow_of:
                self._resolve(trace, target, now)

    def _merge(self, joining_id: int, into_id: int) -> None:
        joining = self._open.pop(joining_id)
        into = self._open[into_id]
        for trace in joining.traces:
            self._flow_of[trace] = into_id
        into.traces.extend(joining.traces)
        for span in joining.spans.values():
            into.keep(span, self._policy.max_spans_per_flow)
        into.truncated = into.truncated or joining.truncated
        into.shapes.extend(joining.shapes)
        into.last_joined = max(into.last_joined, joining.last_joined)

    def _close(self, flow_id: int, now: datetime) -> _OpenFlow:
        """Removes an open flow; its traces enter the completed index."""
        flow = self._open.pop(flow_id)
        for trace in flow.traces:
            del self._flow_of[trace]
            self._completed[trace] = now
        return flow

    def _drop_late(self, flow_id: int, now: datetime) -> None:
        flow = self._close(flow_id, now)
        self.counters.late_spans += len(flow.spans)
        self.counters.late_shapes += len(flow.shapes)

    def _route(self, shape: ShapeRef, at: datetime) -> None:
        """Puts a shape with a trace id in its open flow, or waits for the trace's first span."""
        trace = shape.trace_id
        if trace is None:
            return
        if trace in self._completed:
            self.counters.late_shapes += 1
        elif trace in self._flow_of:
            self._open[self._flow_of[trace]].shapes.append(shape)
        else:
            self._pending[trace].append((shape, at))

    def _expire(self, now: datetime) -> None:
        for pending in (self._pending, self._pending_work):
            for key in list(pending):
                kept = [(shape, at) for shape, at in pending[key] if now - at < self._settle]
                self.counters.unattached_shapes += len(pending[key]) - len(kept)
                if kept:
                    pending[key] = kept
                else:
                    del pending[key]
        retention = self._settle * COMPLETED_RETENTION
        self._completed = {
            trace: at for trace, at in self._completed.items() if now - at < retention
        }
        self._work = {
            work: trace
            for work, trace in self._work.items()
            if trace in self._flow_of or trace in self._pending or trace in self._completed
        }
        waiting = {
            target: alive
            for target, traces in self._waiting.items()
            if (alive := [trace for trace in traces if trace in self._flow_of])
        }
        self._waiting = defaultdict(list, waiting)
