"""Incident correlation: the window that decides incident identity, the merged dossier, the
snapshots handed out for diagnosis, and the correlator under concurrent callers.
"""

from __future__ import annotations

import io
import logging
import threading
from collections import defaultdict
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Any

import pytest
from pydantic import ValidationError
from spans import T0, Clock, at

from fire.adapters.narrators.template import TemplateNarrator
from fire.adapters.notifiers.console import ConsoleNotifier
from fire.adapters.reasoners.echo import EchoReasoner
from fire.apps.incident_engine.wiring import log_joined, log_updated
from fire.core.domain.correlation import AcceptedTrigger, CorrelationPolicy, correlated_incident
from fire.core.domain.dossier import (
    Dossier,
    DossierTrigger,
    EvidenceFragment,
    EvidenceQuality,
    ProcessHop,
    ProcessPath,
    Shape,
    ShapeField,
    ShapeFingerprint,
    ValueType,
)
from fire.core.domain.incident import Incident, Trigger
from fire.core.domain.trigger_event import TriggerEvent
from fire.core.services.incident_correlator import Correlation, IncidentCorrelator

SYSTEM = "shop"
OTHER = "billing"
WINDOW_S = 60
PRESENT = EvidenceQuality.PRESENT
MISSING = EvidenceQuality.MISSING
SHAPE = Shape(fields=[ShapeField(path="amount", value_type=ValueType.INTEGER)])
FINGERPRINT = ShapeFingerprint(schema_hash="a1")
OTHER_SHAPE = Shape(fields=[ShapeField(path="coupon", value_type=ValueType.STRING)])
ENTRY = ProcessHop(ref="orders-api", operation="POST /orders", span_kind="server", status="ok")
PUT = ProcessHop(ref="orders-api", operation="DynamoDB.PutItem orders", span_kind="client")


def _fragment(collector: str, quality: EvidenceQuality = PRESENT, **data: Any) -> EvidenceFragment:
    return EvidenceFragment(collector=collector, quality=quality, summary=collector, data=data)


def _event(
    trigger: Trigger = Trigger.SHAPE_DRIFT,
    *fragments: EvidenceFragment,
    system: str = SYSTEM,
    summary: str = "summary",
    started: float = 0,
    service: str | None = None,
    shape: Shape | None = None,
    fingerprint: ShapeFingerprint | None = None,
    hops: Sequence[ProcessHop] = (),
) -> TriggerEvent:
    return TriggerEvent(
        system=system,
        trigger=trigger,
        summary=summary,
        started_at=at(started),
        service=service,
        shape=shape,
        fingerprint=fingerprint,
        process_path=ProcessPath(hops=list(hops)),
        fragments=list(fragments),
    )


def _correlator(clock: Clock, *systems: str) -> IncidentCorrelator:
    ids = iter(f"incident-{index}" for index in range(10_000))
    policies = {system: CorrelationPolicy(window_s=WINDOW_S) for system in systems or (SYSTEM,)}
    return IncidentCorrelator(policies, clock=clock, new_id=lambda: next(ids))


def _merged(*events: TriggerEvent) -> tuple[Incident, Dossier]:
    triggers = [AcceptedTrigger(event, at(index)) for index, event in enumerate(events)]
    return correlated_incident("incident-0", triggers)


@pytest.fixture
def clock() -> Clock:
    return Clock()


# The window


@pytest.mark.unit
def test_the_first_trigger_opens_an_incident_and_later_ones_inside_the_window_join_it(
    clock: Clock,
) -> None:
    correlator = _correlator(clock)

    first = correlator.add(_event(Trigger.SHAPE_DRIFT))
    clock.advance(10)
    second = correlator.add(_event(Trigger.SHAPE_DRIFT))
    clock.advance(10)
    third = correlator.add(_event(Trigger.HTTP_ERRORS))

    assert first == Correlation("incident-0", SYSTEM, Trigger.SHAPE_DRIFT, True, 1)
    assert second == Correlation("incident-0", SYSTEM, Trigger.SHAPE_DRIFT, False, 2)
    assert third == Correlation("incident-0", SYSTEM, Trigger.HTTP_ERRORS, False, 3)


@pytest.mark.unit
def test_a_trigger_exactly_window_s_after_the_first_opens_the_next_incident(clock: Clock) -> None:
    correlator = _correlator(clock)
    correlator.add(_event())

    clock.advance(WINDOW_S - 0.001)
    assert correlator.add(_event()).incident_id == "incident-0"
    clock.advance(0.001)
    assert correlator.add(_event()) == Correlation(
        "incident-1", SYSTEM, Trigger.SHAPE_DRIFT, True, 1
    )


@pytest.mark.unit
def test_the_window_is_fixed_from_the_first_trigger_and_never_slides(clock: Clock) -> None:
    correlator = _correlator(clock)
    ids = []
    for _ in range(3):
        ids.append(correlator.add(_event()).incident_id)
        clock.advance(WINDOW_S / 2)

    assert ids == ["incident-0", "incident-0", "incident-1"]


@pytest.mark.unit
def test_the_window_runs_on_acceptance_not_on_the_triggers_start(clock: Clock) -> None:
    correlator = _correlator(clock)
    correlator.add(_event(Trigger.HTTP_ERRORS, started=0))
    clock.advance(1)

    # A DLQ event starts at its first abandonment, hours before it is raised.
    late = correlator.add(_event(Trigger.DLQ, started=-6 * 3600))
    clock.advance(WINDOW_S)
    recent = correlator.add(_event(Trigger.SHAPE_DRIFT, started=WINDOW_S - 1))

    assert (late.incident_id, late.opened) == ("incident-0", False)
    assert (recent.incident_id, recent.opened) == ("incident-1", True)


@pytest.mark.unit
def test_two_systems_never_share_an_incident(clock: Clock) -> None:
    correlator = _correlator(clock, SYSTEM, OTHER)

    shop = correlator.add(_event(system=SYSTEM))
    billing = correlator.add(_event(system=OTHER))
    shop_again = correlator.add(_event(system=SYSTEM))

    assert (shop.incident_id, shop.opened) == ("incident-0", True)
    assert (billing.incident_id, billing.opened) == ("incident-1", True)
    assert (shop_again.incident_id, shop_again.opened) == ("incident-0", False)


@pytest.mark.unit
def test_a_system_without_a_policy_opens_one_incident_per_trigger(clock: Clock) -> None:
    correlator = _correlator(clock, OTHER)

    first = correlator.add(_event())
    second = correlator.add(_event())

    assert (first.opened, second.opened) == (True, True)
    assert first.incident_id != second.incident_id
    run = correlator.take(first.incident_id)
    assert run is not None and run[0].triggers == [Trigger.SHAPE_DRIFT]
    assert correlator.take(first.incident_id) is None
    assert [incident.id for incident, _ in correlator.close()] == [second.incident_id]
    assert correlator.close() == []


# Snapshots for diagnosis


@pytest.mark.unit
def test_take_returns_the_incident_with_every_trigger_so_far_and_then_nothing(
    clock: Clock,
) -> None:
    correlator = _correlator(clock)
    opened = correlator.add(_event(Trigger.SHAPE_DRIFT))
    correlator.add(_event(Trigger.HTTP_ERRORS))

    run = correlator.take(opened.incident_id)

    assert run is not None
    incident, dossier = run
    assert incident.triggers == [Trigger.SHAPE_DRIFT, Trigger.HTTP_ERRORS]
    assert [trigger.trigger for trigger in dossier.triggers] == incident.triggers
    assert correlator.take(opened.incident_id) is None
    assert correlator.take("unknown") is None


@pytest.mark.unit
def test_triggers_that_join_after_the_take_are_diagnosed_once_when_the_window_closes(
    clock: Clock,
) -> None:
    correlator = _correlator(clock)
    opened = correlator.add(_event(Trigger.SHAPE_DRIFT))
    assert correlator.take(opened.incident_id) is not None
    clock.advance(5)
    correlator.add(_event(Trigger.SHAPE_DRIFT))
    correlator.add(_event(Trigger.HTTP_ERRORS))

    clock.advance(WINDOW_S - 6)
    assert correlator.close() == []
    clock.advance(1)
    [(incident, dossier)] = correlator.close()

    assert incident.id == opened.incident_id
    assert incident.triggers == [Trigger.SHAPE_DRIFT, Trigger.SHAPE_DRIFT, Trigger.HTTP_ERRORS]
    assert [trigger.accepted_at for trigger in dossier.triggers] == [T0, at(5), at(5)]
    assert correlator.close() == []


@pytest.mark.unit
def test_a_window_with_nothing_new_since_its_take_closes_silently(clock: Clock) -> None:
    correlator = _correlator(clock)
    opened = correlator.add(_event())
    correlator.add(_event())
    assert correlator.take(opened.incident_id) is not None

    clock.advance(WINDOW_S)

    assert correlator.close() == []
    assert correlator.take(opened.incident_id) is None


@pytest.mark.unit
def test_a_window_closed_before_its_take_is_diagnosed_by_the_close_alone(clock: Clock) -> None:
    correlator = _correlator(clock)
    opened = correlator.add(_event())
    clock.advance(WINDOW_S)

    [(incident, _dossier)] = correlator.close()

    assert incident.id == opened.incident_id
    assert correlator.take(opened.incident_id) is None


@pytest.mark.unit
def test_a_trigger_after_the_window_ends_opens_the_next_incident_before_the_last_one_closes(
    clock: Clock,
) -> None:
    correlator = _correlator(clock)
    first = correlator.add(_event())
    correlator.take(first.incident_id)
    clock.advance(1)
    correlator.add(_event())
    clock.advance(WINDOW_S)

    second = correlator.add(_event())
    runs = correlator.close()

    assert second.opened and second.incident_id != first.incident_id
    assert [(incident.id, len(incident.triggers)) for incident, _ in runs] == [
        (first.incident_id, 2)
    ]
    assert correlator.add(_event()).incident_id == second.incident_id


@pytest.mark.unit
def test_snapshots_are_copies_of_the_correlated_evidence(clock: Clock) -> None:
    correlator = _correlator(clock, OTHER)
    event = _event(Trigger.HTTP_ERRORS, _fragment("http_errors", errors=3), hops=[ENTRY])
    run = correlator.take(correlator.add(event).incident_id)
    assert run is not None
    _incident, dossier = run

    dossier.fragments[0].data["errors"] = 99
    dossier.process_path.hops.clear()

    assert event.fragments[0].data["errors"] == 3
    assert event.process_path.hops == [ENTRY]


# The merged incident and dossier


@pytest.mark.unit
def test_a_single_trigger_incident_holds_its_events_evidence_unchanged() -> None:
    event = _event(
        Trigger.SHAPE_DRIFT,
        _fragment("shape_drift", stream="s"),
        _fragment("baseline", MISSING, source="p", stream="s"),
        _fragment("coverage", MISSING),
        _fragment("process_path", MISSING),
        summary="Shape drift on s",
        started=-3,
        shape=SHAPE,
        fingerprint=FINGERPRINT,
    )

    incident, dossier = _merged(event)

    assert incident.model_dump() == {
        "id": "incident-0",
        "system": SYSTEM,
        "trigger": Trigger.SHAPE_DRIFT,
        "triggers": [Trigger.SHAPE_DRIFT],
        "service": None,
        "summary": "Shape drift on s",
        "started_at": at(-3),
    }
    assert (dossier.incident_id, dossier.system, dossier.service) == ("incident-0", SYSTEM, None)
    assert (dossier.shape, dossier.fingerprint) == (SHAPE, FINGERPRINT)
    assert dossier.process_path == event.process_path
    assert dossier.fragments == event.fragments
    assert dossier.triggers == [
        DossierTrigger(
            trigger=Trigger.SHAPE_DRIFT,
            summary="Shape drift on s",
            started_at=at(-3),
            accepted_at=T0,
            fragments=[0, 1, 2, 3],
        )
    ]


@pytest.mark.unit
def test_the_incident_takes_the_opening_trigger_and_summary_and_the_earliest_start() -> None:
    incident, dossier = _merged(
        _event(Trigger.HTTP_ERRORS, summary="HTTP errors on POST /orders", started=10),
        _event(Trigger.DLQ, summary="Abandoned work units on e", started=-3600),
        _event(Trigger.SHAPE_DRIFT, summary="Shape drift on s", started=20),
    )

    assert (incident.trigger, incident.summary) == (
        Trigger.HTTP_ERRORS,
        "HTTP errors on POST /orders",
    )
    assert incident.triggers == [Trigger.HTTP_ERRORS, Trigger.DLQ, Trigger.SHAPE_DRIFT]
    assert incident.started_at == at(-3600)
    assert [(t.summary, t.started_at, t.accepted_at) for t in dossier.triggers] == [
        ("HTTP errors on POST /orders", at(10), at(0)),
        ("Abandoned work units on e", at(-3600), at(1)),
        ("Shape drift on s", at(20), at(2)),
    ]


@pytest.mark.unit
def test_equal_fragments_and_hops_are_kept_once_and_listed_by_each_trigger_gathering_them() -> (
    None
):
    coverage = _fragment("coverage", declared_unseen=3)
    _incident, dossier = _merged(
        _event(Trigger.SHAPE_DRIFT, _fragment("shape_drift", stream="a"), coverage),
        _event(Trigger.SHAPE_DRIFT, _fragment("shape_drift", stream="b"), coverage, hops=[ENTRY]),
        _event(Trigger.HTTP_ERRORS, _fragment("http_errors"), hops=[ENTRY, PUT]),
    )

    assert [(f.collector, f.data) for f in dossier.fragments] == [
        ("shape_drift", {"stream": "a"}),
        ("coverage", {"declared_unseen": 3}),
        ("shape_drift", {"stream": "b"}),
        ("http_errors", {}),
    ]
    assert [trigger.fragments for trigger in dossier.triggers] == [[0, 1], [2, 1], [3]]
    assert dossier.process_path.hops == [ENTRY, PUT]


@pytest.mark.unit
def test_a_missing_fragment_gives_way_to_present_evidence_of_its_collector_and_subject() -> None:
    _incident, dossier = _merged(
        _event(
            Trigger.INCIDENT_IO,
            _fragment("incident_io"),
            _fragment("process_path", MISSING),
            _fragment("shape", MISSING),
        ),
        _event(
            Trigger.SHAPE_DRIFT,
            _fragment("shape_drift", stream="a"),
            _fragment("baseline", MISSING, source="p", stream="a"),
            _fragment("coverage", MISSING),
            shape=SHAPE,
        ),
        _event(
            Trigger.HTTP_ERRORS,
            _fragment("http_errors"),
            _fragment("baseline", PRESENT, source="p", stream="a", samples=4),
            _fragment("baseline", MISSING, route={"operation": "POST /refunds"}),
            _fragment("failing_shapes", MISSING, route="POST /refunds"),
            _fragment("coverage", PRESENT, declared_unseen=2),
            _fragment("process_path", PRESENT, flows=1),
            hops=[ENTRY],
        ),
        _event(Trigger.HTTP_ERRORS, _fragment("failing_shapes", PRESENT, route="POST /orders")),
    )

    assert [(f.collector, f.quality, f.data) for f in dossier.fragments] == [
        ("incident_io", PRESENT, {}),
        ("shape_drift", PRESENT, {"stream": "a"}),
        ("http_errors", PRESENT, {}),
        ("baseline", PRESENT, {"source": "p", "stream": "a", "samples": 4}),
        ("baseline", MISSING, {"route": {"operation": "POST /refunds"}}),
        ("failing_shapes", MISSING, {"route": "POST /refunds"}),
        ("coverage", PRESENT, {"declared_unseen": 2}),
        ("process_path", PRESENT, {"flows": 1}),
        ("failing_shapes", PRESENT, {"route": "POST /orders"}),
    ]
    assert [trigger.fragments for trigger in dossier.triggers] == [
        [0],
        [1],
        [2, 3, 4, 5, 6, 7],
        [8],
    ]


@pytest.mark.unit
def test_a_shape_or_hops_from_any_trigger_fill_the_dossier_and_clear_its_missing_ones() -> None:
    no_evidence = _event(
        Trigger.INCIDENT_IO, _fragment("process_path", MISSING), _fragment("shape", MISSING)
    )

    _incident, alone = _merged(no_evidence)
    _incident, merged = _merged(no_evidence, _event(Trigger.DLQ, shape=SHAPE, hops=[PUT]))

    assert [f.collector for f in alone.missing] == ["process_path", "shape"]
    assert merged.missing == []
    assert (merged.shape, merged.process_path.hops) == (SHAPE, [PUT])
    assert [trigger.fragments for trigger in merged.triggers] == [[], []]


@pytest.mark.unit
def test_shape_and_fingerprint_come_together_from_the_first_trigger_with_either() -> None:
    _incident, dossier = _merged(
        _event(Trigger.HTTP_ERRORS),
        _event(Trigger.SHAPE_DRIFT, fingerprint=FINGERPRINT),
        _event(Trigger.DLQ, shape=OTHER_SHAPE, fingerprint=ShapeFingerprint(schema_hash="b2")),
    )

    assert (dossier.shape, dossier.fingerprint) == (None, FINGERPRINT)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("services", "expected"),
    [
        ((None, "orders-api", None, "orders-api"), "orders-api"),
        (("orders-api", "billing-api"), None),
        ((None, None), None),
    ],
    ids=["agreed", "two-services", "none"],
)
def test_the_service_is_the_one_every_trigger_that_names_one_agrees_on(
    services: tuple[str | None, ...], expected: str | None
) -> None:
    incident, dossier = _merged(*(_event(service=service) for service in services))

    assert incident.service == dossier.service == expected
    assert [trigger.service for trigger in dossier.triggers] == list(services)


@pytest.mark.unit
def test_an_incident_lists_its_opening_trigger_first() -> None:
    assert Incident(id="i", system=SYSTEM, trigger=Trigger.MANUAL).triggers == [Trigger.MANUAL]
    with pytest.raises(ValidationError):
        Incident(id="i", system=SYSTEM, trigger=Trigger.DLQ, triggers=[Trigger.SHAPE_DRIFT])
    with pytest.raises(ValueError, match="at least one trigger"):
        correlated_incident("i", [])


@pytest.mark.unit
def test_a_dossier_trigger_rejects_unknown_fields_and_naive_times() -> None:
    fields = {"trigger": "dlq", "summary": "s", "started_at": T0, "accepted_at": T0}
    assert DossierTrigger.model_validate(fields).fragments == []
    with pytest.raises(ValidationError):
        DossierTrigger.model_validate({**fields, "payload": {}})
    with pytest.raises(ValidationError):
        DossierTrigger.model_validate({**fields, "accepted_at": T0.replace(tzinfo=None)})


# Reasoner, narrator, and notifier


def _correlated() -> tuple[Incident, Dossier]:
    return _merged(
        _event(
            Trigger.SHAPE_DRIFT,
            _fragment("shape_drift", stream="a"),
            _fragment("process_path", MISSING),
            summary="Shape drift on a",
            shape=SHAPE,
        ),
        _event(
            Trigger.SHAPE_DRIFT, _fragment("shape_drift", stream="b"), summary="Shape drift on b"
        ),
        _event(
            Trigger.HTTP_ERRORS,
            _fragment("http_errors"),
            summary="HTTP errors on POST /orders",
            hops=[ENTRY],
        ),
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_the_reasoner_reads_every_triggers_evidence_in_the_merged_dossier() -> None:
    _incident, dossier = _correlated()

    decision = await EchoReasoner().decide(dossier)

    assert [p.id for p in decision.possibilities] == ["hop-0", "drift-0", "drift-1", "burst-0"]
    assert (decision.confidence, decision.noul) == (0.45, False)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_the_narration_lists_the_triggers_of_a_correlated_incident_only() -> None:
    incident, dossier = _correlated()
    _single, alone = _merged(_event(summary="Shape drift on a"))
    narrator = TemplateNarrator()
    reasoner = EchoReasoner()

    narration = await narrator.narrate(dossier, await reasoner.decide(dossier))
    text = narration.channel_text
    single = (await narrator.narrate(alone, await reasoner.decide(alone))).channel_text

    assert (
        "Triggers (3):\n"
        "• `shape_drift` Shape drift on a\n"
        "• `shape_drift` Shape drift on b\n"
        "• `http_errors` HTTP errors on POST /orders\n"
    ) in text
    assert "Triggers" not in single
    stream = io.StringIO()
    await ConsoleNotifier(stream).publish(incident, narration)
    assert stream.getvalue().splitlines()[0] == (
        "[fire] notify incident=incident-0 system=shop trigger=shape_drift triggers=3"
    )


@pytest.mark.unit
def test_a_join_and_an_update_each_log_one_line(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    incident, _dossier = _correlated()

    log_joined(Correlation("incident-0", SYSTEM, Trigger.HTTP_ERRORS, False, 3))
    log_updated(incident)

    assert caplog.messages == [
        "incident joined: system=shop incident=incident-0 trigger=http_errors triggers=3",
        "incident updated: system=shop incident=incident-0 triggers=3",
    ]


# Concurrency


@pytest.mark.unit
def test_concurrent_triggers_of_one_system_open_exactly_one_incident(clock: Clock) -> None:
    correlator = _correlator(clock)
    callers = 16
    barrier = threading.Barrier(callers)

    def add(index: int) -> Correlation:
        barrier.wait()
        return correlator.add(_event(summary=f"trigger {index}"))

    with ThreadPoolExecutor(callers) as pool:
        correlations = list(pool.map(add, range(callers)))

    assert {c.incident_id for c in correlations} == {"incident-0"}
    assert sum(c.opened for c in correlations) == 1
    assert sorted(c.triggers for c in correlations) == list(range(1, callers + 1))
    run = correlator.take("incident-0")
    assert run is not None and len(run[0].triggers) == callers


class _TickingClock:
    """A clock that moves 10 ms on every read, safe across threads."""

    def __init__(self) -> None:
        self._now = T0
        self._lock = threading.Lock()

    def __call__(self) -> Any:
        with self._lock:
            self._now += timedelta(milliseconds=10)
            return self._now


@pytest.mark.unit
def test_concurrent_adds_takes_and_closes_diagnose_every_trigger_exactly_once() -> None:
    correlator = IncidentCorrelator(
        {SYSTEM: CorrelationPolicy(window_s=0.5)}, clock=_TickingClock()
    )
    adders, per_adder = 4, 150
    totals: defaultdict[str, int] = defaultdict(int)
    snapshots: defaultdict[str, list[int]] = defaultdict(list)
    lock = threading.Lock()
    done = threading.Event()

    def record(runs: Sequence[tuple[Incident, Dossier]]) -> None:
        with lock:
            for incident, dossier in runs:
                assert len(dossier.triggers) == len(incident.triggers)
                snapshots[incident.id].append(len(incident.triggers))

    def add() -> None:
        for _ in range(per_adder):
            correlation = correlator.add(_event())
            with lock:
                totals[correlation.incident_id] = max(
                    totals[correlation.incident_id], correlation.triggers
                )
            if correlation.opened:
                run = correlator.take(correlation.incident_id)
                record([run] if run is not None else [])

    def close() -> None:
        while not done.is_set():
            record(correlator.close())

    closer = threading.Thread(target=close)
    closer.start()
    with ThreadPoolExecutor(adders) as pool:
        for future in [pool.submit(add) for _ in range(adders)]:
            future.result()
    done.set()
    closer.join()
    for _ in range(100):
        record(correlator.close())

    assert sum(totals.values()) == adders * per_adder
    assert len(totals) > 1
    assert set(snapshots) == set(totals)
    for incident_id, counts in snapshots.items():
        assert len(counts) == len(set(counts)), incident_id
        assert max(counts) == totals[incident_id], incident_id
