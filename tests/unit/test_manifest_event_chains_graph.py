"""EventChainsGraphParser: event graph document → SystemInventory."""

from __future__ import annotations

from typing import Any

import pytest

from fire.adapters.manifests.event_chains_graph import EventChainsGraphParser
from fire.core.ports.manifest_parser import ManifestParser

DOCUMENT: dict[str, Any] = {
    "externalSources": [
        {
            "id": "external:api",
            "label": "API",
            "transport": "http_sync",
            "firstEvents": ["order.created", "order.created", "exchange.created"],
        },
        {"id": "external:rx", "label": "Rx", "transport": "kinesis", "firstEvents": []},
    ],
    "nodes": [
        {"id": "order.created", "kind": "event"},
        {"id": "order.validated", "kind": "event"},
    ],
    "edges": [
        {"from": "external:api", "to": "order.created"},
        {"from": "order.created", "to": "order.validated", "emitter": "sales-order-processor"},
        {"from": "order.validated", "to": "fulfillment.allocated"},
    ],
    "consumers": {
        "order.created": ["sales-order-processor", "audit-logger", "audit-logger"],
        "notification.send": ["notification-publisher"],
    },
    "payloads": {
        "order.created": {
            "type": "OrderCreatedPayload",
            "fields": [{"name": "order_id", "type": "str"}],
        }
    },
    "processors": {
        "sales-order-processor": {"regionPolicy": "origin_only"},
        "audit-logger": {"regionPolicy": "all_regions"},
    },
}


def _parse(document: Any = DOCUMENT):
    return EventChainsGraphParser().parse("example", document)


@pytest.mark.unit
def test_satisfies_port() -> None:
    assert isinstance(EventChainsGraphParser(), ManifestParser)


@pytest.mark.unit
def test_events_are_every_name_the_graph_mentions() -> None:
    inventory = _parse()
    assert [event.name for event in inventory.events] == [
        "exchange.created",
        "fulfillment.allocated",
        "notification.send",
        "order.created",
        "order.validated",
    ]
    assert inventory.system == "example"
    assert inventory.manifest_kind == "event_chains_graph"


@pytest.mark.unit
def test_event_carries_declared_payload_and_unique_consumers() -> None:
    event = next(e for e in _parse().events if e.name == "order.created")
    assert event.payload_type == "OrderCreatedPayload"
    assert [(f.name, f.type) for f in event.payload_fields] == [("order_id", "str")]
    assert event.consumers == ["audit-logger", "sales-order-processor"]


@pytest.mark.unit
def test_event_without_payload_declaration() -> None:
    event = next(e for e in _parse().events if e.name == "order.validated")
    assert event.payload_type is None
    assert event.payload_fields == []
    assert event.consumers == []


@pytest.mark.unit
def test_processors_and_ingress_sorted() -> None:
    inventory = _parse()
    assert [(p.name, p.region_policy) for p in inventory.processors] == [
        ("audit-logger", "all_regions"),
        ("sales-order-processor", "origin_only"),
    ]
    assert [i.id for i in inventory.ingress] == ["external:api", "external:rx"]
    assert inventory.ingress[0].first_events == ["exchange.created", "order.created"]


@pytest.mark.unit
def test_empty_document_gives_empty_inventory() -> None:
    inventory = _parse({})
    assert inventory.events == []
    assert inventory.processors == []
    assert inventory.ingress == []


@pytest.mark.unit
@pytest.mark.parametrize(
    "document",
    [
        [],
        {"nodes": {}},
        {"externalSources": [{"label": "no id"}]},
        {"nodes": [{"kind": "event"}]},
        {"payloads": {"e": {"fields": [{"name": "x"}]}}},
    ],
    ids=["not-object", "nodes-not-list", "ingress-without-id", "node-without-id", "field-no-type"],
)
def test_malformed_documents_raise_value_error(document: Any) -> None:
    with pytest.raises(ValueError):
        _parse(document)
