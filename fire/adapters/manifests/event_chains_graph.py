"""Parser for the `event_chains_graph` manifest format: an event graph generated from code."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fire.core.domain.inventory import (
    DeclaredEvent,
    DeclaredField,
    DeclaredIngress,
    DeclaredProcessor,
    SystemInventory,
)

KIND = "event_chains_graph"
_EXTERNAL_PREFIX = "external:"


def _section(document: Mapping[str, Any], key: str, kind: type) -> Any:
    value = document.get(key, kind())
    if not isinstance(value, kind):
        raise ValueError(f"{KIND}: {key!r} must be a {kind.__name__}")
    return value


class EventChainsGraphParser:
    """An event is any name the graph mentions: a node, an edge end, a consumer or payload key,
    or an ingress first event. Edge ends prefixed `external:` are ingress, not events.
    """

    kind = KIND

    def parse(self, system: str, document: Mapping[str, Any]) -> SystemInventory:
        if not isinstance(document, Mapping):
            raise ValueError(f"{KIND}: document must be an object")
        try:
            return self._parse(system, document)
        except (KeyError, TypeError, AttributeError) as error:
            raise ValueError(f"{KIND}: malformed document ({error!r})") from error

    def _parse(self, system: str, document: Mapping[str, Any]) -> SystemInventory:
        nodes = _section(document, "nodes", list)
        edges = _section(document, "edges", list)
        consumers = _section(document, "consumers", dict)
        payloads = _section(document, "payloads", dict)
        processors = _section(document, "processors", dict)
        sources = _section(document, "externalSources", list)

        ingress = [
            DeclaredIngress(
                id=source["id"],
                label=source.get("label"),
                transport=source.get("transport"),
                first_events=sorted(set(source.get("firstEvents") or [])),
            )
            for source in sources
        ]

        names = {node["id"] for node in nodes if node.get("kind", "event") == "event"}
        names |= {
            end
            for edge in edges
            for end in (edge.get("from"), edge.get("to"))
            if end and not end.startswith(_EXTERNAL_PREFIX)
        }
        names |= set(consumers) | set(payloads)
        names |= {name for source in ingress for name in source.first_events}

        events = []
        for name in sorted(names):
            payload = payloads.get(name) or {}
            events.append(
                DeclaredEvent(
                    name=name,
                    payload_type=payload.get("type"),
                    payload_fields=[
                        DeclaredField(name=field["name"], type=field["type"])
                        for field in payload.get("fields") or []
                    ],
                    consumers=sorted(set(consumers.get(name) or [])),
                )
            )

        return SystemInventory(
            system=system,
            manifest_kind=KIND,
            events=events,
            processors=[
                DeclaredProcessor(name=name, region_policy=info.get("regionPolicy"))
                for name, info in sorted(processors.items())
            ],
            ingress=sorted(ingress, key=lambda source: source.id),
        )
