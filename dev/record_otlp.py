"""Records what an OpenTelemetry collector exports to FIRE, scrubbed, as L1 fixtures.

    uv run python dev/record_otlp.py <out dir> [--listen 127.0.0.1:4318]

Listens where the tentacle's receiver would (stop the tentacle first) for a collector's
`otlphttp` exporter with `encoding: json`, and writes each `POST /v1/traces` body to
`<out dir>/NNNN.json`. Only the receiver's allowlisted attributes keep their values. Every other
value (attributes, span events, the status message, link and resource attributes) becomes a
placeholder of the same type, so its key stays and tests can check it is dropped. A URL loses
its query values and userinfo; a captured body keeps its JSON shape with placeholder values.
Review each file before committing it: dev/README.md, Recording fixtures.
"""

from __future__ import annotations

import argparse
import contextlib
import itertools
import json
import threading
from collections.abc import Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from fire.adapters.sources.otlp import (
    EVENT_ATTRIBUTES,
    RESOURCE_ATTRIBUTES,
    RESPONSE_BODY_EVENT,
    SPAN_ATTRIBUTES,
)

PLACEHOLDER = "redacted"
URL_ATTRIBUTES = frozenset({"url.full", "http.url"})
BODY_ATTRIBUTES = frozenset({"http.request.body", RESPONSE_BODY_EVENT})


def scrub(document: dict[str, Any]) -> dict[str, Any]:
    """The request with every value outside the allowlist replaced, in place."""
    for resource_spans in document.get("resourceSpans") or []:
        _scrub_attributes(resource_spans.get("resource") or {}, RESOURCE_ATTRIBUTES)
        for scope_spans in resource_spans.get("scopeSpans") or []:
            for span in scope_spans.get("spans") or []:
                _scrub_attributes(span, SPAN_ATTRIBUTES)
                if "message" in (span.get("status") or {}):
                    span["status"]["message"] = PLACEHOLDER
                for event in span.get("events") or []:
                    kept = (
                        EVENT_ATTRIBUTES
                        if event.get("name") == RESPONSE_BODY_EVENT
                        else frozenset()
                    )
                    _scrub_attributes(event, kept)
                for link in span.get("links") or []:
                    _scrub_attributes(link, frozenset())
    return document


def _scrub_attributes(container: dict[str, Any], kept: frozenset[str]) -> None:
    for attribute in container.get("attributes") or []:
        key, value = attribute.get("key"), attribute.get("value") or {}
        if key not in kept:
            attribute["value"] = _placeholder(value)
        elif key in URL_ATTRIBUTES and isinstance(value.get("stringValue"), str):
            value["stringValue"] = _scrub_url(value["stringValue"])
        elif key in BODY_ATTRIBUTES and isinstance(value.get("stringValue"), str):
            value["stringValue"] = _scrub_body(value["stringValue"])


def _placeholder(value: Mapping[str, Any]) -> dict[str, Any]:
    """A value of the same OTLP type that carries nothing."""
    if "intValue" in value:
        return {"intValue": "0"}
    if "doubleValue" in value:
        return {"doubleValue": 0.0}
    if "boolValue" in value:
        return {"boolValue": False}
    if "arrayValue" in value:
        values = (value["arrayValue"] or {}).get("values") or []
        return {"arrayValue": {"values": [_placeholder(item) for item in values]}}
    if "kvlistValue" in value:
        pairs = (value["kvlistValue"] or {}).get("values") or []
        return {
            "kvlistValue": {
                "values": [
                    {**pair, "value": _placeholder(pair.get("value") or {})} for pair in pairs
                ]
            }
        }
    if "bytesValue" in value:
        return {"bytesValue": ""}
    return {"stringValue": PLACEHOLDER}


def _scrub_url(url: str) -> str:
    parts = urlsplit(url)
    netloc = parts.hostname or ""
    if parts.port is not None:
        netloc = f"{netloc}:{parts.port}"
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    query = urlencode([(name, PLACEHOLDER) for name, _ in pairs])
    return urlunsplit((parts.scheme, netloc, parts.path, query, ""))


def _scrub_body(body: str) -> str:
    try:
        return json.dumps(_scrub_json(json.loads(body)))
    except ValueError:
        return PLACEHOLDER


def _scrub_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _scrub_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_scrub_json(item) for item in value]
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int | float):
        return 0
    return PLACEHOLDER


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("out", type=Path, help="directory the NNNN.json files go to")
    parser.add_argument("--listen", default="127.0.0.1:4318", help="host:port to listen on")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    host, _, port = args.listen.rpartition(":")
    serial = itertools.count(1)
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers.get("content-length") or 0))
            with lock:
                path = args.out / f"{next(serial):04d}.json"
            path.write_text(json.dumps(scrub(json.loads(body)), indent=1) + "\n", encoding="utf-8")
            print(f"recorded {path}", flush=True)
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", "2")
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, *args: object) -> None:
            pass

    print(f"recording to {args.out} on {args.listen}; Ctrl-C to stop", flush=True)
    with contextlib.suppress(KeyboardInterrupt):
        ThreadingHTTPServer((host, int(port)), Handler).serve_forever()


if __name__ == "__main__":
    main()
