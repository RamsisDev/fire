"""openapi_routes: OpenAPI `paths` to routes, and the acme documents through RouteTable."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from fire.adapters.manifests.openapi import KIND, OPERATIONS, OpenApiRoutes, openapi_routes
from fire.core.domain.route_template import MAX_TEMPLATE_LENGTH, RouteTable

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "acme" / "openapi"
API = FIXTURES / "acme_api.json"
PRICING = FIXTURES / "acme_pricing.json"
IDS = ("300000123456789", "01J9ZKX4W3V8Q2T6M5N7P0R1S2", "SO-1234567")
PLACEHOLDER = re.compile(r"\{[^/{}]+\}")


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _pairs(result: OpenApiRoutes) -> list[tuple[str, str]]:
    return [(route.method, route.template.template) for route in result.routes]


@pytest.mark.unit
def test_one_route_per_declared_operation() -> None:
    result = openapi_routes(
        {
            "paths": {
                "/items": {"get": {}, "post": {}},
                "/items/{item_id}": {operation: {} for operation in OPERATIONS},
                "/healthcheck": {"get": {"operationId": "healthcheck"}},
            }
        }
    )
    assert _pairs(result) == [
        ("GET", "/items"),
        ("POST", "/items"),
        *((operation.upper(), "/items/{item_id}") for operation in OPERATIONS),
        ("GET", "/healthcheck"),
    ]
    assert result.dropped == 0


@pytest.mark.unit
def test_non_operation_keys_are_ignored() -> None:
    result = openapi_routes(
        {
            "paths": {
                "/items/{item_id}": {
                    "summary": "One item",
                    "parameters": [{"name": "item_id", "in": "path"}],
                    "servers": [{"url": "/v2"}],
                    "x-internal": True,
                    "GET": {},
                    "delete": {},
                },
                "/described": {"summary": "No operations", "parameters": []},
            }
        }
    )
    assert _pairs(result) == [("DELETE", "/items/{item_id}")]
    assert result.dropped == 0


@pytest.mark.unit
def test_servers_prefix_is_not_applied() -> None:
    result = openapi_routes({"servers": [{"url": "/base"}], "paths": {"/items": {"get": {}}}})
    assert _pairs(result) == [("GET", "/items")]


@pytest.mark.unit
def test_invalid_keys_and_non_object_items_are_dropped_and_counted() -> None:
    paths: dict[Any, Any] = {
        "/ok/{id}": {"get": {}},
        "no-leading-slash": {"get": {}},
        "/files/{name}.json": {"get": {}},
        "/" + "a" * MAX_TEMPLATE_LENGTH: {"get": {}},
        "/with space": {"get": {}},
        1: {"get": {}},
        "/list": [{"get": {}}],
        "/null": None,
        "/text": "get",
    }
    result = openapi_routes({"paths": paths})
    assert _pairs(result) == [("GET", "/ok/{id}")]
    assert result.dropped == 8


@pytest.mark.unit
@pytest.mark.parametrize(
    "document",
    [{}, {"openapi": "3.0.2"}, {"paths": []}, {"paths": None}, [], "paths", None],
    ids=["empty", "no-paths", "paths-list", "paths-null", "list", "string", "null"],
)
def test_document_without_paths_object_raises(document: Any) -> None:
    with pytest.raises(ValueError, match=f"^{KIND}: "):
        openapi_routes(document)


@pytest.mark.unit
def test_api_fixture_gives_one_route_per_operation() -> None:
    document = _load(API)
    expected = [
        (operation.upper(), path) for path, item in document["paths"].items() for operation in item
    ]
    result = openapi_routes(document)
    assert sorted(_pairs(result)) == sorted(expected)
    assert len(set(expected)) == len(expected)
    assert result.dropped == 0


@pytest.mark.unit
def test_pricing_fixture_routes() -> None:
    result = openapi_routes(_load(PRICING))
    assert _pairs(result) == [("POST", "/v1/quote"), ("GET", "/healthcheck")]
    assert result.dropped == 0


@pytest.mark.unit
@pytest.mark.parametrize("fixture", [API, PRICING], ids=["api", "pricing"])
@pytest.mark.parametrize("value", IDS)
def test_every_route_routes_its_own_concrete_path(fixture: Path, value: str) -> None:
    result = openapi_routes(_load(fixture))
    assert all(value not in route.template.literals for route in result.routes)
    table = RouteTable(result.routes)
    for method, template in _pairs(result):
        path = PLACEHOLDER.sub(value, template)
        assert table.match(method, path) == template, f"{method} {path}"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("method", "path", "template"),
    [
        ("GET", "/v1/orders/search", "/v1/orders/search"),
        ("GET", "/v1/orders/SO-1234567", "/v1/orders/{order_id}"),
        ("POST", "/v1/orders/checkout", "/v1/orders/checkout"),
        ("POST", "/v1/orders/123/cancel", "/v1/orders/{order_id}/cancel"),
        ("GET", "/v1/orders/123/items", "/v1/orders/{order_id}/items"),
        ("GET", "/v1/orders/123/items/9", "/v1/orders/{order_id}/items/{item_id}"),
        ("PATCH", "/v1/carts/1", "/v1/carts/{cart_id}"),
        ("POST", "/v1/tasks/close", "/v1/tasks/close"),
        ("GET", "/v1/tasks/01J9ZKX4W3V8Q2T6M5N7P0R1S2", "/v1/tasks/{task_id}"),
    ],
)
def test_api_literal_beats_placeholder(method: str, path: str, template: str) -> None:
    assert RouteTable(openapi_routes(_load(API)).routes).match(method, path) == template


@pytest.mark.unit
def test_api_method_without_the_literal_falls_to_the_placeholder() -> None:
    table = RouteTable(openapi_routes(_load(API)).routes)
    assert table.match("GET", "/v1/orders/checkout") == "/v1/orders/{order_id}"
    assert table.match("GET", "/v1/tasks/close") == "/v1/tasks/{task_id}"
    assert table.match("GET", "/v1/files/report.csv") == "/v1/files/{path}"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("DELETE", "/v1/orders/checkout"),
        ("POST", "/v1/orders/search"),
        ("GET", "/v1/catalog/resolve"),
        ("PUT", "/v1/carts/1"),
        ("GET", "/v1/files/a/b"),
        ("GET", "/v1/orders/123/cancel"),
        ("HEAD", "/healthcheck"),
        ("get", "/healthcheck"),
        ("GET", "/healthcheck/"),
        ("GET", "/v1/orders/"),
        ("GET", "/v1/tasks/"),
    ],
)
def test_api_unmatched_requests(method: str, path: str) -> None:
    assert RouteTable(openapi_routes(_load(API)).routes).match(method, path) is None


@pytest.mark.unit
def test_pricing_fixture_matches() -> None:
    table = RouteTable(openapi_routes(_load(PRICING)).routes)
    assert table.match("POST", "/v1/quote") == "/v1/quote"
    assert table.match("GET", "/healthcheck") == "/healthcheck"
    assert table.match("GET", "/v1/quote") is None
    assert table.match("HEAD", "/healthcheck") is None
