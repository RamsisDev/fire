"""Discovery at startup: inventory from the declared manifest, and each inbound proxy's routes
from its declared OpenAPI document, or a declared reason why not.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

import fire.apps.discovery as discovery
from fire.apps.config import SystemConfig
from fire.apps.discovery import check_manifest_kinds, discover, discover_routes
from fire.apps.incident_engine.wiring import build_container, manifest_parsers

MANIFEST_ENV = "EXAMPLE_MANIFEST_PATH"
GRAPH = {"nodes": [{"id": "order.created", "kind": "event"}]}
# Whether the decoder overflows its stack on deep nesting depends on the platform, so the
# `too_deep` fixture makes discovery's decoder raise RecursionError for this input.
TOO_DEEP = "[[[["


@pytest.fixture
def too_deep(monkeypatch: pytest.MonkeyPatch) -> None:
    loads = json.loads

    def overflowing(text: str) -> Any:
        if text == TOO_DEEP:
            raise RecursionError("maximum recursion depth exceeded")
        return loads(text)

    monkeypatch.setattr(discovery.json, "loads", overflowing)


def _config(manifest: dict[str, Any] | None = None) -> SystemConfig:
    raw: dict[str, Any] = {
        "system": "example",
        "display_name": "Example",
        "notify": {"slack": {"channel_id": "C0123456789"}},
    }
    if manifest is not None:
        raw["discovery"] = {"manifest": manifest}
    return SystemConfig.model_validate(raw)


def _with_manifest(kind: str = "event_chains_graph") -> SystemConfig:
    return _config({"kind": kind, "path_env": MANIFEST_ENV})


def _write_json(tmp_path: Path, content: str) -> dict[str, str]:
    path = tmp_path / "graph.json"
    path.write_text(content, encoding="utf-8")
    return {MANIFEST_ENV: str(path)}


@pytest.mark.unit
def test_no_manifest_is_observation_only() -> None:
    result = discover(_config(), manifest_parsers(), environ={})
    assert result.inventory is None
    assert result.missing_reason == "No manifest declared (observation only)"


@pytest.mark.unit
def test_unset_env_var_is_declared() -> None:
    result = discover(_with_manifest(), manifest_parsers(), environ={})
    assert result.inventory is None
    assert result.missing_reason == f"{MANIFEST_ENV} is not set"


@pytest.mark.unit
def test_missing_file_is_declared(tmp_path: Path) -> None:
    environ = {MANIFEST_ENV: str(tmp_path / "absent.json")}
    result = discover(_with_manifest(), manifest_parsers(), environ=environ)
    assert result.inventory is None
    assert result.missing_reason is not None
    assert result.missing_reason.startswith("Manifest unreadable")


@pytest.mark.unit
def test_invalid_json_is_declared(tmp_path: Path) -> None:
    environ = _write_json(tmp_path, "{not json")
    result = discover(_with_manifest(), manifest_parsers(), environ=environ)
    assert result.missing_reason is not None
    assert result.missing_reason.startswith("Manifest is not JSON")


@pytest.mark.unit
@pytest.mark.parametrize(
    ("content", "reason"),
    [
        (b'{"nodes": [{"id": "caf\xe9"}]}', "Manifest is not UTF-8"),
        (TOO_DEEP.encode(), "Manifest is not JSON: nested too deeply"),
    ],
    ids=["not-utf-8", "too-deep"],
)
@pytest.mark.usefixtures("too_deep")
def test_undecodable_manifest_is_declared(tmp_path: Path, content: bytes, reason: str) -> None:
    path = tmp_path / "graph.json"
    path.write_bytes(content)
    result = discover(_with_manifest(), manifest_parsers(), environ={MANIFEST_ENV: str(path)})
    assert (result.inventory, result.missing_reason) == (None, reason)


@pytest.mark.unit
def test_empty_env_var_is_declared() -> None:
    result = discover(_with_manifest(), manifest_parsers(), environ={MANIFEST_ENV: ""})
    assert result.missing_reason == f"{MANIFEST_ENV} is not set"


@pytest.mark.unit
@pytest.mark.usefixtures("too_deep")
def test_build_container_starts_with_an_undecodable_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "fire.yml").write_text("{}", encoding="utf-8")
    systems = tmp_path / "systems"
    systems.mkdir()
    raw = _with_manifest().model_dump(exclude_none=True)
    (systems / "example.yml").write_text(yaml.safe_dump(raw), encoding="utf-8")
    graph = tmp_path / "graph.json"
    graph.write_text(TOO_DEEP, encoding="utf-8")
    monkeypatch.setenv(MANIFEST_ENV, str(graph))

    discovery = build_container(tmp_path / "fire.yml").discoveries["example"]

    assert discovery.missing_reason == "Manifest is not JSON: nested too deeply"


@pytest.mark.unit
def test_rejected_document_is_declared(tmp_path: Path) -> None:
    environ = _write_json(tmp_path, json.dumps({"nodes": {}}))
    result = discover(_with_manifest(), manifest_parsers(), environ=environ)
    assert result.missing_reason is not None
    assert result.missing_reason.startswith("Manifest rejected")


@pytest.mark.unit
def test_valid_manifest_gives_inventory(tmp_path: Path) -> None:
    environ = _write_json(tmp_path, json.dumps(GRAPH))
    result = discover(_with_manifest(), manifest_parsers(), environ=environ)
    assert result.missing_reason is None
    assert result.inventory is not None
    assert result.inventory.event_names() == {"order.created"}


@pytest.mark.unit
def test_unknown_manifest_kind_fails_fast() -> None:
    with pytest.raises(ValueError, match="no parser"):
        check_manifest_kinds({"example": _with_manifest("unknown")}, manifest_parsers())


@pytest.mark.unit
def test_build_container_loads_systems_and_discoveries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "fire.yml").write_text("{}", encoding="utf-8")
    systems = tmp_path / "systems"
    systems.mkdir()
    raw = _with_manifest().model_dump(exclude_none=True)
    (systems / "example.yml").write_text(yaml.safe_dump(raw), encoding="utf-8")
    graph = tmp_path / "graph.json"
    graph.write_text(json.dumps(GRAPH), encoding="utf-8")
    monkeypatch.setenv(MANIFEST_ENV, str(graph))

    container = build_container(tmp_path / "fire.yml")

    assert list(container.systems) == ["example"]
    inventory = container.discoveries["example"].inventory
    assert inventory is not None
    assert inventory.event_names() == {"order.created"}


@pytest.mark.unit
def test_engine_discovers_acme_from_its_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, acme_fire_config: Path
) -> None:
    graph = tmp_path / "graph.json"
    graph.write_text(json.dumps(GRAPH), encoding="utf-8")
    monkeypatch.setenv("ACME_MANIFEST_PATH", str(graph))

    inventory = build_container(acme_fire_config).discoveries["acme"].inventory

    assert inventory is not None
    assert inventory.system == "acme"
    assert inventory.event_names() == {"order.created"}


@pytest.mark.unit
def test_engine_starts_without_acme_manifest(
    monkeypatch: pytest.MonkeyPatch, acme_fire_config: Path
) -> None:
    monkeypatch.delenv("ACME_MANIFEST_PATH", raising=False)

    discovery = build_container(acme_fire_config).discoveries["acme"]

    assert discovery.inventory is None
    assert discovery.missing_reason == "ACME_MANIFEST_PATH is not set"


@pytest.mark.unit
def test_build_container_rejects_unknown_manifest_kind(tmp_path: Path) -> None:
    (tmp_path / "fire.yml").write_text("{}", encoding="utf-8")
    systems = tmp_path / "systems"
    systems.mkdir()
    raw = _with_manifest("unknown").model_dump(exclude_none=True)
    (systems / "example.yml").write_text(yaml.safe_dump(raw), encoding="utf-8")

    with pytest.raises(ValueError, match="no parser"):
        build_container(tmp_path / "fire.yml")


OPENAPI_ENV = "EXAMPLE_OPENAPI_PATH"
OPENAPI = {
    "paths": {
        "/orders": {"get": {}, "post": {}},
        "/orders/{order_id}": {"get": {}},
        "orders": {"get": {}},
    }
}


def _with_proxies(*proxies: dict[str, Any]) -> SystemConfig:
    raw = _config().model_dump(exclude_none=True)
    raw["sources"] = [
        {"id": "traces", "kind": "otlp", "listen_env": "EXAMPLE_OTLP_LISTEN"},
        *(
            {"kind": "http_proxy", "listen_port": 8100 + index, "upstream_env": "U", **proxy}
            for index, proxy in enumerate(proxies)
        ),
    ]
    return SystemConfig.model_validate(raw)


def _write_openapi(tmp_path: Path, content: str | bytes) -> dict[str, str]:
    path = tmp_path / "openapi.json"
    path.write_bytes(content if isinstance(content, bytes) else content.encode())
    return {OPENAPI_ENV: str(path)}


@pytest.mark.unit
def test_routes_are_every_operation_of_the_document(tmp_path: Path) -> None:
    environ = _write_openapi(tmp_path, json.dumps(OPENAPI))
    config = _with_proxies({"id": "api", "openapi_file_env": OPENAPI_ENV})
    [scope] = discover_routes(config, environ=environ)
    assert (scope.source, scope.missing_reason) == ("api", None)
    assert scope.routes == {"GET /orders", "POST /orders", "GET /orders/{order_id}"}


@pytest.mark.unit
def test_routes_are_discovered_per_proxy_in_config_order(tmp_path: Path) -> None:
    environ = _write_openapi(tmp_path, json.dumps(OPENAPI))
    config = _with_proxies({"id": "web"}, {"id": "api", "openapi_file_env": OPENAPI_ENV})
    web, api = discover_routes(config, environ=environ)
    assert (web.source, web.routes) == ("web", None)
    assert web.missing_reason == "No OpenAPI document declared (openapi_file_env)"
    assert api.routes is not None


@pytest.mark.unit
def test_a_system_without_proxies_has_no_routes() -> None:
    assert discover_routes(_config(), environ={}) == ()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("content", "reason"),
    [
        ("", "OpenAPI document is not JSON: Expecting value"),
        (b'{"paths": {"/caf\xe9": {}}}', "OpenAPI document is not UTF-8"),
        (TOO_DEEP, "OpenAPI document is not JSON: nested too deeply"),
        (
            json.dumps({"openapi": "3.0.2"}),
            "OpenAPI document rejected: openapi: document has no paths object",
        ),
        (json.dumps({"paths": {}}), "OpenAPI document declares no usable routes"),
        (
            json.dumps({"paths": {"orders": {"get": {}}, "/orders": {"summary": "None"}}}),
            "OpenAPI document declares no usable routes",
        ),
    ],
    ids=["not-json", "not-utf-8", "too-deep", "no-paths", "empty-paths", "all-dropped"],
)
@pytest.mark.usefixtures("too_deep")
def test_routes_that_cannot_be_read_are_declared(
    tmp_path: Path, content: str | bytes, reason: str
) -> None:
    environ = _write_openapi(tmp_path, content)
    config = _with_proxies({"id": "api", "openapi_file_env": OPENAPI_ENV})
    [scope] = discover_routes(config, environ=environ)
    assert (scope.routes, scope.missing_reason) == (None, reason)


@pytest.mark.unit
@pytest.mark.parametrize("environ", [{}, {OPENAPI_ENV: ""}], ids=["unset", "empty"])
def test_routes_without_a_document_path_are_declared(environ: dict[str, str]) -> None:
    config = _with_proxies({"id": "api", "openapi_file_env": OPENAPI_ENV})
    [scope] = discover_routes(config, environ=environ)
    assert (scope.routes, scope.missing_reason) == (None, f"{OPENAPI_ENV} is not set")


@pytest.mark.unit
def test_an_unreadable_openapi_document_is_declared(tmp_path: Path) -> None:
    config = _with_proxies({"id": "api", "openapi_file_env": OPENAPI_ENV})
    environ = {OPENAPI_ENV: str(tmp_path / "absent.json")}
    [scope] = discover_routes(config, environ=environ)
    assert scope.missing_reason == "OpenAPI document unreadable: No such file or directory"


@pytest.mark.unit
def test_build_container_scopes_each_systems_routes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "fire.yml").write_text("{}", encoding="utf-8")
    systems = tmp_path / "systems"
    systems.mkdir()
    raw = _with_proxies({"id": "api", "openapi_file_env": OPENAPI_ENV}).model_dump(
        exclude_none=True
    )
    (systems / "example.yml").write_text(yaml.safe_dump(raw), encoding="utf-8")
    monkeypatch.setenv(OPENAPI_ENV, _write_openapi(tmp_path, json.dumps(OPENAPI))[OPENAPI_ENV])

    [scope] = build_container(tmp_path / "fire.yml").coverage["example"].routes

    assert scope.source == "api"
    assert scope.routes is not None
    assert len(scope.routes) == 3
