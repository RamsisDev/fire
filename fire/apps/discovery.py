"""Load each system's inventory from its declared manifest, and each inbound proxy's routes from
its declared OpenAPI document. Missing is declared, never guessed.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fire.adapters.manifests.openapi import openapi_routes
from fire.apps.config import HttpProxySourceConfig, SystemConfig
from fire.core.domain.inventory import SystemInventory
from fire.core.ports.manifest_parser import ManifestParser
from fire.core.services.coverage import RouteScope


@dataclass(frozen=True)
class Discovery:
    """`inventory` is None exactly when `missing_reason` says why."""

    system: str
    inventory: SystemInventory | None = None
    missing_reason: str | None = None


def check_manifest_kinds(
    systems: Mapping[str, SystemConfig], parsers: Mapping[str, ManifestParser]
) -> None:
    """Fail fast on a config naming a manifest format no parser handles."""
    for config in systems.values():
        manifest = config.discovery.manifest
        if manifest is not None and manifest.kind not in parsers:
            raise ValueError(
                f"system {config.system!r}: manifest kind {manifest.kind!r} has no parser "
                f"(known: {', '.join(sorted(parsers)) or 'none'})"
            )


def _read_json(path_env: str, label: str, environ: Mapping[str, str]) -> tuple[Any, str | None]:
    """The decoded document the env var points at, or None and why not."""
    location = environ.get(path_env)
    if not location:
        return None, f"{path_env} is not set"
    try:
        content = Path(location).read_bytes()
    except OSError as error:
        return None, f"{label} unreadable: {error.strerror}"
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        return None, f"{label} is not UTF-8"
    try:
        return json.loads(text), None
    except json.JSONDecodeError as error:
        return None, f"{label} is not JSON: {error.msg}"
    except RecursionError:
        return None, f"{label} is not JSON: nested too deeply"


def discover(
    config: SystemConfig,
    parsers: Mapping[str, ManifestParser],
    environ: Mapping[str, str] = os.environ,
) -> Discovery:
    manifest = config.discovery.manifest
    if manifest is None:
        return Discovery(config.system, missing_reason="No manifest declared (observation only)")
    document, reason = _read_json(manifest.path_env, "Manifest", environ)
    if reason is not None:
        return Discovery(config.system, missing_reason=reason)
    try:
        inventory = parsers[manifest.kind].parse(config.system, document)
    except ValueError as error:
        return Discovery(config.system, missing_reason=f"Manifest rejected: {error}")
    return Discovery(config.system, inventory=inventory)


def discover_routes(
    config: SystemConfig, environ: Mapping[str, str] = os.environ
) -> tuple[RouteScope, ...]:
    """One `RouteScope` per `http_proxy` source, in config order: the `METHOD template` of every
    operation in the document its `openapi_file_env` points at, or why there are none.
    """
    return tuple(
        _routes(source, environ)
        for source in config.sources
        if isinstance(source, HttpProxySourceConfig)
    )


def _routes(source: HttpProxySourceConfig, environ: Mapping[str, str]) -> RouteScope:
    if source.openapi_file_env is None:
        return RouteScope(source.id, None, "No OpenAPI document declared (openapi_file_env)")
    document, reason = _read_json(source.openapi_file_env, "OpenAPI document", environ)
    if reason is not None:
        return RouteScope(source.id, None, reason)
    try:
        found = openapi_routes(document)
    except ValueError as error:
        return RouteScope(source.id, None, f"OpenAPI document rejected: {error}")
    if not found.routes:
        return RouteScope(source.id, None, "OpenAPI document declares no usable routes")
    return RouteScope(
        source.id,
        frozenset(f"{route.method} {route.template.template}" for route in found.routes),
    )
