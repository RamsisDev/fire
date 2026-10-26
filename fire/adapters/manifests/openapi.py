"""Routes from an OpenAPI 3 document: every `paths` key, with the operations it declares."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from fire.core.domain.route_template import Route, RouteTemplate

KIND = "openapi"
OPERATIONS = ("get", "put", "post", "delete", "options", "head", "patch", "trace")


@dataclass(frozen=True)
class OpenApiRoutes:
    """The routes of a document, and how many `paths` keys were not route templates."""

    routes: list[Route]
    dropped: int


def openapi_routes(document: Any) -> OpenApiRoutes:
    """One route per operation of every `paths` key that parses as a route template.

    A `servers` prefix is not applied. A key that is no template, or whose item is not an
    object, is dropped and counted. Raises ValueError when the document has no `paths` object.
    """
    if not isinstance(document, Mapping) or not isinstance(document.get("paths"), Mapping):
        raise ValueError(f"{KIND}: document has no paths object")
    routes: list[Route] = []
    dropped = 0
    for path, item in document["paths"].items():
        if not isinstance(path, str) or not isinstance(item, Mapping):
            dropped += 1
            continue
        try:
            template = RouteTemplate.parse(path)
        except ValueError:
            dropped += 1
            continue
        routes.extend(
            Route(method=operation.upper(), template=template)
            for operation in OPERATIONS
            if operation in item
        )
    return OpenApiRoutes(routes=routes, dropped=dropped)
