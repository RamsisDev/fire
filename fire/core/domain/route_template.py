"""Route templates: the template a concrete request path is routed by.

A template is a path such as `/v1/orders/{order_id}`. Each segment between `/` is a literal or
a whole-segment `{name}` placeholder. Templates come from code, such as an upstream's OpenAPI
document, never from a request, so a template may become a channel name. Contract:
docs/specs/http-and-span-capture.md#route-templates.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import NamedTuple

# `{METHOD} {template}` must fit a channel name, at most 256 characters.
MAX_TEMPLATE_LENGTH = 240

PLACEHOLDER = re.compile(r"\{[A-Za-z_][A-Za-z0-9_.-]*\}")
_LITERAL = re.compile(r"[^{}\s\x00-\x1f\x7f-\x9f]*")


@dataclass(frozen=True)
class RouteTemplate:
    """A parsed template. `literals` holds one entry per segment after the leading `/`: the
    literal a path segment must equal, or None for a placeholder, which matches any non-empty
    segment.
    """

    template: str
    literals: tuple[str | None, ...]

    @classmethod
    def parse(cls, template: str) -> RouteTemplate:
        """Raises ValueError for a template that does not start with `/`, is longer than
        `MAX_TEMPLATE_LENGTH`, or has a segment that is neither a literal nor a placeholder.
        """
        if not template.startswith("/"):
            raise ValueError("a route template starts with /")
        if len(template) > MAX_TEMPLATE_LENGTH:
            raise ValueError(f"a route template has at most {MAX_TEMPLATE_LENGTH} characters")
        literals: list[str | None] = []
        for segment in template.split("/")[1:]:
            if PLACEHOLDER.fullmatch(segment):
                literals.append(None)
            elif _LITERAL.fullmatch(segment):
                literals.append(segment)
            else:
                raise ValueError(f"segment {segment!r} is neither a literal nor a placeholder")
        return cls(template=template, literals=tuple(literals))

    def matches(self, segments: Sequence[str]) -> bool:
        """Whether a path, split on `/` after its leading `/`, fits this template."""
        if len(segments) != len(self.literals):
            return False
        return all(
            segment != "" if literal is None else segment == literal
            for literal, segment in zip(self.literals, segments, strict=True)
        )

    @property
    def rank(self) -> tuple[int, ...]:
        """Per segment, 0 for a literal and 1 for a placeholder: the lower rank wins, so the
        first literal where two templates differ, reading left to right, beats a placeholder.
        """
        return tuple(0 if literal is not None else 1 for literal in self.literals)


class Route(NamedTuple):
    method: str
    template: RouteTemplate


def best_template(
    templates: Iterable[RouteTemplate], segments: Sequence[str]
) -> RouteTemplate | None:
    """Among the templates that match `segments`, the lowest `rank`; between equal ranks, the
    first given. None when none matches.
    """
    best: RouteTemplate | None = None
    for template in templates:
        if template.matches(segments) and (best is None or template.rank < best.rank):
            best = template
    return best


class RouteTable:
    """Templates by method, uppercase. A request matches on its method, exactly, and its path.

    Among the templates that match, the lowest `rank` wins; between equal ranks, the first
    given. `path` is the request path percent-decoded, as the upstream routes it.
    """

    def __init__(self, routes: Iterable[Route]) -> None:
        self._routes: dict[str, list[RouteTemplate]] = {}
        for method, template in routes:
            self._routes.setdefault(method.upper(), []).append(template)

    def __len__(self) -> int:
        return sum(len(templates) for templates in self._routes.values())

    def match(self, method: str, path: str) -> str | None:
        """The template that routes `method` and `path`, or None when none does."""
        candidates = self._routes.get(method)
        if not candidates or not path.startswith("/"):
            return None
        best = best_template(candidates, path.split("/")[1:])
        return None if best is None else best.template
