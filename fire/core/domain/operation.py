"""Operation names: what a span did, built only from names that code or infrastructure chose.

Methods, route templates, hosts, path literals, AWS service and operation names, and resource
names. A path or span-name segment that looks like a value becomes `{id}`, so ids never create
new operations. Contract: docs/specs/http-and-span-capture.md#operation-names.
"""

from __future__ import annotations

import ipaddress
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Annotated
from urllib.parse import SplitResult, unquote, urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from fire.core.domain.observation import MAX_STR_LENGTH, SpanKind
from fire.core.domain.route_template import (
    MAX_TEMPLATE_LENGTH,
    PLACEHOLDER,
    RouteTemplate,
    best_template,
)

MAX_OPERATION_LENGTH = MAX_STR_LENGTH
MAX_SEGMENT_LENGTH = 64
# The methods OpenTelemetry's HTTP instrumentors keep; they name any other one `HTTP`.
HTTP_METHODS = frozenset(
    {"GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH", "QUERY"}
)
OTHER_METHOD = "HTTP"
AWS_RPC_SYSTEM = "aws-api"
ID = "{id}"
IP = "{ip}"
UNROUTED = "{unrouted}"
UNLISTED = "{unlisted}"

_DIGITS = re.compile(r"[0-9]+")
_UUID = re.compile(r"[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}")
_ULID = re.compile(r"[0-7][0-9A-HJKMNP-TV-Za-hjkmnp-tv-z]{25}")
_HEX = re.compile(r"[0-9A-Fa-f]{16,}")
_SAFE = re.compile(r"[A-Za-z0-9._~-]*")
_HOST = re.compile(r"[a-z0-9._-]+")
_SEPARATORS = re.compile(r"([ /])")
_DEFAULT_PORTS = frozenset({80, 443})
# "CONNECT " and "OPTIONS " are the longest method prefixes of a name.
_LONGEST_METHOD_PREFIX = len("OPTIONS ")
_HOST_TEMPLATE_SEGMENT = re.compile(r"\{[a-z_][a-z0-9_]*\}|[A-Za-z0-9._~-]*")


def _is_digits(segment: str) -> bool:
    return _DIGITS.fullmatch(segment) is not None


def _is_uuid(segment: str) -> bool:
    return _UUID.fullmatch(segment) is not None


def _is_ulid(segment: str) -> bool:
    return _ULID.fullmatch(segment) is not None


def _is_hex(segment: str) -> bool:
    return _HEX.fullmatch(segment) is not None


def _is_mixed(segment: str) -> bool:
    return len(segment) >= 8 and any(char in "0123456789" for char in segment)


def _is_unsafe(segment: str) -> bool:
    return _SAFE.fullmatch(segment) is None


def _is_long(segment: str) -> bool:
    return len(segment) > MAX_SEGMENT_LENGTH


# Each rule matches one form a value takes; a segment that any rule matches becomes `{id}`.
ID_RULES: dict[str, Callable[[str], bool]] = {
    "digits": _is_digits,
    "uuid": _is_uuid,
    "ulid": _is_ulid,
    "hex": _is_hex,
    "mixed": _is_mixed,
    "unsafe": _is_unsafe,
    "long": _is_long,
}


def is_id(segment: str) -> bool:
    """Whether any id rule matches a segment, already percent-decoded."""
    return any(rule(segment) for rule in ID_RULES.values())


def generalize_segment(segment: str) -> str:
    """The segment percent-decoded, or `{id}` when an id rule matches it. A segment already of
    the form `{name}` is kept.
    """
    if PLACEHOLDER.fullmatch(segment):
        return segment
    decoded = unquote(segment)
    return ID if is_id(decoded) else decoded


def generalize_path(path: str) -> str:
    """Each segment between `/` through `generalize_segment`."""
    return "/".join(generalize_segment(segment) for segment in path.split("/"))


def generalize_span_name(name: str) -> str:
    """Each segment between spaces and `/` through `generalize_segment`."""
    return "".join(
        part if part in {" ", "/"} else generalize_segment(part)
        for part in _SEPARATORS.split(name)
    )


def http_method(method: str) -> str:
    """The method in uppercase when it is one of `HTTP_METHODS`, else `HTTP`."""
    upper = method.upper()
    return upper if upper in HTTP_METHODS else OTHER_METHOD


HostPattern = Annotated[str, Field(pattern=r"^(\*\.)?[a-z0-9-]+(\.[a-z0-9-]+)*$", max_length=253)]


class HostRule(BaseModel):
    """How calls to some hosts are named: `alias` replaces the host and its port, and a path
    must match one of `paths`; a path that matches none is `{alias}/{unlisted}`.

    A host pattern is an exact name, or `*.` and a suffix, which matches the names that end
    with `.` and that suffix. A template segment is a literal or a whole-segment `{name}`.
    """

    model_config = ConfigDict(extra="forbid")

    hosts: list[HostPattern] = Field(min_length=1)
    alias: str = Field(pattern=r"^[a-z0-9.-]+$", max_length=64)
    paths: list[str] = Field(default_factory=list)

    @field_validator("paths")
    @classmethod
    def _paths_are_distinct_templates(cls, paths: list[str]) -> list[str]:
        seen: set[tuple[str | None, ...]] = set()
        for path in paths:
            literals = _host_template(path).literals
            if literals in seen:
                raise ValueError(f"duplicate template: {path}")
            seen.add(literals)
        return paths

    @model_validator(mode="after")
    def _names_fit(self) -> HostRule:
        room = MAX_OPERATION_LENGTH - _LONGEST_METHOD_PREFIX - len(self.alias)
        too_long = [path for path in self.paths if len(path) > room]
        if too_long:
            raise ValueError(f"template too long for an operation name: {too_long[0]}")
        return self

    def matches(self, host: str) -> bool:
        return any(
            host.endswith(pattern[1:]) if pattern.startswith("*.") else host == pattern
            for pattern in self.hosts
        )


def _host_template(path: str) -> RouteTemplate:
    if not path.startswith("/") or len(path) > MAX_TEMPLATE_LENGTH:
        raise ValueError(
            f"a template starts with / and has {MAX_TEMPLATE_LENGTH} characters or less"
        )
    for segment in path.split("/")[1:]:
        if _HOST_TEMPLATE_SEGMENT.fullmatch(segment) is None:
            raise ValueError(f"segment {segment!r} is neither a literal nor a {{name}}")
    return RouteTemplate.parse(path)


class OperationRules(BaseModel):
    """The system YAML's `operations` block: per-host templates for outbound HTTP calls."""

    model_config = ConfigDict(extra="forbid")

    http: list[HostRule] = Field(default_factory=list)


@dataclass(frozen=True)
class OperationInputs:
    """The span attributes an operation name may be built from; a span's other attributes never
    reach it. `url` is parsed in memory, and only its host, port, and path are read. The
    messaging destinations are read for SQS only: for SNS they hold the topic's ARN.
    """

    kind: SpanKind
    span_name: str
    method: str | None = None
    route: str | None = None
    server_address: str | None = None
    server_port: int | None = None
    url: str | None = field(default=None, repr=False)
    rpc_system: str | None = None
    rpc_service: str | None = None
    rpc_method: str | None = None
    dynamodb_tables: tuple[str, ...] = ()
    sns_topic_arn: str | None = field(default=None, repr=False)
    messaging_destination_name: str | None = field(default=None, repr=False)
    messaging_destination: str | None = field(default=None, repr=False)
    s3_bucket: str | None = None


@dataclass(frozen=True)
class OperationName:
    """`unlisted` is True when a declared host's path matched none of its templates."""

    name: str
    unlisted: bool = False


class OperationNamer:
    """Names spans by the first rule that applies:

    1. `rpc.system` is `aws-api`: `{service}.{method} {resource}`, or without the resource.
    2. Another `rpc.system`: `{service}/{method}`.
    3. A server span with an HTTP method: `{METHOD} {route}`, or `{METHOD} {unrouted}`.
    4. A client span with an HTTP method and a host: `{METHOD} {host}{path}`, or the declared
       host's `{METHOD} {alias}{template}`.
    5. Any other: the span name, each segment through the id rules.
    """

    def __init__(self, rules: OperationRules | None = None) -> None:
        self._hosts = [
            (rule, [RouteTemplate.parse(path) for path in rule.paths])
            for rule in (rules or OperationRules()).http
        ]

    def name(self, span: OperationInputs) -> OperationName | None:
        """None when no rule builds a name, or the name is longer than 256 characters."""
        named = _rpc(span) or _server(span) or self._client(span) or _span_name(span)
        if named is None or len(named.name) > MAX_OPERATION_LENGTH:
            return None
        return named

    def _client(self, span: OperationInputs) -> OperationName | None:
        if span.kind is not SpanKind.CLIENT or not span.method:
            return None
        url = _split_url(span.url)
        raw_host = span.server_address or (url.hostname if url is not None else None)
        if not raw_host:
            return None
        host = raw_host.lower().removeprefix("[").removesuffix("]")
        ip = _is_ip(host)
        if not ip and _HOST.fullmatch(host) is None:
            return None
        method = http_method(span.method)
        path = None if url is None else url.path if url.path.startswith("/") else "/" + url.path
        for rule, templates in self._hosts:
            if rule.matches(host):
                return _declared(method, rule.alias, templates, path)
        label = IP if ip else host
        port = span.server_port if span.server_port is not None else _url_port(url)
        if port is not None and 0 < port < 65536 and port not in _DEFAULT_PORTS:
            label = f"{label}:{port}"
        if path is None:
            return OperationName(f"{method} {label}")
        return OperationName(f"{method} {label}{generalize_path(path)}")


def _rpc(span: OperationInputs) -> OperationName | None:
    if not (span.rpc_system and span.rpc_service and span.rpc_method):
        return None
    if span.rpc_system != AWS_RPC_SYSTEM:
        return OperationName(f"{span.rpc_service}/{span.rpc_method}")
    name = f"{span.rpc_service}.{span.rpc_method}"
    resource = _aws_resource(span)
    return OperationName(f"{name} {resource}" if resource else name)


def _aws_resource(span: OperationInputs) -> str | None:
    """The infrastructure name an AWS call acted on, as botocore's instrumentation records it."""
    match span.rpc_service:
        case "DynamoDB":
            return ",".join(sorted(table for table in span.dynamodb_tables if table)) or None
        case "SNS":
            topic = (span.sns_topic_arn or "").rsplit(":", 1)[-1]
            return topic or None
        case "SQS":
            return span.messaging_destination_name or span.messaging_destination or None
        case "S3":
            return span.s3_bucket or None
    return None


def _server(span: OperationInputs) -> OperationName | None:
    if span.kind is not SpanKind.SERVER or not span.method:
        return None
    return OperationName(f"{http_method(span.method)} {span.route or UNROUTED}")


def _span_name(span: OperationInputs) -> OperationName | None:
    name = generalize_span_name(span.span_name)
    return OperationName(name) if name.strip() else None


def _declared(
    method: str, alias: str, templates: Sequence[RouteTemplate], path: str | None
) -> OperationName:
    if path is None:
        return OperationName(f"{method} {alias}")
    template = best_template(templates, [unquote(segment) for segment in path.split("/")[1:]])
    if template is None:
        return OperationName(f"{method} {alias}/{UNLISTED}", unlisted=True)
    return OperationName(f"{method} {alias}{template.template}")


def _split_url(url: str | None) -> SplitResult | None:
    """The URL's parts; None when absent or when it does not parse."""
    if not url:
        return None
    try:
        return urlsplit(url)
    except ValueError:
        return None


def _url_port(url: SplitResult | None) -> int | None:
    """The URL's port; None when absent, or not a number from 0 to 65535."""
    if url is None:
        return None
    try:
        return url.port
    except ValueError:
        return None


def _is_ip(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return False
    return True
