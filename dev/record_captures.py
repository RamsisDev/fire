"""Records capture pairs from an S3-compatible bucket, scrubbed, as L1 fixtures.

    uv run python dev/record_captures.py <out dir> --bucket <name> [--prefix <prefix>]...
        [--endpoint <url>] [--limit N] [--system acme] [--config config/fire.yml]

Reads the request and response pairs under each `--prefix` in `--bucket` (every capture prefix
of the system's `s3_capture` sources when none is given) and writes each pair to
`<out dir>/<label>/NNN.json`, as
`{"prefix", "name", "request": {"metadata", "document"} or null, "response": {...}}`. Each
prefix's label and layout come from the system YAML, `<config dir>/systems/<system>.yml`.
`--endpoint` points boto3 at an S3-compatible store with path-style addressing, as does
AWS_ENDPOINT_URL; credentials and region come from boto3's own environment. Per prefix, pairs
with a status class or body shapes not seen yet come first, then the rest in key order, so a
small `--limit` takes the most varied.

Every key and every shape stays, never a value. Each body leaf becomes a fixed placeholder of
the type FIRE's generalizer gives it, keeping keys and list lengths, and a pair whose scrubbed
body fingerprints differently from the original is refused. Header values, `url`, and `error`
become placeholders; `method`, `status_code`, `api_version`, and `error_type` stay. In the key,
each work segment becomes a fake ULID and each entity id `ENTITY<n>`; `unattributed`,
`unknown`, the action, and the timestamp stay. Metadata keeps `action`, `direction`,
`api_version`, `channel`, and `notification_type`; `work_unit_id` takes the key's fake ULID and
every other value becomes a placeholder. Body keys are kept as they are: review each file before
committing it, keys included (dev/README.md, Recording fixtures).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.apps.config import S3CaptureSourceConfig, load_system_config
from fire.apps.tentacle.runner import DEFAULT_CONFIG
from fire.core.domain.capture import (
    JSON_SUFFIX,
    REQUESTS,
    RESPONSES,
    CaptureLayout,
    CaptureRule,
    checked_action,
    read_capture_key,
)
from fire.core.domain.dossier import ShapeFingerprint, ValueType
from fire.core.domain.observation import StatusClass
from fire.core.domain.operation import is_id

PLACEHOLDER = "redacted"
DEFAULT_SYSTEM = "acme"
# Literals a client may write in place of a work unit or an entity id it does not know.
UNATTRIBUTED = "unattributed"
UNKNOWN_ENTITY = "unknown"
FAKE_ULID_HEAD = "0FAKE"
ULID = re.compile(r"[0-7][0-9A-HJKMNP-TV-Z]{25}")
ENTITY_PREFIX = "ENTITY"
# One fixed example per string form the generalizer refines.
STRING_PLACEHOLDERS = {
    ValueType.UUID: "00000000-0000-0000-0000-000000000000",
    ValueType.EMAIL: "redacted@example.com",
    ValueType.URI: "https://example.com/",
    ValueType.DATE: "1970-01-01",
    ValueType.DATETIME: "1970-01-01T00:00:00Z",
    ValueType.STRING: PLACEHOLDER,
}
REDACTED_FIELDS = frozenset({"url", "error"})
KEPT_METADATA = frozenset({"action", "direction", "api_version", "channel", "notification_type"})
_METHOD = re.compile(r"[A-Z]{1,16}")
_API_VERSION = re.compile(r"[a-z0-9][a-z0-9._-]{0,15}")

GENERALIZER = FlatStarShapeGeneralizer()
FINGERPRINTER = Sha256ShapeFingerprinter()


class Refused(Exception):
    """A pair that cannot be written without a value or a changed shape. Never holds a value."""


@dataclass
class Half:
    metadata: dict[str, str]
    document: dict[str, Any]


@dataclass
class Pair:
    name: str
    request: Half | None
    response: Half


@dataclass
class Pseudonyms:
    """One fake ULID per source work segment and one `ENTITY<n>` per entity id, in first use."""

    works: dict[str, str] = field(default_factory=dict)
    entities: dict[str, str] = field(default_factory=dict)

    def work(self, value: str) -> str:
        if value == UNATTRIBUTED:
            return value
        if ULID.fullmatch(value) is None:
            raise Refused("work segment is neither a ULID nor unattributed")
        fake = self.works.setdefault(value, f"{FAKE_ULID_HEAD}{len(self.works) + 1:021d}")
        assert ULID.fullmatch(fake)
        return fake

    def entity(self, value: str) -> str:
        if value == UNKNOWN_ENTITY:
            return value
        return self.entities.setdefault(value, f"{ENTITY_PREFIX}{len(self.entities) + 1}")


def scrub_body(value: Any) -> Any:
    """`value` with its keys and list lengths, each leaf a placeholder of its shape type."""
    if isinstance(value, dict):
        return {key: scrub_body(item) for key, item in value.items()}
    if isinstance(value, list):
        return [scrub_body(item) for item in value]
    if value is None:
        return None
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return 0
    if isinstance(value, float):
        return 0.0
    if isinstance(value, str):
        return STRING_PLACEHOLDERS[GENERALIZER.generalize(value).fields[0].value_type]
    return PLACEHOLDER


def scrub_document(document: Mapping[str, Any]) -> dict[str, Any]:
    """The capture document with every value replaced but the kept names and status code."""
    scrubbed: dict[str, Any] = {}
    for key, value in document.items():
        if key == "headers" and isinstance(value, dict):
            scrubbed[key] = dict.fromkeys(value, PLACEHOLDER)
        elif key in REDACTED_FIELDS and isinstance(value, str):
            scrubbed[key] = PLACEHOLDER
        elif key != "body" and _is_kept(key, value):
            scrubbed[key] = value
        else:
            scrubbed[key] = scrub_body(value)
    if "body" in document and _fingerprint(document["body"]) != _fingerprint(scrubbed["body"]):
        raise Refused("the scrubbed body has another shape")
    return scrubbed


def _is_kept(key: str, value: Any) -> bool:
    if key == "status_code":
        return value is None or (isinstance(value, int) and not isinstance(value, bool))
    if not isinstance(value, str):
        return False
    if key == "method":
        return _METHOD.fullmatch(value) is not None
    if key == "api_version":
        return _API_VERSION.fullmatch(value) is not None
    if key == "error_type":
        return value.isidentifier() and len(value) <= 100
    return False


def _fingerprint(body: Any) -> ShapeFingerprint:
    return FINGERPRINTER.fingerprint(GENERALIZER.generalize(body))


def scrub_metadata(metadata: Mapping[str, str], names: Pseudonyms) -> dict[str, str]:
    kept: dict[str, str] = {}
    for key, value in sorted(metadata.items()):
        # Some S3-compatible stores return metadata names with dashes for underscores.
        name = key.replace("-", "_")
        if name in KEPT_METADATA:
            if checked_action(value) is None:
                raise Refused(f"metadata {key} is not a name")
            kept[key] = value
        elif name == "work_unit_id":
            kept[key] = names.work(value)
        else:
            kept[key] = PLACEHOLDER if value else value
    return kept


def scrub_name(name: str, layout: CaptureLayout, names: Pseudonyms) -> str:
    """The key after `{prefix}responses/` with its work segment and entity id replaced."""
    key = read_capture_key(name, layout)
    if key is None:
        raise Refused(f"the key does not read as {layout}")
    head = name.partition("/")[2].removesuffix(JSON_SUFFIX).removesuffix(f"_{key.stamp}")
    if layout is CaptureLayout.WORK_UNIT_ENTITY:
        head = names.entity(head)
    else:
        entity, _, action = head.partition("_")
        if layout is CaptureLayout.WORK_UNIT_ACTION:
            entity, action = "", head
        if checked_action(action) is None:
            raise Refused("the key's action is not a name")
        head = f"{names.entity(entity)}_{action}" if entity else action
    return f"{names.work(key.work)}/{head}_{key.stamp}{JSON_SUFFIX}"


def scrub_pair(pair: Pair, rule: CaptureRule, names: Pseudonyms) -> dict[str, Any]:
    def half(part: Half) -> dict[str, Any]:
        return {
            "metadata": scrub_metadata(part.metadata, names),
            "document": scrub_document(part.document),
        }

    return {
        "prefix": rule.prefix,
        "name": scrub_name(pair.name, rule.layout, names),
        "request": half(pair.request) if pair.request is not None else None,
        "response": half(pair.response),
    }


def status_class(document: Mapping[str, Any]) -> str:
    code = document.get("status_code")
    if isinstance(code, int) and not isinstance(code, bool):
        return str(StatusClass.of(code) or "invalid")
    return str(StatusClass.NO_RESPONSE)


def channel(rule: CaptureRule, pair: Pair) -> str:
    """The channel the pair's response gives, as the tentacle names it."""
    key = read_capture_key(pair.name, rule.layout)
    action = key.action if key else None
    if rule.layout is not CaptureLayout.WORK_UNIT_ACTION:
        action = pair.response.metadata.get("action") or (
            pair.request.metadata.get("action") if pair.request else None
        )
    return (
        f"{rule.label}:{action or '(no action)'} response {status_class(pair.response.document)}"
    )


def id_like_keys(value: Any) -> int:
    """How many body keys look like ids: a map keyed by data puts values into keys."""
    if isinstance(value, dict):
        return sum(is_id(key) + id_like_keys(item) for key, item in value.items())
    if isinstance(value, list):
        return sum(id_like_keys(item) for item in value)
    return 0


def _signature(pair: Pair) -> tuple[Any, ...]:
    request = pair.request.document if pair.request else None
    return (
        status_class(pair.response.document),
        request is None,
        _fingerprint(request.get("body")).schema_hash if request else None,
        _fingerprint(pair.response.document.get("body")).schema_hash,
    )


def by_variety(pairs: list[Pair]) -> list[Pair]:
    """Pairs with a signature not seen yet first, then the rest, each in key order."""
    seen: set[tuple[Any, ...]] = set()
    first, rest = [], []
    for pair in pairs:
        signature = _signature(pair)
        (rest if signature in seen else first).append(pair)
        seen.add(signature)
    return first + rest


def capture_rules(prefixes: list[str], system_config: Path) -> list[CaptureRule]:
    """The system's capture rules of `prefixes`, in that order; all of them when none is given."""
    system = load_system_config(system_config)
    rules = {
        rule.prefix: rule
        for source in system.sources
        if isinstance(source, S3CaptureSourceConfig)
        for rule in source.captures
    }
    if not prefixes:
        return list(rules.values())
    if unknown := [prefix for prefix in prefixes if prefix not in rules]:
        raise SystemExit(
            f"{system_config} declares no capture prefix {', '.join(unknown)};"
            f" it has: {', '.join(rules)}"
        )
    return [rules[prefix] for prefix in prefixes]


def s3_client(endpoint: str | None) -> Any:
    if endpoint or os.environ.get("AWS_ENDPOINT_URL"):
        return boto3.client(
            "s3", endpoint_url=endpoint, config=Config(s3={"addressing_style": "path"})
        )
    return boto3.client("s3")


def read_half(s3: Any, bucket: str, key: str) -> Half | None:
    try:
        stored = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
            return None
        raise
    try:
        document = json.loads(stored["Body"].read())
    except ValueError:
        raise Refused("an object is not JSON") from None
    if not isinstance(document, dict):
        raise Refused("an object is not a JSON object")
    return Half(metadata=dict(stored.get("Metadata") or {}), document=document)


def read_pairs(s3: Any, bucket: str, rule: CaptureRule) -> list[Pair]:
    responses = f"{rule.prefix}{RESPONSES}/"
    names = sorted(
        item["Key"].removeprefix(responses)
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=responses)
        for item in page.get("Contents") or []
    )
    pairs = []
    for serial, name in enumerate(names, start=1):
        try:
            response = read_half(s3, bucket, f"{responses}{name}")
            if response is None:
                continue
            request = read_half(s3, bucket, f"{rule.prefix}{REQUESTS}/{name}")
        except Refused as refused:
            print(f"skipped listed pair {serial}: {refused}", file=sys.stderr)
            continue
        pairs.append(Pair(name=name, request=request, response=response))
    return pairs


def record(out: Path, pairs: list[Pair], rule: CaptureRule, names: Pseudonyms) -> int:
    """Writes the pairs of one prefix to `out`; returns how many were refused."""
    out.mkdir(parents=True, exist_ok=True)
    written = refused = 0
    for pair in pairs:
        try:
            fixture = scrub_pair(pair, rule, names)
        except Refused as reason:
            refused += 1
            print(f"refused a pair: {reason}", file=sys.stderr)
            continue
        written += 1
        path = out / f"{written:03d}.json"
        path.write_text(json.dumps(fixture, indent=1) + "\n", encoding="utf-8")
        print(f"recorded {path}: {channel(rule, pair)}", flush=True)
        bodies = [pair.response.document.get("body")]
        if pair.request is not None:
            bodies.append(pair.request.document.get("body"))
        if suspicious := sum(id_like_keys(body) for body in bodies):
            print(f"  review {path}: {suspicious} body keys look like ids", flush=True)
    print(
        f"{rule.prefix}: {written} recorded, {refused} refused, from {len(pairs)} pairs",
        flush=True,
    )
    return refused


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("out", type=Path, help="directory each prefix's <label>/ goes to")
    parser.add_argument("--bucket", required=True)
    parser.add_argument(
        "--prefix",
        action="append",
        default=[],
        help="a capture prefix of the system YAML; repeat for more; all of them by default",
    )
    parser.add_argument("--endpoint", default=None, help="an S3-compatible endpoint URL")
    parser.add_argument("--limit", type=int, default=None, help="pairs per prefix")
    parser.add_argument("--system", default=DEFAULT_SYSTEM)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="FIRE's fire.yml")
    args = parser.parse_args()
    system_config = args.config.parent / "systems" / f"{args.system}.yml"
    s3 = s3_client(args.endpoint)
    names = Pseudonyms()
    refused = 0
    for rule in capture_rules(args.prefix, system_config):
        pairs = by_variety(read_pairs(s3, args.bucket, rule))[: args.limit]
        refused += record(args.out / rule.label, pairs, rule, names)
    if refused:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
