"""Canary harness: plants unique values in records and finds any that leak.

Every planted value must appear nowhere the tentacle writes. Envelope values (ids, names,
timestamps) may appear only in their own fields of an observation. Values of a rejected record
must appear nowhere. Contract: docs/specs/tentacle.md#canary-harness.
"""

from __future__ import annotations

import itertools
import json
import secrets
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from aws_streams import create_stream, create_table, fanout, publish, put_items, put_records

from fire.adapters.sources.sns_fanout import queue_name

# The fields an envelope value may fill; every other field must hold no canary.
OWN_FIELDS = ("observation_id", "work_id", "correlation_id", "occurred_at")


class Canary:
    """Unique values to plant, and the checks that find them.

    Tokens have a fixed width, so none is a substring of another.
    """

    def __init__(self) -> None:
        self._prefix = f"cnry{secrets.token_hex(4)}"
        self._serial = itertools.count(1)
        self._string_forms = itertools.cycle(("plain", "email", "uri"))
        self._number_base = 10**12 + secrets.randbelow(10**6) * 10**6
        self.planted: set[str] = set()
        self.ids: set[str] = set()
        self.names: set[str] = set()
        self.timestamps: set[str] = set()

    @property
    def envelope(self) -> set[str]:
        return self.ids | self.names | self.timestamps

    def token(self) -> str:
        value = f"{self._prefix}t{next(self._serial):06d}"
        self.planted.add(value)
        return value

    def number(self) -> int:
        value = self._number_base + next(self._serial)
        self.planted.add(str(value))
        return value

    def string(self) -> str:
        """A planted string, in turn plain, an email, or a URI, so the refinements run."""
        token = self.token()
        return {
            "plain": token,
            "email": f"{token}@example.com",
            "uri": f"https://example.com/{token}",
        }[next(self._string_forms)]

    def plant(self, value: Any) -> Any:
        """Replaces every leaf with a canary; booleans and nulls stay. Keys are not planted."""
        if isinstance(value, dict):
            return {key: self.plant(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self.plant(item) for item in value]
        if value is None or isinstance(value, bool):
            return value
        if isinstance(value, int | float):
            return self.number()
        return self.string()

    def envelope_id(self) -> str:
        value = f"{self._prefix}i{next(self._serial):06d}"
        self.ids.add(value)
        return value

    def envelope_name(self) -> str:
        value = f"canary.{self._prefix}n{next(self._serial):06d}"
        self.names.add(value)
        return value

    def envelope_timestamp(self) -> str:
        serial = next(self._serial)
        value = f"2026-10-11T{serial // 3600 % 24:02d}:{serial // 60 % 60:02d}:"
        value += f"{serial % 60:02d}.{serial:06d}+00:00"
        self.timestamps.add(value)
        return value

    def rejected(self, *values: str) -> None:
        """The envelope values of a rejected record, which must then appear nowhere."""
        for value in values:
            for kind in (self.ids, self.names, self.timestamps):
                kind.discard(value)
            self.planted.add(value)

    def leaks(self, text: str, *, envelope: bool = True) -> list[str]:
        """The canaries in `text`: planted ones, and envelope ones unless `envelope` is False."""
        values = self.planted | (self.envelope if envelope else set())
        return sorted(value for value in values if value in text)

    def observation_leaks(self, shape: dict[str, Any]) -> list[str]:
        """Canaries out of place in a posted shape observation.

        Its own fields must hold envelope values of the right kind; the rest holds none.
        """
        leaks = [
            f"{field}={shape[field]}"
            for field in ("observation_id", "work_id")
            if field in shape and shape[field] not in self.ids
        ]
        if shape["channel"]["name"] not in self.names:
            leaks.append(f"channel.name={shape['channel']['name']}")
        rest = {key: value for key, value in shape.items() if key not in OWN_FIELDS}
        rest["channel"] = {key: value for key, value in shape["channel"].items() if key != "name"}
        return leaks + self.leaks(json.dumps(rest))


@dataclass(frozen=True)
class WireContext:
    """Where a wire form writes: the system and source that read it, and `token`, which plants
    a value in what surrounds a record, such as a partition key or an SNS attribute.
    """

    system: str
    source: str
    token: Callable[[], str]


# One wire form per source kind: it writes records as that kind reads them and returns the
# value its location env var takes. A kind with a reader and no wire form fails a test. The AWS
# kinds write to moto in `aws_streams.REGION`, so they run inside `mock_aws`.
type WireForm = Callable[[Path, Iterable[Any], Iterable[str], WireContext], str]


def replay_wire_form(
    directory: Path, records: Iterable[Any], broken: Iterable[str], _context: WireContext
) -> str:
    """A `.json` list, one `.json` file per record, and a `.jsonl` file with `broken` lines."""
    directory.mkdir(parents=True, exist_ok=True)
    records = list(records)
    third = len(records) // 3
    (directory / "a-list.json").write_text(json.dumps(records[:third]), encoding="utf-8")
    for index, record in enumerate(records[third : 2 * third]):
        (directory / f"b-{index:03d}.json").write_text(json.dumps(record), encoding="utf-8")
    lines = [json.dumps(record) for record in records[2 * third :]] + list(broken)
    (directory / "c-lines.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(directory)


def kinesis_wire_form(
    _directory: Path, records: Iterable[Any], broken: Iterable[str], context: WireContext
) -> str:
    """A stream named by a planted token; each record as its JSON under a planted partition
    key, then each `broken` string as its bytes.
    """
    stream = context.token()
    client = create_stream(stream)
    put_records(client, stream, [*records, *(line.encode() for line in broken)], context.token)
    return stream


def dynamodb_stream_wire_form(
    _directory: Path, records: Iterable[Any], broken: Iterable[str], context: WireContext
) -> str:
    """A table named by a planted token; each record put as an item, then an item keyed by each
    `broken` string, put and deleted.
    """
    table = context.token()
    client = create_table(table)
    put_items(client, table, records)
    for line in broken:
        key = {"PK": {"S": line}, "SK": {"S": line}}
        client.put_item(TableName=table, Item=key)
        client.delete_item(TableName=table, Key=key)
    return table


def sns_fanout_wire_form(
    _directory: Path, records: Iterable[Any], broken: Iterable[str], context: WireContext
) -> str:
    """A topic named by a planted token, with the source's queue subscribed; each record
    published as its JSON and each `broken` string as is, with a planted subject and attribute.
    """
    topic = context.token()
    sns, topic_arn = fanout(topic, queue_name(context.system, context.source))
    publish(sns, topic_arn, [*records, *broken], context.token)
    return topic


WIRE_FORMS: dict[str, WireForm] = {
    "replay": replay_wire_form,
    "kinesis": kinesis_wire_form,
    "dynamodb_stream": dynamodb_stream_wire_form,
    "sns_fanout": sns_fanout_wire_form,
}
