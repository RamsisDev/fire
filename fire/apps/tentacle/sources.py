"""Openers by source kind: readers for record sources, the inbound proxy, the OTLP receiver, and
the S3 capture source. Nothing here calls AWS: an AWS source's first call is its first read.
"""

from __future__ import annotations

import re
import socket
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import httpx
from botocore.exceptions import BotoCoreError

from fire.adapters.sources.aws import make_client
from fire.adapters.sources.dynamodb_stream import (
    DynamoDbStreamRecordSource,
    RegionClients,
    StreamFilter,
)
from fire.adapters.sources.http_proxy import HttpProxy
from fire.adapters.sources.kinesis import KinesisRecordSource
from fire.adapters.sources.listener import listen
from fire.adapters.sources.otlp import OtlpReceiver
from fire.adapters.sources.replay import SUFFIXES, ReplayRecordSource
from fire.adapters.sources.s3_capture import S3CaptureSource, make_s3_client
from fire.adapters.sources.sns_fanout import SnsFanoutRecordSource, queue_name
from fire.apps.config import (
    DynamoDbStreamSourceConfig,
    HttpProxySourceConfig,
    KinesisSourceConfig,
    OtlpSourceConfig,
    ReplaySourceConfig,
    S3CaptureSourceConfig,
    SnsFanoutSourceConfig,
    SourceConfig,
)
from fire.core.ports.record_source import RecordSource, StartPosition

# Opens a source from its config, the value of its `location_env`, the start position, and
# the id of the system that declares it.
type Reader = Callable[[SourceConfig, str, StartPosition, str], RecordSource]

_BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
_STREAM = re.compile(r"[A-Za-z0-9_.-]{1,128}")
_TABLE = re.compile(r"[A-Za-z0-9_.-]{3,255}")
_TOPIC = re.compile(r"[A-Za-z0-9_-]{1,256}")
_QUEUE = re.compile(r"[A-Za-z0-9_-]{1,80}")


class SourceUnavailable(Exception):
    """The source cannot be read as configured. The message names env vars, never their values."""


def _replay(
    config: SourceConfig, location: str, _start: StartPosition, _system: str
) -> RecordSource:
    if not isinstance(config, ReplaySourceConfig):
        raise TypeError(f"replay reader got a {config.kind} source")
    path = Path(location)
    if path.is_dir() or (path.is_file() and path.suffix in SUFFIXES):
        return ReplayRecordSource(config.id, path)
    raise SourceUnavailable(f"{config.path_env} names no .json or .jsonl file and no directory")


def _kinesis(
    config: SourceConfig, stream: str, start: StartPosition, _system: str
) -> RecordSource:
    if not isinstance(config, KinesisSourceConfig):
        raise TypeError(f"kinesis reader got a {config.kind} source")
    if _STREAM.fullmatch(stream) is None:
        raise SourceUnavailable(f"{config.stream_env} is not a Kinesis stream name")
    return KinesisRecordSource(
        source=config.id,
        stream=stream,
        clients=_clients("kinesis", config.regions),
        start=start,
        poll_s=config.poll_s,
    )


def _dynamodb_stream(
    config: SourceConfig, table: str, start: StartPosition, _system: str
) -> RecordSource:
    if not isinstance(config, DynamoDbStreamSourceConfig):
        raise TypeError(f"dynamodb_stream reader got a {config.kind} source")
    if _TABLE.fullmatch(table) is None:
        raise SourceUnavailable(f"{config.table_env} is not a DynamoDB table name")
    tables = _clients("dynamodb", config.regions)
    streams = _clients("dynamodbstreams", config.regions)
    found = config.filter
    return DynamoDbStreamRecordSource(
        source=config.id,
        table=table,
        clients={region: RegionClients(tables[region], streams[region]) for region in tables},
        start=start,
        poll_s=config.poll_s,
        stream_filter=None if found is None else StreamFilter(found.attribute, found.equals),
    )


def _sns_fanout(
    config: SourceConfig, topic: str, start: StartPosition, system: str
) -> RecordSource:
    if not isinstance(config, SnsFanoutSourceConfig):
        raise TypeError(f"sns_fanout reader got a {config.kind} source")
    if topic.endswith(".fifo"):
        raise SourceUnavailable(f"{config.topic_env} names a FIFO topic; only standard ones")
    if _TOPIC.fullmatch(topic) is None:
        raise SourceUnavailable(f"{config.topic_env} is not an SNS topic name")
    queue = queue_name(system, config.id)
    if _QUEUE.fullmatch(queue) is None:
        raise SourceUnavailable(f"source id {config.id} makes a queue name over 80 characters")
    return SnsFanoutRecordSource(
        source=config.id,
        topic=topic,
        queue=queue,
        clients=_clients("sqs", config.regions),
        raw_delivery=config.raw_delivery,
        start=start,
        wait_s=config.wait_s,
        visibility_s=config.visibility_s,
        poll_s=config.poll_s,
    )


def _clients(service: str, regions: Sequence[str]) -> dict[str, Any]:
    """A client per region. An AWS environment no client can be built from is named by
    exception type only: its text quotes the environment.
    """
    try:
        return {region: make_client(service, region) for region in regions}
    except BotoCoreError as error:
        raise SourceUnavailable(
            f"cannot build a {service} client ({type(error).__name__})"
        ) from None


READERS: Mapping[str, Reader] = {
    "replay": _replay,
    "kinesis": _kinesis,
    "dynamodb_stream": _dynamodb_stream,
    "sns_fanout": _sns_fanout,
}


def open_http_proxy(config: HttpProxySourceConfig, upstream: str) -> HttpProxy:
    """Checks the upstream URL and binds the listen address, so a busy port fails at startup."""
    try:
        parsed = httpx.URL(upstream)
    except httpx.InvalidURL:
        parsed = None
    if parsed is None or parsed.scheme not in {"http", "https"} or not parsed.host:
        raise SourceUnavailable(f"{config.upstream_env} is not an http or https URL")
    return HttpProxy(
        source=config.id,
        sock=_listen(config.listen_host, config.listen_port),
        upstream=upstream,
        openapi_path=config.openapi_path,
        openapi_refresh_s=config.openapi_refresh_s,
        max_body_bytes=config.max_body_bytes,
    )


def open_otlp_receiver(config: OtlpSourceConfig, address: str) -> OtlpReceiver:
    """Checks the listen address and binds it, so a busy port fails at startup."""
    host, _, port = address.rpartition(":")
    host = host.removeprefix("[").removesuffix("]") if host.startswith("[") else host
    if not host or not (port.isascii() and port.isdigit()) or not 0 < int(port) < 65536:
        raise SourceUnavailable(f"{config.listen_env} is not a host:port address")
    return OtlpReceiver(
        source=config.id, sock=_listen(host, int(port)), capture_event=config.capture_event
    )


def open_s3_capture(
    config: S3CaptureSourceConfig, bucket: str, start: StartPosition
) -> S3CaptureSource:
    """Checks the bucket name and builds the source; nothing calls AWS until it reads.

    An AWS environment the client cannot be built from, such as an `AWS_PROFILE` that names no
    profile, is named by exception type only: its text quotes the environment.
    """
    if _BUCKET.fullmatch(bucket) is None:
        raise SourceUnavailable(f"{config.bucket_env} is not an S3 bucket name")
    try:
        client = make_s3_client()
    except BotoCoreError as error:
        raise SourceUnavailable(f"cannot build an S3 client ({type(error).__name__})") from None
    return S3CaptureSource(
        source=config.id,
        bucket=bucket,
        rules=config.captures,
        client=client,
        start=start,
        poll_s=config.poll_s,
        settle_s=config.settle_s,
        max_object_bytes=config.max_object_bytes,
    )


def _listen(host: str, port: int) -> socket.socket:
    try:
        return listen(host, port)
    except OSError as error:
        reason = error.strerror or type(error).__name__
        raise SourceUnavailable(f"cannot listen on {host}:{port} ({reason})") from None
