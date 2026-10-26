"""Replays a drift corpus of dev/corpora/ into the body through the tentacle's own sources.

    uv run python dev/replay_corpus.py outbound|path [--system acme] [--config config/fire.yml]
        [--drift-copies N]

Needs a running body (FIRE_BODY_URL, FIRE_API_TOKEN) that loads the system, and neither the
system's stack nor AWS. Each corpus holds a `baseline/` and a `drift/`. The replay sends
`drift.min_samples` copies of the baseline items, in turn, then `--drift-copies` copies of the
drift items, each copy with fresh ids, so a second replay into the same body adds observations
instead of duplicates.

- `outbound`: capture pairs, in the format of tests/fixtures/acme/captures/. They are written to
  a bucket of moto's in-process S3, one fake work unit per copy, and read by the system's
  `s3_capture` source, which waits its `settle_s`.
- `path`: OTLP recordings, in the format of tests/fixtures/acme/otlp/, one directory per item.
  Each copy is posted to the system's `otlp` source, listening on a free local port. The script
  then waits for the flows to settle (`paths.settle_s`).

The script prints what the body's `/streams` or `/paths` then says about the corpus; the
notifications are in the body's output (dev/README.md, Drift demo).
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import json
import logging
import os
import secrets
import socket
import sys
from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from pathlib import Path
from typing import Any

import boto3
import httpx
from moto import mock_aws

from fire.apps.config import (
    OtlpSourceConfig,
    S3CaptureSourceConfig,
    SourceConfig,
    SystemConfig,
    load_fire_config,
    load_system_config,
)
from fire.apps.env import load_fire_env
from fire.apps.tentacle.runner import (
    DEFAULT_CONFIG,
    EXIT_OK,
    EXIT_STARTUP_ERROR,
    EXIT_UNSENT,
    Plan,
    StartupError,
    prepare,
    run,
)
from fire.apps.tentacle.sender import logger as sender_logger
from fire.core.domain.capture import REQUESTS, RESPONSES
from fire.core.ports.record_source import StartPosition

DEFAULT_SYSTEM = "acme"
CORPORA = Path(__file__).resolve().parent / "corpora"
OUTBOUND, PATH = "outbound", "path"
BASELINE, DRIFT = "baseline", "drift"
# A fake ULID: this head, a per-replay salt, the phase, and the copy number.
FAKE_ULID_HEAD = "0FAKE"
PHASES = {BASELINE: "B", DRIFT: "D"}
CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
DEMO_BUCKET = "fire-drift-demo-captures"
DEMO_REGION = "us-east-1"
AWS_DROPPED = ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_PROFILE", "AWS_SESSION_TOKEN")
# How long past the source's own waits a replay may take before it gives up.
SLACK_S = 30.0

logger = logging.getLogger("replay_corpus")

type Document = dict[str, Any]
type Feed = Callable[["AckCounter"], Awaitable[None]]


def new_salt() -> str:
    return "".join(secrets.choice(CROCKFORD) for _ in range(4))


class AckCounter(httpx.AsyncBaseTransport):
    """Counts the shapes and spans of every batch the body answered `202`."""

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self.inner = inner
        self.shapes = 0
        self.spans = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await self.inner.handle_async_request(request)
        if response.status_code == 202:
            batch = json.loads(request.content)
            self.shapes += len(batch.get("shapes", ()))
            self.spans += len(batch.get("spans", ()))
        return response

    async def wait_for_shapes(self, count: int) -> None:
        while self.shapes < count:
            await asyncio.sleep(0.05)

    async def aclose(self) -> None:
        await self.inner.aclose()


def system_config(config_path: Path, system: str) -> SystemConfig:
    return load_system_config(config_path.parent / "systems" / f"{system}.yml")


def source_of[T: SourceConfig](system: SystemConfig, kind: type[T]) -> T:
    [found] = [source for source in system.sources if isinstance(source, kind)]
    return found


def _copies[T](items: list[T], count: int) -> list[T]:
    return [items[number % len(items)] for number in range(count)] if items else []


def outbound_items(phase: str) -> list[Document]:
    return [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((CORPORA / OUTBOUND / phase).glob("*.json"))
    ]


def path_items(phase: str) -> list[list[Document]]:
    return [
        [json.loads(path.read_text(encoding="utf-8")) for path in sorted(directory.glob("*.json"))]
        for directory in sorted((CORPORA / PATH / phase).iterdir())
        if directory.is_dir()
    ]


def outbound_pairs(salt: str, baseline_copies: int, drift_copies: int) -> list[Document]:
    """The pairs in replay order. Each copy gets its own work unit, which sorts the drift copies
    after the baseline ones in the bucket's listing.
    """
    pairs: list[Document] = []
    for phase, count in ((BASELINE, baseline_copies), (DRIFT, drift_copies)):
        for number, item in enumerate(_copies(outbound_items(phase), count)):
            work = f"{FAKE_ULID_HEAD}{salt}{PHASES[phase]}{number:016d}"
            pair = copy.deepcopy(item)
            pair["name"] = f"{work}/{item['name'].partition('/')[2]}"
            for half in (pair["request"], pair["response"]):
                if half is not None and "work_unit_id" in half["metadata"]:
                    half["metadata"]["work_unit_id"] = work
            pairs.append(pair)
    return pairs


def _fresh(value: str, seed: str) -> str:
    """The same new id for the same id and seed, of the same length; an empty id stays."""
    return hashlib.sha256(f"{seed}:{value}".encode()).hexdigest()[: len(value)] if value else value


def _refreshed(document: Document, seed: str) -> Document:
    """An OTLP request with every trace and span id, links included, mapped to a fresh one."""
    document = copy.deepcopy(document)
    for resource in document["resourceSpans"]:
        for scope in resource["scopeSpans"]:
            for span in scope["spans"]:
                for key in ("traceId", "spanId", "parentSpanId"):
                    if key in span:
                        span[key] = _fresh(span[key], seed)
                for link in span.get("links", ()):
                    link["traceId"] = _fresh(link["traceId"], seed)
                    link["spanId"] = _fresh(link["spanId"], seed)
    return document


def path_documents(salt: str, baseline_copies: int, drift_copies: int) -> list[Document]:
    """The OTLP requests in replay order, every copy's ids fresh."""
    documents: list[Document] = []
    for phase, count in ((BASELINE, baseline_copies), (DRIFT, drift_copies)):
        for number, item in enumerate(_copies(path_items(phase), count)):
            seed = f"{salt}:{PHASES[phase]}{number}"
            documents.extend(_refreshed(document, seed) for document in item)
    return documents


class UnsentBatches(logging.Handler):
    """Sets `seen` when the tentacle's sender gives a batch up; it logs why."""

    def __init__(self) -> None:
        super().__init__(logging.WARNING)
        self.seen = asyncio.Event()

    def emit(self, record: logging.LogRecord) -> None:
        if str(record.msg).startswith("batch unsent"):
            self.seen.set()


def _prepare(
    system: str, source: SourceConfig, environ: Mapping[str, str], config_path: Path
) -> Plan:
    """The tentacle's startup checks for `source` alone, read from the oldest object."""
    return prepare(
        system,
        source=source.id,
        start=StartPosition.OLDEST,
        config_path=config_path,
        environ=environ,
    )


async def _run_tentacle(
    plan: Plan,
    feed: Feed,
    *,
    timeout_s: float,
    transport: httpx.AsyncBaseTransport | None,
) -> int:
    """Runs the tentacle until `feed` returns, the sender gives a batch up, or the tentacle
    ends; then stops it, which sends what is pending, and returns its exit code.

    Raises:
        TimeoutError: when none of them happens within `timeout_s`.
    """
    counter = AckCounter(transport or httpx.AsyncHTTPTransport())
    unsent = UnsentBatches()
    sender_logger.addHandler(unsent)
    try:
        stop = asyncio.Event()
        running = asyncio.create_task(run(plan, stop, transport=counter))
        feeding = asyncio.create_task(feed(counter))
        given_up = asyncio.create_task(unsent.seen.wait())
        try:
            async with asyncio.timeout(timeout_s):
                await asyncio.wait(
                    {running, feeding, given_up}, return_when=asyncio.FIRST_COMPLETED
                )
        finally:
            feeding.cancel()
            given_up.cancel()
            stop.set()
            exit_code = await running
    finally:
        sender_logger.removeHandler(unsent)
    feed_error = feeding.exception() if feeding.done() and not feeding.cancelled() else None
    if unsent.seen.is_set():
        logger.error(
            "replay stopped: the tentacle gave a batch up; its batch unsent line says why"
        )
    elif feed_error is not None:
        raise feed_error
    return exit_code


async def replay_outbound(
    s3: Any,
    *,
    environ: Mapping[str, str],
    system: str = DEFAULT_SYSTEM,
    config_path: Path = DEFAULT_CONFIG,
    transport: httpx.AsyncBaseTransport | None = None,
    salt: str,
    drift_copies: int = 1,
    baseline_copies: int | None = None,
) -> int:
    """Writes the outbound corpus to the bucket the `s3_capture` source's env var names, then
    runs that source until the body took as many shapes as the pairs give, two each, so the
    bucket must start empty.
    """
    config = system_config(config_path, system)
    source = source_of(config, S3CaptureSourceConfig)
    plan = _prepare(system, source, environ, config_path)
    bucket = environ[source.location_env]
    if baseline_copies is None:
        baseline_copies = config.drift.min_samples
    pairs = outbound_pairs(salt, baseline_copies, drift_copies)
    for pair in pairs:
        for part, half in ((REQUESTS, pair["request"]), (RESPONSES, pair["response"])):
            if half is not None:
                s3.put_object(
                    Bucket=bucket,
                    Key=f"{pair['prefix']}{part}/{pair['name']}",
                    Body=json.dumps(half["document"]).encode(),
                    Metadata=half["metadata"],
                )
    logger.info("corpus written: corpus=%s pairs=%d", OUTBOUND, len(pairs))

    async def feed(counter: AckCounter) -> None:
        await counter.wait_for_shapes(2 * len(pairs))

    return await _run_tentacle(
        plan,
        feed,
        timeout_s=source.settle_s + 2 * source.poll_s + SLACK_S,
        transport=transport,
    )


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


async def replay_path(
    *,
    environ: Mapping[str, str],
    system: str = DEFAULT_SYSTEM,
    config_path: Path = DEFAULT_CONFIG,
    transport: httpx.AsyncBaseTransport | None = None,
    salt: str,
    drift_copies: int = 1,
    baseline_copies: int | None = None,
) -> int:
    """Runs the `otlp` source on a free local port, posts the path corpus to it as a collector
    does, and stops it once every request was taken.
    """
    config = system_config(config_path, system)
    source = source_of(config, OtlpSourceConfig)
    if baseline_copies is None:
        baseline_copies = config.drift.min_samples
    listen = f"127.0.0.1:{free_port()}"
    plan = _prepare(system, source, {**environ, source.location_env: listen}, config_path)
    documents = path_documents(salt, baseline_copies, drift_copies)

    async def feed(_counter: AckCounter) -> None:
        async with httpx.AsyncClient(trust_env=False, timeout=10) as client:
            for document in documents:
                await _post_traces(client, f"http://{listen}/v1/traces", document)
        logger.info("corpus posted: corpus=%s requests=%d", PATH, len(documents))

    return await _run_tentacle(plan, feed, timeout_s=SLACK_S, transport=transport)


async def _post_traces(client: httpx.AsyncClient, url: str, document: Document) -> None:
    """Posts one OTLP request, again after `Retry-After` while the receiver's buffer is full."""
    content = json.dumps(document).encode()
    while True:
        response = await client.post(
            url, content=content, headers={"content-type": "application/json"}
        )
        if response.status_code != 503:
            response.raise_for_status()
            return
        await asyncio.sleep(float(response.headers.get("retry-after", "1")))


async def _get(base: str, token: str, path: str) -> Document:
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        response = await client.get(path, headers={"X-FIRE-Token": token})
    response.raise_for_status()
    return response.json()


async def print_streams(base: str, token: str, system: str, source: str) -> None:
    """Each stream of `source`, with its status and drift signals."""
    body = await _get(base, token, f"/v0/systems/{system}/streams")
    for stream in body["streams"]:
        if stream["source"] != source:
            continue
        drift = [
            {"diff": signal["diff"], "occurrences": signal["occurrences"]}
            for signal in stream["drift"]
        ]
        summary = {"status": stream["status"], "samples": stream["samples"], "drift": drift}
        print(f"{stream['key']} {json.dumps(summary)}", flush=True)


async def wait_and_print_paths(base: str, token: str, system: str, settle_s: float) -> bool:
    """Polls `/paths` until no flow is open, then prints each path; False on a timeout."""
    try:
        async with asyncio.timeout(settle_s + SLACK_S):
            while (body := await _get(base, token, f"/v0/systems/{system}/paths"))["open_flows"]:
                await asyncio.sleep(2)
    except TimeoutError:
        logger.error("flows still open after %ss", settle_s + SLACK_S)
        return False
    for path in body["paths"]:
        entry = f"{path['entry']['service']}/{path['entry']['operation']}"
        drift = [
            {
                "rule": signal["rule"],
                "node": signal["node"]["operation"],
                "occurrences": signal["occurrences"],
            }
            for signal in path["drift"]
        ]
        summary = {
            "status": path["status"],
            "flows": path["flows"],
            "drifted_flows": path["drifted_flows"],
            "drift": drift,
        }
        print(f"{entry} {json.dumps(summary)}", flush=True)
    return True


def isolate_aws(environ: MutableMapping[str, str]) -> None:
    """Keeps the shell's AWS setup away from moto's S3: no endpoint, profile, session, or AWS
    file, and the region moto's `create_bucket` takes without a location constraint.
    """
    for name in AWS_DROPPED:
        environ.pop(name, None)
    environ.update(
        AWS_DEFAULT_REGION=DEMO_REGION,
        AWS_CONFIG_FILE=os.devnull,
        AWS_SHARED_CREDENTIALS_FILE=os.devnull,
    )


async def _replay(
    corpus: str, system: str, config_path: Path, environ: dict[str, str], drift_copies: int
) -> int:
    salt = new_salt()
    if corpus == PATH:
        return await replay_path(
            environ=environ,
            system=system,
            config_path=config_path,
            salt=salt,
            drift_copies=drift_copies,
        )
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=DEMO_BUCKET)
        try:
            return await replay_outbound(
                s3,
                environ=environ,
                system=system,
                config_path=config_path,
                salt=salt,
                drift_copies=drift_copies,
            )
        finally:
            # S3 calls still in worker threads end inside the mock.
            await asyncio.get_running_loop().shutdown_default_executor()


async def _main(corpus: str, system: str, config_path: Path, drift_copies: int) -> int:
    try:
        fire = load_fire_config(config_path)
        config = system_config(config_path, system)
    except (OSError, ValueError) as error:
        logger.error("tentacle not started: %s", error)
        return EXIT_STARTUP_ERROR
    environ = dict(os.environ)
    captures = source_of(config, S3CaptureSourceConfig) if corpus == OUTBOUND else None
    if captures is not None:
        environ[captures.location_env] = DEMO_BUCKET
    base = environ.get(fire.tentacle.body_url_env, "").rstrip("/")
    token = environ.get(fire.api.token_env, "")
    try:
        exit_code = await _replay(corpus, system, config_path, environ, drift_copies)
    except StartupError as error:
        logger.error("tentacle not started: %s", error)
        return EXIT_STARTUP_ERROR
    except TimeoutError:
        logger.error("the body did not take the whole corpus in time")
        return EXIT_UNSENT
    if exit_code:
        return exit_code
    if captures is not None:
        await print_streams(base, token, system, captures.id)
        return exit_code
    logger.info("waiting for the flows to settle: settle_s=%s", config.paths.settle_s)
    settled = await wait_and_print_paths(base, token, system, config.paths.settle_s)
    return EXIT_OK if settled else 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("corpus", choices=[OUTBOUND, PATH])
    parser.add_argument(
        "--system",
        default=DEFAULT_SYSTEM,
        help="the system whose sources replay the corpus, as <config dir>/systems/<system>.yml",
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="FIRE's fire.yml")
    parser.add_argument("--drift-copies", type=int, default=1)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    load_fire_env()
    isolate_aws(os.environ)
    sys.exit(asyncio.run(_main(args.corpus, args.system, args.config, args.drift_copies)))


if __name__ == "__main__":
    main()
