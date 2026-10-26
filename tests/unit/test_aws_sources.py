"""What the AWS sources share: regions merged with backpressure, failures logged once by type or
code, and clients from the standard AWS environment.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from aws_streams import isolate_aws
from botocore.exceptions import ClientError, EndpointConnectionError

from fire.adapters.sources.aws import FailureLog, ReadProblem, error_text, make_client, merge

LOGGER = logging.getLogger("tests.aws")


async def _items(*items: str, fail: bool = False, live: bool = False) -> AsyncIterator[str]:
    for item in items:
        await asyncio.sleep(0)
        yield item
    if fail:
        raise RuntimeError("planted")
    if live:
        await asyncio.Event().wait()


@pytest.mark.unit
async def test_merge_yields_every_item_of_every_stream_and_ends_when_all_have() -> None:
    merged = [item async for item in merge([_items("a1", "a2"), _items("b1"), _items()])]
    assert sorted(merged) == ["a1", "a2", "b1"]


@pytest.mark.unit
async def test_merge_ends_with_the_first_failure_and_cancels_the_rest() -> None:
    cancelled = asyncio.Event()

    async def live() -> AsyncIterator[str]:
        try:
            yield "b1"
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    read: list[str] = []
    with pytest.raises(RuntimeError):
        async for item in merge([_items("a1", fail=True), live()]):
            read.append(item)
    assert cancelled.is_set()
    assert "a1" in read


@pytest.mark.unit
async def test_merge_holds_one_item_so_a_paused_consumer_pauses_every_stream() -> None:
    pulled: list[str] = []

    async def counted(prefix: str) -> AsyncIterator[str]:
        for index in range(10):
            pulled.append(f"{prefix}{index}")
            yield f"{prefix}{index}"

    merged = merge([counted("a"), counted("b")])
    first = await anext(merged)
    await asyncio.sleep(0.05)

    # One item handed over, one waiting in the handoff, one per stream waiting to enter it.
    assert first in pulled
    assert len(pulled) <= 4
    await merged.aclose()


@pytest.mark.unit
async def test_cancelling_a_merge_leaves_no_task_behind() -> None:
    before = asyncio.all_tasks()

    async def consume() -> None:
        async for _ in merge([_items(live=True), _items(live=True)]):
            pass

    consuming = asyncio.create_task(consume())
    await asyncio.sleep(0.05)
    consuming.cancel()
    with pytest.raises(asyncio.CancelledError):
        await consuming
    await asyncio.sleep(0)

    assert asyncio.all_tasks() - before == set()


@pytest.mark.unit
async def test_merge_of_one_stream_is_that_stream() -> None:
    assert [item async for item in merge([_items("a1", "a2")])] == ["a1", "a2"]


@pytest.mark.unit
def test_failure_log_logs_each_text_once_until_a_clean_pass(
    caplog: pytest.LogCaptureFixture,
) -> None:
    failures = FailureLog(LOGGER, "event read failed", "events", "us-west-2")
    error = ClientError({"Error": {"Code": "AccessDenied", "Message": "planted"}}, "ListShards")
    for _ in range(2):
        failures.failed(error)
        failures.end_pass()
    failures.end_pass()
    failures.failed(error)
    failures.failed(ReadProblem("no_stream"))

    assert [record.getMessage() for record in caplog.records] == [
        "event read failed: source=events region=us-west-2 error=ClientError:AccessDenied",
        "event read failed: source=events region=us-west-2 error=ClientError:AccessDenied",
        "event read failed: source=events region=us-west-2 error=no_stream",
    ]
    assert "planted" not in caplog.text


@pytest.mark.unit
@pytest.mark.parametrize(
    ("error", "text"),
    [
        (ClientError({"Error": {"Code": "Throttling"}}, "X"), "ClientError:Throttling"),
        (ClientError({"Error": {"Code": "odd code with spaces"}}, "X"), "ClientError"),
        (ClientError({}, "X"), "ClientError"),
        (EndpointConnectionError(endpoint_url="http://planted"), "EndpointConnectionError"),
        (ReadProblem("no_new_image"), "no_new_image"),
        (ConnectionResetError("planted"), "ConnectionResetError"),
    ],
)
def test_error_text_names_a_type_or_code_only(error: Exception, text: str) -> None:
    assert error_text(error) == text


@pytest.mark.unit
def test_make_client_reads_the_endpoint_from_the_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    isolate_aws(monkeypatch, tmp_path)
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:4566")

    client = make_client("kinesis", "us-west-2")

    assert client.meta.region_name == "us-west-2"
    assert client.meta.endpoint_url == "http://localhost:4566"
    assert (client.meta.config.connect_timeout, client.meta.config.read_timeout) == (5, 30)
