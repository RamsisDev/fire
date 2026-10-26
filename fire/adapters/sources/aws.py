"""What the AWS sources share: clients from the standard AWS environment, failure texts that name
no resource, failures logged once, regions read in parallel, and boto loggers held at INFO.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import AsyncIterator, Sequence
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

READ_ERRORS = (ClientError, BotoCoreError, OSError)
CONNECT_TIMEOUT_S = 5
# Above the longest SQS long poll, 20 s, so a receive that waits is not cut short.
READ_TIMEOUT_S = 30
_ERROR_CODE = re.compile(r"[A-Za-z0-9._-]{1,64}")


class ReadProblem(Exception):
    """A read that cannot go on as configured, named by a reason code only."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def make_client(service: str, region: str) -> Any:
    """A client for `service` in `region` from the standard AWS environment
    (`AWS_ENDPOINT_URL` for a local AWS emulator), with botocore's standard retries and short
    timeouts, so a call in a worker thread does not hold up shutdown for botocore's default
    60 s.
    """
    config = Config(
        retries={"mode": "standard"},
        connect_timeout=CONNECT_TIMEOUT_S,
        read_timeout=READ_TIMEOUT_S,
    )
    return boto3.client(service, region_name=region, config=config)


def error_code(error: ClientError) -> object:
    return error.response.get("Error", {}).get("Code")


def error_text(error: Exception) -> str:
    """The exception's type name; `ClientError:<code>` for an AWS error response, whose
    exception boto3 may also name after the code; a `ReadProblem`'s reason.
    """
    if isinstance(error, ReadProblem):
        return error.reason
    if not isinstance(error, ClientError):
        return type(error).__name__
    code = error_code(error)
    if isinstance(code, str) and _ERROR_CODE.fullmatch(code):
        return f"ClientError:{code}"
    return "ClientError"


class FailureLog:
    """Logs `<message>: source=<id> [region=<region> ]error=<text>` once per distinct text
    until a pass ends with no failure.
    """

    def __init__(
        self, logger: logging.Logger, message: str, source: str, region: str | None = None
    ) -> None:
        self._logger = logger
        self._prefix = f"{message}: source={source} "
        if region is not None:
            self._prefix += f"region={region} "
        self._logged: set[str] = set()
        self._failed = False

    def failed(self, error: Exception) -> None:
        self._failed = True
        text = error_text(error)
        if text not in self._logged:
            self._logged.add(text)
            self._logger.warning("%serror=%s", self._prefix, text)

    def end_pass(self) -> None:
        if not self._failed:
            self._logged.clear()
        self._failed = False


async def merge[T](streams: Sequence[AsyncIterator[T]]) -> AsyncIterator[T]:
    """Items of every stream as each yields them, one at a time, so a slow consumer pauses all.

    The first stream to raise ends the merge with its exception, and the others are cancelled;
    the merge ends when every stream has.
    """
    if len(streams) == 1:
        async for item in streams[0]:
            yield item
        return
    handoff: asyncio.Queue[T] = asyncio.Queue(maxsize=1)
    ended: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    remaining = len(streams)

    async def pump(stream: AsyncIterator[T]) -> None:
        nonlocal remaining
        try:
            async for item in stream:
                await handoff.put(item)
        except Exception as error:
            if not ended.done():
                ended.set_exception(error)
            return
        remaining -= 1
        if not remaining and not ended.done():
            ended.set_result(None)

    pumps = [asyncio.create_task(pump(stream)) for stream in streams]
    getting: asyncio.Future[T] | None = None
    try:
        while True:
            getting = asyncio.ensure_future(handoff.get())
            await asyncio.wait({getting, ended}, return_when=asyncio.FIRST_COMPLETED)
            if getting.done():
                yield getting.result()
                continue
            getting.cancel()
            ended.result()
            while not handoff.empty():
                yield handoff.get_nowait()
            return
    finally:
        tasks: list[asyncio.Future[Any]] = [*pumps, *([getting] if getting is not None else [])]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def quiet_boto() -> None:
    """botocore and urllib3 log request URLs, which hold resource names and keys, and request
    and response bodies at DEBUG.
    """
    for name in ("boto3", "botocore", "urllib3"):
        quieted = logging.getLogger(name)
        if quieted.level < logging.INFO:
            quieted.setLevel(logging.INFO)
