"""The body in-process and a tentacle started on it, for the event source tests: config written to
`tmp_path`, every request the sender posts recorded, and waits on the counters lines.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

import fire.apps.incident_engine.app as app_module
from fire.apps.incident_engine.app import app
from fire.apps.incident_engine.wiring import build_container
from fire.apps.tentacle.runner import prepare, run
from fire.core.ports.record_source import StartPosition

TOKEN = "events-test-token"
HEADERS = {"X-FIRE-Token": TOKEN}
COUNTERS = "tentacle counters: "


class RecordingTransport(httpx.AsyncBaseTransport):
    """Keeps every request body the sender posts, then hands the request to the body."""

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self.inner = inner
        self.bodies: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.bodies.append(request.content.decode())
        return await self.inner.handle_async_request(request)

    def shapes(self) -> list[dict[str, Any]]:
        return [shape for body in self.bodies for shape in json.loads(body)["shapes"]]


@dataclass
class Tentacle:
    transport: RecordingTransport
    stop: asyncio.Event
    running: asyncio.Task[int]

    async def finish(self) -> int:
        self.stop.set()
        return await asyncio.wait_for(self.running, 15)


@dataclass
class Body:
    """A system's config in `tmp_path`, the body built on it, and the tentacles started."""

    fire_yml: Path
    system: str
    caplog: pytest.LogCaptureFixture
    started: list[Tentacle] = field(default_factory=list)

    @classmethod
    def configure(
        cls,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        system: Mapping[str, Any],
    ) -> Body:
        fire_yml = tmp_path / "config" / "fire.yml"
        (fire_yml.parent / "systems").mkdir(parents=True)
        fire_yml.write_text(yaml.safe_dump({"tentacle": {"flush_ms": 10}}), encoding="utf-8")
        path = fire_yml.parent / "systems" / f"{system['system']}.yml"
        path.write_text(yaml.safe_dump(dict(system)), encoding="utf-8")
        monkeypatch.setenv("FIRE_API_TOKEN", TOKEN)
        app_module._container = build_container(fire_yml)
        return cls(fire_yml, system["system"], caplog)

    def start(
        self,
        environ: Mapping[str, str],
        *,
        start: StartPosition = StartPosition.OLDEST,
        source: str | None = None,
    ) -> Tentacle:
        env = {"FIRE_API_TOKEN": TOKEN, "FIRE_BODY_URL": "http://body", **environ}
        plan = prepare(
            self.system, source=source, start=start, config_path=self.fire_yml, environ=env
        )
        transport = RecordingTransport(httpx.ASGITransport(app=app))
        stop = asyncio.Event()
        running = asyncio.create_task(
            run(plan, stop, transport=transport, counters_interval_s=0.05)
        )
        tentacle = Tentacle(transport, stop, running)
        self.started.append(tentacle)
        return tentacle

    async def read_and_sent(
        self, counts: Mapping[str, int], timeout_s: float = 15, *, settled: str = ""
    ) -> None:
        """Waits for a counters line per source with its `read` count, none pending, and
        `settled` in it.
        """

        def done(source: str, read: int) -> bool:
            prefix = f"{COUNTERS}source={source} read={read} "
            return any(
                message.startswith(prefix) and " pending=0 " in message and settled in message
                for message in self.caplog.messages
            )

        async with asyncio.timeout(timeout_s):
            while not all(done(source, read) for source, read in counts.items()):
                await asyncio.sleep(0.02)

    async def finish(self) -> None:
        for tentacle in self.started:
            if not tentacle.running.done():
                await tentacle.finish()
        # AWS calls still in worker threads end inside the mock.
        await asyncio.get_running_loop().shutdown_default_executor()
        app_module._container = None


def counters(messages: Sequence[str]) -> dict[str, str]:
    """The last counters line of each source."""
    lines: dict[str, str] = {}
    for message in messages:
        if message.startswith(COUNTERS):
            source = message.removeprefix(f"{COUNTERS}source=").split(" ", 1)[0]
            lines[source] = message
    return lines


async def streams(system: str) -> dict[tuple[str, str], int]:
    """Observations per `(source, channel key)` of the body's `/streams`."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://body") as client:
        response = await client.get(f"/v0/systems/{system}/streams", headers=HEADERS)
    assert response.status_code == 200
    return {
        (stream["source"], stream["key"]): stream["observations"]
        for stream in response.json()["streams"]
    }


async def streams_text(system: str) -> str:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://body") as client:
        response = await client.get(f"/v0/systems/{system}/streams", headers=HEADERS)
    return response.text
