"""The uvicorn server behind the tentacle's listeners, the inbound proxy and the OTLP receiver.

It writes no access log, adds no `Date` or `Server` header, and leaves SIGINT and SIGTERM to
the tentacle. uvicorn's own lines carry no request data.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import socket
from collections.abc import Awaitable, Callable, Iterator
from typing import Any

import uvicorn
from uvicorn.protocols.http.h11_impl import H11Protocol

type Scope = dict[str, Any]
type Message = dict[str, Any]
type Receive = Callable[[], Awaitable[Message]]
type Send = Callable[[Message], Awaitable[None]]
type App = Callable[[Scope, Receive, Send], Awaitable[None]]

BACKLOG = 2048


def listen(host: str, port: int) -> socket.socket:
    """A bound TCP socket listening on host:port; port 0 picks a free one. Raises OSError."""
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    return socket.create_server((host, port), family=family, backlog=BACKLOG)


def quiet_server(app: App, *, shutdown_s: float) -> uvicorn.Server:
    """A uvicorn server for `app` that finishes the requests in flight within `shutdown_s` once
    asked to exit.
    """
    config = uvicorn.Config(
        app,
        interface="asgi3",
        http=_QuietH11Protocol,
        ws="none",
        lifespan="off",
        access_log=False,
        log_config=None,
        server_header=False,
        date_header=False,
        proxy_headers=False,
        timeout_graceful_shutdown=math.ceil(shutdown_s),
    )
    config.load()
    # At TRACE level uvicorn wraps the app in a logger that writes every scope, path,
    # query, and headers included.
    config.loaded_app = app
    return _Server(config)


async def serve_until_cancelled(server: uvicorn.Server, sock: socket.socket) -> None:
    """Serves on `sock` until cancelled; then stops accepting, lets the server finish what is in
    flight, and re-raises the cancellation. The server closes `sock` when it stops.
    """
    serving = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        await asyncio.wait({serving})
    except asyncio.CancelledError:
        server.should_exit = True
        await asyncio.wait({serving})
        raise
    finally:
        serving.cancel()
    serving.result()


class _Server(uvicorn.Server):
    """uvicorn, leaving SIGINT and SIGTERM to the tentacle."""

    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        yield


class _QuietH11Protocol(H11Protocol):
    """uvicorn's h11 protocol without its access log, which writes each path and query. uvicorn
    turns it on per connection whenever `uvicorn.access` has a handler, `access_log=False` or not.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.access_log = False
