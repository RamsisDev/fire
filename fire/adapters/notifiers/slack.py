"""Slack notifier: posts each narration to its system's channel with `chat.postMessage`.

Contract: docs/specs/dossier-and-decision.md#slack-notifier.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from typing import Any

import httpx

from fire.core.domain.incident import Incident
from fire.core.domain.narration import Narration

logger = logging.getLogger(__name__)

POST_MESSAGE_URL = "https://slack.com/api/chat.postMessage"
# Slack's error codes are snake_case words; anything else in `error` is not logged.
_ERROR_CODE = re.compile(r"^[a-z0-9_]{1,64}$")


class SlackNotifier:
    """Posts one message per incident to the channel id `channels` maps its system to.

    A failed post never raises: Slack's `ok: false`, a non-2xx status, a body that is not
    JSON, a timeout, or a transport error is logged as one WARNING line with the incident, the
    system, and an error code, never the narration or the token. An incident whose system has
    no channel is logged and skipped.
    """

    def __init__(
        self,
        token: str,
        channels: Mapping[str, str],
        *,
        timeout_s: float = 5.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._token = token
        self._channels = dict(channels)
        self._timeout = httpx.Timeout(timeout_s)
        self._transport = transport

    async def publish(self, incident: Incident, narration: Narration) -> None:
        channel = self._channels.get(incident.system)
        if channel is None:
            logger.warning(
                "slack notify skipped: incident=%s system=%s reason=no_channel",
                incident.id,
                incident.system,
            )
            return
        error = await self._post(_message(channel, narration))
        if error is None:
            logger.info(
                "slack notified: incident=%s system=%s channel=%s",
                incident.id,
                incident.system,
                channel,
            )
        else:
            logger.warning(
                "slack notify failed: incident=%s system=%s channel=%s error=%s",
                incident.id,
                incident.system,
                channel,
                error,
            )

    async def _post(self, message: dict[str, Any]) -> str | None:
        """The error code of a failed post, else `None`."""
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json; charset=utf-8",
        }
        try:
            async with httpx.AsyncClient(
                transport=self._transport, timeout=self._timeout
            ) as client:
                response = await client.post(POST_MESSAGE_URL, headers=headers, json=message)
        except httpx.HTTPError as error:
            return type(error).__name__
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "")
            return f"http_429 retry_after={retry_after if retry_after.isdigit() else 'unset'}"
        if not response.is_success:
            return f"http_{response.status_code}"
        try:
            body = response.json()
        except ValueError:
            return "invalid_response"
        if not isinstance(body, dict):
            return "invalid_response"
        if body.get("ok") is True:
            return None
        code = body.get("error")
        return code if isinstance(code, str) and _ERROR_CODE.match(code) else "unknown_error"


def _message(channel: str, narration: Narration) -> dict[str, Any]:
    return {"channel": channel, "text": narration.channel_text}
