"""Narrator output: readable channel prose derived only from Decision + safe fragments."""

from __future__ import annotations

from pydantic import BaseModel, Field


class Narration(BaseModel):
    incident_id: str
    channel_text: str
    used_possibility_ids: list[str] = Field(default_factory=list)
