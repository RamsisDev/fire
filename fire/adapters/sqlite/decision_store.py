"""DecisionStore in a SQLite file: decisions and their feedback survive a restart."""

from __future__ import annotations

import os
import sqlite3
import threading
from collections.abc import Collection
from datetime import UTC, datetime
from typing import Any

from fire.core.domain.feedback import DecisionRecord, Feedback, TrainingStatus, Verdict

SCHEMA_VERSION = 1

_SCHEMA = """
CREATE TABLE IF NOT EXISTS decisions (
    incident_id TEXT PRIMARY KEY,
    system TEXT NOT NULL,
    decided_at TEXT NOT NULL,
    record TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS feedback (
    id TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL,
    verdict TEXT NOT NULL,
    possibility_id TEXT,
    reviewer TEXT NOT NULL,
    note TEXT,
    created_at TEXT NOT NULL,
    status TEXT NOT NULL,
    batch_id TEXT
);
CREATE INDEX IF NOT EXISTS feedback_status ON feedback (status);
CREATE INDEX IF NOT EXISTS feedback_incident ON feedback (incident_id);
"""

_FIELDS = (
    "id",
    "incident_id",
    "verdict",
    "possibility_id",
    "reviewer",
    "note",
    "created_at",
    "status",
    "batch_id",
)
_INSERT = f"INSERT INTO feedback ({', '.join(_FIELDS)}) VALUES ({', '.join('?' * len(_FIELDS))})"


def _text(moment: datetime) -> str:
    """Fixed-width UTC, so text order is time order."""
    return moment.astimezone(UTC).isoformat(timespec="microseconds")


def _feedback(row: tuple[Any, ...]) -> Feedback:
    feedback_id, incident_id, verdict, possibility_id, reviewer, note, created, status, batch = row
    return Feedback(
        id=feedback_id,
        incident_id=incident_id,
        verdict=Verdict(verdict),
        possibility_id=possibility_id,
        reviewer=reviewer,
        note=note,
        created_at=datetime.fromisoformat(created),
        status=TrainingStatus(status),
        batch_id=batch,
    )


class SqliteDecisionStore:
    """Thread-safe: one connection, used only under the store's lock, and one transaction per
    write. Each record is one JSON column; feedback rows are plain columns. The file and its
    directory are created when missing.
    """

    def __init__(self, path: str | os.PathLike[str]) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.Lock()
        try:
            self._open()
        except Exception:
            self._db.close()
            raise

    def _open(self) -> None:
        version = self._db.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, SCHEMA_VERSION):
            raise ValueError(
                f"decision store schema version {version} is not {SCHEMA_VERSION}: "
                "move the file aside"
            )
        self._db.execute("PRAGMA journal_mode = WAL")
        self._db.execute("PRAGMA synchronous = NORMAL")
        self._db.executescript(_SCHEMA)
        self._db.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

    def save_decision(self, record: DecisionRecord) -> None:
        with self._lock, self._db:
            self._db.execute(
                "INSERT INTO decisions VALUES (?, ?, ?, ?) ON CONFLICT (incident_id) DO UPDATE "
                "SET system = excluded.system, decided_at = excluded.decided_at, "
                "record = excluded.record",
                (
                    record.incident_id,
                    record.system,
                    _text(record.decided_at),
                    record.model_dump_json(),
                ),
            )

    def get_decision(self, incident_id: str) -> DecisionRecord | None:
        with self._lock:
            row = self._db.execute(
                "SELECT record FROM decisions WHERE incident_id = ?", (incident_id,)
            ).fetchone()
        return None if row is None else DecisionRecord.model_validate_json(row[0])

    def add_feedback(self, feedback: Feedback) -> None:
        try:
            with self._lock, self._db:
                self._db.execute(
                    _INSERT,
                    (
                        feedback.id,
                        feedback.incident_id,
                        feedback.verdict.value,
                        feedback.possibility_id,
                        feedback.reviewer,
                        feedback.note,
                        _text(feedback.created_at),
                        feedback.status.value,
                        feedback.batch_id,
                    ),
                )
        except sqlite3.IntegrityError as error:
            raise ValueError(f"feedback {feedback.id!r} is already stored") from error

    def list_feedback(
        self,
        status: TrainingStatus | None = None,
        system: str | None = None,
        limit: int | None = None,
        *,
        incident_id: str | None = None,
    ) -> list[Feedback]:
        query = f"SELECT {', '.join(f'f.{name}' for name in _FIELDS)} FROM feedback f"
        where: list[str] = []
        params: list[Any] = []
        if system is not None:
            query += " JOIN decisions d ON d.incident_id = f.incident_id"
            where.append("d.system = ?")
            params.append(system)
        if status is not None:
            where.append("f.status = ?")
            params.append(TrainingStatus(status).value)
        if incident_id is not None:
            where.append("f.incident_id = ?")
            params.append(incident_id)
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY f.rowid"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        with self._lock:
            rows = self._db.execute(query, params).fetchall()
        return [_feedback(row) for row in rows]

    def mark(
        self, ids: Collection[str], status: TrainingStatus, batch_id: str | None = None
    ) -> None:
        with self._lock, self._db:
            self._db.executemany(
                "UPDATE feedback SET status = ?, batch_id = COALESCE(?, batch_id) WHERE id = ?",
                [(TrainingStatus(status).value, batch_id, feedback_id) for feedback_id in ids],
            )

    def close(self) -> None:
        with self._lock:
            self._db.close()
