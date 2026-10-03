"""VaultStream incremental sync manifest library (stdlib + SQLite)."""

from __future__ import annotations

import sqlite3
import time
import uuid
from dataclasses import dataclass
from typing import Callable, List, Optional, Union

PAGE_SIZE = 100
SESSION_TTL_SECONDS = 600.0


@dataclass(frozen=True)
class Change:
    file_id: str
    name: Optional[str]
    content_hash: Optional[str]
    deleted: bool


@dataclass(frozen=True)
class Page:
    records: List[Change]
    cursor: Optional[str]
    checkpoint: Optional[int]


class VaultSyncError(Exception):
    pass


class CheckpointTooOldError(VaultSyncError):
    pass


class CursorExpiredError(VaultSyncError):
    pass


class InvalidCursorError(VaultSyncError):
    pass


class InvalidCheckpointError(VaultSyncError):
    pass


class VaultSync:
    def __init__(self, path: str, clock: Callable[[], float] = time.time) -> None:
        self._clock = clock
        self._db = sqlite3.connect(path)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS commits (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id TEXT NOT NULL,
                name TEXT,
                content_hash TEXT,
                deleted INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_commits_file ON commits(file_id, seq);
            CREATE TABLE IF NOT EXISTS sessions (
                sid TEXT PRIMARY KEY,
                c INTEGER NOT NULL,
                h INTEGER NOT NULL,
                expires_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT OR IGNORE INTO meta(key, value) VALUES ('floor', '0');
            """
        )
        self._db.commit()

    def close(self) -> None:
        self._db.close()

    def _max_seq(self) -> int:
        row = self._db.execute("SELECT COALESCE(MAX(seq), 0) FROM commits").fetchone()
        return int(row[0])

    def _floor(self) -> int:
        row = self._db.execute("SELECT value FROM meta WHERE key='floor'").fetchone()
        return int(row[0])

    def commit(self, file_id: str, state: Optional[dict]) -> int:
        if state is None:
            name, content_hash, deleted = None, None, 1
        else:
            name = state["name"]
            content_hash = state["content_hash"]
            deleted = 0
        cur = self._db.execute(
            "INSERT INTO commits(file_id, name, content_hash, deleted) VALUES (?,?,?,?)",
            (file_id, name, content_hash, deleted),
        )
        self._db.commit()
        return int(cur.lastrowid)

    def changes(self, token: Union[int, str]) -> Page:
        if isinstance(token, str):
            sid, position = self._parse_cursor(token)
            row = self._db.execute(
                "SELECT c, h, expires_at FROM sessions WHERE sid=?", (sid,)
            ).fetchone()
            if row is None:
                raise InvalidCursorError("unknown cursor")
            if self._clock() > float(row[2]):
                raise CursorExpiredError("cursor session expired")
            c, h = int(row[0]), int(row[1])
        else:
            c = token
            sid = self._open_session(c)
            h = self._max_seq()
            position = c
        return self._page(sid, c, h, position)

    def _open_session(self, checkpoint: int) -> str:
        if isinstance(checkpoint, bool) or not isinstance(checkpoint, int):
            raise InvalidCheckpointError("checkpoint must be an int")
        max_seq = self._max_seq()
        if checkpoint < 0 or checkpoint > max_seq:
            raise InvalidCheckpointError(f"checkpoint {checkpoint} out of range")
        if checkpoint < self._floor():
            raise CheckpointTooOldError(
                f"checkpoint {checkpoint} below floor {self._floor()}"
            )
        sid = uuid.uuid4().hex
        expires_at = self._clock() + SESSION_TTL_SECONDS
        self._db.execute(
            "INSERT INTO sessions(sid, c, h, expires_at) VALUES (?,?,?,?)",
            (sid, checkpoint, max_seq, expires_at),
        )
        self._db.commit()
        return sid

    def _parse_cursor(self, cursor: str) -> tuple:
        try:
            sid, pos = cursor.rsplit(":", 1)
            return sid, int(pos)
        except (ValueError, AttributeError):
            raise InvalidCursorError("malformed cursor") from None

    def _baseline(self, file_id: str, c: int) -> Optional[tuple]:
        return self._db.execute(
            "SELECT name, content_hash, deleted FROM commits "
            "WHERE file_id=? AND seq<=? ORDER BY seq DESC LIMIT 1",
            (file_id, c),
        ).fetchone()

    def _page(self, sid, c, h, position):
        records: List[Change] = []
        pos = position
        exhausted = False
        while len(records) < PAGE_SIZE and not exhausted:
            rows = self._db.execute(
                "SELECT file_id, MAX(seq) AS k, name, content_hash, deleted "
                "FROM commits WHERE seq>? AND seq<=? GROUP BY file_id "
                "ORDER BY k LIMIT 128",
                (pos, h),
            ).fetchall()
            examined_all = True
            for file_id, k, name, content_hash, deleted in rows:
                if len(records) >= PAGE_SIZE:
                    examined_all = False
                    break
                pos = int(k)
                base = self._baseline(file_id, c)
                base_state = None
                if base is not None and not base[2]:
                    base_state = (base[0], base[1])
                head_state = None if deleted else (name, content_hash)
                if base_state == head_state:
                    continue
                if head_state is None:
                    records.append(Change(file_id, None, None, True))
                else:
                    records.append(Change(file_id, head_state[0], head_state[1], False))
            if examined_all and len(rows) < 128:
                exhausted = True
        if not exhausted:
            more = self._db.execute(
                "SELECT 1 FROM commits WHERE seq>? AND seq<=? LIMIT 1", (pos, h)
            ).fetchone()
            exhausted = more is None
        if exhausted:
            return Page(records, None, h)
        return Page(records, f"{sid}:{pos}", None)

    def compact(self, floor: int) -> None:
        current_floor = self._floor()
        max_seq = self._max_seq()
        if floor < current_floor or floor > max_seq:
            raise ValueError(f"invalid compact floor {floor}")
        now = self._clock()
        row = self._db.execute(
            "SELECT MIN(c) FROM sessions WHERE expires_at > ?", (now,)
        ).fetchone()
        r = floor if row[0] is None else min(floor, int(row[0]))
        self._db.execute("UPDATE meta SET value=? WHERE key='floor'", (str(floor),))
        self._db.execute(
            "DELETE FROM commits WHERE seq <= ? AND seq NOT IN "
            "(SELECT MAX(seq) FROM commits WHERE seq <= ? GROUP BY file_id)",
            (r, r),
        )
        self._db.commit()
