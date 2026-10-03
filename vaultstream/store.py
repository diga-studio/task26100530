"""VaultStream 增量同步清单库：基于 SQLite 的提交历史与分页同步。"""

from __future__ import annotations

import sqlite3
import time
import uuid
from typing import Callable, List, Optional, Tuple, Union

from .errors import (
    CheckpointTooOldError,
    CompactFloorError,
    CursorExpiredError,
    InvalidCheckpointError,
    InvalidCursorError,
)
from .types import ChangeRecord, ChangesPage, FileState

PAGE_SIZE = 100
DEFAULT_SESSION_TTL = 600.0

_SCHEMA = """
CREATE TABLE IF NOT EXISTS commits (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id TEXT NOT NULL,
    name TEXT,
    content_hash TEXT
);
CREATE INDEX IF NOT EXISTS idx_commits_file_seq ON commits(file_id, seq);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY,
    c INTEGER NOT NULL,
    h INTEGER NOT NULL,
    created_at REAL NOT NULL
);
"""


class VaultStore:
    """增量同步清单库。

    每个公开方法各自开启并提交一个短事务；不跨调用保持事务、
    读快照或数据库锁。会话视图完全由 (C, H, 翻页位置) 与历史版本表推导，
    会话行持久化在 SQLite 中，关闭重开后未过期游标仍然有效。
    """

    def __init__(
        self,
        path: str,
        *,
        clock: Callable[[], float] = time.time,
        session_ttl: float = DEFAULT_SESSION_TTL,
    ) -> None:
        self._path = path
        self._clock = clock
        self._session_ttl = session_ttl
        self._conn = sqlite3.connect(path)
        self._init_schema()

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.executescript(_SCHEMA)
            self._conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES ('floor', 0)"
            )

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "VaultStore":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ---- 内部查询 ----

    def _floor(self) -> int:
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key = 'floor'"
        ).fetchone()
        return int(row[0])

    def _max_seq(self) -> int:
        row = self._conn.execute("SELECT COALESCE(MAX(seq), 0) FROM commits").fetchone()
        return int(row[0])

    def _state_at(self, file_id: str, seq: int) -> Optional[Tuple[Optional[str], Optional[str]]]:
        """file_id 在 seq 时刻（含）的最后一次提交；(name, content_hash)，name 为 None 表示删除。"""
        return self._conn.execute(
            "SELECT name, content_hash FROM commits "
            "WHERE file_id = ? AND seq <= ? ORDER BY seq DESC LIMIT 1",
            (file_id, seq),
        ).fetchone()

    # ---- commit ----

    def commit(self, file_id: str, state: Optional[FileState]) -> int:
        """提交一个文件的完整状态（FileState）或删除标记（None）。

        返回严格递增的全局提交序号。即使状态相同也产生新序号。
        """
        if state is None:
            name, content_hash = None, None
        else:
            name, content_hash = state.name, state.content_hash
        with self._conn:
            cur = self._conn.execute(
                "INSERT INTO commits(file_id, name, content_hash) VALUES (?, ?, ?)",
                (file_id, name, content_hash),
            )
        return int(cur.lastrowid)

    # ---- changes ----

    def changes(self, checkpoint_or_cursor: Union[int, str]) -> ChangesPage:
        """传 int checkpoint 开启会话；传 str cursor 继续翻页。"""
        if isinstance(checkpoint_or_cursor, bool):
            raise InvalidCursorError(f"无法识别的参数: {checkpoint_or_cursor!r}")
        if isinstance(checkpoint_or_cursor, int):
            return self._open_session(checkpoint_or_cursor)
        if isinstance(checkpoint_or_cursor, str):
            return self._resume_session(checkpoint_or_cursor)
        raise InvalidCursorError(f"无法识别的参数: {checkpoint_or_cursor!r}")

    def _open_session(self, checkpoint: int) -> ChangesPage:
        max_seq = self._max_seq()
        if checkpoint < 0 or checkpoint > max_seq:
            raise InvalidCheckpointError(
                f"checkpoint {checkpoint} 超出合法范围 [0, {max_seq}]"
            )
        floor = self._floor()
        if checkpoint < floor:
            raise CheckpointTooOldError(
                f"checkpoint {checkpoint} 低于当前下限 {floor}"
            )
        token = uuid.uuid4().hex
        with self._conn:
            self._conn.execute(
                "INSERT INTO sessions(token, c, h, created_at) VALUES (?, ?, ?, ?)",
                (token, checkpoint, max_seq, self._clock()),
            )
        return self._page(token, checkpoint, max_seq, checkpoint)

    def _resume_session(self, cursor: str) -> ChangesPage:
        token, sep, pos_text = cursor.rpartition(":")
        if not sep:
            raise InvalidCursorError(f"非法游标: {cursor!r}")
        try:
            position = int(pos_text)
        except ValueError:
            raise InvalidCursorError(f"非法游标: {cursor!r}") from None
        row = self._conn.execute(
            "SELECT c, h, created_at FROM sessions WHERE token = ?", (token,)
        ).fetchone()
        if row is None:
            raise InvalidCursorError(f"未知游标: {cursor!r}")
        c, h, created_at = int(row[0]), int(row[1]), float(row[2])
        if self._clock() - created_at >= self._session_ttl:
            raise CursorExpiredError(
                f"会话已过期（开启于 {created_at}，有效期 {self._session_ttl} 秒）"
            )
        return self._page(token, c, h, position)

    def _page(self, token: str, c: int, h: int, position: int) -> ChangesPage:
        """从位置 position（不含）之后取一页净变化。

        候选文件按“截至 H 的最后一次提交序号”升序枚举；对每个候选文件
        计算 C 与 H 两端的净差异，无差异的不产出记录但位置照常推进。
        """
        records: List[ChangeRecord] = []
        pos = position
        while len(records) < PAGE_SIZE:
            batch = self._conn.execute(
                "SELECT file_id, MAX(seq) AS k FROM commits "
                "WHERE seq > ? AND seq <= ? GROUP BY file_id ORDER BY k LIMIT ?",
                (pos, h, PAGE_SIZE),
            ).fetchall()
            if not batch:
                break
            for file_id, k in batch:
                pos = int(k)
                record = self._diff_record(file_id, c, pos)
                if record is not None:
                    records.append(record)
                    if len(records) == PAGE_SIZE:
                        break
            if len(batch) < PAGE_SIZE:
                break
        if len(records) < PAGE_SIZE:
            # 候选已耗尽：末页。
            return ChangesPage(tuple(records), cursor=None, checkpoint=h)
        # 已集满一页；探测其后是否还有候选，没有则本页即末页。
        more = self._conn.execute(
            "SELECT 1 FROM commits WHERE seq > ? AND seq <= ? LIMIT 1",
            (pos, h),
        ).fetchone()
        if more is None:
            return ChangesPage(tuple(records), cursor=None, checkpoint=h)
        return ChangesPage(
            tuple(records), cursor=f"{token}:{pos}", checkpoint=None
        )

    def _diff_record(self, file_id: str, c: int, k: int) -> Optional[ChangeRecord]:
        """计算 file_id 在 C 与 H（其最后提交为 k）之间的净变化。"""
        head = self._conn.execute(
            "SELECT name, content_hash FROM commits WHERE seq = ?", (k,)
        ).fetchone()
        base = self._state_at(file_id, c)
        exists_h = head[0] is not None
        exists_c = base is not None and base[0] is not None
        if exists_h:
            if not exists_c or (base[0], base[1]) != (head[0], head[1]):
                return ChangeRecord(
                    file_id=file_id, seq=k, deleted=False,
                    name=head[0], content_hash=head[1],
                )
            return None
        if exists_c:
            return ChangeRecord(file_id=file_id, seq=k, deleted=True)
        return None

    # ---- compact ----

    def compact(self, floor: int) -> None:
        """把允许开启新会话的最小 checkpoint 推进到 floor 并清理历史。

        调用方保证 floor 单调不减且不超过当前最大提交序号。
        令 R 为 floor 与所有未过期会话起点 C 的最小值（无有效会话时 R=floor）；
        清理后每个文件至多保留一个序号不大于 R 的历史版本。
        """
        now = self._clock()
        with self._conn:
            current_floor = self._floor()
            max_seq = self._max_seq()
            if floor < current_floor or floor > max_seq:
                raise CompactFloorError(
                    f"下限 {floor} 非法：当前下限 {current_floor}，最大序号 {max_seq}"
                )
            self._conn.execute(
                "UPDATE meta SET value = ? WHERE key = 'floor'", (floor,)
            )
            row = self._conn.execute(
                "SELECT MIN(c) FROM sessions WHERE created_at > ?",
                (now - self._session_ttl,),
            ).fetchone()
            r = floor if row[0] is None else min(floor, int(row[0]))
            self._conn.execute(
                "DELETE FROM commits WHERE seq <= ? AND seq NOT IN ("
                "  SELECT MAX(seq) FROM commits WHERE seq <= ? GROUP BY file_id"
                ")",
                (r, r),
            )
