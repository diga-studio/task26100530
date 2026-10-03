"""VaultStream 增量同步清单库的自动化测试。

运行：python3 -m unittest discover -s tests -v
"""

import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from vaultstream import (
    ChangeRecord,
    CheckpointTooOldError,
    CompactFloorError,
    CursorExpiredError,
    FileState,
    InvalidCheckpointError,
    InvalidCursorError,
    VaultStore,
)
from vaultstream.store import PAGE_SIZE


class FakeClock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


class RefLog:
    """测试侧独立维护的完整提交日志，用于全量回放核对净差异。"""

    def __init__(self):
        self.ops = []  # [(seq, file_id, FileState|None)]

    def commit(self, store, file_id, state):
        seq = store.commit(file_id, state)
        self.ops.append((seq, file_id, state))
        return seq

    def diff(self, c, h):
        """独立全量回放：文件集合在 C 与 H 两时刻的净差异。"""
        at_c = {}
        at_h = {}
        last_seq = {}
        for seq, fid, state in self.ops:
            if seq > h:
                break
            at_h[fid] = state
            last_seq[fid] = seq
            if seq <= c:
                at_c[fid] = state
        expected = []
        for fid, k in last_seq.items():
            sc = at_c.get(fid)
            sh = at_h.get(fid)
            if sc == sh:
                continue
            if sh is None:
                expected.append(ChangeRecord(fid, k, True))
            else:
                expected.append(
                    ChangeRecord(fid, k, False, sh.name, sh.content_hash)
                )
        expected.sort(key=lambda r: r.seq)
        return expected


def collect_all(store, start):
    """翻完一个会话，校验分页约束，返回 (records, checkpoint)。"""
    records = []
    page = store.changes(start)
    while True:
        if page.cursor is not None:
            # 非末页必须满 PAGE_SIZE，且不能带 checkpoint
            assert len(page.records) == PAGE_SIZE, len(page.records)
            assert page.checkpoint is None
            records.extend(page.records)
            page = store.changes(page.cursor)
        else:
            assert page.checkpoint is not None
            records.extend(page.records)
            return records, page.checkpoint


class VaultStreamTestCase(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(self.path)
        self.clock = FakeClock()
        self.store = VaultStore(self.path, clock=self.clock)
        self.log = RefLog()

    def tearDown(self):
        self.store.close()
        if os.path.exists(self.path):
            os.unlink(self.path)

    def raw_rows(self, where=""):
        conn = sqlite3.connect(self.path)
        try:
            return conn.execute(
                f"SELECT seq, file_id, name, content_hash FROM commits {where}"
            ).fetchall()
        finally:
            conn.close()


class TestPagingAndIsolation(VaultStreamTestCase):
    def test_multi_page_with_interleaved_commits_and_cursor_retry(self):
        # 250 个文件产生 250 条净变化 -> 3 页（100/100/50）
        for i in range(250):
            self.log.commit(self.store, f"f{i:03d}", FileState(f"n{i}", f"h{i}"))
        h_pinned = 250

        p1 = self.store.changes(0)
        self.assertEqual(len(p1.records), PAGE_SIZE)
        self.assertIsNotNone(p1.cursor)
        self.assertIsNone(p1.checkpoint)

        # 翻页间隙：覆盖尚未输出的文件，并插入全新文件（序号均 > H）
        for i in range(150, 250):
            self.log.commit(self.store, f"f{i:03d}", FileState(f"n{i}v2", f"h{i}v2"))
        for i in range(250, 260):
            self.log.commit(self.store, f"f{i:03d}", FileState(f"n{i}", f"h{i}"))

        # 同一 cursor 重复调用，结果必须完全一致
        p2a = self.store.changes(p1.cursor)
        p2b = self.store.changes(p1.cursor)
        self.assertEqual(p2a, p2b)
        self.assertEqual(len(p2a.records), PAGE_SIZE)

        p3 = self.store.changes(p2a.cursor)
        self.assertIsNone(p3.cursor)
        self.assertEqual(p3.checkpoint, h_pinned)
        self.assertEqual(len(p3.records), 50)

        # 已读完的会话，用旧 cursor 重放仍得到相同的末页
        p3_again = self.store.changes(p2a.cursor)
        self.assertEqual(p3_again, p3)

        # 拼接所有页 == 独立回放的 (0, 250] 净差异；H 之后的提交不可见
        all_records = list(p1.records) + list(p2a.records) + list(p3.records)
        self.assertEqual(all_records, self.log.diff(0, h_pinned))
        for rec in all_records:
            self.assertFalse(rec.file_id >= "f150" and rec.content_hash.endswith("v2"))

        # 下一次从 H 同步，取得 H 之后的全部后续变化
        max_seq = self.log.ops[-1][0]
        records, checkpoint = collect_all(self.store, h_pinned)
        self.assertEqual(checkpoint, max_seq)
        self.assertEqual(records, self.log.diff(h_pinned, max_seq))


class TestNetDiffSemantics(VaultStreamTestCase):
    def test_create_delete_modify_revert_delete_restore(self):
        st = FileState
        self.log.commit(self.store, "W", st("w", "1"))     # seq1：C 时存在
        self.log.commit(self.store, "Y", st("y", "orig"))  # seq2：C 时存在
        self.log.commit(self.store, "Z", st("z", "keep"))  # seq3：C 时存在
        self.log.commit(self.store, "U", st("u", "gone"))  # seq4：C 时存在
        self.log.commit(self.store, "V", st("v", "old"))   # seq5：C 时存在
        c = 5

        self.log.commit(self.store, "X", st("x", "1"))     # 区间内创建
        self.log.commit(self.store, "X", None)             # 又删除 -> 净无变化
        self.log.commit(self.store, "Y", st("y", "tmp"))   # 修改
        self.log.commit(self.store, "Y", st("y", "orig"))  # 改回 -> 净无变化
        self.log.commit(self.store, "Z", None)             # 删除
        self.log.commit(self.store, "Z", st("z", "keep"))  # 同状态恢复 -> 净无变化
        self.log.commit(self.store, "U", None)             # 删除不恢复 -> 净删除
        self.log.commit(self.store, "V", st("v", "new"))   # 修改 -> 净更新
        self.log.commit(self.store, "W", st("w", "1"))     # 同状态重提 -> 净无变化
        h = self.log.ops[-1][0]

        records, checkpoint = collect_all(self.store, c)
        self.assertEqual(checkpoint, h)
        # 与独立完整状态回放逐条核对（不只核对数量）
        self.assertEqual(records, self.log.diff(c, h))
        by_id = {r.file_id: r for r in records}
        self.assertEqual(set(by_id), {"U", "V"})
        self.assertTrue(by_id["U"].deleted)
        self.assertEqual((by_id["V"].name, by_id["V"].content_hash), ("v", "new"))


class TestCompactAndExpiry(VaultStreamTestCase):
    def _populate(self):
        st = FileState
        # 3 个文件各 3 个版本，序号交错
        versions = [("f1", "a"), ("f2", "a"), ("f3", "a"),
                    ("f1", "b"), ("f2", "b"), ("f3", "b"),
                    ("f1", "c"), ("f2", "c"), ("f3", "c")]
        for fid, tag in versions:
            self.log.commit(self.store, fid, st(f"{fid}-{tag}", f"h-{fid}-{tag}"))

    def test_floor_advance_rejects_new_but_keeps_old_session(self):
        self._populate()  # seq 1..9
        c = 4
        expected_diff = self.log.diff(c, 9)

        # 开启会话（C=4, H=9），不足一页，直接返回末页
        p1 = self.store.changes(c)
        self.assertEqual(p1.checkpoint, 9)
        self.assertIsNone(p1.cursor)
        self.assertEqual(list(p1.records), expected_diff)

        # 活跃会话期间把下限推进到其 C 之后
        self.store.compact(6)
        # 新会话：低于下限被拒绝；等于下限允许
        with self.assertRaises(CheckpointTooOldError):
            self.store.changes(5)
        with self.assertRaises(CheckpointTooOldError):
            self.store.changes(c)
        self.assertEqual(self.store.changes(6).checkpoint, 9)

    def test_old_session_survives_floor_and_expires(self):
        self._populate()  # seq 1..9
        # 制造超过一页的变化，验证旧游标翻页
        for i in range(150):
            self.log.commit(self.store, f"g{i:03d}", FileState(f"g{i}", f"gh{i}"))
        h = self.log.ops[-1][0]  # 159
        expected = self.log.diff(4, h)

        p1 = self.store.changes(4)
        self.assertEqual(len(p1.records), PAGE_SIZE)
        cursor = p1.cursor

        # 推进下限越过会话起点 C=4；R 被活跃会话钉在 4
        self.store.compact(6)
        with self.assertRaises(CheckpointTooOldError):
            self.store.changes(4)
        # 旧会话翻页不受影响，且重放一致
        p2a = self.store.changes(cursor)
        p2b = self.store.changes(cursor)
        self.assertEqual(p2a, p2b)
        rest = list(p2a.records)
        page = p2a
        while page.cursor is not None:
            page = self.store.changes(page.cursor)
            rest.extend(page.records)
        self.assertEqual(page.checkpoint, h)
        self.assertEqual(list(p1.records) + rest, expected)

        # R=4：每个文件至多保留一个 seq<=4 的版本
        rows = self.raw_rows("WHERE seq <= 4")
        per_file = {}
        for _, fid, _, _ in rows:
            per_file[fid] = per_file.get(fid, 0) + 1
        self.assertTrue(all(v <= 1 for v in per_file.values()))
        # f1 的 seq1 必须已被删除（保留 seq4 作为基线）
        seqs = [r[0] for r in self.raw_rows()]
        self.assertNotIn(1, seqs)
        self.assertIn(4, seqs)

        # 推进时钟使会话过期：明确报错，不能悄悄开新会话
        self.clock.advance(601)
        with self.assertRaises(CursorExpiredError):
            self.store.changes(cursor)

        # 过期后再清理：R = 当前下限 6，历史进一步收缩
        self.store.compact(6)
        rows = self.raw_rows("WHERE seq <= 6")
        per_file = {}
        for _, fid, _, _ in rows:
            per_file[fid] = per_file.get(fid, 0) + 1
        self.assertTrue(all(v <= 1 for v in per_file.values()))
        seqs = [r[0] for r in self.raw_rows()]
        self.assertNotIn(2, seqs)  # f2 的 seq2 被 seq5 取代
        self.assertNotIn(3, seqs)  # f3 的 seq3 被 seq6 取代
        self.assertIn(5, seqs)
        self.assertIn(6, seqs)
        # 清理后，从下限开启的新会话仍与独立回放一致
        records, checkpoint = collect_all(self.store, 6)
        self.assertEqual(checkpoint, h)
        self.assertEqual(records, self.log.diff(6, h))

    def test_compact_validation(self):
        self.log.commit(self.store, "a", FileState("a", "1"))
        with self.assertRaises(CompactFloorError):
            self.store.compact(5)  # 超过最大序号
        self.store.compact(1)
        with self.assertRaises(CompactFloorError):
            self.store.compact(0)  # 下限回退


class TestPersistenceAndMisc(VaultStreamTestCase):
    def test_cursor_survives_reopen(self):
        for i in range(150):
            self.log.commit(self.store, f"f{i:03d}", FileState(f"n{i}", f"h{i}"))
        p1 = self.store.changes(0)
        cursor = p1.cursor
        self.store.close()

        # 重新打开库：未过期游标仍有效
        self.store = VaultStore(self.path, clock=self.clock)
        p2 = self.store.changes(cursor)
        self.assertEqual(len(p2.records), 50)
        self.assertEqual(p2.checkpoint, 150)
        self.assertEqual(
            list(p1.records) + list(p2.records), self.log.diff(0, 150)
        )

        # 过期后明确报错
        self.clock.advance(601)
        with self.assertRaises(CursorExpiredError):
            self.store.changes(cursor)

    def test_empty_and_edge_cases(self):
        # 空库：checkpoint 0 表示空库，直接返回末页
        page = self.store.changes(0)
        self.assertEqual(page.records, ())
        self.assertEqual(page.checkpoint, 0)
        self.assertIsNone(page.cursor)

        # 相同状态重复提交也产生严格递增序号
        s1 = self.store.commit("a", FileState("a", "1"))
        s2 = self.store.commit("a", FileState("a", "1"))
        s3 = self.store.commit("a", None)
        s4 = self.store.commit("a", FileState("a", "1"))  # 删除后同 ID 恢复
        self.assertTrue(s1 < s2 < s3 < s4)

        # 删除一个本就不存在的文件：两端均不存在，不输出
        self.store.commit("ghost", None)
        records, checkpoint = collect_all(self.store, 0)
        self.assertEqual(records, [ChangeRecord("a", s4, False, "a", "1")])

        # 非法参数
        with self.assertRaises(InvalidCheckpointError):
            self.store.changes(10**9)
        with self.assertRaises(InvalidCheckpointError):
            self.store.changes(-1)
        with self.assertRaises(InvalidCursorError):
            self.store.changes("not-a-cursor")
        with self.assertRaises(InvalidCursorError):
            self.store.changes("deadbeef:3")


if __name__ == "__main__":
    unittest.main()
