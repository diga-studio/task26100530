"""Tests for vaultstream."""

import os
import tempfile
import unittest

from vaultstream import (
    Change,
    CheckpointTooOldError,
    CursorExpiredError,
    InvalidCursorError,
    VaultSync,
)


class FakeClock:
    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def state(name, content_hash):
    return {"name": name, "content_hash": content_hash}


class Model:
    """Independent full-state replay of the commit history."""

    def __init__(self):
        self.history = []  # (seq, file_id, state-or-None)

    def commit(self, seq, file_id, st):
        self.history.append((seq, file_id, st))

    def state_at(self, seq):
        files = {}
        for s, fid, st in self.history:
            if s > seq:
                break
            if st is None:
                files.pop(fid, None)
            else:
                files[fid] = (st["name"], st["content_hash"])
        return files

    def expected_diff(self, c, h):
        at_c = self.state_at(c)
        at_h = self.state_at(h)
        touched = {}
        for s, fid, st in self.history:
            if c < s <= h:
                touched[fid] = s  # last commit seq <= h is the max in (c, h]
        out = []
        for fid, k in touched.items():
            before = at_c.get(fid)
            after = at_h.get(fid)
            if before == after:
                continue
            if after is None:
                out.append((k, Change(fid, None, None, True)))
            else:
                out.append((k, Change(fid, after[0], after[1], False)))
        out.sort(key=lambda item: item[0])
        return [rec for _, rec in out]


class VaultSyncTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "vault.db")
        self.clock = FakeClock()
        self.vs = VaultSync(self.path, clock=self.clock)
        self.model = Model()

    def tearDown(self):
        self.vs.close()
        self.tmp.cleanup()

    def commit(self, file_id, st):
        seq = self.vs.commit(file_id, st)
        self.model.commit(seq, file_id, st)
        return seq

    def collect(self, checkpoint):
        records = []
        page = self.vs.changes(checkpoint)
        while True:
            records.extend(page.records)
            if page.cursor is None:
                return records, page.checkpoint
            page = self.vs.changes(page.cursor)


class MultiPageTests(VaultSyncTestCase):
    def test_multipage_interleaved_and_replay(self):
        for i in range(30):
            self.commit(f"f{i}", state(f"f{i}.txt", "v1"))
        c = self.vs._max_seq()
        for i in range(30):
            self.commit(f"f{i}", state(f"f{i}.txt", "v2"))
        for i in range(220):
            self.commit(f"n{i}", state(f"n{i}.txt", "new"))

        h_expected = self.vs._max_seq()
        page1 = self.vs.changes(c)
        self.assertEqual(len(page1.records), 100)
        self.assertIsNotNone(page1.cursor)
        self.assertIsNone(page1.checkpoint)

        # Interleave: overwrite not-yet-output files and add brand new commits.
        for i in range(150, 220):
            self.commit(f"n{i}", state(f"n{i}.txt", "v2"))
        for i in range(10):
            self.commit(f"x{i}", state(f"x{i}.txt", "x"))

        page2 = self.vs.changes(page1.cursor)
        page2_retry = self.vs.changes(page1.cursor)
        self.assertEqual(page2, page2_retry)
        self.assertEqual(len(page2.records), 100)

        page3 = self.vs.changes(page2.cursor)
        self.assertIsNone(page3.cursor)
        h = page3.checkpoint
        self.assertEqual(h, h_expected)  # H pinned at session open
        self.assertEqual(len(page3.records), 50)

        # Retrying an already-consumed cursor still yields the same page.
        self.assertEqual(self.vs.changes(page2.cursor), page3)

        got = page1.records + page2.records + page3.records
        self.assertEqual(got, self.model.expected_diff(c, h))

        # Next sync from H picks up exactly the interleaved commits.
        later, end = self.collect(h)
        self.assertEqual(later, self.model.expected_diff(h, end))
        self.assertEqual(end, self.vs._max_seq())


class NettingTests(VaultSyncTestCase):
    def test_create_delete_revert_restore_net_out(self):
        self.commit("a", state("a.txt", "v1"))
        self.commit("b", state("b.txt", "v1"))
        self.commit("c", state("c.txt", "v1"))
        c = self.vs._max_seq()

        self.commit("d", state("d.txt", "tmp"))
        self.commit("d", None)  # created then deleted inside interval
        self.commit("a", state("a.txt", "v2"))
        self.commit("a", state("a.txt", "v1"))  # modified then reverted
        self.commit("b", None)
        self.commit("b", state("b.txt", "v1"))  # deleted then restored identically
        self.commit("c", state("c.txt", "v2"))  # real change
        self.commit("e", state("e.txt", "new"))  # real creation

        records, checkpoint = self.collect(c)
        self.assertEqual(records, self.model.expected_diff(c, checkpoint))
        self.assertEqual(
            records,
            [
                Change("c", "c.txt", "v2", False),
                Change("e", "e.txt", "new", False),
            ],
        )

    def test_same_state_commit_still_bumps_seq(self):
        s1 = self.commit("a", state("a.txt", "v1"))
        s2 = self.commit("a", state("a.txt", "v1"))
        records, checkpoint = self.collect(s1)
        self.assertEqual(records, [])  # no net change between s1 and s2
        self.assertEqual(checkpoint, s2)
        s3 = self.commit("a", None)
        self.assertLess(s1, s2)
        self.assertLess(s2, s3)
        records, _ = self.collect(s2)
        self.assertEqual(records, [Change("a", None, None, True)])

    def test_empty_library(self):
        page = self.vs.changes(0)
        self.assertEqual(page.records, [])
        self.assertIsNone(page.cursor)
        self.assertEqual(page.checkpoint, 0)

    def test_invalid_cursor(self):
        with self.assertRaises(InvalidCursorError):
            self.vs.changes("not-a-cursor")
        with self.assertRaises(InvalidCursorError):
            self.vs.changes("deadbeef:5")


class CompactTests(VaultSyncTestCase):
    def test_compact_with_active_session_and_expiry(self):
        self.commit("p", state("p.txt", "v1"))
        self.commit("q", state("q.txt", "v1"))
        self.commit("r", state("r.txt", "v1"))
        c1 = self.vs._max_seq()
        for version in ("v2", "v3", "v4"):
            for fid in ("p", "q", "r"):
                self.commit(fid, state(f"{fid}.txt", version))

        page1 = self.vs.changes(c1)
        self.assertEqual(len(page1.records), 3)
        self.assertIsNone(page1.cursor)  # single-page session
        h = page1.checkpoint

        # A multi-page session to keep a live cursor.
        for i in range(150):
            self.commit(f"m{i}", state(f"m{i}.txt", "new"))
        h2 = self.vs._max_seq()
        mp1 = self.vs.changes(c1)
        self.assertEqual(len(mp1.records), 100)
        live_cursor = mp1.cursor
        self.assertIsNotNone(live_cursor)

        f = c1 + 2  # advance floor beyond the active session's C
        self.vs.compact(f)

        # New sessions below the floor are rejected; at/above the floor work.
        with self.assertRaises(CheckpointTooOldError):
            self.vs.changes(c1)
        ok_page = self.vs.changes(f)
        self.assertIsNotNone(ok_page)

        # The old session still pages and replays identically.
        mp2 = self.vs.changes(live_cursor)
        mp2_retry = self.vs.changes(live_cursor)
        self.assertEqual(mp2, mp2_retry)
        got = mp1.records + mp2.records
        self.assertEqual(got, self.model.expected_diff(c1, h2))
        self.assertEqual(mp2.checkpoint, h2)

        # Unexpired cursors survive close/reopen.
        self.vs.close()
        self.vs = VaultSync(self.path, clock=self.clock)
        self.assertEqual(self.vs.changes(live_cursor), mp2)

        # Expire the session, then compact again with no live sessions.
        self.clock.advance(601)
        with self.assertRaises(CursorExpiredError):
            self.vs.changes(live_cursor)

        f2 = self.vs._max_seq()
        self.vs.compact(f2)

        rows = self.vs._db.execute(
            "SELECT file_id, seq, name, content_hash, deleted FROM commits ORDER BY seq"
        ).fetchall()
        # R == f2 == max seq, so only one baseline row per file may remain.
        per_file = {}
        for fid, seq, name, content_hash, deleted in rows:
            self.assertNotIn(fid, per_file)
            per_file[fid] = (seq, name, content_hash, deleted)
        self.assertEqual(len(rows), len(per_file))
        # Each retained baseline matches the model's state at R.
        at_r = self.model.state_at(f2)
        for fid, (seq, name, content_hash, deleted) in per_file.items():
            if deleted:
                self.assertNotIn(fid, at_r)
            else:
                self.assertEqual(at_r[fid], (name, content_hash))
        for fid, st in at_r.items():
            self.assertIn(fid, per_file)

        # Sync from the new floor still works and is empty at the head.
        records, checkpoint = self.collect(f2)
        self.assertEqual(records, [])
        self.assertEqual(checkpoint, f2)

    def test_compact_preserves_allowed_ranges(self):
        for i in range(5):
            self.commit(f"k{i}", state(f"k{i}.txt", "v1"))
        c = self.vs._max_seq()
        for i in range(5):
            self.commit(f"k{i}", state(f"k{i}.txt", "v2"))
        self.commit("k0", None)
        head = self.vs._max_seq()
        self.vs.compact(c)
        records, checkpoint = self.collect(c)
        self.assertEqual(records, self.model.expected_diff(c, head))
        self.assertEqual(checkpoint, head)


if __name__ == "__main__":
    unittest.main()
