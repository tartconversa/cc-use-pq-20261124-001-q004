from __future__ import annotations

from datetime import datetime, timedelta, timezone
from tempfile import TemporaryDirectory
import unittest
from pathlib import Path

from guizhou_stay.errors import DomainError
from guizhou_stay import quota
from guizhou_stay.storage import Store

AT = datetime(2026, 11, 25, 1, tzinfo=timezone.utc)


class QuotaTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name) / "quota.sqlite3")
        self.source = "partner-map"

    def allowance(self, pages=3, restored=AT + timedelta(hours=2)):
        return quota.QuotaAllowance(source=self.source, read_pages=pages, restored_at=restored)

    def receipt(self, read_id, *, remaining, version=1, outcome="success",
                receipt_id=None, restored=AT + timedelta(hours=2)):
        return quota.QuotaReceipt(
            receipt_id=receipt_id or "rcp-" + read_id, source=self.source, read_id=read_id,
            outcome=outcome, charged_pages=1, restored_at=restored, remaining=remaining,
            state_version=version)

    def test_receipts_identify_reads_and_replay_does_not_change_balance(self):
        with self.store.transaction(write=True) as db:
            quota.provision(db, self.allowance(), at=AT)
            reserved = quota.reserve(db, self.source, at=AT)
            first = quota.apply_receipt(db, self.receipt(reserved["read_id"], remaining=2), at=AT)
            self.assertFalse(first["replayed"])
            self.assertEqual(first["remaining"], 2)
            # 重送同一回执：幂等返回，余额不重复扣减也不重复增加。
            replay = quota.apply_receipt(db, self.receipt(reserved["read_id"], remaining=2), at=AT)
            self.assertTrue(replay["replayed"])
            self.assertEqual(replay["remaining"], 2)
            self.assertEqual(quota.status(db, self.source, at=AT)["remaining"], 2)

    def test_same_read_cannot_bind_two_different_receipts(self):
        with self.store.transaction(write=True) as db:
            quota.provision(db, self.allowance(), at=AT)
            reserved = quota.reserve(db, self.source, at=AT)
            quota.apply_receipt(db, self.receipt(reserved["read_id"], remaining=2), at=AT)
            other = self.receipt(reserved["read_id"], remaining=2, receipt_id="rcp-other")
            with self.assertRaises(DomainError) as raised:
                quota.apply_receipt(db, other, at=AT)
            self.assertEqual(raised.exception.code, "receipt_changed")

    def test_all_workers_share_recovery_moment_and_cannot_overdraw(self):
        with self.store.transaction(write=True) as db:
            quota.provision(db, self.allowance(pages=1), at=AT)
            quota.reserve(db, self.source, at=AT)
            with self.assertRaises(DomainError) as waiting:
                quota.reserve(db, self.source, at=AT)
            self.assertEqual(waiting.exception.code, "quota_waiting")
            self.assertEqual(waiting.exception.details["restored_at"],
                             (AT + timedelta(hours=2)).isoformat())
            # 恢复时刻之前，任何进程都不能读取。
            with self.assertRaises(DomainError):
                quota.reserve(db, self.source, at=AT + timedelta(hours=1))
        # 到达恢复时刻后额度自动恢复为整轮预算（1 页），恢复结算推进状态版本。
        with self.store.transaction(write=True) as db:
            state = quota.status(db, self.source, at=AT + timedelta(hours=2, seconds=1))
            self.assertEqual(state["remaining"], 1)
            self.assertEqual(state["state_version"], 2)
            recovered = quota.reserve(db, self.source, at=AT + timedelta(hours=2, seconds=1))
            self.assertEqual(recovered["remaining"], 0)

    def test_unacknowledged_read_stays_reserved_and_is_visible(self):
        with self.store.transaction(write=True) as db:
            quota.provision(db, self.allowance(pages=2), at=AT)
            reserved = quota.reserve(db, self.source, at=AT)
            hanging = quota.open_reads(db, self.source)
            self.assertEqual([row["read_id"] for row in hanging], [reserved["read_id"]])
            # 余额已被扣出，即使有剩余额度也不会把同一读取释放给别人。
            self.assertEqual(quota.status(db, self.source, at=AT)["remaining"], 1)
            quota.apply_receipt(db, self.receipt(reserved["read_id"], remaining=1), at=AT)
            self.assertEqual(quota.open_reads(db, self.source), [])

    def test_older_round_receipt_cannot_overwrite_newer_state(self):
        with self.store.transaction(write=True) as db:
            quota.provision(db, self.allowance(pages=1), at=AT)
            reserved = quota.reserve(db, self.source, at=AT)
            quota.apply_receipt(db, self.receipt(reserved["read_id"], remaining=0, version=1), at=AT)
        # 恢复后合作方状态版本推进到 2。
        with self.store.transaction(write=True) as db:
            quota.status(db, self.source, at=AT + timedelta(hours=2, seconds=1))
            stale = self.receipt("read-late", remaining=99, version=1,
                                 restored=AT + timedelta(hours=2, seconds=1))
            with self.assertRaises(DomainError) as raised:
                quota.apply_receipt(db, stale, at=AT + timedelta(hours=2, seconds=1))
            self.assertEqual(raised.exception.code, "receipt_stale")
            self.assertNotEqual(quota.status(db, self.source,
                                             at=AT + timedelta(hours=2, seconds=1))["remaining"], 99)

    def test_partial_failure_receipt_still_charges_and_is_idempotent(self):
        with self.store.transaction(write=True) as db:
            quota.provision(db, self.allowance(pages=3), at=AT)
            reserved = quota.reserve(db, self.source, at=AT)
            # 部分失败的读取同样返回回执并按合作方权威余额入帐。
            receipt = self.receipt(reserved["read_id"], remaining=2,
                                   outcome="partial_failure")
            applied = quota.apply_receipt(db, receipt, at=AT)
            self.assertEqual(applied["outcome"], "partial_failure")
            self.assertEqual(quota.status(db, self.source, at=AT)["remaining"], 2)
            # 重送部分失败回执同样幂等。
            again = quota.apply_receipt(db, receipt, at=AT)
            self.assertTrue(again["replayed"])
            self.assertEqual(quota.status(db, self.source, at=AT)["remaining"], 2)

    def test_cursor_notice_scoped_to_generation_does_not_block_new_session(self):
        with self.store.transaction(write=True) as db:
            quota.provision(db, self.allowance(), at=AT)
            notice = quota.CursorNotice(source=self.source, cursor="p2", state="expired",
                                        expires_at=AT + timedelta(minutes=5),
                                        scope_session="job-a-s1")
            quota.record_cursor_notice(db, notice, at=AT)
            expired = quota.cursor_state(db, self.source, "p2", at=AT,
                                         scope_session="job-a-s1")
            self.assertTrue(expired["expired"])
            # 重开后的新会话查询同名游标：旧通知已被作用域隔离。
            fresh = quota.cursor_state(db, self.source, "p2", at=AT,
                                       scope_session="job-a-s2")
            self.assertFalse(fresh["expired"])
            self.assertTrue(fresh["superseded"])
            quota.supersede_cursors(db, self.source, "job-a-s1", at=AT)
            self.assertTrue(quota.cursor_state(db, self.source, "p2", at=AT,
                                               scope_session="job-a-s1")["superseded"])


if __name__ == "__main__":
    unittest.main()
