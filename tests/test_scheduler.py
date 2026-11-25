from __future__ import annotations

import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from guizhou_stay import quota, scheduler
from guizhou_stay.errors import DomainError
from guizhou_stay.replay import ScriptedReader
from guizhou_stay.storage import Store

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "partner"
AT = datetime(2026, 11, 25, 1, tzinfo=timezone.utc)
SOURCE = "partner-map"


class SchedulerCase(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = Store(self.root / "schedule.sqlite3")
        # 每个测试独立的时钟，避免类级常量在测试方法间共享恢复时刻。
        self.clock = AT

    def portal(self, name="portal", *, quota_pages=5, restored_hours=8, **scenario):
        directory = self.root / name
        shutil.copytree(FIXTURES, directory)
        config = {"source": SOURCE, "quota": quota_pages,
                  "restored_at": (AT + timedelta(hours=restored_hours)).isoformat(),
                  "fail_on": scenario.get("fail_on", {}),
                  "cursor_expiry": scenario.get("cursor_expiry", {}),
                  "notices": scenario.get("notices", []),
                  "revision_swaps": scenario.get("revision_swaps", {})}
        (directory / "scenario.json").write_text(json.dumps(config), encoding="utf-8")
        return ScriptedReader(directory)

    def provision(self, pages=5, restored_hours=8):
        allowance = quota.QuotaAllowance(
            source=SOURCE, read_pages=pages,
            restored_at=AT + timedelta(hours=restored_hours))
        with self.store.transaction(write=True) as db:
            quota.provision(db, allowance, at=self.clock)

    def job(self, job_id, *, necessity="required", source=SOURCE, query="贵州旅居"):
        return scheduler.register_job(
            self.store,
            scheduler.NewJob(job_id=job_id, source=source, query=query,
                             necessity=necessity), at=self.clock)

    def run_round(self, reader, *, minutes=0):
        return scheduler.run_once(self.store, reader,
                                  at=self.clock + timedelta(minutes=minutes))

    def board(self):
        return scheduler.board(self.store, at=self.clock)


class SchedulingTest(SchedulerCase):
    def test_resumable_cursor_is_protected_against_fast_restarting_queries(self):
        self.provision(pages=1, restored_hours=1)
        slow_reader = self.portal(name="slow-portal", quota_pages=1, restored_hours=1)
        self.job("fast-query")
        self.job("slow-query")
        # 快查询先读首页，持有可接续游标 p2。
        self.run_round(slow_reader)
        with self.store.transaction() as db:
            from guizhou_stay.scheduler import score_job, eligible_jobs
            jobs = {row["job_id"]: row for row in eligible_jobs(db, at=self.clock)}
            fast_with_cursor = score_job(db, jobs["fast-query"], at=self.clock)
            # 快查询持有游标时获得保护加分，评分远高于无游标基线。
            self.assertGreaterEqual(fast_with_cursor, scheduler.CURSOR_PROTECTION)
            # 慢查询尚未开始、无游标，评分只是基线，因此快查询不会被未启动的作业挤掉。
            slow_baseline = score_job(db, jobs["slow-query"], at=self.clock)
            self.assertLess(slow_baseline, scheduler.CURSOR_PROTECTION)
            self.assertGreater(fast_with_cursor, slow_baseline)

    def test_holding_cursor_survives_transient_failure_and_is_not_released(self):
        self.provision(pages=2, restored_hours=1)
        # 第二页持续瞬时失败的慢查询门户。
        slow_reader = self.portal(
            name="slow-portal", quota_pages=2, restored_hours=1,
            fail_on={"p2": {"code": "document_missing", "times": 10}})
        self.job("slow-query")
        self.run_round(slow_reader)
        stalled = self.run_round(slow_reader, minutes=1)
        self.assertEqual(stalled["reason"], "document_missing")
        entry = next(item for item in self.board()["jobs"] if item["job_id"] == "slow-query")
        # 瞬时失败不丢游标，会话仍可接续，读取未被释放给别人重开。
        self.assertTrue(entry["resumable"])
        with self.store.transaction() as db:
            row = db.execute(
                "SELECT cursor,next_page FROM collections WHERE session_id='slow-query-s1'"
            ).fetchone()
            self.assertEqual((row["cursor"], row["next_page"]), ("p2", 2))

    def test_quota_waiting_blocks_all_workers_until_shared_recovery(self):
        self.provision(pages=1, restored_hours=2)
        reader = self.portal(quota_pages=1, restored_hours=2)
        self.job("only-query")
        first = self.run_round(reader)
        self.assertEqual(first["outcome"], "success")
        blocked = self.run_round(reader, minutes=1)
        self.assertFalse(blocked["ran"])
        self.assertEqual(blocked["blocked"][0]["reason"], "quota_waiting")
        self.assertEqual(self.board()["next_recovery"],
                         (AT + timedelta(hours=2)).isoformat())

    def test_optional_job_can_be_paused_without_erasing_absorbed_facts(self):
        self.provision()
        reader = self.portal()
        self.job("routine-query", necessity="optional")
        self.run_round(reader)
        paused = scheduler.set_paused(
            self.store, "routine-query",
            scheduler.JobPause(paused=True, reason="运营暂缓常规补采"), at=self.clock)
        self.assertEqual(paused["state"], "paused")
        board = self.board()
        entry = next(item for item in board["jobs"] if item["job_id"] == "routine-query")
        self.assertEqual(entry["block_reason"], "operator_paused")
        # 已采集的首页事实仍在。
        with self.store.transaction() as db:
            pages = db.execute("SELECT COUNT(*) AS n FROM pages").fetchone()["n"]
            self.assertEqual(pages, 1)
        # 必采作业不能被暂缓。
        self.job("must-query", necessity="required")
        with self.assertRaises(DomainError) as raised:
            scheduler.set_paused(self.store, "must-query",
                                 scheduler.JobPause(paused=True), at=self.clock)
        self.assertEqual(raised.exception.code, "job_required")

    def test_aging_gives_long_waiting_optional_job_a_fair_turn(self):
        self.provision(pages=10)
        reader = self.portal()
        self.job("required-a", necessity="required")
        self.job("optional-b", necessity="optional")
        # 必采作业先占用调度，可选作业连续被阻塞会累积老化分。
        for minute in range(3):
            self.run_round(reader, minutes=minute)
        board = self.board()
        optional = next(item for item in board["jobs"] if item["job_id"] == "optional-b")
        required = next(item for item in board["jobs"] if item["job_id"] == "required-a")
        # 老化分随阻塞次数增长，长期排队的常规查询评分追上必采查询。
        self.assertGreater(optional["score"],
                           scheduler.BASE_PRIORITY_OPTIONAL + scheduler.AGING_PER_BLOCK)
        self.assertGreaterEqual(optional["score"], required["score"] - scheduler.CURSOR_PROTECTION)

    def test_cursor_expiry_reopens_with_distinct_reason_and_keeps_old_session(self):
        self.provision()
        reader = self.portal(notices=[
            {"at_cursor": "p2", "attempt": 1, "cursor": "p2", "state": "expired",
             "expires_at": (AT + timedelta(minutes=5)).isoformat()}])
        self.job("cursor-job")
        self.run_round(reader)
        expired = self.run_round(reader, minutes=1)
        self.assertEqual(expired["reason"], "cursor_expired")
        self.assertTrue(expired["next_session"].endswith("-s2"))
        job = scheduler.read_job(self.store, "cursor-job")
        reasons = {session["end_reason"] for session in job["sessions"]}
        self.assertIn("cursor_expired", reasons)
        # 旧会话现场保留，没有被删除。
        with self.store.transaction() as db:
            self.assertEqual(db.execute(
                "SELECT state FROM sessions WHERE session_id='cursor-job-s1'").fetchone()["state"],
                "failed")
        # 重开后两页采全，作业完成。
        self.run_round(reader, minutes=2)
        done = self.run_round(reader, minutes=3)
        self.assertTrue(done["progress"]["complete"])
        self.assertEqual(scheduler.read_job(self.store, "cursor-job")["state"], "done")

    def test_failure_at_cursor_deadline_is_distinct_from_transient_failure(self):
        # 失败响应恰在游标期限末尾到达：伴随过期通知，按 cursor_expired 而非
        # 普通瞬时失败处理，且该读取仍产生部分失败回执。
        self.provision()
        reader = self.portal(notices=[
            {"at_cursor": "p2", "attempt": 1, "cursor": "p2", "state": "expired",
             "expires_at": (AT + timedelta(seconds=1)).isoformat()}])
        self.job("deadline-job")
        self.run_round(reader)
        outcome = self.run_round(reader, minutes=1)
        self.assertEqual(outcome["outcome"], "partial_failure")
        self.assertEqual(outcome["reason"], "cursor_expired")
        self.assertEqual(outcome["receipt"]["outcome"], "partial_failure")
        self.assertIsNotNone(outcome["next_session"])

    def test_two_workers_observe_one_shared_recovery_moment(self):
        # 同一来源上两个作业（两组运营人员）共享额度与恢复时刻。
        self.provision(pages=1, restored_hours=2)
        reader_a = self.portal(name="portal-a", quota_pages=1, restored_hours=2)
        reader_b = self.portal(name="portal-b", quota_pages=1, restored_hours=2)
        self.job("team-a")
        self.job("team-b")
        self.run_round(reader_a)
        # 第二组也被同一恢复时刻挡住，不能透支。
        blocked = self.run_round(reader_b, minutes=1)
        self.assertFalse(blocked["ran"])
        self.assertEqual(blocked["blocked"][0]["reason"], "quota_waiting")
        # 恢复时刻前，任意进程再试都被挡。
        still = self.run_round(reader_b, minutes=119)
        self.assertFalse(still["ran"])
        # 越过恢复时刻，两个进程看到同一恢复结果，状态版本因恢复结算而推进。
        recovered_a = self.run_round(reader_a, minutes=121)
        self.assertTrue(recovered_a["ran"])
        with self.store.transaction() as db:
            state = quota.status(db, SOURCE, at=AT + timedelta(minutes=121))
            self.assertGreaterEqual(state["state_version"], 2)

    def test_source_revision_change_is_a_separate_reason_and_restarts(self):
        self.provision()
        reader = self.portal(revision_swaps={"p2": {"revision": 5, "times": 1}})
        self.job("revision-job")
        self.run_round(reader)
        changed = self.run_round(reader, minutes=1)
        self.assertEqual(changed["reason"], "source_revision")
        self.assertTrue(changed["next_session"].endswith("-s2"))
        events = {entry["reason"] for entry in scheduler.events(self.store)}
        self.assertIn("source_revision", events)
        self.assertNotIn("cursor_expired", events)

    def test_quota_waiting_is_recorded_as_its_own_reason(self):
        self.provision(pages=1, restored_hours=2)
        reader = self.portal(quota_pages=1, restored_hours=2)
        self.job("waiting-job")
        self.run_round(reader)
        blocked_round = self.run_round(reader, minutes=1)
        self.assertFalse(blocked_round["ran"])
        self.assertEqual(blocked_round["blocked"][0]["reason"], "quota_waiting")
        self.assertEqual(self.board()["next_recovery"],
                         (AT + timedelta(hours=2)).isoformat())
        reasons = {e["reason"] for e in scheduler.events(self.store, limit=500)}
        self.assertIn("quota_waiting", reasons)

    def test_cursor_expiry_is_recorded_as_its_own_reason(self):
        self.provision()
        reader = self.portal(notices=[
            {"at_cursor": "p2", "attempt": 1, "cursor": "p2", "state": "expired",
             "expires_at": (AT + timedelta(minutes=5)).isoformat()}])
        self.job("cursor-job")
        self.run_round(reader)
        expired = self.run_round(reader, minutes=1)
        self.assertEqual(expired["reason"], "cursor_expired")
        reasons = {e["reason"] for e in scheduler.events(self.store, limit=500)}
        self.assertIn("cursor_expired", reasons)
        self.assertNotIn("source_revision", reasons)
        self.assertNotIn("quota_waiting", reasons)

    def test_source_revision_is_recorded_as_its_own_reason(self):
        self.provision()
        reader = self.portal(revision_swaps={"p2": {"revision": 5, "times": 1}})
        self.job("revision-job")
        self.run_round(reader)
        revised = self.run_round(reader, minutes=1)
        self.assertEqual(revised["reason"], "source_revision")
        reasons = {e["reason"] for e in scheduler.events(self.store, limit=500)}
        self.assertIn("source_revision", reasons)
        self.assertNotIn("cursor_expired", reasons)
        self.assertNotIn("quota_waiting", reasons)


if __name__ == "__main__":
    unittest.main()
