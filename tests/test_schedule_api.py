from __future__ import annotations

import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from tests.support import ApiCase, evidence, item, page, stamp
from tests.test_capture import FIXTURES

AT = "2026-11-25T01:00:00+00:00"
AT_DT = datetime(2026, 11, 25, 1, tzinfo=timezone.utc)
SOURCE = "partner-map"


class ScheduleApiTest(ApiCase):
    CLOCK = {"X-Stay-Clock": AT}

    def setUp(self):
        super().setUp()
        self.portal_dir = Path(self.temp.name) / "portal"
        shutil.copytree(FIXTURES, self.portal_dir)
        (self.portal_dir / "scenario.json").write_text(json.dumps({
            "source": SOURCE, "quota": 2,
            "restored_at": "2026-11-25T09:00:00+00:00",
            "fail_on": {}, "cursor_expiry": {}, "notices": [], "revision_swaps": {},
        }), encoding="utf-8")

    def allowance(self, pages=2, restored="2026-11-25T09:00:00+00:00"):
        return {"source": SOURCE, "read_pages": pages, "restored_at": restored}

    def test_quota_endpoints_enforce_shared_balance_and_idempotent_receipts(self):
        self.call("PUT", f"/quota/{SOURCE}", self.allowance(), headers=self.CLOCK)
        state = self.call("GET", f"/quota/{SOURCE}", headers=self.CLOCK)
        self.assertEqual(state["remaining"], 2)
        self.assertIsNone(state["waiting_until"])
        # 跑一轮调度产生一次实际读取与回执。
        self.call("POST", "/schedule/jobs", {
            "job_id": "api-job", "source": SOURCE, "query": "贵州旅居"}, headers=self.CLOCK)
        ran = self.call("POST", "/schedule/run",
                        params={"directory": str(self.portal_dir)}, headers=self.CLOCK)
        self.assertTrue(ran["ran"])
        after = self.call("GET", f"/quota/{SOURCE}", headers=self.CLOCK)
        self.assertEqual(after["remaining"], 1)
        # 重送同一回执不改变余额。
        receipt = {k: v for k, v in ran["receipt"].items() if k != "replayed"}
        replayed = self.call("POST", f"/quota/{SOURCE}/receipts", receipt,
                             headers=self.CLOCK)
        self.assertTrue(replayed["replayed"])
        self.assertEqual(self.call("GET", f"/quota/{SOURCE}",
                                   headers=self.CLOCK)["remaining"], 1)
        # 旧状态版本回执被拒绝。
        stale = {**receipt, "receipt_id": "rcp-stale", "read_id": "read-stale",
                 "state_version": 0, "remaining": 99}
        self.call("POST", f"/quota/{SOURCE}/receipts", stale, status=422,
                  headers=self.CLOCK)

    def test_board_exposes_three_distinct_block_reasons(self):
        # 独立门户，恢复时刻与额度登记对齐为 03:00。
        wait_dir = Path(self.temp.name) / "wait-portal"
        shutil.copytree(FIXTURES, wait_dir)
        (wait_dir / "scenario.json").write_text(json.dumps({
            "source": SOURCE, "quota": 1,
            "restored_at": "2026-11-25T03:00:00+00:00",
            "fail_on": {}, "cursor_expiry": {}, "notices": [], "revision_swaps": {},
        }), encoding="utf-8")
        self.call("PUT", f"/quota/{SOURCE}",
                  self.allowance(pages=1, restored="2026-11-25T03:00:00+00:00"),
                  headers=self.CLOCK)
        self.call("POST", "/schedule/jobs", {
            "job_id": "wait-job", "source": SOURCE, "query": "贵州旅居"}, headers=self.CLOCK)
        self.call("POST", "/schedule/run",
                  params={"directory": str(wait_dir)}, headers=self.CLOCK)
        blocked = self.call("POST", "/schedule/run",
                            params={"directory": str(wait_dir)}, headers=self.CLOCK)
        self.assertFalse(blocked["ran"])
        board = self.call("GET", "/schedule/board", headers=self.CLOCK)
        self.assertEqual(board["jobs"][0]["block_reason"], "quota_waiting")
        self.assertEqual(board["next_recovery"], "2026-11-25T03:00:00+00:00")
        events = self.call("GET", "/schedule/events")["items"]
        self.assertIn("quota_waiting", {entry["reason"] for entry in events})

    def test_optional_job_pause_keeps_absorbed_facts_and_blocks_required_pause(self):
        self.call("PUT", f"/quota/{SOURCE}", self.allowance(), headers=self.CLOCK)
        self.call("POST", "/schedule/jobs", {
            "job_id": "opt-job", "source": SOURCE, "query": "贵州旅居",
            "necessity": "optional"}, headers=self.CLOCK)
        self.call("POST", "/schedule/run",
                  params={"directory": str(self.portal_dir)}, headers=self.CLOCK)
        paused = self.call("POST", "/schedule/jobs/opt-job/pause",
                           {"paused": True, "reason": "运营暂缓"}, headers=self.CLOCK)
        self.assertEqual(paused["state"], "paused")
        board = self.call("GET", "/schedule/board", headers=self.CLOCK)
        self.assertEqual(board["jobs"][0]["block_reason"], "operator_paused")
        # 已吸收的首页仍可回查。
        pages = self.call("GET", "/sessions/opt-job-s1/pages")["pages"]
        self.assertEqual(len(pages), 1)
        # 必采作业不能暂缓。
        self.call("POST", "/schedule/jobs", {
            "job_id": "must-job", "source": SOURCE, "query": "必采查询"}, headers=self.CLOCK)
        self.call("POST", "/schedule/jobs/must-job/pause", {"paused": True},
                  status=409, headers=self.CLOCK)
        # 恢复后作业重新进入调度。
        self.call("POST", "/schedule/jobs/opt-job/pause", {"paused": False},
                  headers=self.CLOCK)
        self.assertEqual(self.call("GET", "/schedule/board",
                                   headers=self.CLOCK)["jobs"][0]["state"], "active")


class LatenessTest(ApiCase):
    def _plan_with_required_slot(self):
        target = {"slot": "directory", "source": "partner-map", "query": "贵州旅居",
                  "source_revision": 4}
        return self.call("POST", "/plans", {
            "plan_id": "late-plan", "region": "贵阳",
            "max_capture_age_hours": 72, "targets": [target]})

    def test_incomplete_session_never_counts_as_sufficient_coverage(self):
        self._plan_with_required_slot()
        # 只有一页的未完成会话，不能分配给必采槽。
        self.open(pages=2)
        self.send_page(page(1, [item("stay-1", "云上小院")]))
        result = self.call("PUT", "/plans/late-plan/slots/directory",
                           {"session_id": "hk-001", "expected_generation": 0}, status=409)
        self.assertEqual(result["code"], "plan_unfinished")
        # 迟到影响报告把该槽列为缺口，部分结果不充当足够覆盖。
        report = self.call("GET", "/plans/late-plan/lateness",
                           params={"deadline": "2026-11-26T00:00:00+00:00"})
        self.assertFalse(report["ready"])
        self.assertEqual(report["gaps"][0]["reason"], "missing")
        self.assertTrue(report["slots"][0]["incomplete"])

    def test_lateness_report_flags_evidence_that_expires_before_deadline(self):
        self._plan_with_required_slot()
        self.open(pages=1)
        rows = [item("stay-1", "云上小院", "residence")]
        self.send_page(page(1, rows, total=1, has_next=False))
        proof = evidence("ev-stay-1", "stay-1", "residence")
        proof["issued_at"] = "2026-11-20T00:00:00+00:00"
        proof["expires_at"] = "2026-11-27T00:00:00+00:00"
        self.add_evidence(proof)
        self.call("PUT", "/plans/late-plan/slots/directory",
                  {"session_id": "hk-001", "expected_generation": 0})
        # 截止时刻晚于证明到期：必采迟到会使该证据过期。
        report = self.call("GET", "/plans/late-plan/lateness",
                           params={"deadline": "2026-11-28T00:00:00+00:00"})
        slot = report["slots"][0]
        self.assertEqual(slot["state"], "evidence_expiring")
        self.assertEqual(slot["expiring_evidence"][0]["evidence_id"], "ev-stay-1")
        self.assertFalse(report["ready"])
        # 截止时刻在到期之前则覆盖仍然有效。
        early = self.call("GET", "/plans/late-plan/lateness",
                          params={"deadline": "2026-11-26T00:00:00+00:00"})
        self.assertTrue(early["ready"])
        self.assertEqual(early["slots"][0]["expiring_evidence"], [])

    def test_stale_capture_is_reported_when_deadline_exceeds_age_window(self):
        plan = self._plan_with_required_slot()
        self.open(pages=1)
        self.send_page(page(1, [item("stay-1", "云上小院")], total=1, has_next=False))
        self.call("PUT", "/plans/late-plan/slots/directory",
                  {"session_id": "hk-001", "expected_generation": 0})
        # 计划窗口 72 小时，截止时刻远超采集时效。
        report = self.call("GET", "/plans/late-plan/lateness",
                           params={"deadline": stamp(days=-10)})
        self.assertEqual(report["slots"][0]["state"], "stale_capture")
        self.assertTrue(report["slots"][0]["stale_pages"])
