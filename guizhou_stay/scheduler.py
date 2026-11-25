"""多份未完成采集之间的下一次访问安排。

调度台在共享来源的读页额度、游标有效期与采集优先级之间做决定：

* 游标仍可接续的查询优先保护，避免慢查询的游标被快查询反复抢占而失效；
* 长期排在后面的常规（可选）查询按等待时长老化，获得合理的超车机会；
* 来源要求等待、游标失效后重开、来源修订变化分别以不同原因登记；
* 可选任务可由运营暂缓，已吸收的事实不会因此抹掉；
* 未采全的会话不能充当足够覆盖，必采查询迟到会使哪些证据过期可查询。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field

from guizhou_stay import collector, crawl, plans, quota
from guizhou_stay.contracts import Key, StrictModel, Text
from guizhou_stay.errors import conflict, missing
from guizhou_stay.portal import CaptureFailure, convert
from guizhou_stay.replay import ReadResult
from guizhou_stay.storage import Store, dump

Necessity = Literal["required", "optional"]

# 可接续游标保护加分；每轮阻塞/等待的老化超车加分。
CURSOR_PROTECTION = 1000
AGING_PER_BLOCK = 5
BASE_PRIORITY_REQUIRED = 100
BASE_PRIORITY_OPTIONAL = 50


def utc(at: datetime) -> datetime:
    if at.tzinfo is None:
        raise ValueError("时间必须带时区")
    return at.astimezone(timezone.utc).replace(microsecond=0)


class NewJob(StrictModel):
    job_id: Key
    source: Key
    query: Text
    locale: Text = "zh-HK"
    necessity: Necessity = "required"
    plan_id: Key | None = None
    slot: Key | None = None
    priority: Annotated[int, Field(ge=0, le=10_000)] = 100

    def model_post_init(self, __context):
        if (self.plan_id is None) != (self.slot is None):
            raise ValueError("计划编号与采集槽必须同时给出")


class JobPause(StrictModel):
    paused: bool
    reason: Text | None = None


def _event(db, job_id, source, reason, detail, *, at):
    db.execute(
        "INSERT INTO schedule_events(job_id,source,reason,detail,observed_at) VALUES(?,?,?,?,?)",
        (job_id, source, reason, dump(detail), at.isoformat()),
    )


def register_job(store: Store, body: NewJob, *, at: datetime) -> dict:
    at = utc(at)
    with store.transaction(write=True) as db:
        if body.plan_id is not None:
            plan = plans.load(db, body.plan_id)
            target = next((row for row in plan.targets if row.slot == body.slot), None)
            if target is None:
                raise missing("slot", body.slot)
            if (target.source, target.query, target.locale) != (
                body.source, body.query, body.locale
            ):
                raise conflict("job_scope", "作业与计划槽的来源、查询或语言不一致")
            if target.necessity != body.necessity:
                raise conflict("job_necessity", "作业的必采属性必须与计划槽一致")
        if db.execute("SELECT 1 FROM collection_jobs WHERE job_id=?", (body.job_id,)).fetchone():
            return read_job(store, body.job_id)
        db.execute(
            """INSERT INTO collection_jobs(job_id,plan_id,slot,source,query,locale,necessity,state,
               priority,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (body.job_id, body.plan_id, body.slot, body.source, body.query, body.locale,
             body.necessity, "active", body.priority, at.isoformat(), at.isoformat()),
        )
        _event(db, body.job_id, body.source, "job_registered",
               {"necessity": body.necessity, "plan_id": body.plan_id, "slot": body.slot}, at=at)
    return read_job(store, body.job_id)


def set_paused(store: Store, job_id: str, body: JobPause, *, at: datetime) -> dict:
    at = utc(at)
    with store.transaction(write=True) as db:
        row = db.execute("SELECT * FROM collection_jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise missing("job", job_id)
        if row["state"] in {"done", "abandoned"}:
            raise conflict("job_closed", "已结束的作业不能暂缓或恢复")
        if body.paused and row["necessity"] != "optional":
            raise conflict("job_required", "必采查询不允许运营暂缓")
        new_state = "paused" if body.paused else "active"
        if row["state"] != new_state:
            db.execute("UPDATE collection_jobs SET state=?,updated_at=? WHERE job_id=?",
                       (new_state, at.isoformat(), job_id))
            _event(db, job_id, row["source"],
                   "job_paused" if body.paused else "job_resumed",
                   {"reason": body.reason}, at=at)
    return read_job(store, job_id)


def _active_session(db, job_id: str):
    return db.execute(
        """SELECT * FROM job_sessions WHERE job_id=? AND ended_at IS NULL
           ORDER BY generation DESC LIMIT 1""", (job_id,),
    ).fetchone()


def _session_row(db, session_id: str):
    return db.execute("SELECT * FROM sessions WHERE session_id=?", (session_id,)).fetchone()


def _collection_row(db, session_id: str):
    return db.execute("SELECT * FROM collections WHERE session_id=?",
                      (session_id,)).fetchone()


def _continue_cursor(db, job, at: datetime):
    """仍可接续的查询返回其下一页游标；会话已结束或游标已失效返回 None。"""
    active = _active_session(db, job["job_id"])
    if active is None:
        return None
    session = _session_row(db, active["session_id"])
    if session is None or session["state"] != "open":
        return None
    collection = _collection_row(db, active["session_id"])
    if collection is None or collection["generation"]:
        return None
    cursor = collection["cursor"]
    if cursor and quota.cursor_state(db, job["source"], cursor, at=at)["expired"]:
        return None
    return cursor


def score_job(db, job, *, at: datetime) -> int:
    at = utc(at)
    base = job["priority"] + (BASE_PRIORITY_REQUIRED if job["necessity"] == "required"
                              else BASE_PRIORITY_OPTIONAL)
    if _continue_cursor(db, job, at) is not None:
        # 游标仍可接续：优先保护，慢查询不会被快查询挤到失效。
        base += CURSOR_PROTECTION
    if job["last_scheduled_at"]:
        waiting = at - datetime.fromisoformat(job["last_scheduled_at"])
        base += int(waiting.total_seconds()) * AGING_PER_BLOCK // 60
    base += job["times_blocked"] * AGING_PER_BLOCK
    return base


def eligible_jobs(db, *, at: datetime) -> list[dict]:
    at = utc(at)
    jobs = []
    for row in db.execute(
        "SELECT * FROM collection_jobs WHERE state='active' ORDER BY created_at,job_id"
    ).fetchall():
        job = dict(row)
        active = _active_session(db, job["job_id"])
        # 只有作业本身已收尾（无活动会话且状态已 done）才离场；open/stale/failed
        # 会话都需要继续安排，分别对应续采、重开或失败重试。
        if active is None and job["state"] != "active":
            continue
        jobs.append(job)
    return sorted(jobs, key=lambda job: (-score_job(db, job, at=at),
                                         job["created_at"], job["job_id"]))


def _next_recovery(db, sources: list[str], at: datetime) -> str | None:
    candidates = []
    for source in sources:
        state = quota.status(db, source, at=at)
        if state["waiting_until"]:
            candidates.append(state["waiting_until"])
    return min(candidates) if candidates else None


def board(store: Store, *, at: datetime) -> dict:
    """调度台视图：各作业的可接续性、阻塞原因与优先级评分。"""
    at = utc(at)
    with store.transaction() as db:
        items = []
        for row in db.execute(
            "SELECT * FROM collection_jobs ORDER BY created_at,job_id"
        ).fetchall():
            job = dict(row)
            active = _active_session(db, job["job_id"])
            block_reason = None
            cursor_state_value = None
            if job["state"] == "paused":
                block_reason = "operator_paused"
            elif active is not None:
                session = _session_row(db, active["session_id"])
                if session is not None:
                    if session["state"] == "stale":
                        block_reason = "source_revision"
                    elif session["state"] == "failed":
                        block_reason = "collection_failed"
                    elif session["state"] == "open":
                        collection = _collection_row(db, active["session_id"])
                        cursor = collection["cursor"] if collection else None
                        if cursor:
                            notice = quota.cursor_state(db, job["source"], cursor, at=at)
                            cursor_state_value = notice
                            if notice["expired"]:
                                block_reason = "cursor_expired"
            quota_state = quota.status(db, job["source"], at=at)
            if block_reason is None and quota_state["waiting_until"]:
                block_reason = "quota_waiting"
            items.append({
                **job, "score": score_job(db, job, at=at),
                "block_reason": block_reason,
                "resumable": _continue_cursor(db, job, at) is not None,
                "active_session": active["session_id"] if active else None,
                "cursor": cursor_state_value, "quota": quota_state,
            })
        sources = sorted({job["source"] for job in items})
        return {"as_of": at.isoformat(), "jobs": items,
                "next_recovery": _next_recovery(db, sources, at)}


def choose_next(db, *, at: datetime) -> dict:
    """选下一份可执行采集；全部受额度阻塞或暂无可执行作业时返回阻塞说明。

    已预留但未收回执的读取由作业自身的领取租约保护，不会被释放给其他进程；
    额度层只通过共享余额与统一恢复时刻约束并发，因此这里不再按挂起读取全局
    阻塞，避免一个作业的回执延迟卡死同一来源的其他作业。
    """
    at = utc(at)
    blocked = []
    blocked_sources = set()
    for job in eligible_jobs(db, at=at):
        state = quota.status(db, job["source"], at=at)
        if state["waiting_until"]:
            blocked.append({"job_id": job["job_id"], "source": job["source"],
                            "reason": "quota_waiting", "restored_at": state["waiting_until"]})
            blocked_sources.add(job["source"])
            continue
        return {"job": job, "quota": state}
    return {"blocked": blocked,
            "next_recovery": _next_recovery(db, sorted(blocked_sources), at)}


def _reader_label(reader) -> str:
    directory = getattr(reader, "directory", None)
    if directory is not None:
        return directory.as_uri() if isinstance(directory, Path) else str(directory)
    return getattr(reader, "base", "scripted-portal")


def _start_session(db, job, reader, *, at: datetime, reopen_reason: str | None) -> str:
    """为作业开启新的采集会话（首采或游标/修订失效后的重开）。"""
    previous = db.execute(
        "SELECT COALESCE(MAX(generation),0) AS g FROM job_sessions WHERE job_id=?",
        (job["job_id"],),
    ).fetchone()["g"]
    session_id = f"{job['job_id']}-s{previous + 1}"
    if len(session_id) > 48:
        session_id = f"job{previous + 1}-" + job["job_id"][-32:]
    # 只登记会话边界，首页由本轮 run_once 在额度预留后实际读取。
    collector.open_collection(db, session_id, _reader_label(reader), reader, at=at)
    db.execute(
        "INSERT INTO job_sessions(job_id,session_id,generation,started_at) VALUES(?,?,?,?)",
        (job["job_id"], session_id, previous + 1, at.isoformat()),
    )
    reason = reopen_reason or "session_started"
    _event(db, job["job_id"], job["source"], reason,
           {"session_id": session_id, "generation": previous + 1}, at=at)
    return session_id


def _close_session(db, job_id: str, session_id: str, reason: str, *, at: datetime):
    db.execute(
        "UPDATE job_sessions SET ended_at=?,end_reason=? WHERE job_id=? AND session_id=?",
        (at.isoformat(), reason, job_id, session_id),
    )


def _perform_read(reader, cursor: str, read_id: str) -> ReadResult:
    if hasattr(reader, "read") and _takes_read_id(reader):
        return reader.read(cursor, read_id=read_id)
    html, url = reader.read(cursor)
    return ReadResult(html, url, None, None)


def _takes_read_id(reader) -> bool:
    import inspect

    signature = inspect.signature(reader.read)
    return "read_id" in signature.parameters


def _settle(reader, db, result: ReadResult, read_id: str, source: str,
            outcome: str, at: datetime) -> dict:
    hint = result.receipt_hint
    current = quota.status(db, source, at=at)
    # 回执的恢复时刻与状态版本一律以账本已知的最新来源状态为准，避免回放场景的
    # 静态时间戳让账本误判“已到恢复时刻”而重复发放额度。
    restored_at = datetime.fromisoformat(current["restored_at"])
    if hint is not None and hasattr(reader, "make_receipt"):
        hint = {**hint, "state_version": max(hint.get("state_version", 1),
                                             current["state_version"])}
        payload = reader.make_receipt(read_id, hint.get("outcome", outcome),
                                      current["remaining"], detail=hint,
                                      restored_at=restored_at)
        return quota.apply_receipt(db, quota.QuotaReceipt(**payload), at=at)
    # 普通目录/HTTP 回放没有合作方回执头：按账本余额合成回执，保证读取不挂起。
    synthetic = quota.QuotaReceipt(
        receipt_id="rcp-" + read_id, source=source, read_id=read_id,
        outcome=outcome, charged_pages=1, remaining=current["remaining"],
        restored_at=restored_at,
        state_version=current["state_version"], detail={"synthetic": True},
    )
    return quota.apply_receipt(db, synthetic, at=at)


def run_once(store: Store, reader, *, at: datetime) -> dict:
    """执行一轮调度：至多安排一次实际读取并回传完整决策说明。

    阻塞原因与读取结果分三类登记：``quota_waiting``（额度等待）、
    ``cursor_expired``（游标失效后重开）、``source_revision``（来源修订变化）。
    """
    at = utc(at)
    with store.transaction(write=True) as db:
        choice = choose_next(db, at=at)
        if "blocked" in choice:
            for entry in choice["blocked"]:
                db.execute(
                    "UPDATE collection_jobs SET times_blocked=times_blocked+1 WHERE job_id=?",
                    (entry["job_id"],),
                )
                _event(db, entry["job_id"], entry["source"], entry["reason"], entry, at=at)
            return {"ran": False, **choice}
        job = choice["job"]
        source = job["source"]
        db.execute(
            """UPDATE collection_jobs SET first_scheduled_at=COALESCE(first_scheduled_at,?),
               last_scheduled_at=?,updated_at=? WHERE job_id=?""",
            (at.isoformat(), at.isoformat(), at.isoformat(), job["job_id"]),
        )
        reservation = quota.reserve(db, source, at=at)
        read_id = reservation["read_id"]
        active = _active_session(db, job["job_id"])
        reopen_reason = None
        if active is not None:
            session = _session_row(db, active["session_id"])
            if session is not None and session["state"] in {"stale", "failed"}:
                reopen_reason = ("source_revision_reopen" if session["state"] == "stale"
                                 else "failure_reopen")
                _close_session(db, job["job_id"], active["session_id"], reopen_reason, at=at)
                active = None
        if active is None:
            session_id = _start_session(db, job, reader, at=at,
                                        reopen_reason=reopen_reason)
        else:
            session_id = active["session_id"]
            collection = _collection_row(db, session_id)
            cursor = collection["cursor"]
            if cursor and quota.cursor_state(db, source, cursor, at=at)["expired"]:
                crawl.abort_session(db, session_id, "合作方通知游标过期，旧页不能直接续补")
                _close_session(db, job["job_id"], session_id, "cursor_expired", at=at)
                _event(db, job["job_id"], source, "cursor_expired",
                       {"cursor": cursor, "session_id": session_id}, at=at)
                session_id = _start_session(db, job, reader, at=at,
                                            reopen_reason="cursor_expired_reopen")
        collection = _collection_row(db, session_id)
        cursor = collection["cursor"]
        try:
            result = _perform_read(reader, cursor, read_id)
        except CaptureFailure as failure:
            # 瞬时失败不产生回执：读取保持挂起，场景解除后由下一轮重试。
            _event(db, job["job_id"], source, failure.code,
                   {"read_id": read_id, "retryable": failure.retryable,
                    "details": failure.details}, at=at)
            return {"ran": True, "job_id": job["job_id"], "read_id": read_id,
                    "outcome": "transient_failure", "reason": failure.code,
                    "retryable": failure.retryable}
        if result.notice:
            notice = result.notice
            notice_model = quota.CursorNotice(scope_session=session_id, **notice)
            quota.record_cursor_notice(db, notice_model, at=at)
            _event(db, job["job_id"], source, "cursor_notice", notice, at=at)
            if notice["state"] == "expired":
                # 失败响应恰在游标期限末尾到达：旧会话不能续补，开新会话重采。
                crawl.abort_session(db, session_id, "合作方通知游标过期，旧页不能直接续补")
                _close_session(db, job["job_id"], session_id, "cursor_expired", at=at)
                _event(db, job["job_id"], source, "cursor_expired",
                       {"cursor": cursor, "session_id": session_id}, at=at)
                quota.supersede_cursors(db, source, session_id, at=at)
                receipt = _settle(reader, db, result, read_id, source,
                                  "partial_failure", at)
                next_session = _start_session(db, job, reader, at=at,
                                              reopen_reason="cursor_expired_reopen")
                return {"ran": True, "job_id": job["job_id"], "read_id": read_id,
                        "outcome": "partial_failure", "reason": "cursor_expired",
                        "receipt": receipt, "next_session": next_session}
        capture = convert(session_id, result.html, result.url)
        progress = collector.commit_page(db, capture, at=at)
        receipt = _settle(reader, db, result, read_id, source,
                          "success" if progress["state"] != "stale" else "partial_failure", at)
        if progress["state"] == "stale":
            # 来源修订变化：旧会话保留现场并终止，另开会话从第一页重采，
            # 已吸收进目录的事实仍保留在旧会话与 observations 中。
            _close_session(db, job["job_id"], session_id, "source_revision", at=at)
            quota.supersede_cursors(db, source, session_id, at=at)
            _event(db, job["job_id"], source, "source_revision",
                   {"session_id": session_id, "detail": progress.get("reason")}, at=at)
            next_session = _start_session(db, job, reader, at=at,
                                          reopen_reason="source_revision_reopen")
            return {"ran": True, "job_id": job["job_id"], "read_id": read_id,
                    "outcome": "partial_failure", "reason": "source_revision",
                    "receipt": receipt, "progress": progress, "next_session": next_session}
        if progress["state"] == "complete":
            _close_session(db, job["job_id"], session_id, "complete", at=at)
            db.execute("UPDATE collection_jobs SET state='done',updated_at=? WHERE job_id=?",
                       (at.isoformat(), job["job_id"]))
            _event(db, job["job_id"], source, "job_completed",
                   {"session_id": session_id}, at=at)
        return {"ran": True, "job_id": job["job_id"], "read_id": read_id,
                "outcome": "success", "reason": "page_captured",
                "receipt": receipt, "progress": progress}


def read_job(store: Store, job_id: str) -> dict:
    with store.transaction() as db:
        row = db.execute("SELECT * FROM collection_jobs WHERE job_id=?",
                         (job_id,)).fetchone()
        if row is None:
            raise missing("job", job_id)
        job = dict(row)
        job["sessions"] = [
            dict(item) for item in db.execute(
                "SELECT session_id,generation,started_at,ended_at,end_reason FROM job_sessions "
                "WHERE job_id=? ORDER BY generation", (job_id,)).fetchall()
        ]
        job["events"] = [
            {**dict(item), "detail": json.loads(item["detail"])}
            for item in db.execute(
                "SELECT seq,source,reason,detail,observed_at FROM schedule_events WHERE job_id=? "
                "ORDER BY seq DESC LIMIT 30", (job_id,)).fetchall()
        ]
        return job


def events(store: Store, *, job_id: str | None = None, limit: int = 100) -> list[dict]:
    query = "SELECT * FROM schedule_events"
    args: list = []
    if job_id:
        query += " WHERE job_id=?"
        args.append(job_id)
    query += " ORDER BY seq DESC LIMIT ?"
    args.append(limit)
    with store.transaction() as db:
        return [{**dict(row), "detail": json.loads(row["detail"])}
                for row in db.execute(query, args).fetchall()]


def lateness_impact(store: Store, plan_id: str, *, deadline: datetime) -> dict:
    """计划人员视图：本轮必采查询迟到会让哪些当前证据过期。

    ``deadline`` 是必采查询必须完成的时刻。返回每个必采槽在该时刻的证据状态：
    尚未采全、已选会话将超过采集时效、或引用的证明到期。部分结果不会被当作
    足够覆盖。
    """
    deadline = utc(deadline)
    with store.transaction() as db:
        plan = plans.load(db, plan_id)
        targets = [target for target in plan.targets if target.necessity == "required"]
        slots = []
        for target in targets:
            selected = plans.head(db, plan_id, target.slot)
            entry = {"slot": target.slot, "source": target.source, "query": target.query,
                     "state": "missing", "expiring_evidence": [], "stale_pages": [],
                     "incomplete": True, "session_id": None}
            job_row = db.execute(
                "SELECT job_id,state FROM collection_jobs WHERE plan_id=? AND slot=?",
                (plan_id, target.slot),
            ).fetchone()
            entry["job_id"] = job_row["job_id"] if job_row else None
            if selected is None:
                slots.append(entry)
                continue
            entry["session_id"] = selected["session_id"]
            session = _session_row(db, selected["session_id"])
            pages = db.execute(
                "SELECT page_no,captured_at FROM pages WHERE session_id=? ORDER BY page_no",
                (selected["session_id"],),
            ).fetchall()
            complete = (session is not None and session["state"] == "complete"
                        and len(pages) == session["expected_pages"])
            entry["incomplete"] = not complete
            if complete:
                entry["state"] = "covered"
            oldest = min((page["captured_at"] for page in pages), default=None)
            if oldest and deadline - datetime.fromisoformat(oldest) > \
                    timedelta(hours=plan.max_capture_age_hours):
                entry["state"] = "stale_capture"
                entry["stale_pages"] = [
                    page["page_no"] for page in pages
                    if deadline - datetime.fromisoformat(page["captured_at"])
                    > timedelta(hours=plan.max_capture_age_hours)
                ]
            proofs = db.execute(
                """SELECT e.evidence_id,e.supply_id,e.category,e.expires_at,e.source_revision
                   FROM evidence AS e
                   JOIN observations AS o
                     ON o.item_id=e.supply_id AND o.source=e.source
                    AND o.source_revision=e.source_revision
                   WHERE o.session_id=? AND e.status='verified'
                   ORDER BY e.expires_at,e.evidence_id""",
                (selected["session_id"],),
            ).fetchall()
            expiring = [
                {"evidence_id": row["evidence_id"], "supply_id": row["supply_id"],
                 "category": row["category"], "expires_at": row["expires_at"]}
                for row in proofs if datetime.fromisoformat(row["expires_at"]) <= deadline
            ]
            entry["expiring_evidence"] = expiring
            if expiring and entry["state"] == "covered":
                entry["state"] = "evidence_expiring"
            if not complete and entry["state"] == "covered":
                entry["state"] = "incomplete"
            slots.append(entry)
        gaps = [
            {"slot": entry["slot"], "reason": entry["state"]}
            for entry in slots if entry["state"] != "covered"
        ]
        return {"plan_id": plan_id, "deadline": deadline.isoformat(),
                "as_of": deadline.isoformat(),
                "slots": slots, "gaps": gaps, "ready": not gaps}
