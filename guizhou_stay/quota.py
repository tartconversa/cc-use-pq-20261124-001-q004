"""共享来源读页额度账本。

合作方在本地演练接口上以共享来源为单位扣减实际读页额度。每次实际读取
（成功或部分失败）都会带回一份额度回执；回执按读取编号幂等，重送同一回执
不会重复扣减或重复增加可用额度。来源给出恢复时刻后，所有工作进程必须共同
遵守。回执带有来源状态版本号，较早一轮的回执不能覆盖更新的来源状态。
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Annotated, Literal

from pydantic import Field

from guizhou_stay.contracts import Key, StrictModel, Text
from guizhou_stay.digest import digest
from guizhou_stay.errors import conflict, missing
from guizhou_stay.storage import dump

PositivePages = Annotated[int, Field(ge=1, le=1_000_000)]
NonNegative = Annotated[int, Field(ge=0, le=1_000_000)]
Version = Annotated[int, Field(ge=1, le=1_000_000)]
Outcome = Literal["success", "partial_failure", "failure"]


def utc(at: datetime) -> datetime:
    if at.tzinfo is None:
        raise ValueError("时间必须带时区")
    return at.astimezone(timezone.utc).replace(microsecond=0)


class QuotaAllowance(StrictModel):
    """运营为一个共享来源登记的一轮读页额度与恢复时刻。"""

    source: Key
    read_pages: PositivePages
    restored_at: datetime


class QuotaReceipt(StrictModel):
    """合作方对一次实际读取返回的额度回执。

    ``read_id`` 识别对应的实际读取，同一来源内不得重复；``state_version``
    是合作方来源状态的单调版本号，旧轮次回执不能覆盖新状态。
    """

    receipt_id: Key
    source: Key
    read_id: Key
    outcome: Outcome
    charged_pages: Annotated[int, Field(ge=0, le=10_000)]
    restored_at: datetime
    remaining: NonNegative
    state_version: Version
    detail: dict = Field(default_factory=dict)


class CursorNotice(StrictModel):
    """合作方对已发放游标的状态通知：游标仍有效或已明确过期。

    ``scope_session`` 标识通知针对哪一代采集会话；该会话重开后，同名游标的
    旧通知自动作废，不能阻断新会话重新领取的游标。
    """

    source: Key
    cursor: Text
    state: Literal["live", "expired"]
    expires_at: datetime
    scope_session: Key | None = None


def _row(db, source: str):
    row = db.execute("SELECT * FROM source_quotas WHERE source=?", (source,)).fetchone()
    if row is None:
        raise missing("source_quota", source)
    return row


def provision(db, allowance: QuotaAllowance, *, at: datetime) -> dict:
    """登记新一轮读页额度。刷新余额与恢复时刻，不动已入帐回执。"""
    at = utc(at)
    restored = utc(allowance.restored_at)
    row = db.execute("SELECT 1 FROM source_quotas WHERE source=?",
                     (allowance.source,)).fetchone()
    if row is None:
        db.execute(
            """INSERT INTO source_quotas(source,read_pages,remaining,restored_at,
               state_version,updated_at) VALUES(?,?,?,?,1,?)""",
            (allowance.source, allowance.read_pages, allowance.read_pages,
             restored.isoformat(), at.isoformat()),
        )
    else:
        db.execute(
            """UPDATE source_quotas SET read_pages=?,remaining=?,restored_at=?,updated_at=?
               WHERE source=?""",
            (allowance.read_pages, allowance.read_pages, restored.isoformat(),
             at.isoformat(), allowance.source),
        )
    return status(db, allowance.source, at=at)


def _settle_recovery(db, row, at: datetime) -> bool:
    """到达恢复时刻后把额度恢复为整轮读页预算，所有进程看到同一结果。

    恢复意味着合作方进入新一轮状态，状态版本号同步推进，较早一轮的回执
    此后不能再入帐覆盖新状态。
    """
    restored_at = datetime.fromisoformat(row["restored_at"])
    if at < restored_at or row["remaining"] >= row["read_pages"]:
        return False
    db.execute(
        """UPDATE source_quotas SET remaining=read_pages,
           state_version=state_version+1,updated_at=? WHERE source=?""",
        (at.isoformat(), row["source"]),
    )
    return True


def settle(db, source: str, *, at: datetime) -> dict:
    at = utc(at)
    row = _row(db, source)
    _settle_recovery(db, row, at)
    return status(db, source, at=at)


def status(db, source: str, *, at: datetime) -> dict:
    at = utc(at)
    row = _row(db, source)
    _settle_recovery(db, row, at)
    row = _row(db, source)
    waiting_until = None
    if row["remaining"] <= 0 and datetime.fromisoformat(row["restored_at"]) > at:
        waiting_until = row["restored_at"]
    return {
        "source": source, "read_pages": row["read_pages"], "remaining": row["remaining"],
        "restored_at": row["restored_at"], "state_version": row["state_version"],
        "waiting_until": waiting_until,
        "usable": waiting_until is None,
    }


def reserve(db, source: str, *, at: datetime) -> dict:
    """领取一次读取的额度预留。

    预留把一页额度从来源共享余额中扣出，并绑定一个读取编号；在收到对应回执
    前该读取保持挂起，不会释放给其他进程。恢复时刻对所有进程一致，未到时刻
    不得透支。
    """
    at = utc(at)
    row = _row(db, source)
    _settle_recovery(db, row, at)
    row = _row(db, source)
    if row["remaining"] <= 0:
        if datetime.fromisoformat(row["restored_at"]) > at:
            raise conflict("quota_waiting", "来源读页额度已用尽，需等待恢复时刻",
                           {"source": source, "restored_at": row["restored_at"]})
        raise conflict("quota_exhausted", "来源暂无可用读页额度")
    read_id = "read-" + digest([source, at.isoformat(), row["state_version"],
                                row["read_pages"] - row["remaining"]])[:24]
    db.execute(
        "UPDATE source_quotas SET remaining=remaining-1,updated_at=? WHERE source=?",
        (at.isoformat(), source),
    )
    db.execute(
        """INSERT INTO quota_receipts(receipt_id,source,read_id,outcome,charged_pages,
           restored_at,remaining,state_version,received_at,detail)
           VALUES(?,?,?,?,?,?,?,?,?,?)""",
        ("pending-" + read_id, source, read_id, "failure", 0, row["restored_at"],
         row["remaining"] - 1, row["state_version"], at.isoformat(),
         dump({"state": "reserved"})),
    )
    return {"read_id": read_id, "source": source, "reserved_at": at.isoformat(),
            "remaining": row["remaining"] - 1}


def apply_receipt(db, receipt: QuotaReceipt, *, at: datetime) -> dict:
    """入帐一份合作方回执，按 (source, read_id) 幂等。

    重送已入帐回执返回原记录，不重复扣减也不重复增加可用额度。较早一轮的回执
    （state_version 更小）不能覆盖更新的来源状态。余额以合作方回执为准对齐。
    """
    at = utc(at)
    row = _row(db, receipt.source)
    duplicate = db.execute(
        "SELECT * FROM quota_receipts WHERE source=? AND read_id=?",
        (receipt.source, receipt.read_id),
    ).fetchone()
    if duplicate is not None and not duplicate["receipt_id"].startswith("pending-"):
        if duplicate["receipt_id"] != receipt.receipt_id:
            raise conflict("receipt_changed", "同一读取已绑定另一份回执",
                           {"read_id": receipt.read_id, "receipt_id": duplicate["receipt_id"]})
        return _receipt_view(duplicate, replayed=True)
    if receipt.state_version < row["state_version"]:
        raise conflict("receipt_stale", "较早一轮的回执不能覆盖更新的来源状态",
                       {"receipt_version": receipt.state_version,
                        "current_version": row["state_version"]})
    restored = utc(receipt.restored_at)
    if duplicate is not None:
        # 用正式回执替换预留占位；余额以合作方回执为准，不再按本地预留二次扣减。
        db.execute(
            """UPDATE quota_receipts SET receipt_id=?,outcome=?,charged_pages=?,restored_at=?,
               remaining=?,state_version=?,received_at=?,detail=? WHERE source=? AND read_id=?""",
            (receipt.receipt_id, receipt.outcome, receipt.charged_pages, restored.isoformat(),
             receipt.remaining, receipt.state_version, at.isoformat(),
             dump(receipt.detail), receipt.source, receipt.read_id),
        )
    else:
        db.execute(
            """INSERT INTO quota_receipts(receipt_id,source,read_id,outcome,charged_pages,
               restored_at,remaining,state_version,received_at,detail)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (receipt.receipt_id, receipt.source, receipt.read_id, receipt.outcome,
             receipt.charged_pages, restored.isoformat(), receipt.remaining,
             receipt.state_version, at.isoformat(), dump(receipt.detail)),
        )
    db.execute(
        """UPDATE source_quotas SET remaining=?,restored_at=?,state_version=?,updated_at=?
           WHERE source=?""",
        (receipt.remaining, restored.isoformat(),
         max(row["state_version"], receipt.state_version), at.isoformat(), receipt.source),
    )
    saved = db.execute("SELECT * FROM quota_receipts WHERE receipt_id=?",
                       (receipt.receipt_id,)).fetchone()
    return _receipt_view(saved, replayed=False)


def _receipt_view(row, *, replayed: bool) -> dict:
    return {"receipt_id": row["receipt_id"], "source": row["source"],
            "read_id": row["read_id"], "outcome": row["outcome"],
            "charged_pages": row["charged_pages"], "restored_at": row["restored_at"],
            "remaining": row["remaining"], "state_version": row["state_version"],
            "replayed": replayed, "detail": json.loads(row["detail"])}


def open_reads(db, source: str) -> list[dict]:
    """已预留但尚未收到正式回执的读取——不能释放给其他进程。"""
    rows = db.execute(
        """SELECT read_id,received_at,remaining FROM quota_receipts
           WHERE source=? AND receipt_id LIKE 'pending-%' ORDER BY received_at""",
        (source,),
    ).fetchall()
    return [dict(row) for row in rows]


def receipts(db, source: str) -> list[dict]:
    rows = db.execute(
        "SELECT * FROM quota_receipts WHERE source=? ORDER BY received_at,read_id", (source,),
    ).fetchall()
    return [_receipt_view(row, replayed=not row["receipt_id"].startswith("pending-"))
            for row in rows]


def record_cursor_notice(db, notice: CursorNotice, *, at: datetime) -> dict:
    at = utc(at)
    expires = utc(notice.expires_at)
    db.execute(
        """INSERT INTO source_cursors(source,cursor,state,expires_at,observed_at,detail,
           scope_session,superseded) VALUES(?,?,?,?,?,?,?,0)
           ON CONFLICT(source,cursor) DO UPDATE SET state=excluded.state,
             expires_at=excluded.expires_at,observed_at=excluded.observed_at,
             detail=excluded.detail,scope_session=excluded.scope_session,superseded=0""",
        (notice.source, notice.cursor, notice.state, expires.isoformat(), at.isoformat(),
         dump({"notice": notice.state}), notice.scope_session),
    )
    return cursor_state(db, notice.source, notice.cursor, at=at,
                        scope_session=notice.scope_session)


def supersede_cursors(db, source: str, scope_session: str, *, at: datetime) -> int:
    """会话重开后，把针对旧会话的游标通知标记作废，返回作废条数。"""
    at = utc(at)
    cursor = db.execute(
        "UPDATE source_cursors SET superseded=1,observed_at=? WHERE source=? AND scope_session=?",
        (at.isoformat(), source, scope_session),
    )
    return cursor.rowcount


def cursor_state(db, source: str, cursor: str, *, at: datetime,
                 scope_session: str | None = None) -> dict:
    at = utc(at)
    row = db.execute(
        "SELECT * FROM source_cursors WHERE source=? AND cursor=?", (source, cursor),
    ).fetchone()
    if row is None:
        return {"source": source, "cursor": cursor, "state": "unknown", "expires_at": None,
                "expired": False, "superseded": False}
    # 通知属于更早一代会话且当前查询属于新会话时，旧通知不再阻断新游标。
    scoped_out = (scope_session is not None and row["scope_session"] is not None
                  and row["scope_session"] != scope_session)
    superseded = bool(row["superseded"]) or scoped_out
    expired = (not superseded) and (
        row["state"] == "expired" or at >= datetime.fromisoformat(row["expires_at"]))
    return {"source": source, "cursor": cursor,
            "state": "expired" if expired else ("superseded" if superseded else row["state"]),
            "expires_at": row["expires_at"], "expired": expired, "superseded": superseded,
            "scope_session": row["scope_session"]}
