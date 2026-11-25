from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
import sys

from pydantic import ValidationError

from guizhou_stay import bundles, catalog, collector, crawl, quota, scheduler, verification
from guizhou_stay.contracts import Page, SessionOpen
from guizhou_stay.errors import DomainError, conflict
from guizhou_stay.portal import FolderReader
from guizhou_stay.replay import ScriptedReader
from guizhou_stay.storage import Store


def open_session(store, args):
    body = SessionOpen(session_id=args.session, source=args.source, query=args.query,
                       source_revision=args.revision, expected_pages=args.pages)
    with store.transaction(write=True) as db:
        return crawl.open_session(db, body)


def import_page(store, args):
    body = Page.model_validate_json(args.json_file.read_text(encoding="utf-8"))
    with store.transaction(write=True) as db:
        if db.execute("SELECT 1 FROM collections WHERE session_id=?", (body.session_id,)).fetchone():
            raise conflict("managed_collection", "采集会话由采集员入口推进")
        receipt = crawl.save_page(db, body)
        if not receipt.get("stale") and not receipt["replayed"]:
            receipt["ingest"] = catalog.ingest_page(db, body)
    if receipt.get("stale"):
        raise conflict("source_changed", "页面范围已变化，当前会话需要重开", receipt["stale"])
    return receipt


def collect(store, args):
    directory = args.directory.resolve()
    return collector.run(store, args.session, directory.as_uri(), FolderReader(directory), args.limit)


def report(store, args):
    with store.transaction() as db:
        return crawl.session_report(db, args.session)


def audit(store, args):
    with store.transaction() as db:
        return verification.review(db, args.session)


def export(store, args):
    with store.transaction() as db:
        blob = bundles.build(db, args.package, args.edition)
    with args.destination.open("xb") as output:
        output.write(blob)
    return {"path": str(args.destination), "bytes": len(blob)}


def verify(_store, args):
    with args.archive.open("rb") as handle:
        raw = handle.read(26_000_001)
    if len(raw) > 26_000_000:
        from guizhou_stay.errors import invalid

        raise invalid("bundle_limit", "文件超出离线检查大小")
    return bundles.verify(raw)


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def provision_quota(store, args):
    if args.pages is None or not args.restored_at:
        raise DomainError(422, "quota_input", "登记额度需要 --pages 与 --restored-at")
    restored_at = datetime.fromisoformat(args.restored_at)
    if restored_at.tzinfo is None:
        raise DomainError(422, "quota_input", "恢复时刻必须带时区")
    body = quota.QuotaAllowance(source=args.source, read_pages=args.pages,
                                restored_at=restored_at)
    with store.transaction(write=True) as db:
        return quota.provision(db, body, at=_now())


def quota_status(store, args):
    from guizhou_stay.quota import utc

    with store.transaction(write=True) as db:
        return quota.status(db, args.source, at=utc(_now()))


def register_job(store, args):
    body = scheduler.NewJob(
        job_id=args.job, source=args.source, query=args.query,
        necessity=args.necessity, plan_id=args.plan, slot=args.slot,
    )
    return scheduler.register_job(store, body, at=_now())


def pause_job(store, args):
    body = scheduler.JobPause(paused=args.paused, reason=args.reason)
    return scheduler.set_paused(store, args.job, body, at=_now())


def board(store, args):
    from guizhou_stay.quota import utc

    return scheduler.board(store, at=utc(_now()))


def schedule_run(store, args):
    directory = args.directory.resolve()
    reader = ScriptedReader(directory)
    from guizhou_stay.quota import utc

    return scheduler.run_once(store, reader, at=utc(_now()))


def lateness(store, args):
    return scheduler.lateness_impact(
        store, args.plan, deadline=datetime.fromisoformat(args.deadline))


def parser():
    value = argparse.ArgumentParser(prog="python -m guizhou_stay.cli")
    value.add_argument("--db", type=Path)
    commands = value.add_subparsers(dest="command", required=True)

    open_cmd = commands.add_parser("open", help="登记人工提交会话")
    open_cmd.add_argument("session")
    open_cmd.add_argument("source")
    open_cmd.add_argument("query")
    open_cmd.add_argument("--revision", type=int, required=True)
    open_cmd.add_argument("--pages", type=int, required=True)
    open_cmd.set_defaults(handle=open_session)

    page_cmd = commands.add_parser("page", help="提交人工采集的结构化页面")
    page_cmd.add_argument("json_file", type=Path)
    page_cmd.set_defaults(handle=import_page)

    crawl_cmd = commands.add_parser("collect", help="从本地 HTML 按游标采集")
    crawl_cmd.add_argument("session")
    crawl_cmd.add_argument("directory", type=Path)
    crawl_cmd.add_argument("--limit", type=int, default=10)
    crawl_cmd.set_defaults(handle=collect)

    for name, handler in (("report", report), ("audit", audit)):
        command = commands.add_parser(name)
        command.add_argument("session")
        command.set_defaults(handle=handler)

    archive = commands.add_parser("bundle", help="导出封存供给包")
    archive.add_argument("package")
    archive.add_argument("destination", type=Path)
    archive.add_argument("--edition", type=int)
    archive.set_defaults(handle=export)

    check = commands.add_parser("verify-bundle", help="独立核对供给包与页面摘要")
    check.add_argument("archive", type=Path)
    check.set_defaults(handle=verify)

    quota_cmd = commands.add_parser("quota", help="登记或查看共享来源读页额度")
    quota_cmd.add_argument("source")
    quota_cmd.add_argument("--pages", type=int)
    quota_cmd.add_argument("--restored-at", dest="restored_at")
    quota_cmd.set_defaults(handle=provision_quota)

    quota_view = commands.add_parser("quota-status", help="查看来源额度与恢复时刻")
    quota_view.add_argument("source")
    quota_view.set_defaults(handle=quota_status)

    job_cmd = commands.add_parser("schedule-job", help="登记多作业调度采集任务")
    job_cmd.add_argument("job")
    job_cmd.add_argument("source")
    job_cmd.add_argument("query")
    job_cmd.add_argument("--necessity", choices=("required", "optional"), default="required")
    job_cmd.add_argument("--plan")
    job_cmd.add_argument("--slot")
    job_cmd.set_defaults(handle=register_job)

    pause_cmd = commands.add_parser("schedule-pause", help="暂缓或恢复可选采集任务")
    pause_cmd.add_argument("job")
    pause_cmd.add_argument("--paused", type=lambda v: v in {"1", "true", "yes"}, default=True)
    pause_cmd.add_argument("--reason")
    pause_cmd.set_defaults(handle=pause_job)

    board_cmd = commands.add_parser("schedule-board", help="查看调度台阻塞原因与评分")
    board_cmd.set_defaults(handle=board)

    run_cmd = commands.add_parser("schedule-run", help="按调度策略对回放目录执行一轮采集")
    run_cmd.add_argument("directory", type=Path)
    run_cmd.set_defaults(handle=schedule_run)

    late_cmd = commands.add_parser("lateness", help="查询必采迟到对当前证据的影响")
    late_cmd.add_argument("plan")
    late_cmd.add_argument("deadline")
    late_cmd.set_defaults(handle=lateness)
    return value


def main(argv=None):
    command = parser()
    args = command.parse_args(argv)
    if args.handle is not verify and args.db is None:
        command.error("该命令需要 --db")
    try:
        store = Store(args.db) if args.handle is not verify else None
        result = args.handle(store, args)
    except DomainError as exc:
        print(json.dumps(exc.payload(), ensure_ascii=False), file=sys.stderr)
        return 1
    except (OSError, ValueError, ValidationError) as exc:
        print(json.dumps({"code": "input_error", "message": str(exc)}, ensure_ascii=False),
              file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result.get("valid") is False or result.get("state") in {"failed", "stale"} else 0


if __name__ == "__main__":
    raise SystemExit(main())
