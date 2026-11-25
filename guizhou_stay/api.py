from __future__ import annotations

from datetime import datetime, timezone
import httpx
import json
import os
from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, Response

from guizhou_stay import bundles, catalog, collector, coverage, crawl, exporter, filters, handoff, plans, provenance, quota, reconcile, scheduler, verification
from guizhou_stay.contracts import CoveragePolicy, Evidence, Page, PublishRequest, SessionOpen
from guizhou_stay.digest import digest
from guizhou_stay.errors import DomainError, conflict, missing
from guizhou_stay.portal import LoopbackReader
from guizhou_stay.replay import ScriptedReader
from guizhou_stay.storage import Store


def create_app(database: str | Path) -> FastAPI:
    store = Store(database)
    app = FastAPI(title="黔居采集台", version="1.1.0")

    @app.exception_handler(DomainError)
    async def domain_error(request: Request, exc: DomainError):
        return JSONResponse(status_code=exc.status, content=exc.payload())

    def clock(request: Request | None = None) -> datetime:
        from guizhou_stay.quota import utc

        # 本地回放可通过 X-Stay-Clock 头固定决策时刻，便于确定性演练；
        # 缺省使用真实当前时刻。
        if request is not None:
            pinned = request.headers.get("x-stay-clock")
            if pinned:
                return utc(datetime.fromisoformat(pinned))
        return utc(datetime.now(timezone.utc))

    @app.get("/health")
    def health():
        with store.transaction() as db:
            db.execute("SELECT 1").fetchone()
        return {"status": "ready"}

    @app.post("/sessions")
    def open_session(body: SessionOpen):
        with store.transaction(write=True) as db:
            return crawl.open_session(db, body)

    @app.post("/sessions/{session_id}/pages")
    def page(session_id: str, body: Page):
        if body.session_id != session_id:
            raise missing("session", session_id)
        with store.transaction(write=True) as db:
            if db.execute("SELECT 1 FROM collections WHERE session_id=?", (session_id,)).fetchone():
                raise conflict("managed_collection", "采集会话由采集员入口推进")
            result = crawl.save_page(db, body)
            if not result.get("stale") and not result["replayed"]:
                result["ingest"] = catalog.ingest_page(db, body)
        if result.get("stale"):
            raise conflict("source_changed", "页面范围已变化，当前会话需要重开", result["stale"])
        return result

    @app.post("/collections/{session_id}")
    def collect(session_id: str, url: str, limit: int = Query(default=10, ge=1, le=100)):
        with httpx.Client(timeout=5, trust_env=False) as client:
            reader = LoopbackReader(url, client)
            return collector.run(store, session_id, reader.base, reader, limit)

    @app.get("/collections/{session_id}")
    def collection_status(session_id: str):
        return collector.read_progress(store, session_id)

    @app.get("/collections/{session_id}/captures/{page_no}")
    def raw_capture(session_id: str, page_no: int):
        with store.transaction() as db:
            return collector.raw_page(db, session_id, page_no)

    @app.get("/sessions/{session_id}")
    def session_report(session_id: str):
        with store.transaction() as db:
            return crawl.session_report(db, session_id)

    @app.get("/sessions/{session_id}/pages")
    def pages(session_id: str, revision: int | None = Query(default=None, ge=1)):
        with store.transaction() as db:
            return {"pages": crawl.read_pages(db, session_id, revision=revision)}

    @app.post("/sessions/{session_id}/abort")
    def abort(session_id: str, reason: str = Query(min_length=1, max_length=240)):
        with store.transaction(write=True) as db:
            return crawl.abort_session(db, session_id, reason)

    @app.get("/catalog")
    def catalog_items(region: str | None = None, revision: int | None = Query(default=None, ge=1)):
        with store.transaction() as db:
            return {"items": [item.model_dump(mode="json") for item in catalog.current_supplies(db, region=region, source_revision=revision)]}

    @app.get("/identities")
    def identity_list(kind: str | None = None, region: str | None = None):
        with store.transaction() as db:
            return {"items": catalog.identities(db, kind=kind, region=region)}

    @app.get("/catalog/history/{item_id}")
    def catalog_history(item_id: str):
        with store.transaction() as db:
            return {"items": reconcile.history(db, item_id)}

    @app.get("/reconcile")
    def reconcile_revisions(before: int = Query(ge=1), after: int = Query(ge=1), region: str | None = None):
        with store.transaction() as db:
            return reconcile.diff_revisions(db, before, after, region)

    @app.post("/quality")
    def quality_report(region: str, policy: CoveragePolicy, at: datetime, revision: int = Query(ge=1)):
        with store.transaction() as db:
            return exporter.operational_report(db, region, policy, at, revision)

    @app.post("/evidence")
    def evidence(body: Evidence):
        with store.transaction(write=True) as db:
            return provenance.record(db, body)

    @app.get("/evidence")
    def evidence_list(supply_id: str | None = None):
        with store.transaction() as db:
            return {"items": provenance.list_evidence(db, supply_id)}

    @app.post("/coverage/preview")
    def preview(region: str, policy: CoveragePolicy, at: datetime, revision: int = Query(ge=1), plan_id: str | None = None):
        with store.transaction() as db:
            assessment = coverage.assess(db, region, policy, at, revision, plan_id)
            return {**assessment, "preview_digest": digest(assessment)}

    @app.post("/packages")
    def package(body: PublishRequest):
        with store.transaction(write=True) as db:
            return coverage.create_package(db, body)

    @app.get("/packages/{package_id}")
    def package_read(package_id: str, revision: int | None = Query(default=None, ge=1)):
        with store.transaction() as db:
            return coverage.package(db, package_id, revision)

    @app.get("/packages/{package_id}/compare")
    def package_compare(package_id: str, before: int = Query(ge=1), after: int = Query(ge=1)):
        with store.transaction() as db:
            return coverage.compare(db, package_id, before, after)

    @app.get("/packages/{package_id}/export")
    def package_export(package_id: str, revision: int | None = Query(default=None, ge=1)):
        with store.transaction() as db:
            return JSONResponse(content=json.loads(exporter.export_package(db, package_id, revision)))

    @app.get("/sessions/{session_id}/audit")
    def review_session(session_id: str):
        with store.transaction() as db:
            return verification.review(db, session_id)

    @app.get("/catalog/search")
    def catalog_search(region: str | None = None, category: str | None = None,
                       provider: str | None = None, status: str | None = None,
                       text: str | None = None, revision: int | None = Query(default=None, ge=1),
                       offset: int = Query(default=0, ge=0), limit: int = Query(default=50, ge=1, le=200)):
        with store.transaction() as db:
            items = catalog.current_supplies(db, region=region, source_revision=revision)
            selected = filters.apply(items, filters.Filter(region, category, provider, status, text))
            return filters.page(selected, offset, limit)

    @app.post("/plans")
    def create_plan(body: plans.NewPlan):
        with store.transaction(write=True) as db:
            return plans.register(db, body)

    @app.get("/plans/{plan_id}")
    def read_plan(plan_id: str):
        with store.transaction() as db:
            return plans.read(db, plan_id)

    @app.put("/plans/{plan_id}/slots/{slot}")
    def select_session(plan_id: str, slot: str, body: plans.Assignment):
        with store.transaction(write=True) as db:
            return plans.assign(db, plan_id, slot, body)

    @app.get("/plans/{plan_id}/slots/{slot}/history")
    def previous_sessions(plan_id: str, slot: str):
        with store.transaction() as db:
            return {"history": plans.assignment_history(db, plan_id, slot)}

    @app.get("/plans/{plan_id}/readiness")
    def plan_readiness(plan_id: str, at: datetime):
        with store.transaction() as db:
            return plans.readiness(db, plan_id, at)

    @app.get("/packages/{package_id}/archive")
    def archive_package(package_id: str, revision: int | None = Query(default=None, ge=1)):
        with store.transaction() as db:
            content = bundles.build(db, package_id, revision)
        return Response(content, media_type="application/zip")

    @app.post("/deliveries")
    def queue_delivery(body: handoff.Delivery):
        with store.transaction(write=True) as db:
            return handoff.queue(db, body)

    @app.post("/deliveries/{destination}/acknowledge")
    def confirm_delivery(destination: str, body: handoff.Receipt):
        with store.transaction(write=True) as db:
            return handoff.acknowledge(db, destination, body)

    @app.get("/deliveries/{destination}")
    def view_deliveries(destination: str, since: int = Query(default=0, ge=0, le=1_000_000),
                        limit: int = Query(default=50, ge=1, le=200)):
        with store.transaction() as db:
            return handoff.history(db, destination, since, limit)

    @app.get("/deliveries/{destination}/pending")
    def pending_delivery(destination: str):
        with store.transaction() as db:
            return handoff.awaiting(db, destination)

    @app.get("/deliveries/{destination}/content/{delivery_id}")
    def delivery_content(destination: str, delivery_id: str):
        with store.transaction() as db:
            return handoff.package_for_delivery(db, destination, delivery_id)

    # 共享来源读页额度账本
    @app.put("/quota/{source}")
    def provision_quota(source: str, body: quota.QuotaAllowance, request: Request):
        if body.source != source:
            raise missing("source_quota", source)
        with store.transaction(write=True) as db:
            return quota.provision(db, body, at=clock(request))

    @app.get("/quota/{source}")
    def quota_status(source: str, request: Request):
        with store.transaction(write=True) as db:
            return quota.status(db, source, at=clock(request))

    @app.post("/quota/{source}/receipts")
    def quota_receipt(source: str, body: quota.QuotaReceipt, request: Request):
        if body.source != source:
            raise missing("source_quota", source)
        with store.transaction(write=True) as db:
            return quota.apply_receipt(db, body, at=clock(request))

    @app.get("/quota/{source}/receipts")
    def quota_receipts(source: str):
        with store.transaction() as db:
            return {"items": quota.receipts(db, source)}

    @app.post("/quota/{source}/cursors")
    def cursor_notice(source: str, body: quota.CursorNotice, request: Request):
        if body.source != source:
            raise missing("source_quota", source)
        with store.transaction(write=True) as db:
            return quota.record_cursor_notice(db, body, at=clock(request))

    # 多作业调度台（本地回放，保留原单会话 /collections 调用）
    @app.post("/schedule/jobs")
    def create_job(body: scheduler.NewJob, request: Request):
        return scheduler.register_job(store, body, at=clock(request))

    @app.get("/schedule/jobs/{job_id}")
    def get_job(job_id: str):
        return scheduler.read_job(store, job_id)

    @app.post("/schedule/jobs/{job_id}/pause")
    def pause_job(job_id: str, body: scheduler.JobPause, request: Request):
        return scheduler.set_paused(store, job_id, body, at=clock(request))

    @app.get("/schedule/board")
    def schedule_board(request: Request):
        return scheduler.board(store, at=clock(request))

    @app.get("/schedule/events")
    def schedule_events(job_id: str | None = Query(default=None),
                        limit: int = Query(default=100, ge=1, le=500)):
        return {"items": scheduler.events(store, job_id=job_id, limit=limit)}

    @app.post("/schedule/run")
    def schedule_run(request: Request, directory: str = Query(min_length=1, max_length=4096)):
        reader = ScriptedReader(Path(directory))
        return scheduler.run_once(store, reader, at=clock(request))

    @app.get("/plans/{plan_id}/lateness")
    def plan_lateness(plan_id: str, deadline: datetime):
        return scheduler.lateness_impact(store, plan_id, deadline=deadline)

    return app


def from_environment() -> FastAPI:
    return create_app(os.environ.get("GUIZHOU_STAY_DB", "guizhou_stay.sqlite3"))

