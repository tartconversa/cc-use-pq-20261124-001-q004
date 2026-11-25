"""本地演练回放：带额度回执与游标通知的脚本化合作方。

目录回放仍沿用 ``first.html`` 与后续游标命名文件；另可在目录中放置
``scenario.json`` 编排读页额度、恢复时刻、游标失效、瞬时失败以及来源修订
变化。它只解释本地合成数据，不访问任何远程地址。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from guizhou_stay.digest import digest
from guizhou_stay.errors import invalid
from guizhou_stay.portal import CaptureFailure, FolderReader

CURSOR_RE = re.compile(r"[a-z][a-z0-9-]{0,47}")


@dataclass
class Scenario:
    source: str
    quota: int
    restored_at: datetime
    fail_on: dict = field(default_factory=dict)
    cursor_expiry: dict = field(default_factory=dict)
    notices: list = field(default_factory=list)
    revision_swaps: dict = field(default_factory=dict)

    @classmethod
    def load(cls, directory: Path) -> "Scenario | None":
        config = directory / "scenario.json"
        if not config.is_file():
            return None
        data = json.loads(config.read_text(encoding="utf-8"))
        try:
            return cls(
                source=data["source"], quota=int(data["quota"]),
                restored_at=datetime.fromisoformat(data["restored_at"]),
                fail_on=data.get("fail_on", {}),
                cursor_expiry=data.get("cursor_expiry", {}),
                notices=data.get("notices", []),
                revision_swaps=data.get("revision_swaps", {}),
            )
        except (KeyError, ValueError, TypeError) as exc:
            raise invalid("scenario", f"scenario.json 不合法: {exc}") from exc


@dataclass(frozen=True)
class ReadResult:
    html: str
    url: str
    receipt_hint: dict | None
    notice: dict | None


class ScriptedReader:
    """带合作方状态的脚本化读取器，供调度台本地回放。

    每次成功或部分失败的读取返回一份回执提示（含读取编号、合作方权威余额、
    来源状态版本号）；瞬时失败不产生回执，读取保持挂起等待重试。
    """

    def __init__(self, directory: Path, base_url: str = "https://stay.example.test/catalog/"):
        self.directory = directory.resolve()
        self.base_url = base_url
        self.scenario = Scenario.load(self.directory)
        self.folder = FolderReader(self.directory, base_url)
        self._attempts: dict[str, int] = {}
        self._consumed_notices: set[int] = set()
        self._state_version = 1

    # 与 FolderReader 兼容的纯页面读取，保留原单会话调用路径。
    def read_page(self, cursor: str | None) -> tuple[str, str]:
        return self.folder.read(cursor)

    def _key(self, cursor: str | None) -> str:
        key = cursor or "first"
        if not CURSOR_RE.fullmatch(key):
            raise CaptureFailure("cursor", "页面游标不符合合作方格式", retryable=False)
        return key

    def read(self, cursor: str | None, *, read_id: str | None = None,
             charged: bool = True) -> ReadResult:
        key = self._key(cursor)
        if self.scenario is None:
            html, url = self.folder.read(cursor)
            return ReadResult(html, url, None, None)
        if read_id is None:
            # 采集会话注册读取首页时尚未预留额度，不生成回执提示。
            read_id = "preregister-" + digest([key, self.directory.name])[:20]
            charged = False
        scenario = self.scenario
        attempt = self._attempts.get(key, 0) + 1
        self._attempts[key] = attempt
        notice = None
        for index, entry in enumerate(scenario.notices):
            if index in self._consumed_notices:
                continue
            if entry.get("at_cursor") == key and entry.get("attempt", attempt) == attempt:
                self._consumed_notices.add(index)
                notice = {
                    "source": scenario.source, "cursor": entry["cursor"],
                    "state": entry["state"],
                    "expires_at": entry["expires_at"],
                }
        fail = scenario.fail_on.get(key)
        if fail and attempt <= int(fail.get("times", 1)):
            raise CaptureFailure(
                fail.get("code", "portal_response"),
                fail.get("message", "合作方页面暂不可用"),
                retryable=bool(fail.get("retryable", True)),
                details={"cursor": key, "attempt": attempt},
            )
        expiry = scenario.cursor_expiry.get(key)
        if expiry and attempt <= int(expiry.get("times", 1)):
            notice = {
                "source": scenario.source,
                "cursor": expiry.get("cursor", cursor or "first"),
                "state": "expired",
                "expires_at": expiry["expires_at"],
            }
        html, url = self.folder.read(cursor)
        swap = scenario.revision_swaps.get(key)
        if swap and attempt <= int(swap.get("times", 1)):
            target = str(swap["revision"])
            html = re.sub(r'(name="stay-revision" content=")\d+(")',
                          lambda m: m.group(1) + target + m.group(2), html, count=1)
        if notice is not None and notice["state"] == "expired":
            self._state_version += 1
        elif swap:
            self._state_version += 1
        receipt = None
        if charged:
            receipt = {
                "read_id": read_id,
                "outcome": "partial_failure" if notice else "success",
                "charged_pages": 1,
                "state_version": self._state_version,
                "restored_at": scenario.restored_at.isoformat(),
            }
        return ReadResult(html, url, receipt, notice)

    def make_receipt(self, read_id: str, outcome: str, remaining: int, *,
                     receipt_id: str | None = None, detail: dict | None = None,
                     restored_at: datetime | None = None) -> dict:
        scenario = self.scenario
        if scenario is None:
            raise invalid("scenario", "普通目录回放没有额度场景")
        detail = detail or {}
        state_version = max(detail.get("state_version", 1), self._state_version)
        restored = (restored_at or scenario.restored_at)
        return {
            "receipt_id": receipt_id or "rcp-" + digest([read_id, state_version])[:20],
            "source": scenario.source, "read_id": read_id, "outcome": outcome,
            "charged_pages": 1, "remaining": remaining,
            "restored_at": restored.isoformat(),
            "state_version": state_version, "detail": {k: v for k, v in detail.items()
                                                       if k != "state_version"},
        }

    def allowance(self) -> dict:
        scenario = self.scenario
        if scenario is None:
            raise invalid("scenario", "普通目录回放没有额度场景")
        return {"source": scenario.source, "read_pages": scenario.quota,
                "restored_at": scenario.restored_at.isoformat()}
