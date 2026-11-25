# 黔居采集台

这项服务用于采集合作方公开的旅居供给目录，核对住所、线路及交通、医疗、膳食、保险和语言配套，并把选定供给及其页面证据交接给渠道。仓库中的合作方页面和证明均为合成数据。

## 运行与验收

使用 Python 3.11 或更新版本。依赖已锁定在 `requirements.txt`。首次在本地搭建虚拟环境时可按锁定清单安装，预装环境可直接运行。服务面向本机可信操作员。用 `GUIZHOU_STAY_DB` 指定 SQLite 路径，默认使用 `guizhou_stay.sqlite3`。

```text
python -m pip install -r requirements.txt
python -m uvicorn guizhou_stay.api:from_environment --factory --host 127.0.0.1 --port 8000
python -m unittest discover -v
python -m tests.live_service
```

接口模型可从 `/openapi.json` 读取。验收在环回网络启动采集服务与合成合作方门户，实际读两页 HTML、复验原始证据、封存供给包，最后确认渠道回执。

## 采集与恢复

可以直接用目录回放合成页面。

```text
python -m guizhou_stay.cli --db demo.sqlite3 collect demo-capture fixtures/partner --limit 1
python -m guizhou_stay.cli --db demo.sqlite3 collect demo-capture fixtures/partner
python -m guizhou_stay.cli --db demo.sqlite3 audit demo-capture
```

也可启动 `guizhou_stay.local_portal:from_environment` 供主服务从 `POST /collections/{session_id}` 采集。HTTP 回放要求本机环回 URL。目录回放按 `first.html` 和后续游标命名文件。页面头用 `stay-source`、`stay-query`、`stay-revision`、`stay-locale`、`stay-page`、`stay-pages`、`stay-viewport`、`stay-observed`、`stay-cursor` 和 `stay-next` 元字段表明采集边界，空列表需要明示 `stay-empty=confirmed`。HTML 解析仅解释合成门户的静态标记，访问时无需脚本运行环境。

中断后继续使用原会话和原回放来源。后续页认领有有效期，短暂读取失败有次数上限与退避，过期工作不能入库。查询、语言、总页数或来源修订变化会终止会话并保留现场。末页与前页矛盾会阻止整份目录进入供给目录。原始 HTML、摘要、视口和结构化内容可从采集记录回查。审计会重新解析原始页面并核对目录使用的各个事实。

人工已经取得的结构化页面可以通过 `POST /sessions` 和 `POST /sessions/{session_id}/pages` 追加。页码及游标必须连续，新会话从第一页开始。人工记录也保存摘要，导出包会明确标识它只有结构化凭证。

## 多作业调度与共享额度

三组运营人员可能轮流采集同一合作方。合作方在本地演练接口上以**共享来源**为单位扣减实际读页额度，额度有恢复时刻；成功和部分失败的读取都返回额度回执。调度台在多份未完成采集之间安排下一次访问，原单会话入口 `POST /collections/{session_id}` 与 `cli collect` 保持不变。

额度账本（`/quota/{source}`）的规则：

- 每次实际读取先做额度预留并绑定读取编号；在收到对应回执前，该读取不会释放给其他进程。
- 回执按 `(source, read_id)` 幂等，重送同一回执返回原记录，不重复扣减也不重复增加可用额度。
- 恢复时刻对所有工作进程一致，未到时刻不得透支；到达时刻后整轮预算自动恢复，合作方状态版本号同步推进。
- 回执带 `state_version`，较早一轮的回执不能覆盖更新的来源状态（`receipt_stale`）。

调度台（`/schedule`）把三类等待原因明确区分，互不混用：

| 原因 | 含义 | 处置 |
| --- | --- | --- |
| `quota_waiting` | 来源额度用尽，等待统一恢复时刻 | 阻塞到 `next_recovery` |
| `cursor_expired` | 合作方明确通知游标过期，旧页不能直接续补 | 保留旧会话现场，另开新会话从第一页重采 |
| `source_revision` | 来源修订、查询或语言等范围变化 | 终止旧会话，另开新会话重采，已吸收事实保留 |

仍可接续的查询获得游标保护加分，优先于反复重新开始的快查询；长期排在后面的可选（常规补采）作业按等待时长与阻塞次数老化，得到合理的超车机会。运营可通过 `/schedule/jobs/{id}/pause` 暂缓可选任务（必采任务不可暂缓），暂缓不会抹掉已经吸收的页面事实。

`GET /plans/{plan_id}/lateness?deadline=...` 供计划人员核对：本轮必采查询若在截止时刻前未完成，会列出哪些槽未采全、哪些已选会话超过采集时效、哪些引用证明显著到期；未采全的会话永远不能分配给必采槽，部分结果不充当足够覆盖。

本地回放目录在原有 `first.html`、游标页之外可放置 `scenario.json`，编排读页额度、恢复时刻、瞬时失败（`fail_on`）、游标通知（`notices`/`cursor_expiry`）和来源修订替换（`revision_swaps`）。HTTP 回放可带 `X-Stay-Clock` 头固定决策时刻，便于确定性演练。

```text
python -m guizhou_stay.cli --db demo.sqlite3 quota partner-map --pages 2 --restored-at 2030-01-01T00:00:00+00:00
python -m guizhou_stay.cli --db demo.sqlite3 schedule-job job-a partner-map 贵州旅居
python -m guizhou_stay.cli --db demo.sqlite3 schedule-board
python -m guizhou_stay.cli --db demo.sqlite3 schedule-run fixtures/partner
python -m guizhou_stay.cli --db demo.sqlite3 schedule-pause job-a --reason 运营确认暂缓
python -m guizhou_stay.cli --db demo.sqlite3 lateness hk-catalog 2030-01-01T00:00:00+00:00
```

## 选用证据并交接

目录在整个会话齐全后更新。身份按类型、提供方、标题、城市和地区归并。不同合作方若上报同一身份但在关键字段上相互冲突，覆盖评估会显示分歧及相关来源，冲突项会阻止当前类别通过。证明按来源、项目编号、类别及来源修订绑定，有效期起点包含、终点不包含。查询时间必须包含时区。

`/plans` 可以为一个地区预登记必采和可选查询。按计划发布时，可为每个查询选择完整会话，并查看启用代次和历史。晚收、更换会话或证明变化会影响覆盖。预览返回摘要。发布可提交该摘要及预期版次，检查预览和提交之间是否发生变化。只有完整采集覆盖和供给证明达到计划时，封存包才会标为已发布。

封存 JSON 和 ZIP 读取的是保存的同一版内容。ZIP 保存页面原件、依据 CSV 和完整性清单，可用 `python -m guizhou_stay.cli verify-bundle` 离线核验。对渠道交接时，`/deliveries` 登记指定版次。渠道回执确认摘要后才会接受下一次出件。重复交接请求仍指向原出件编号和原封存内容。
