# My Agent 收敛实施方案｜SSE · Token 计量 · 模型策略 · 并发死锁测试

- 更新日期：2026-11-26
- 源码基线：`9d798ac`（本地 main，工作区含未提交改动，见文末）
- 文档性质：**范围变更**。本版取代原 16 项（E01—E16）全量方案，本期只做下列四块；其余项标记为已完成或暂缓。
- 部署范围不变：单机四个实例——一个 Go 进程、一个 Python 进程、一个 RabbitMQ、一个 Redis。不新增进程、worker、队列或集群。

---

## 0. 范围变更

| 原方案条目 | 本期状态 | 说明 |
|---|---|---|
| E01—E08（P0 安全边界） | 已落地，不重复投入 | 幂等、run 登记、outbox/MQ、限流降级、恢复扫描均已在 Go/Python 代码中 |
| E09—E12（P1 知识与基础设施） | 已落地或本期不涉 | 知识版本/索引任务、工具缓存重试、RabbitMQ/Redis 均已接入 |
| **E13 SSE 事件与回放** | **本期做** | 事件表/仓储已就绪，缺 SSE 端点、终态事件、前端订阅 |
| **Token 计量（原 E06 子集）** | **本期做** | 模型未采集 usage，全链路无 token 数据，是 E14 的前置 |
| **E14 模型/上下文策略** | **本期做** | 依赖 Token 计量与 E12 评测口径 |
| **并发死锁/竞争测试** | **本期做** | 目前只有手动脚本，无自动化断言 |

**四块依赖顺序**：Token 计量 → E14 策略评测；E13 可并行；并发死锁测试贯穿四块、作为回归门禁。
不先接入策略优化再补 token 数据；没有稳定收益数据前不切换默认模型。

---

## 1. 现状基线（源码核对结果）

以下均为当前代码事实，不是目标。行号基于基线 `9d798ac`。

### 1.1 E13 SSE 相关

| 能力 | 现状 | 代码依据 |
|---|---|---|
| 事件表与序号 | 已有 `workflow_events`，`UNIQUE(run_id,event_seq)`，含 `(run_id,event_seq)` 索引 | `backend/go/internal/database/schema.go:115`、`:133` |
| 事件仓储 | `AppendEvent` 事务内取 `MAX(event_seq)+1` 并插入，提交后回填；`ListEvents(runID,afterSeq,limit)` 已实现 | `backend/go/internal/repository/event_respository.go:21`、`:107` |
| 事件模型 | `WorkflowEvent{ID,RunID,EventSeq,EventType,NodeID,Payload,CreatedAt}` | `backend/go/internal/domain/work_event.go` |
| 节点事件写入 | Engine 已写 `node.started` / `node.completed` / `node.failed` | `backend/go/internal/workflow/engine.go:157`、`:255`、`:202` |
| 依赖注入 | `App.EventRepo` 已构造并注入 Engine | `backend/go/internal/bootstrap/app.go:99`、`:104` |
| **HTTP SSE 端点** | **无**。`router.go` 仅 `/health`、`POST /api/workflow/run`、`GET /api/workflow/run/:run_id` | `backend/go/internal/api/router.go:26` |
| **Handler 持有 EventRepo** | **无**，只传 `DB` 与 `RunRepo` | `backend/go/internal/api/router.go:31` |
| **run 终态事件** | **无**。consumer 落库 `SaveResult`/`UpdateStatus` 后未写 `run.completed`/`run.failed` | `backend/go/internal/consumer/consumer.go:272`、`:291` |
| **Python 内部事件流** | **无**。`AgentExecuteResponse` 只有 `output` | `backend/python/app/schemas/agentconfig.py:17` |
| **前端订阅** | **无**，`App.tsx` 仍是提交后等待单响应 | `frontend/src/App.tsx` |

### 1.2 Token 计量相关

| 能力 | 现状 | 代码依据 |
|---|---|---|
| 模型响应结构 | `TextResponse` / `ToolResponse` 只有 `content` / `tool_calls`，无 usage、finish_reason、provider request id | `backend/python/app/model/base.py:8`、`:13` |
| 真实模型采集 | `DeepSeekModel.invoke` 未读 `response.usage`，未设 `max_tokens`、无超时/取消信号 | `backend/python/app/model/deepseek_model.py:117` |
| Agent 响应契约 | `AgentExecuteResponse` 只有 `output` | `backend/python/app/schemas/agentconfig.py:17` |
| 请求预算字段 | Python `AgentConfig` 与 Go `domain.AgentConfig` 均无 budgets；Go 只解析 system_prompt/model/allowed_tools | `backend/python/app/schemas/agentconfig.py:6`；`backend/go/internal/executor/agent_executor.go:236` |
| 图步数上限 | `graph.compile()` 未设 recursionLimit | `backend/python/app/agent/graph.py:38` |
| 上下文裁剪 | 无，每轮发送全部消息 | `backend/python/app/agent/nodes/model_node.py:16` |
| 账本表 | Go/Python SQLite 均无 usage/账本表 | `backend/go/internal/database/schema.go`；`backend/python/app/infrastructure/database.py` |

### 1.3 并发与竞争相关

| 风险点 | 现状 | 代码依据 |
|---|---|---|
| 工具并行 | `asyncio.Semaphore(max_concurrency=2)` + `gather`，按 `tool_call_id` 配对结果 | `backend/python/app/agent/nodes/tool_node.py:20`、`:38` |
| 并发测试 | 只有手动 `main()` 脚本，无断言、非 pytest、不进测试套件 | `backend/python/tests/test_tool_concurrency.py:16` |
| SQLite 参数 | Go DSN 只有 `foreign_keys(1)`；Python 只有 `PRAGMA foreign_keys=ON`，均无 WAL/busy_timeout | `backend/go/internal/bootstrap/app.go:73`；`backend/python/app/infrastructure/database.py:12` |
| 事件序号并发 | `AppendEvent` 在事务内 `MAX+1`，高并发同 run 依赖 SQLite 串行化 | `backend/go/internal/repository/event_respository.go:49` |
| 索引任务并发 | 多处 `BEGIN IMMEDIATE` 做状态切换，无 busy_timeout | `backend/python/app/infrastructure/database.py:370`、`:629`、`:829` |
| 会话互斥 | Agent service 直接执行，无会话锁表/锁检查 | `backend/python/app/service/agent_service.py:17` |
| 取消接口 | 无 cancel 路由 | `backend/go/internal/api/router.go:26` |

---

## 2. 工作块 A：E13 SSE 事件与回放

### 目标
在已有事件持久化之上补齐 SSE 端点与回放，前端能按 run 订阅节点进度与终态；重连不重跑、事件不重复、终态唯一。

### 要做的事

1. **终态事件补齐**（Go，前置）
   - consumer 在 run 落库成功后追加 `run.completed`，失败落库后追加 `run.failed`，与 run 状态更新放在同一业务逻辑内。
   - 事件写入失败不回滚业务终态，但必须记录告警日志；SSE 侧以 run 状态表作为终态兜底，避免事件缺失导致前端永久等待。
2. **SSE 端点**（Go）
   - 新增 `GET /api/workflow/run/:run_id/events`（兼容一期接口），ContentType `text/event-stream`，`id` 为 `event_seq`。
   - 首帧按 `Last-Event-ID`（解析为 afterSeq）调用 `ListEvents` 回放；随后轮询新事件并推送，直到 run 进入终态后关闭连接。
   - `WorkflowHandler` 注入 `EventRepo`；`router.go` 注册路由。
   - 事件白名单：`node.started`/`node.completed`/`node.failed`/`run.completed`/`run.failed`。内部模型推理文本与完整 prompt 不进入默认事件流。
3. **前端订阅**（React）
   - `App.tsx` 在拿到 run_id 后建立 `EventSource`，按 `id` 更新节点高亮与状态；终态事件触发停止订阅并展示最终结果。
   - 断线由浏览器自动重连并携带 `Last-Event-ID`；关闭页面/停止订阅不调用 cancel。

### 行为边界
- 事件序号在 run 内单调递增，持久化后推送；去重以 `(run_id, event_seq)` 为准。
- `Last-Event-ID` 非法或缺失时从 0 回放；回放与订阅之间不丢事件（同一仓储按序号查询）。
- 断连只代表取消订阅，不等于取消 run；取消沿用后续接口，不在本期新增 cancel 路由。
- 一个 run 只有一个终态；迟到事件不得覆盖已落库的 run 终态。

### 验收边界
- 断线重连后从 last id 续传，节点事件不重复、不遗漏；终态事件唯一。
- 事件写库失败时，前端仍能通过状态查询拿到终态。
- 无完整 prompt、模型 Key、工具参数原文进入事件流。

### 指标
回放事件数、重连次数、事件推送 p95、终态到达率、事件写入失败数。

---

## 3. 工作块 B：Token 计量（E14 前置，原 E06 子集）

### 目标
把每次真实模型调用的输入/输出 token 从供应商响应一路带到 Go 并落库；未知用量保持 unknown，不记零。这是 E14 评测与策略切换的数据基础。

### 要做的事

1. **响应结构扩展**（Python）
   - `TextResponse`/`ToolResponse` 增加可选 `usage`（input_tokens/output_tokens）、`finish_reason`、`provider_request_id`、`model_call_id`。
   - `DeepSeekModel.invoke` 读取 `response.usage`、`choices[0].finish_reason`；设 `max_tokens`（来自预算）；无法取得用量时字段留空。
   - 识别 `finish_reason=length` 为截断，不作为成功答案。
2. **请求预算字段**（Go → Python）
   - `AgentConfig` 与 `domain.AgentConfig` 增加 budgets：`maxAgentSteps`、`maxModelCalls`、`maxToolCalls`、`maxContextTokens`、`maxOutputTokensPerCall`、`maxTotalTokens`、`maxToolResultChars`。
   - `AgentExecutor.parseAgentConfig` 解析并传递；预算缺省时用本机配置的固定值，不信任前端声明。
3. **账本落地**（Python + Go）
   - Python state.db 增加 usage 账本表，唯一键 `(agent_execution_id, model_call_id, attempt_no)`，记录模型、token、用量状态、计价配置版本；失败与重试也入账。
   - Python `AgentExecuteResponse` 返回 `usage` 与 `usageEntries`；完整已知用量汇总为 `usage`，任一必要值未知则 `usage: null`，条目仍保留。
   - Go workflow.db 增加按同一唯一键去重的归集表；重复 RPC 结果/状态查询/事件不重复累加；run 结果携带 usage 供前端与评测读取。

### 行为边界
- 只统计实际发起的模型调用；缓存命中不产生新账本条目。
- Mock 模型 usage 为 null，不伪造成本数据。
- 未知用量不等于免费；估算费用带配置版本，可追溯。

### 验收边界
- 同一 `(agent_execution_id, model_call_id, attempt_no)` 重复上报只记一次；重试与失败尝试均入账。
- 缺失 usage 时汇总为 null、条目保留，不被写成 0。
- `finish_reason=length` 被标记为截断，不计入成功答案。

### 指标
每成功任务输入/输出 token、unknown usage 数、估算费用、预算拦截数、去重命中数。

---

## 4. 工作块 C：E14 模型与上下文策略

### 目标
在 Token 计量有数据后，按同一任务集评测，再决定上下文裁剪、模型选择与可选重排策略；策略配置版本化、可回退。

### 前置
- 工作块 B 完成（有 token 数据）。
- 评测任务集与通过条件明确（任务、预期工具/来源、质量要求、成功口径），Mock 与真实模型分开统计。

### 要做的事

1. **上下文裁剪**（Python）
   - 在 `model_node.py` 调用模型前按 `maxContextTokens` 裁剪：保留 system 与最近完整对话轮次，按预算从旧到新丢弃；不得拆散 `tool_calls` 与对应 ToolMessage。
   - 裁剪后仍超预算时在下一个付费调用前结束，不发送超长请求。
2. **模型选择策略**（Python，版本化）
   - 策略配置（版本号、模型映射、触发条件）集中管理，可回退到当前默认。
   - 仅在评测显示稳定收益时切换默认模型；无数据不启用新策略。
3. **可选重排/候选池**（Python）
   - 依据评测结果再决定是否启用更大检索候选池或选择性重排；重排失败退回向量结果并记录降级。

### 行为边界
- 策略改动不影响既有 run 的确定性：run 引用保存的策略版本快照。
- 不新增独立模型服务或 worker，仍单机单进程。

### 验收边界
- 同一任务集下，质量/来源正确率不下降，p95 与 token 消耗按可比口径记录。
- 无稳定收益的策略不启用；不填写未经测量的收益比例。

### 指标
任务通过率、来源正确率、端到端 p95、输入/输出 token、每成功任务估算成本；优化前后同一任务集对照。

---

## 5. 工作块 D：并发死锁与竞争测试

### 目标
把并发/锁竞争下的行为固化为自动化测试，作为四块的回归门禁；不靠手动脚本，不靠"跑一次没问题"。

### 现状问题
- 现有 `test_tool_concurrency.py` 是手动脚本，无断言，不进测试套件。
- SQLite 两侧均未开启 WAL/busy_timeout，并发写易出现 `database is locked`。
- 事件序号 `MAX+1`、索引任务 `BEGIN IMMEDIATE`、run 领取与终态更新都是竞争面。

### 要做的事

1. **测试基建**
   - 把 `test_tool_concurrency.py` 改写为 pytest 用例（临时 DB、断言并发上限与结果按 `tool_call_id` 配对）。
   - SQLite 统一开启 WAL 与 busy_timeout：Go DSN 增加 `_pragma=journal_mode(WAL)` 与 `_pragma=busy_timeout(5000)`；Python `get_connection` 增加 `PRAGMA journal_mode=WAL`、`PRAGMA busy_timeout=5000`。
2. **必测场景**
   - **工具并发**：一次返回多个 tool_call，并发上限生效；乱序完成时结果对上正确 call ID；超时不串位。
   - **事件序号竞争**：同一 run 并发写事件，序号唯一连续，无重复/跳号。
   - **run 领取竞争**：同一 run 被多个 consumer 消息命中，只执行一次，终态唯一。
   - **索引任务竞争**：同一版本并发 `start/finish/fail`，状态机不越界，active 指针不被旧版本覆盖。
   - **取消与完成竞争**：终态写入后迟到的结果不覆盖终态。
   - **会话并发**（待会话锁实现后启用）：同一会话并发新执行只接受一个。
3. **运行方式**
   - Python 用例纳入 `pytest.ini`；Go 用例纳入 `go test ./...`；竞争检测用 `-race`。
   - 测试超时统一设上限，避免死锁时挂死整个套件。

### 验收边界
- 所有并发用例有明确断言与超时；`database is locked` 不导致测试失败或状态不一致。
- 竞争场景下终态唯一、序号唯一、结果配对正确；无 goroutine/task 泄漏。

### 指标
并发用例通过率、`database is locked` 次数、事件重复数、终态冲突数、测试耗时上限。

---

## 6. 不变约束（继承原方案，本期继续有效）

- 状态所有权：Go 独占 run/节点/事件/费用归集；Python 独占 agent checkpoint、账本、知识文件与索引 metadata。各自独占 SQLite，不互相打开。
- Go → Python 始终是单 Agent 节点、版本化 HTTP + JSON；MQ 只承载 `workflow.run` 派发。
- 终态为 `completed | failed | cancelled | timed_out`；条件更新提交唯一终态，迟到结果不覆盖。
- 幂等：同键同请求复用原 run，同键不同请求 409；不按相同 prompt 猜测重试。
- 缓存/限流降级有界；Redis 不承载执行状态、幂等、终态或费用真值。
- 不宣称外部模型调用 exactly once；SSE 改善可见进度，不等于吞吐提升。

---

## 7. 实施顺序与完成判断

| 序 | 工作块 | 前置 | 完成标志 |
|---|---|---|---|
| 1 | D（测试基建 + WAL/busy_timeout） | 无 | 现有并发脚本转为 pytest，SQLite 参数生效 |
| 2 | B（Token 计量） | 1 | usage 从模型到 Go 落库，去重/unknown 边界通过 |
| 3 | A（E13 SSE） | 1 | 端点+终态事件+前端订阅打通，回放/重连验收通过 |
| 4 | C（E14 策略） | 2、3 | 评测口径一致，策略版本化且可回退，有数据才切换 |
| 全程 | D（竞争用例） | 随各块 | 竞争场景纳入回归门禁 |

每块只有在**配置、实际行为、数据结果三者可相互核对**且满足对应验收边界时才标记完成。所有条目当前均为待实施。

---

## 附：工作区未提交改动（基线之外）

核对时工作区存在以下改动，实施前需确认是否纳入：
- `backend/go/internal/bootstrap/app.go`、`backend/go/internal/workflow/engine.go`、`backend/go/internal/workflow/engine_test.go`
- `frontend/src/data/nodes.ts`
- 未跟踪：`backend/python/dev.ps1`

这些改动与本方案四块可能相关，动手前先看 diff 并决定是否先提交或合并。
