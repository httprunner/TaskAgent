## 流程概览

TaskAgent webhook 推送机制统一为「Webhook 结果表」：

- **统一机制**：Webhook 结果表（`WEBHOOK_BITABLE_URL`）+ `WebhookResultWorker`
  - 以结果表行的 `Status in {pending, failed}` 作为触发器；
  - 以结果表行的 `BizType+GroupID+Date` 在任务状态表中聚合该组所有任务，作为就绪判定与数据汇总主键；
  - 通过 `BizType` 区分不同业务的汇总策略（例如：综合页搜索→盗版筛查的 Group 聚合、视频录屏采集的 Single 任务）。

## 统一机制：Webhook 结果表（WEBHOOK_BITABLE_URL）

说明：
- `BizType=piracy_general_search`（综合页搜索→盗版筛查，Group 聚合推送）。
- `BizType=video_screen_capture`（视频录屏采集，Single 推送）及其 Creator/回填器（外部系统创建任务时需要）。
- `BizType=single_url_capture`（单个链接采集），推送计划可由外部工作流或 `WebhookResultCreator` 创建；`WebhookResultWorker` 按 `BizType+GroupID+Date` 聚合任务状态并推送，同时回写 `TaskIDs` 状态分布。

### 表定义（Feishu 多维表格）

Webhook 结果表用于存储 webhook 的关联信息和推送结果，核心字段如下（字段名可通过 `WEBHOOK_FIELD_*` 覆盖）：

- `BizType`：业务类型（例如 `piracy_general_search` / `video_screen_capture`）
- `ParentTaskID`：综合页搜索 TaskID（用于区分同一个 GroupID 在不同父任务下的唯一性）
- `GroupID`：`{App}_{BookID}_{UserID}`
- `Status`：`pending/success/failed/error`
- `TaskIDs`：文本（JSON），存储为 `{status: [taskID...]}` 的 map，用于展示该 webhook 推送计划下各任务的状态分布（由 worker 回写）
  - 示例：`{"pending":[123,456],"running":[789],"success":[321],"failed":[654],"error":[987],"unknown":[999]}`
  - 说明：`unknown` 表示任务 Status 为空（或历史数据/异常数据导致无法解析）
- `DramaInfo`：文本（JSON），创建时按 `BookID` 从剧单表拉取整行 fields 后序列化
- `UserInfo`：文本（JSON，占位）
- `Records`：文本（JSON，占位/或写入扁平化 records）
- `Date`：任务逻辑日期（从任务表 `Date` 派生的 ExactDate，用于去重/按日筛选）
- `CreateAt`：记录创建时间
- `StartAt`：首次开始处理时间（只写一次）
- `EndAt`：最后一次处理完成时间（每次尝试都会更新）
- `RetryCount`：重试次数
- `LastError`：最近一次错误信息

字段含义补充（按 BizType）：
- `BizType=piracy_general_search`：`ParentTaskID`、`GroupID` 必填，用于确定“同一父任务下的同一组”。
- `BizType=video_screen_capture`：当前只强依赖单个 TaskID（写入 `TaskIDs={"pending":[TaskID]}`）；`ParentTaskID/GroupID/DramaInfo` 可先为空（后续 BookID 修复后再补齐）。
- `BizType=single_url_capture`：推送计划按 `(BizType, GroupID, Date)` 去重；任务集合由 worker 在任务表中按 `BizType+GroupID+Date` 聚合并回写到 `TaskIDs`。

环境变量：
- `WEBHOOK_BITABLE_URL`：Webhook 结果表链接
  - 需要设置 `WEBHOOK_BITABLE_URL`

字段名覆盖（可选）：`WEBHOOK_FIELD_*`，详见 `docs/ENVIRONMENT.md`。

### 记录创建（Creator）

#### 综合页搜索→盗版筛查（Group 聚合）

上游在“综合页搜索任务成功完成并创建子任务之后”，按 GroupID 维度创建 webhook 结果表记录：

- 入口：`pkg/webhook.CreateWebhookResultsForGroups`
- 去重键：`<BizType, GroupID, Date(日)>`；若已存在同键记录则跳过创建
- 写入内容：
  - `BizType=piracy_general_search`
  - `Status=pending`
  - `TaskIDs`：该组所有需要聚合的子任务 TaskID（1-N）**以及**同一 BookID + 当日下所有「综合页搜索」父任务的 TaskID，去重后写入
  - `DramaInfo`：按 `BookID` 从剧单表查询整行 fields 后序列化写入
  - `Date`：父任务/子任务所在业务日期（任务表 `Date` 的日粒度）
  - `CreateAt`：记录创建时间

#### 视频录屏采集（Single，外部系统创建任务）

视频录屏采集任务由外部系统直接写入「任务状态表」，因此需要额外的 “Creator/回填器” 来在结果表创建 webhook 记录，供 `WebhookResultWorker` 消费：

- 职责：定时扫描任务表中 `Scene=视频录屏采集` 的任务行，批量创建 webhook 结果表记录
- 每个录屏 TaskID 对应结果表一条记录：
  - `BizType=video_screen_capture`
  - `Status=pending`
  - `TaskIDs`：文本字段，写入状态 map（例如 `{"pending":[123]}`）
  - `DramaInfo`：若任务表已填 `BookID`，可按 `BookID` 查询剧单表并序列化写入；若缺失 `BookID` 则先写 `{}`，worker 仍可继续推送（仅 drama 维度信息为空）
  - `Date`：任务表 `Date` 的日粒度值（ExactDate），用于后续按日筛选
  - `CreateAt`：记录创建时间
- 建议的扫描条件（可按实际落地调整）：
  - 只处理 `Status=success` 的任务（避免对未完成任务提前创建/重复创建）
  - 要求 `ItemID` 非空（否则即使创建也无法查询结果记录，最终会走失败/错误）

> 建议外部系统创建 `Scene=视频录屏采集` 任务时尽量补齐 `BookID`/`UserID`/`ItemID`，以提升 webhook payload 完整性与排查效率。

#### 单个链接采集（SingleURL Capture，可选）

`pkg/webhook.WebhookResultCreator` 支持可选扫描任务表中的 `Scene=单个链接采集` 任务并按 `(GroupID, DateDay)` 聚合创建 webhook 结果表记录（用于上层业务把“单链采集”也纳入统一的 webhook 结果表机制）。

该能力由 `WebhookResultCreatorConfig.EnableSingleURLCapture=true` 开启。开启后 creator 会额外向 Feishu 发起一条筛选查询，过滤条件包含：

- `Scene is "单个链接采集"`
- `Date is ExactDate(YYYY-MM-DD)`（若配置了 `ScanDate`）
- `Status in {"success","failed","error"}`（通过 `children(or)` 组合实现）

如果希望常驻进程**仅扫描当天任务**，可配置 `WebhookResultCreatorConfig.ScanDateToday=true`。
creator 会在每轮扫描前把 `ScanDate` 更新为本地当天日期，从而只拉取当天的单链任务；
但 webhook 结果表 `Date` 字段仍以**任务自身的 `Date`** 为准（仅在任务缺失 `Date` 时才回退到 `ScanDate`）。

注意：若任务表中 `Scene`/`Status` 是单选（枚举）字段，且其选项里不包含上述 value（最常见是 `Status` 没有 `error` 选项），Feishu 会返回 `code=1254018 msg=InvalidFilter`，导致本轮扫描失败。此时请在任务表中补齐对应枚举选项（确保 `Scene`/`Status` 的选项包含筛选值），再开启该功能。

### 轮询/处理逻辑（WebhookResultWorker）

worker 定时轮询 webhook 结果表：

1. 候选行：`Status in {pending, failed}`（`error` 跳过）；可通过 `WebhookResultWorkerConfig.DatePresets` 限制只扫描特定日期（例如 `DatePresets=[Today, Yesterday]` 仅扫描“今天 + 昨天”）。
2. 就绪判定：对候选行按 `BizType+GroupID+Date` 到任务表筛选出该组所有任务并查询状态
   - 若所有任务的 `Status in {success, error}` → 触发推送
   - 若存在 `pending/failed/dispatched/running/空` 等非终态 → 本轮跳过等待下一次轮询
   - 若按筛选条件查不到任何任务行 → 将该结果行标记为 `Status=error` 并写入 `LastError`
3. 推送 payload：结构保持不变（drama fields + records）
   - `BizType=piracy_general_search`：
     - `Drama`：优先使用结果表里的 `DramaInfo`（fields JSON）构造 payload
     - `Records`：按 `TaskIDs` 从 SQLite（优先）或 Feishu 结果表汇总记录
   - `BizType=video_screen_capture`：
     - 结果表仅提供单个 TaskID（写入 `TaskIDs={"pending":[TaskID]}`）；worker 需要回查任务表拿到该 TaskID 的 `App/Scene/Params/ItemID/...`
     - `Records`：按 “`Scene=视频录屏采集` + `ItemID`” 查询采集结果表，仅取最新 1 条
     - `Drama`：可先为空或由 Params 兜底（当前 BookID 为空，暂不强依赖剧单表）
   - `BizType=single_url_capture`：
     - 直接使用任务表中的行构造 capture records；
     - 单链任务的内部日志从任务表 `Logs` 字段读取，并写入 webhook payload 中 `records[*].Extra` 字段，作为下游 webhook 的日志来源。
4. 状态回写：
   - 首次开始处理：写 `StartAt`（后续重试不更新）
   - 每次处理结束：更新 `EndAt`、`LastError`
   - 成功：`Status=success`，`RetryCount=0`
   - 失败：`Status=failed`，`RetryCount += 1`
   - 达到上限：重试 3 次后转 `Status=error`（不再重试）

> **补充说明（Task 表 RetryCount）**：除了 Webhook 结果表外，TaskAgent 也在任务表中维护 `RetryCount` 字段：当某个 Feishu 任务从 `failed` 再次切换到 `running` 时，TaskAgent 会在同一次状态更新中将任务表的 `RetryCount` 自增；一旦发现某个 `Status=failed` 的任务其 `RetryCount` 已超过 3 次，TaskAgent 会将该任务标记为 `Status=error`，后续调度不再拉取该任务。这样任务表与 Webhook 结果表在重试语义上保持一致。

## 运行与接入

目前 `WebhookResultWorker` / `WebhookResultCreator` 可由上层业务进程（例如 fox agent 或独立服务）在进程内启动；核心实现位于：

- `pkg/webhook/webhook_worker.go`：`WebhookResultWorker`（轮询结果表并推送）
- `pkg/webhook/webhook_create.go`：`CreateWebhookResultsForGroups`、`WebhookResultCreator`（创建 group 结果行、回填录屏结果行）

对外只依赖：

- 任务状态表：用于就绪判定/读取任务字段（含 `ItemID` 等）
- 采集结果表或本地 SQLite：用于聚合 `records`
- Webhook 结果表：用于驱动状态机（`Status/RetryCount/LastError/StartAt/EndAt`）

视频录屏采集由于任务由外部系统直接写入任务表，需要额外的 Creator/回填器先把待推送任务写入结果表（`BizType=video_screen_capture`），worker 才会消费。

TaskAgent CLI（可选）：

- `go run ./cmd webhook-worker --task-url "$TASK_BITABLE_URL" --webhook-bitable-url "$WEBHOOK_BITABLE_URL" --webhook-url "$SUMMARY_WEBHOOK_URL"`：默认轮询 `WEBHOOK_BITABLE_URL` 中 `Status in (pending, failed)` 的结果行。
  - 支持定向调试：
    - `--group-id "<GroupID>"`：仅处理指定 `GroupID` 的结果行；
    - `--date "2025-12-17"`：仅处理逻辑日期为 `2025-12-17` 的结果行（对应结果表的 `Date` 字段，按本地时区格式化为 `YYYY-MM-DD`）。
    - 任一参数非空时 **仅执行单次扫描**，不会进入轮询，用于线上按 Group/日期精准复现问题。
- `go run ./cmd webhook-creator --task-url "$TASK_BITABLE_URL" --webhook-bitable-url "$WEBHOOK_BITABLE_URL" --app kwai`（默认单次执行；如需轮询加 `--poll-interval 30s`）

## 故障排查速查表

- 若 `WEBHOOK_BITABLE_URL` 行长期 `pending/failed`：
  - 检查任务表中该 `BizType+GroupID+Date` 下的任务 `Status` 是否已全部到 `success/error`
  - 检查结果表的 `TaskIDs` 状态分布（由 worker 回写），确认是否仍存在 `dispatched/running/pending/failed` 等非终态
  - 查看 `LastError` 和 `RetryCount`，达到 3 次后会转 `error`
- `BizType=video_screen_capture` 额外检查项（统一方案）：
  - Creator/回填器是否已覆盖到该 TaskID（否则结果表不会出现待推送行）
  - 任务表中该 TaskID 的 `ItemID` 是否为空（为空时无法按 `Scene+ItemID` 查询结果记录）
  - 采集结果表中是否已落到该 `ItemID` 的记录（预期仅推送最新 1 条）
- 若启用了 `EnableSingleURLCapture=true` 且出现 `code=1254018 msg=InvalidFilter`：
  - 重点检查任务表 `Scene`/`Status` 字段是否为单选枚举，以及其选项是否包含 `单个链接采集` / `error`（缺失时 Feishu 会拒绝该 Filter）

## 日志与测试
- 日志建议统一带上 `biz_type`、`group_id`、`task_ids`、`webhook_status`，方便在 `zerolog` 输出中追踪一整条链路。
- 相关测试：`go test ./pkg/webhook/...`（覆盖 webhook 结果表 worker、Feishu/SQLite 数据源等）。修改 worker 逻辑时务必扩充这些测试用例。

## 字段约定

- 任务表的 `BookID` 字段由 `TASK_FIELD_BOOKID` 覆盖（默认列名 `BookID`）。
- 剧单表的「短剧 ID」字段由 `DRAMA_FIELD_ID` 覆盖（默认列名 `短剧 ID`）。
- Group webhook 的 drama 关联规则为：`task.BookID == drama.短剧 ID`。
