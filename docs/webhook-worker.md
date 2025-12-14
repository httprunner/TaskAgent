## 流程概览

TaskAgent 当前存在两套 webhook 触发机制：

1. **任务表 Webhook 字段（旧机制，仍保留）**
   - 触发器：任务表行满足 `Status=success` 且 `Webhook in {pending,failed}`。
   - 适用范围：例如「视频录屏采集」等仍使用任务表 `Webhook` 字段的场景。
   - 实现：`pkg/webhook/webhook_worker.go`，CLI：`cmd webhook-worker`。

2. **Webhook 结果表（新机制，推荐用于“综合页搜索→盗版筛查”）**
   - 触发器：Webhook 结果表行满足 `Status in {pending,failed}`，并且该行的 `TaskIDs` 对应的任务都已结束（`success/error`）。
   - 适用范围：仅针对“综合页搜索”驱动的盗版筛查聚合推送；与「视频录屏采集」解耦，互不影响。
   - 实现：`pkg/webhook/webhook_result_*.go`；通常由上层业务（例如 fox agent）在进程内启动 worker，而不是通过 TaskAgent CLI。

CLI 入口：

```bash
go run ./cmd webhook-worker \
  --task-url "$TASK_BITABLE_URL" \
  --result-url "$RESULT_BITABLE_URL" \
  --app kwai \
  --batch-limit 200 \
  --source sqlite           # 或 feishu
```

主要参数：

| Flag | 默认值 | 作用 |
| --- | --- | --- |
| `--task-url` | 读取 `TASK_BITABLE_URL` | 指定任务表，若省略则使用环境变量。 |
| `--result-url` | 读取 `RESULT_BITABLE_URL` | 当 Source=feishu 时需要；SQLite 模式可为空。 |
| `--app` | 空 | 仅处理匹配 App 列的行。 |
| `--batch-limit` | 100 | 每次扫描最多处理的任务数量（pending+failed 总数）。 |
| `--source` | `sqlite` | `sqlite` 先查本地 DB，`feishu` 直接访问多维表。 |

流程示意：

```
任务表 (Status=success, Webhook in {pending,failed})
        │
        ▼
WebhookWorker processOnce
        │ fetchWebhookCandidates(scene priority)
        ▼
handleVideoCaptureRow / handleGroupRow / handleSingleRow
        │
        ▼
sendSummary / sendGroupWebhook → Feishu 群机器人 or 内部 API
        │
        ▼
Update Webhook 字段 (success/failed/error)
```

Webhook worker（实现见 `pkg/webhook/webhook_worker.go`，CLI 入口见 `cmd/webhook.go`）以固定优先级轮询任务表，只处理以下场景（按顺序填满 `BatchLimit` 即停）：

1. `视频录屏采集` + Webhook=pending
2. `视频录屏采集` + Webhook=failed
3. `个人页搜索` + Webhook=pending
4. `个人页搜索` + Webhook=failed

所有查询都会强制 `Status=success`，并可选按照 `--app` 过滤。

## 新机制：Webhook 结果表（WEBHOOK_BITABLE_URL）

### 表定义（Feishu 多维表格）

Webhook 结果表用于存储 webhook 的关联信息和推送结果，核心字段如下（字段名可通过 `WEBHOOK_FIELD_*` 覆盖）：

- `ParentTaskID`：综合页搜索 TaskID（用于区分同一个 GroupID 在不同父任务下的唯一性）
- `GroupID`：`{App}_{BookID}_{UserID}`
- `Status`：`pending/success/failed/error`
- `TaskIDs`：多选文本（每个选项是 TaskID 字符串，例如 `123`；UI 上可能展示为 `123,456`）
- `DramaInfo`：文本（JSON），创建时按 `BookID` 从剧单表拉取整行 fields 后序列化
- `UserInfo`：文本（JSON，占位）
- `Records`：文本（JSON，占位/或写入扁平化 records）
- `CreateAt`：创建时间
- `StartAt`：首次开始处理时间（只写一次）
- `EndAt`：最后一次处理完成时间（每次尝试都会更新）
- `RetryCount`：重试次数
- `LastError`：最近一次错误信息

环境变量：
- `WEBHOOK_BITABLE_URL`：Webhook 结果表链接
  - 兼容：若未设置 `WEBHOOK_BITABLE_URL`，worker 会尝试读取旧变量 `PUSH_RESULT_BITABLE_URL`（仅用于兼容历史配置，建议尽快迁移）

字段名覆盖（可选）：`WEBHOOK_FIELD_*`，详见 `docs/ENVIRONMENT.md`。

### 创建逻辑（CreateWebhookResultsForGroups）

新机制要求上游在“综合页搜索任务成功完成并创建子任务之后”，按 GroupID 维度创建 webhook 结果表记录：

- 入口：`pkg/webhook.CreateWebhookResultsForGroups`
- 去重键：`(ParentTaskID, GroupID)`；若已存在同键记录则跳过创建
- 写入内容：
  - `Status=pending`
  - `TaskIDs`：该组所有需要聚合的子任务 TaskID（1-N）
  - `DramaInfo`：按 `BookID` 从剧单表查询整行 fields 后序列化写入
  - `CreateAt`：创建时间

### 轮询/处理逻辑（WebhookResultWorker）

worker 定时轮询 webhook 结果表：

1. 候选行：`Status in {pending, failed}`（`error` 跳过）
2. 就绪判定：对候选行的 `TaskIDs` 到任务表按 TaskID 查询状态
   - 若所有 TaskID 的 `Status in {success, error}` → 触发推送
   - 若存在 `pending/failed/dispatched/running/空/缺失行` → 本轮跳过等待下一次轮询
3. 推送 payload：保持与旧机制一致（drama fields + records）
   - `Drama`：优先使用结果表里的 `DramaInfo`（fields JSON）构造 payload，并确保 `DramaName` 不为空
   - `Records`：按 `TaskIDs` 从 SQLite（优先）或 Feishu 结果表汇总记录
4. 状态回写：
   - 首次开始处理：写 `StartAt`（后续重试不更新）
   - 每次处理结束：更新 `EndAt`、`LastError`
   - 成功：`Status=success`，`RetryCount=0`
   - 失败：`Status=failed`，`RetryCount += 1`
   - 达到上限：重试 3 次后转 `Status=error`（不再重试）

## 预期逻辑（Webhook Worker）

以下为当前预期的 webhook 处理规则（以任务表 `Webhook` 字段作为触发器）：

1. **仅当 `Webhook in {pending, failed}` 时触发检测**：
   - `Webhook=""`（空）或 `Webhook=error`：跳过检测、也不会更新其 webhook 状态。
2. **触发检测后按 `GroupID` 做就绪判定**：
   - 拉取同一 `GroupID` 的所有任务行；
   - 对 `Status=""`（空）或 `Status=error` 的任务：**忽略，不参与就绪判定**；
   - 对其余任务：要求 `Status=success` 才算就绪；只要存在 `pending/failed/running/dispatched/...` 任意非 success 状态则本轮跳过，等待下一次检测。
3. **执行 webhook 时按 TaskID 汇总采集结果**：
   - 汇总同一 `GroupID` 下、满足 `Status=success` 的所有任务的 `TaskID`；
   - 仅以这些 `TaskID` 作为查询条件，从「采集结果表」汇总所有关联记录（`Status=""` 的任务不参与、也不纳入结果汇总）。
4. **Drama 信息按 BookID 关联**：
   - 同一 `GroupID` 视为同一 `BookID`；
   - 使用 `DRAMA_BITABLE_URL` 指向的剧单表：以 `BookID` 对齐该表的「短剧 ID」字段（即 `DRAMA_FIELD_ID` 对应的列），拉取 drama 元信息并构造 webhook payload。

## 行级派发逻辑

`processRow` 对每一条候选记录执行以下校验：
- `App` / `Scene` 为空直接跳过（Group 任务会顺带标记 `processedGroups`，避免重复告警）。
- `个人页搜索` + `GroupID`≠空 → `handleGroupRow`
- `视频录屏采集` → `handleVideoCaptureRow`
- 其他任务 → `handleSingleRow`，但要求 `Params` 必填。

每个分支都会返回本次附带的成功/失败/错误任务 ID，主循环统一汇总并在最后输出统计日志。

## 场景细节

### 个人页搜索（Group Task）
- 针对同一 `GroupID`，worker 会拉取该组的所有子任务（个人页、合集、视频锚点等），用于做组内就绪判定与结果汇总。
- 就绪判定规则：
  - `Status=""`（空）或 `Status=error`：忽略；
  - 其余任务必须全部 `Status=success`，否则视为未就绪，本轮跳过等待下一次检测。
- webhook 下发的数据来源：
  - 仅汇总 `Status=success` 的任务行对应的 `TaskID`；
  - 从采集结果表按 `TaskID` 过滤并汇总所有记录，拼装成单次 group webhook payload。
- 成功/失败后只更新该组内 `Webhook in {pending, failed}` 的任务行；并通过 `processedGroups` 保证同一轮只处理一次。

### 视频录屏采集（Single Task）
- 要求 `ItemID` 非空，否则直接记为 error 并回写 `Webhook=error`。
- `WebhookOptions` 自动开启 `SkipDramaLookup`、`PreferLatest`，并将 `RecordLimit` 固定为 1，从结果表取最新一条记录下发。
- 根据 sendSummary 的返回结果写回 `Webhook=success/failed/error`。

### 其他单任务
- 目前仅剩 `个人页搜索`（无 GroupID 的旧任务）或后续扩展场景。
- `Params` 必填，用于定位采集结果；同样根据 sendSummary 的结果写回 `Webhook` 字段。

## Group Webhook 判定

Webhook 只有在以下条件同时满足时才会针对某个 GroupID 触发：

1. 候选行满足：`Status=success` 且 `Webhook in {pending, failed}`。
2. 拉取同组任务后，对 `Status=""`（空）或 `Status=error` 的任务忽略；对其余任务要求全部 `Status=success`。
3. 汇总结果时仅使用 `Status=success` 的任务 `TaskID`，从采集结果表捞取并汇总所有记录。

处理流程：

```
WebhookWorker.processOnce()
    |
    |-- fetchWebhookCandidates(scene=个人页搜索)
    |-- for row in rows:
            if groupID processed -> continue
            tasks := fetchTasksByGroupID(groupID)
            readiness := group tasks excluding status=="" and status==error
            if readiness contains non-success -> continue
            taskIDs := group tasks where status==success
            sendGroupWebhook(taskIDs)
            update webhook status (success/failed) for rows with webhook pending/failed
```

如果某行缺失 GroupID（旧任务或独立任务），则会走「其他单任务」逻辑，直接依据该行的 Params/App/Scene 下发，无需等待其他子任务完成。

## sendSummary 配置
- 视频录屏：`SkipDramaLookup=true`、`PreferLatest=true`、`RecordLimit=1`。
- Group 任务：以 `TaskID` 列表为主键汇总采集结果（不依赖 Params/Scene 过滤），并补齐 drama 元信息后下发。
- 其他任务：沿用默认配置，必要时可通过 `WebhookOptions` 继续扩展。

## 故障排查速查表

| 症状 | 常见原因 | 排查 & 解决 |
| --- | --- | --- |
| Webhook 长期 pending | 组内仍有任务未 success 或 `Scene`/`App` 为空 | 在任务表中按 GroupID 查看状态；补齐失败任务或清理脏数据。 |
| Webhook=error | 缺少 `Params`/`ItemID` 等关键字段 | 修补缺失字段或使用 `--source feishu` 直接从结果表补数。 |
| Webhook=failed 且不断重试 | 下游接口返回错误 | 查看 worker 日志中的 `webhook_error` 字段，修复下游或设置临时熔断后手动 `Webhook=pending` 重试。 |
| 处理速度慢 | `--batch-limit` 太小或 SQLite 磁盘慢 | 调大 `--batch-limit`，确认 `TRACKING_STORAGE_DB_PATH` 所在磁盘性能，必要时切换到 Feishu source。 |

新机制（Webhook 结果表）排查要点：
- 若 `WEBHOOK_BITABLE_URL` 行长期 `pending/failed`：
  - 检查 `TaskIDs` 是否完整且为数字字符串；worker 会以该列表为准做就绪判定
  - 检查任务表中这些 TaskID 的 `Status` 是否已到 `success/error`
  - 查看 `LastError` 和 `RetryCount`，达到 3 次后会转 `error`

## 日志与测试
- 日志统一带上 `app`、`scene`、`group_id`、`task_id`、`webhook_status`，方便在 `zerolog` 输出中追踪一整个链路。
- 相关测试：`go test ./pkg/webhook/...`（覆盖 webhook worker、Feishu/SQLite 数据源等）。修改 worker 逻辑时务必扩充这些测试用例。

## 字段约定

- 任务表的 `BookID` 字段由 `TASK_FIELD_BOOKID` 覆盖（默认列名 `BookID`）。
- 剧单表的「短剧 ID」字段由 `DRAMA_FIELD_ID` 覆盖（默认列名 `短剧 ID`）。
- Group webhook 的 drama 关联规则为：`task.BookID == drama.短剧 ID`。
