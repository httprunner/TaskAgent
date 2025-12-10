## 流程概览

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

## 行级派发逻辑

`processRow` 对每一条候选记录执行以下校验：
- `App` / `Scene` 为空直接跳过（Group 任务会顺带标记 `processedGroups`，避免重复告警）。
- `个人页搜索` + `GroupID`≠空 → `handleGroupRow`
- `视频录屏采集` → `handleVideoCaptureRow`
- 其他任务 → `handleSingleRow`，但要求 `Params` 必填。

每个分支都会返回本次附带的成功/失败/错误任务 ID，主循环统一汇总并在最后输出统计日志。

## 场景细节

### 个人页搜索（Group Task）
- 针对同一 `GroupID`，`fetchTasksByGroupID` 会拉取该组的所有子任务（个人页、合集、视频锚点等），并忽略 `Status` 为空的记录。
- 仅当剩余任务全部 `success` 时才触发 webhook；否则保持等待状态，让下次扫描继续校验。
- 下发时使用 `sendGroupWebhook` 汇总 App/Params/User 信息，`RecordLimit=-1`，全量同步该线索的采集结果。
- 成功/失败后统一更新该组任务的 Webhook 状态，并通过 `processedGroups` 保证同一轮只处理一次。

### 视频录屏采集（Single Task）
- 要求 `ItemID` 非空，否则直接记为 error 并回写 `Webhook=error`。
- `WebhookOptions` 自动开启 `SkipDramaLookup`、`PreferLatest`，并将 `RecordLimit` 固定为 1，从结果表取最新一条记录下发。
- 根据 sendSummary 的返回结果写回 `Webhook=success/failed/error`。

### 其他单任务
- 目前仅剩 `个人页搜索`（无 GroupID 的旧任务）或后续扩展场景。
- `Params` 必填，用于定位采集结果；同样根据 sendSummary 的结果写回 `Webhook` 字段。

## Group Webhook 判定

Webhook 只有在以下条件同时满足时才会针对某个 GroupID 触发：

1. 该 Group 下至少有一条任务，且 `Status` 列全部为 `success`（空值会被忽略）。
2. 触发人所在的行 `Webhook` 状态为 `pending` 或 `failed`，表示尚未成功推送。
3. App / Scene / GroupID 均非空，以避免下发到错误的业务。

处理流程：

```
WebhookWorker.processOnce()
    |
    |-- fetchWebhookCandidates(scene=个人页搜索)
    |-- for row in rows:
            if groupID processed -> continue
            tasks := fetchTasksByGroupID(groupID)
            ready := filter(status==success)
            if ready not all success -> continue
            sendGroupWebhook(ready)
            update webhook status (success/failed)
```

如果某行缺失 GroupID（旧任务或独立任务），则会走「其他单任务」逻辑，直接依据该行的 Params/App/Scene 下发，无需等待其他子任务完成。

## sendSummary 配置
- 视频录屏：`SkipDramaLookup=true`、`PreferLatest=true`、`RecordLimit=1`。
- Group 任务：`RecordLimit=-1`（内部会被解释为“无限制”，即取全量记录）。
- 其他任务：沿用默认配置，必要时可通过 `WebhookOptions` 继续扩展。

## 故障排查速查表

| 症状 | 常见原因 | 排查 & 解决 |
| --- | --- | --- |
| Webhook 长期 pending | 组内仍有任务未 success 或 `Scene`/`App` 为空 | 在任务表中按 GroupID 查看状态；补齐失败任务或清理脏数据。 |
| Webhook=error | 缺少 `Params`/`ItemID` 等关键字段 | 修补缺失字段或使用 `--source feishu` 直接从结果表补数。 |
| Webhook=failed 且不断重试 | 下游接口返回错误 | 查看 worker 日志中的 `webhook_error` 字段，修复下游或设置临时熔断后手动 `Webhook=pending` 重试。 |
| 处理速度慢 | `--batch-limit` 太小或 SQLite 磁盘慢 | 调大 `--batch-limit`，确认 `TRACKING_STORAGE_DB_PATH` 所在磁盘性能，必要时切换到 Feishu source。 |

## 日志与测试
- 日志统一带上 `app`、`scene`、`group_id`、`task_id`、`webhook_status`，方便在 `zerolog` 输出中追踪一整个链路。
- 相关测试：`go test ./pkg/webhook/...`（覆盖 webhook worker、Feishu/SQLite 数据源等）。修改 worker 逻辑时务必扩充这些测试用例。
