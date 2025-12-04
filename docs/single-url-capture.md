# 单个链接采集任务

`单个链接采集`（Single URL Capture）是 TaskAgent 中专门面向“单条视频链接快速处理”场景的任务类型。与传统依赖物理设备的采集不同，这类任务不会分配到设备池，而是由 `SingleURLWorker` 直接调用 `content_web_crawler` 的下载服务（`POST /download/tasks` + `GET /download/tasks/<job_id>`）完成采集。

## 任务字段要求

任务表需要提供以下字段（字段名可通过 `TASK_FIELD_*` 环境变量覆盖）：

| 字段 | 说明 |
| --- | --- |
| `Scene` | 固定为 `单个链接采集`，调度器据此识别任务类型。|
| `BookID` | 短剧/Book 编号，作为业务主键之一。|
| `UserID` | 统筹维权账号，用来构建 GroupID。|
| `URL` | 视频/链接地址，后续调用 API 时直接使用。|
| `Params` | 备用 JSON 字段，可携带额外上下文。|

`SingleURLWorker` 会校验以上字段：缺少 `BookID`/`UserID`/`URL` 任意一项时，任务将被标记为 `failed`，并在 `Extra` 中记录失败原因，防止重复重试。

## Cookies 管理

- 在 `COOKIE_BITABLE_URL` 对应的多维表中维护账号 cookies：
  - `Cookies`：Web 登录态字符串；
  - `Platform`：平台名称。
  - `Status`：`valid` / `invalid`。SingleURLWorker 只会使用 `Status=valid` 的行。
- Worker 每次刷新时会批量抓取 `valid` cookies，并采用随机/轮询的方式挑选，避免同一 cookie 被连续使用。
- 预留了 cookie 有效性检测与失效回写能力：未来可实现检测逻辑，若发现失效即把对应行的 `Status` 写回 `invalid` 并继续尝试下一条。

## 调度与状态流转

1. `TASK_FIELD_SCENE = 单个链接采集` 且 `Datetime=Today` 的任务会插队在 `视频录屏采集` 之后、搜索类任务之前（参见 `fetchTodayPendingFeishuTasks`）。
2. `SingleURLWorker`（`pool/single_url_worker.go`）按照 `pending → failed` 顺序批量拉取任务，每轮最多 `fetch-tasks-limit` 条，可通过 `--single-url-poll-interval` 改写扫描频率。
3. 对于所有字段完整的任务：
   - Worker 会调用下载服务 `POST /download/tasks`，请求体为 `{url, cookies: [], sync_to_hive: true}`；
   - 成功后会把任务 `Status` 更新为 `queued`，将 `GroupID` 写成 `BookID_UserID`，并把 `{job_id: <xxx>}` 序列化到 `Extra`；
   - `DispatchedAt/StartAt` 同步为当前时间，用于后续统计；若创建失败则立即标记 `failed` 并写入错误信息。
4. 下载服务通过 `JobStatusManager` 将 job 状态存入 Redis，`SingleURLWorker` 继续在每轮 `ProcessOnce` 中拉取 `Status ∈ {queued,running}` 的任务并轮询 `GET /download/tasks/<job_id>`：

| 下载服务状态 | Task 表 Status | 行为 |
| --- | --- | --- |
| `queued` | `queued` | 维持排队状态，如缺失 job_id 会自动补齐/报错。 |
| `running` | `running` | 通过 `updateFeishuTaskStatuses` 更新状态，保持原始时间戳。 |
| `done` | `success` | 写入 `vid` 到 `Extra`（`{"job_id":"...","vid":"..."}`），并把 `EndAt`/`ElapsedSeconds` 补齐。 |
| `failed` / 404 | `failed` | 将失败原因附加到 `Extra`，同时保留 `job_id` 方便排查。 |

这样即可形成 `pending → queued → running → success/failed` 的闭环，无需人工介入。

> **状态存储在哪里？** 下载服务通过 `models/redis/job_status.JobStatusManager` 把 job 元信息写入 Redis，`SingleURLWorker` 只负责调用 HTTP 接口获取/落盘，不会把状态保留在内存中。

## foxagent 入口

`foxagent search` 在以下模式中自动携带单链任务 worker：

| 模式 | 开关 | 说明 |
| --- | --- | --- |
| Auto-pool | `foxagent search --auto-pool --single-url-poll-interval=30s` | 启动设备池时同步运行 `SingleURLWorker`，常驻轮询。|
| 单次执行 | `foxagent search`（非 auto-pool） | 拉起任务前后可手动调用 `SingleURLWorker.ProcessOnce`（如需预处理单链任务，可在业务侧调用）。|

`--fetch-tasks-limit` 同时决定了单链 worker 每轮的抓取上限。想要暂停该能力，可将任务表中 `Scene=单个链接采集` 的数量保持为 0，或显式关闭自定义 worker。

## 关联文件

- 代码：`pool/single_url_worker.go`（核心逻辑）、`pool/single_url_client.go`（HTTP 客户端）、`pool/tasks.go`（Scene 优先级）、`biz/fox/cli/run_search.go`（CLI 入口）。
- 配置：`docs/ENVIRONMENT.md` 中的 `CRAWLER_SERVICE_BASE_URL`、`COOKIE_BITABLE_URL`、`TASK_FIELD_BOOKID`、`TASK_FIELD_URL` 等说明。
- 下载服务：`/Users/debugtalk/MyProjects/ByteDance/content_web_crawler`（`routes/download_routes.py` + `services/download_service.py`），`JobStatusManager` 会把 job 状态持久化在 Redis。
