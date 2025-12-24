# TaskAgent 飞书 API 使用文档

本文档总结了 TaskAgent 中使用的飞书多维表格（Bitable）API，并解释了对应的代码入口、鉴权方式、分页策略以及频控/错误处理技巧。

## 鉴权与基础配置
- TaskAgent 通过 `internal/feishusdk.Client` 统一封装鉴权，所有请求都会使用 `.env` 中的 `FEISHU_APP_ID` / `FEISHU_APP_SECRET` 获取 tenant access token，可选 `FEISHU_TENANT_KEY` / `FEISHU_BASE_URL`。
- `internal/env.Ensure` 会自动加载 `.env`，无需在命令行手动 `export`（但在 `go test` 下默认不会加载；如需在测试里启用，设置 `GOTEST_LOAD_DOTENV=1`）。
- 结果表写入存在全局限速器（`FEISHU_REPORT_RPS`，默认 1 RPS），位于 `internal/feishusdk/storage.go`，用来避免频繁触发 99991400 频控错误。

## 分页、筛选与重试策略
- `Client.listBitableRecords` 封装了分页逻辑：传入 `page_size`（TaskAgent 默认 200）、`page_token`，并在 `has_more` 为真时继续抓取。
- 查询接口统一支持 filter/sort/view 参数，`internal/feishusdk/filters.go` 提供了构建过滤表达式的 helpers。
- **筛选参数填写说明（官方）**: [记录筛选参数填写说明](https://open.larkoffice.com/document/docs/bitable-v1/app-table-record/record-filter-guide)
- 所有请求都会带上 `context.Context`，设备离线或 CLI 退出时可立即取消未完成的 HTTP 调用。
- 网络抖动或 5xx 会由上层调用（如 `internal/storage/sqlite_reporter.go`）捕获并记录，失败记录设置 `reported=-1`，下次重试。

## 1. 新增记录 (Create Record)

- **功能**: 向指定数据表中添加单条记录。
- **官方文档**: [新增记录](https://open.larkoffice.com/document/server-docs/docs/bitable-v1/app-table-record/create)
- **HTTP 方法**: `POST`
- **请求路径**: `/open-apis/bitable/v1/apps/:app_token/tables/:table_id/records`
- **关键请求参数**:
    - `fields`: (Map) 记录的字段内容，Key 为字段名，Value 为字段值。
- **关键响应数据**:
    - `record`: (Object) 创建成功的记录信息，包含 `record_id` 和 `fields`。
- **接口频率限制**: 50 次/秒（受全局 RPS 限制器保护）
- **代码实现**: `Client.CreateBitableRecord` (内部方法), `Client.CreateTaskRecord`, `Client.CreateResultRecord`
- **示例**:
  - SDK（推荐，带 `.env` 自动加载与 schema 映射）：`examples/create_task_with_sdk`
  - 纯 HTTP（标准库，无 SDK 依赖）：`examples/create_task_with_http`（注意该示例通过 `os.Getenv` 读取环境变量，不会自动加载 `.env`）

## 2. 更新记录 (Update Record)

- **功能**: 更新指定数据表中的单条记录。
- **官方文档**: [更新记录](https://open.larkoffice.com/document/server-docs/docs/bitable-v1/app-table-record/update)
- **HTTP 方法**: `PUT`
- **请求路径**: `/open-apis/bitable/v1/apps/:app_token/tables/:table_id/records/:record_id`
- **关键请求参数**:
    - `fields`: (Map) 需要更新的字段内容。
- **关键响应数据**:
    - `record`: (Object) 更新后的记录信息。
- **接口频率限制**: 50 次/秒
- **代码实现**: `Client.updateBitableRecord` (内部方法), `Client.UpdateTaskFields`, `Client.UpdateTaskStatus`

## 3. 查询记录 (Search Record)

- **功能**: 根据条件查询数据表中的记录。
- **官方文档**: [查询记录](https://open.larkoffice.com/document/docs/bitable-v1/app-table-record/search)
- **HTTP 方法**: `POST`
- **请求路径**: `/open-apis/bitable/v1/apps/:app_token/tables/:table_id/records/search`
- **关键请求参数**:
    - `view_id`: (String) 视图 ID，可选。
    - `filter`: (Object) 筛选条件，可选。
    - `sort`: (Array) 排序条件，可选。
    - `page_size`: (Integer) 分页大小，默认 20，最大 500。
    - `page_token`: (String) 分页标记。
- **关键响应数据**:
    - `items`: (Array) 记录列表。
    - `has_more`: (Boolean) 是否还有更多记录。
    - `page_token`: (String) 下一页的分页标记。
    - `total`: (Integer) 总记录数。
- **接口频率限制**: 20 次/秒；配合 `page_token` 逐页拉取
- **代码实现**: `Client.listBitableRecords` (内部方法), `Client.FetchTaskTable`, `Client.FetchBitableRows`

## 4. 新增多条记录 (Batch Create Records)

- **功能**: 向指定数据表中批量添加记录。
- **官方文档**: [新增多条记录](https://open.larkoffice.com/document/server-docs/docs/bitable-v1/app-table-record/batch_create)
- **HTTP 方法**: `POST`
- **请求路径**: `/open-apis/bitable/v1/apps/:app_token/tables/:table_id/records/batch_create`
- **关键请求参数**:
    - `records`: (Array) 记录列表，每项包含 `fields`。
- **关键响应数据**:
    - `records`: (Array) 创建成功的记录列表，包含 `record_id`。
- **接口频率限制**: 50 次/秒
- **代码实现**: `Client.batchCreateBitableRecords` (内部方法), `Client.CreateTaskRecords`, `Client.CreateResultRecords`

## 5. 更新多条记录 (Batch Update Records)

- **功能**: 批量更新指定数据表中的记录。
- **官方文档**: [更新多条记录](https://open.larkoffice.com/document/server-docs/docs/bitable-v1/app-table-record/batch_update)
- **HTTP 方法**: `POST`
- **请求路径**: `/open-apis/bitable/v1/apps/:app_token/tables/:table_id/records/batch_update`
- **关键请求参数**:
    - `records`: (Array) 记录列表，每项需包含 `record_id` 和需要更新的 `fields`。
- **关键响应数据**:
    - `records`: (Array) 更新后的记录列表。
- **接口频率限制**: 50 次/秒
- **代码实现**: `Client.BatchUpdateBitableRecords`

## 6. 批量获取记录 (Batch Get Records)

- **功能**: 根据 record_id 列表批量获取记录详情。
- **官方文档**: [批量获取记录](https://open.larkoffice.com/document/docs/bitable-v1/app-table-record/batch_get)
- **HTTP 方法**: `POST`
- **请求路径**: `/open-apis/bitable/v1/apps/:app_token/tables/:table_id/records/batch_get`
- **关键请求参数**:
    - `record_ids`: (Array) 需要获取的记录 ID 列表。
    - `user_id_type`: (String) 用户 ID 类型，可选。
- **关键响应数据**:
    - `records`: (Array) 获取到的记录列表。
    - `absent_record_ids`: (Array) 未找到的记录 ID。
    - `forbidden_record_ids`: (Array) 无权限的记录 ID。
- **接口频率限制**: 20 次/秒
- **代码实现**: `Client.BatchGetBitableRecords`, `Client.getBitableRecord` (内部方法，基于此接口封装)

## 错误处理与排查
- **鉴权失败**：确认 `.env` 中 `FEISHU_APP_ID`、`FEISHU_APP_SECRET`、`FEISHU_TENANT_KEY` 是否正确，或刷新凭证。
- **频控 (99991400)**：调低 `FEISHU_REPORT_RPS`、增加 `RESULT_REPORT_POLL_INTERVAL`、或减小 `RESULT_REPORT_BATCH`。
- **字段不匹配**：检查 `TASK_FIELD_* / RESULT_FIELD_* / DEVICE_FIELD_*` 是否覆盖为自定义列名，并调用 `feishusdk.RefreshFieldMappings()` 重新加载。
- **分页缺失**：确保消费 `has_more` 和 `page_token`，TaskAgent 的 helper 已自动完成，但自定义脚本需自行循环。

## 代码入口速查表

| 功能 | 文件 | 函数 |
| --- | --- | --- |
| 获取 Client | `internal/feishusdk/client.go` | `NewClientFromEnv` |
| 任务表 CRUD | `internal/feishusdk/bitable.go` | `CreateTaskRecord(s)`, `UpdateTaskStatus`, `FetchTaskTable`, `FetchBitableRows` |
| 结果表写入 | `internal/feishusdk/storage.go` | `NewResultStorage`, `CreateResultRecord`, RPS 限速器 |
| 设备表写入 | `internal/feishusdk/device_info.go` | `UpsertDevice`, `DeviceFieldsFromEnv` |
| 设备任务表 | `internal/devrecorder/recorder.go` / `internal/storage/storage.go` | 通过 `DEVICE_TASK_BITABLE_URL` 记录派发历史 |

更多字段/环境变量说明见 [`ENVIRONMENT.md`](ENVIRONMENT.md)。
