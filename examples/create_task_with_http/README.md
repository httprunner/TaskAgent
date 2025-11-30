# 飞书 Bitable 任务 HTTP 创建示例

该示例演示如何在不依赖官方 SDK 的情况下，通过纯 HTTP 请求在任务表中创建一条 `Status=pending` 的记录，方便 TaskAgent 随后自动调度执行。示例代码位于 `examples/create_task_with_http/main.go`。

## 功能亮点

- **轻量客户端**：只依赖 Go 标准库即可完成鉴权、请求发送与错误处理。
- **Token 缓存**：自动复用 2 小时有效期的 Tenant Access Token，减少重复鉴权。
- **URL 解析**：兼容 base 与 wiki 两种格式的 Bitable 链接，并支持 wiki→bitable token 自动转换。
- **字段白名单**：默认写入 `App / Scene / Params / Status`，可按需追加 `UserID / UserName / Extra / DeviceSerial` 等字段。

## 运行前准备

### 必需环境变量

| 变量名 | 说明 |
| --- | --- |
| `FEISHU_APP_ID` | 飞书应用 ID，用于获取租户级 Token |
| `FEISHU_APP_SECRET` | 飞书应用密钥 |
| `TASK_BITABLE_URL` | 任务表链接，支持 `https://.../base/{app_token}?table={table_id}` 或 wiki 链接 |

可选：`FEISHU_BASE_URL`（默认为 `https://open.feishu.cn`）。

### 命令行参数

| 参数 | 必填 |
| --- | --- |
| `-aid` | 是 |
| `-eid` | 是 |

## 快速开始

```bash
export FEISHU_APP_ID="your_app_id"
export FEISHU_APP_SECRET="your_app_secret"
export TASK_BITABLE_URL="https://bytedance.larkoffice.com/wiki/xxx?table=tblxxx"

go run ./examples/create_task_with_http/main.go -aid your_aid -eid your_eid
# 输出: Created task record rectxxxxxxxxxxxxxxx
```

默认会创建如下字段：

- `App`: `com.smile.gifmaker`
- `Scene`: `单个视频录屏采集`
- `Status`: `pending`
- `Params`: `{"type":"auto_additional_crawl","aid":"your_aid","eid":"your_eid"}`

## 请求流程摘要

1. **获取 Tenant Access Token**：`POST /open-apis/auth/v3/tenant_access_token/internal`，请求体包含 `app_id` 与 `app_secret`。
2. **解析 Bitable URL**：对 base 链接直接提取 `app_token`，对 wiki 链接先调用 `GET /open-apis/wiki/v2/spaces/get_node?token=...` 再取 `obj_token`。
3. **创建记录**：`POST /open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records`，`fields` 中至少包含 `App`、`Scene`、`Params`、`Status(pending)`。

示例请求体：

```json
{
  "fields": {
    "App": "com.smile.gifmaker",
    "Scene": "单个视频录屏采集",
    "Params": "{\"type\":\"auto_additional_crawl\",\"aid\":\"xxx\",\"eid\":\"xxx\"}",
    "Status": "pending"
  }
}
```

## 字段说明

| 字段 | 是否必填 | 说明 |
| --- | --- | --- |
| `App` | 是 | 任务目标应用，示例固定为 `com.smile.gifmaker` |
| `Scene` | 是 | 采集场景描述 |
| `Params` | 是 | JSON 字符串，TaskAgent 会解析并执行业务逻辑 |
| `Status` | 是 | 初始状态必须为 `pending` |
| `UserID` / `UserName` / `Extra` / `DeviceSerial` | 否 | 视业务需要在 `TaskRecordInput` 中赋值即可写入 |

## 批量写入（可选）

若需要一次提交多条记录，可调用 `POST /records/batch_create`，请求体中提供 `records: [{"fields": ...}]`。官方建议每批不超过 50 条并加上限频控制。

## 常见错误与排查

| 状态码 | 错误码 | 处理建议 |
| --- | --- | --- |
| 400 | 100001 | `fields` 缺失或 JSON 非法，检查参数及必填字段 |
| 401 | 99991663 | Token 失效或权限不足，重新获取 Token 并确认应用对目标表拥有写权限 |
| 404 | 99991400 | `app_token` / `table_id` 不存在，核对 Bitable 链接 |
| 429 | varies | 触发限频，降低 QPS 或使用批量接口 |

## 注意事项

- Tenant Access Token 有效期约 7200 秒，示例已在客户端内缓存，避免频繁请求。
- `Params` 必须是合法的 JSON 字符串，否则 TaskAgent 在消费记录时会报错。
- 创建任务时务必保持 `Status = pending`，后续由 TaskAgent 流程自动更新为 `dispatched → running → success/failed`。
