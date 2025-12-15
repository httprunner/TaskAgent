# 飞书 Bitable 任务 SDK 创建示例

该示例演示如何使用 TaskAgent 提供的 Feishu 客户端（`taskagent.NewFeishuClientFromEnv`）在任务表中创建一条 `Status=pending` 的任务记录。示例代码位于 `examples/create_task_with_sdk/main.go`。

## 适用场景

- 你希望复用 TaskAgent 的 Bitable URL 解析（含 wiki→app_token）、字段映射（`TASK_FIELD_*`）与错误处理，而不是手写 HTTP 调用。
- 你需要在外部系统中创建 `Scene=视频录屏采集` 等任务，交给 TaskAgent 后续调度/回写状态。

## 运行前准备

### 必需环境变量

该示例会通过 `internal/env.Ensure` 自动加载 `.env`（无需手动 `export`，当然也支持导出环境变量）。

| 变量名 | 说明 |
| --- | --- |
| `FEISHU_APP_ID` | 飞书应用 ID |
| `FEISHU_APP_SECRET` | 飞书应用密钥 |
| `TASK_BITABLE_URL` | 任务表链接（base 或 wiki 链接均可） |

可选：`FEISHU_BASE_URL`（默认为 `https://open.feishu.cn`）。

### 命令行参数

| 参数 | 必填 |
| --- | --- |
| `-bid` | 是 |
| `-uid` | 是 |
| `-eid` | 是 |
| `-table` | 否（默认读取 `TASK_BITABLE_URL`） |
| `-timeout` | 否（默认 `30s`） |

## 快速开始

```bash
go run ./examples/create_task_with_sdk/main.go -bid your_bid -uid your_uid -eid your_eid
# 输出: Created task record rectxxxxxxxxxxxxxxx
```

默认写入字段（可按需在代码中调整）：

- `App`: `com.smile.gifmaker`
- `Scene`: `视频录屏采集`
- `Status`: `pending`
- `BookID`: `your_bid`
- `UserID`: `your_uid`
- `ItemID`: `your_eid`
- `GroupID`: `快手_{BookID}_{UserID}`
- `Extra`: `auto_additional_crawl`

> 说明：该示例主要展示 TaskAgent SDK 的创建流程；如需携带额外上下文，建议使用任务表的业务字段（例如 `Extra`）或按你们的 schema 扩展字段。
