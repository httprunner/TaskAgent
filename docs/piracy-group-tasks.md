# 盗版检测子任务创建与 Webhook 机制

本文档描述盗版检测工作流中子任务的创建逻辑。Webhook 触发与分发流程详见 `docs/webhook-worker.md`。

## 1. 概述

当「综合页搜索」任务完成盗版初筛后，会根据检测结果自动创建子任务。子任务的类型由视频详情数据动态决定，同一盗版线索（AID = App + BookID + UserID）的所有子任务共享 GroupID，Webhook 在同组所有任务成功后触发。

## 2. 子任务类型

| 场景 | 创建条件 | 主要字段写入 |
|------|----------|----------------|
| 个人页搜索 | 每个盗版线索必创建 | `Params`=短剧名称、`Extra`=ratio=xx.xx%、`Status`=`pending`、`Webhook`=`pending` |
| 合集视频采集 | 视频 Tags 包含「合集」或「短剧」 | `Params`=短剧名称、`ItemID`=匹配视频 ID、`Status`/`Webhook`留空，等待后续调度更新 |
| 视频锚点采集 | 视频 AnchorPoint 包含 appLink | `Params`=短剧名称、`Extra`=appLink、`Status`/`Webhook`留空 |

## 3. 数据流程

```
综合页搜索任务 (TaskID=123)
    │
    ▼
┌─────────────────────────────────┐
│  DetectMatchesWithDetails       │
│  - 执行盗版初筛                  │
│  - 获取视频详情 (ItemID, Tags,   │
│    AnchorPoint)                 │
└─────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────┐
│  CreateGroupTasksForPiracyMatches    │
│  - 根据视频详情创建子任务         │
│  - 分配 GroupID                  │
└─────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│  子任务 (GroupID=快手_B1234_U5678)                      │
│  ├─ 个人页搜索 (Params=短剧X)                            │
│  ├─ 合集视频采集 (Params=短剧X, ItemID=3xz7ghufhmggzfy) [可选] │
│  └─ 视频锚点采集 (Params=短剧X, Extra=kwai://...) [可选, 可多个] │
└─────────────────────────────────────────────────────────┘
    │
    ▼
Webhook worker (见 `docs/webhook-worker.md`) 监控 GroupID，全部任务
`Status=success` 后触发一次 webhook。
```

## 4. GroupID 格式

```
{MapAppValue(App)}_{BookID}_{UserID}
```

- `MapAppValue(App)`: 将包名映射成可读平台名（实现见 `pkg/feishu/fields`），例如 `com.smile.gifmaker → 快手`
- `BookID`: 短剧 ID
- `UserID`: 盗版账号 ID

同一 AID 只允许存在一个 GroupID；若检测到重复 AID，会跳过创建以避免重复子任务。

## 5. 视频详情获取

### 5.1 数据源优先级

1. **SQLite（优先）**: 本地 `capture_results` 表，数据采集后立即可用，查询条件 = `Params` + `UserID`，延迟最小。
2. **飞书多维表格（fallback）**: 当 SQLite 查询失败或结果为空时，使用 `feishu.FetchBitableRows` 再次尝试。

> SQLite 写入与飞书上报解耦：`pkg/storage/sqlite_reporter` 异步推送 `RESULT_BITABLE_URL`，因此即时数据一定先出现在 SQLite。保持数据库在线即可避免“刚采集完但飞书尚未可见”的窗口。

### 5.2 数据结构

```go
type VideoDetail struct {
    ItemID      string // 视频 ID
    Tags        string // 视频标签（合集/短剧/空值）
    AnchorPoint string // 锚点信息 JSON，可能包含 appLink
}
```

**查询条件**: `Params = 短剧名称 AND UserID = 用户ID`

**AnchorPoint 格式示例**:
```json
{
    "appLink": "kwai://episode/play?serialType=1&serialId=11962550&..",
    "other": "..."
}
```

## 6. 子任务创建逻辑

```go
for _, detail := range matchDetails {
    groupID := fmt.Sprintf("%s_%s_%s", MapAppValue(detail.Match.App), bookID, detail.Match.UserID)
    params := detail.Match.DramaName
    userID := detail.Match.UserID
    userName := detail.Match.UserName

    // 1. 必创建：个人页搜索（带 ratio 信息，初始状态 pending）
    createTask(TaskInput{
        Scene:    "个人页搜索",
        Params:   params,
        Extra:    fmt.Sprintf("ratio=%.2f%%", detail.Match.Ratio*100),
        Status:   "pending",
        Webhook:  "pending",
        GroupID:  groupID,
        UserID:   userID,
        UserName: userName,
    })

    // 2. 条件创建：合集视频采集（只写 ItemID，状态留空）
    if itemID := FindFirstCollectionVideo(detail.Videos); itemID != "" {
        createTask(TaskInput{
            Scene:  "合集视频采集",
            Params: params,
            ItemID: itemID,
            GroupID: groupID,
            UserID: userID,
            UserName: userName,
        })
    }

    // 3. 条件创建：视频锚点采集（Extra 保存 appLink，状态留空）
    for _, video := range detail.Videos {
        if appLink := ExtractAppLink(video.AnchorPoint); appLink != "" {
            createTask(TaskInput{
                Scene:  "视频锚点采集",
                Params: params,
                Extra:  appLink,
                GroupID: groupID,
                UserID: userID,
                UserName: userName,
            })
        }
    }
}
```

**去重规则**:
- AID（App+BookID+UserID）去重：若同一组合已写入过子任务，本轮检测会跳过，避免重复 GroupID。
- 合集视频采集：同一 GroupID 只创建 1 个（取第一个匹配的视频）
- 视频锚点采集：按 appLink 去重，同一锚点不重复建任务

## 7. 任务表字段

子任务创建时设置的字段：

| 字段 | 值 |
|------|-----|
| App | 继承父任务 |
| Scene | 个人页搜索 / 合集视频采集 / 视频锚点采集 |
| Params | 始终沿用父任务短剧名称 |
| UserID | 继承盗版线索的用户 ID |
| UserName | 继承盗版线索的用户名 |
| GroupID | `{MapAppValue(App)}_{BookID}_{UserID}` |
| Datetime | 继承父任务 |
| Status | 个人页搜索=`pending`；其他场景留空等待调度后更新 |
| Webhook | 个人页搜索=`pending`；其他场景留空 |
| Extra | 个人页搜索=ratio=xx.xx%；视频锚点采集=appLink；合集视频为空 |

## 8. 代码入口

| 组件 | 文件 | 函数/方法 |
|------|------|----------|
| 检测+获取视频详情 | pkg/piracy/reporter.go | `DetectMatchesWithDetails` |
| 创建子任务 | pkg/piracy/reporter.go | `CreateGroupTasksForPiracyMatches` |
| 工作流调度 | biz/fox/search/piracy_workflow.go | `handleGeneralSearch` |
| Webhook 处理 | pkg/piracy/webhook_worker.go | `processOnce`、`handleGroupRow` |
| SQLite/飞书查询 | pkg/piracy/sqlite_source.go / pkg/feishu/bitable.go | `FetchFromSQLite`, `FetchBitableRows` |
| 辅助函数 | pkg/piracy/helpers.go | `FindFirstCollectionVideo`, `ExtractAppLink` |

## 9. 飞书表配置

需在任务表中添加 `GroupID` 字段（文本类型）用于任务分组。

## 10. 端到端示例

| 步骤 | 输入/状态 | 说明 |
| --- | --- | --- |
| 1 | 综合页搜索任务 TaskID=123，Params="短剧A"，用户=U1 | 任务完成后将结果写入 SQLite + Feishu。 |
| 2 | `DetectMatchesWithDetails` 返回 2 条嫌疑线索 (ItemID=I1/I2) | 同时包含 Tags、AnchorPoint。 |
| 3 | `CreateGroupTasksForPiracyMatches` 生成 | group `123_1`: 个人页搜索 + 合集视频(I1) + 两个锚点任务；group `123_2`: 个人页搜索 + 合集视频(I2)；appLink 重复会被丢弃。 |
| 4 | 设备执行子任务 | `TaskLifecycle` 驱动状态从 pending→dispatched→running→success。 |
| 5 | Webhook worker (`cmd/piracy webhook-worker`) 轮询 | 发现 GroupID=`123_1` 下所有任务 `Status=success` 且 `Webhook` 仍为 pending。 |
| 6 | `sendGroupWebhook` 汇总 SQLite/Feishu 结果 | `RecordLimit=-1`，推送完整线索；成功后整组 `Webhook=success`，失败则 `Webhook=failed` 并等待下一轮重试。 |

这样一来，同一盗版线索只会触发一次 webhook，且数据来源优先使用 SQLite 保证实时性。
