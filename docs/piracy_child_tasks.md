# 盗版检测子任务创建与 Webhook 机制

本文档描述盗版检测工作流中子任务的创建逻辑和 Webhook 触发机制。

## 1. 概述

当「综合页搜索」任务完成盗版初筛后，会根据检测结果自动创建子任务。子任务的类型由视频详情数据动态决定，同一盗版线索的所有子任务共享 GroupID，Webhook 在同组所有任务成功后触发。

## 2. 子任务类型

| 场景 | 创建条件 | Params 值 |
|------|----------|-----------|
| 个人页搜索 | 每个盗版线索必创建 | 短剧名称（与父任务相同） |
| 合集视频采集 | 视频 Tags 包含「合集」或「短剧」 | 视频 ItemID |
| 视频锚点采集 | 视频 AnchorPoint 包含 appLink | appLink (kwai:// URL) |

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
│  ReportMatchesWithChildTasks    │
│  - 根据视频详情创建子任务         │
│  - 分配 GroupID                  │
└─────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│  子任务 (GroupID=123_1)                                  │
│  ├─ 个人页搜索 (Params=短剧X)                            │
│  ├─ 合集视频采集 (Params=3xz7ghufhmggzfy) [可选]         │
│  └─ 视频锚点采集 (Params=kwai://...) [可选, 可多个]      │
└─────────────────────────────────────────────────────────┘
```

## 4. GroupID 格式

```
{parentTaskID}_{index}
```

- `parentTaskID`: 父任务（综合页搜索）的 TaskID
- `index`: 盗版线索序号，从 1 开始

示例：父任务 TaskID=123，检测到 2 个盗版线索
- 第 1 个线索的子任务 GroupID: `123_1`
- 第 2 个线索的子任务 GroupID: `123_2`

## 5. 视频详情获取

### 5.1 数据源优先级

1. **SQLite（优先）**: 本地 `capture_results` 表，数据采集后立即可用
2. **飞书多维表格（fallback）**: 当 sqlite 查询失败或返回空时使用

> 优先使用 sqlite 的原因：飞书多维表格的上报是异步的且有频率控制，采集完成后数据可能还未上报完成，导致读取不到或读取不完整。

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
for idx, detail := range matchDetails {
    groupID := fmt.Sprintf("%d_%d", parentTaskID, idx+1)

    // 1. 必创建：个人页搜索
    createTask("个人页搜索", detail.Params, groupID)

    // 2. 条件创建：合集视频采集
    if itemID := FindFirstCollectionVideo(detail.Videos); itemID != "" {
        createTask("合集视频采集", itemID, groupID)
    }

    // 3. 条件创建：视频锚点采集（可能多个）
    for _, video := range detail.Videos {
        if appLink := ExtractAppLink(video.AnchorPoint); appLink != "" {
            createTask("视频锚点采集", appLink, groupID)
        }
    }
}
```

**去重规则**:
- 合集视频采集：同一 GroupID 只创建 1 个（取第一个匹配的视频）
- 视频锚点采集：按 appLink 去重

## 7. Webhook 触发机制

### 7.1 触发条件

Webhook 仅在以下条件全部满足时触发：

1. 任务状态为 `success`
2. Webhook 状态为 `pending`
3. 同一 GroupID 下**所有任务**状态均为 `success`

### 7.2 处理流程

```
WebhookWorker.processOnce()
    │
    ▼
查询 Status=success & Webhook=pending 的任务
    │
    ▼
按 GroupID 分组
    │
    ▼
┌─────────────────────────────────┐
│  对每个 GroupID:                 │
│  1. 查询该 GroupID 下所有任务     │
│  2. 检查是否全部 success         │
│  3. 若是，发送 Webhook           │
│  4. 更新 Webhook 状态            │
└─────────────────────────────────┘
```

### 7.3 GroupID 为空的任务

若任务无 GroupID（旧任务或独立任务），按原有逻辑单独触发 Webhook。

## 8. 任务表字段

子任务创建时设置的字段：

| 字段 | 值 |
|------|-----|
| App | 继承父任务 |
| Scene | 个人页搜索 / 合集视频采集 / 视频锚点采集 |
| Params | 根据场景类型设置 |
| UserID | 继承盗版线索的用户 ID |
| UserName | 继承盗版线索的用户名 |
| GroupID | `{parentTaskID}_{index}` |
| Datetime | 继承父任务 |
| Status | pending |
| Webhook | pending |
| Extra | ratio=xx.xx%（仅个人页搜索） |

## 9. 代码入口

| 组件 | 文件 | 函数/方法 |
|------|------|----------|
| 检测+获取视频详情 | pkg/piracy/reporter.go | `DetectMatchesWithDetails` |
| 创建子任务 | pkg/piracy/reporter.go | `ReportMatchesWithChildTasks` |
| 工作流调度 | biz/fox/search/piracy_workflow.go | `handleGeneralSearch` |
| Webhook 处理 | pkg/piracy/webhook_worker.go | `processOnce` |
| 辅助函数 | pkg/piracy/helpers.go | `FindFirstCollectionVideo`, `ExtractAppLink` |

## 10. 飞书表配置

需在任务表中添加 `GroupID` 字段（文本类型）用于任务分组。
