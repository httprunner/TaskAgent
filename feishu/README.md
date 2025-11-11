# Feishu 模块

`feishu/` 包含了 AnyGrab 与飞书生态的集成能力，当前涵盖：

- 获取租户级访问令牌、统一发送开放平台请求（`client.go`）。
- 操作多维表格（Bitable）任务表、结果表，包括列表、创建、更新状态等（`bitable.go`）。
- 解析并写入飞书多维表格/电子表格链接（`spreadsheet.go`）。
- 单元测试与可选的真实环境验证（`feishu_test.go`）。

## 环境变量

| 变量 | 说明 |
| --- | --- |
| `FEISHU_APP_ID` | 飞书开放平台应用 App ID（必需） |
| `FEISHU_APP_SECRET` | 飞书开放平台应用 App Secret（必需） |
| `FEISHU_TENANT_KEY` | 企业自建应用租户 key，可选 |
| `FEISHU_BASE_URL` | 自定义开放平台域名，默认 `https://open.feishu.cn` |
| `FEISHU_LIVE_TEST` | 置为 `1` 可启用真实 API 测试（需可访问的 Bitable） |

建议在仓库根目录创建 `.env` 并通过 `godotenv` 自动加载。

## 多维表格任务表

`TargetFields`/`TargetRecordInput` 对应任务调度表（包含 TaskID、App、Scene、Status 等）。常见流程：

- `DeviceSerial`：指定任务的目标设备，仅该序列号可以在设备池中获取该任务；若留空则由任意空闲设备领取。
- `DispatchedDevice`：记录实际领取并执行任务的设备序列号，`pool.DevicePoolAgent` 在派发/回调时会自动填写。

```go
client, _ := feishu.NewClientFromEnv()
ctx := context.Background()
recordID, err := client.CreateTargetRecord(ctx, taskTableURL, feishu.TargetRecordInput{
    TaskID:  123,
    Params:  `{"keyword":"jay"}`,
    App:     "netease",
    Scene:   "batch",
    Status:  "pending",
    User:    "scheduler",
})
```

也可使用 `FetchTargetTable` 读取全部行，并通过 `UpdateTargetStatus`/`UpdateTargetStatuses` 批量更新状态。`TargetFields` 支持自定义列名（`override *TargetFields`）。

## 采集结果记录表

根据需求新增的结果表（表 ID `tblzoZuR6aminfye`）由以下字段组成：

- Datetime（毫秒时间戳或可解析时间字符串）
- DeviceSerial / App / Scene / Params
- ItemID / ItemCaption / ItemCDNURL / ItemURL / ItemDuration（秒）
- UserName / UserID / Tags
- TaskID
- PayloadJSON（完整资源信息 JSON）

API 使用 `ResultFields` / `ResultRecordInput`：

```go
durationSeconds := 180.0
resID, err := client.CreateResultRecord(ctx, resultTableURL, feishu.ResultRecordInput{
    DeviceSerial: "dev-001",
    App:          "douyin",
    Scene:        "ugc",
    Params:       `{"task":42}`,
    ItemID:       "vid123",
    ItemCaption:  "示例标题",
    ItemCDNURL:      "https://cdn.example.com/vid123.mp4",
    ItemURL:      "https://www.kuaishou.com/short-video/vid123",
    // 可选：新增 ItemDuration，单位为秒
    ItemDurationSeconds: &durationSeconds,
    UserName:     "作者",
    UserID:       "user123",
    Tags:         "热门,音乐",
    TaskID:       123456,
    PayloadJSON: map[string]any{
        "duration": 180,
        "metrics":  map[string]int{"likes": 123},
    },
})
```

`Datetime` 可通过：

- `DatetimeRaw`: 传入可解析的字符串或毫秒/秒时间戳。
- `Datetime`: 直接传入 `*time.Time`，SDK 自动转换为 UTC 毫秒值。

`PayloadJSON` 支持字符串、`[]byte`、`json.RawMessage` 或任意 Go 结构体；内部会校验/序列化成字符串。

### Result storage helper

`feishu.NewResultStorageFromEnv` 读取 `RESULT_BITABLE_URL`、Feishu 凭证等环境变量，若值有效则返回可直接调用 `Write(ctx, feishu.ResultRecord)` 的存储辅助实例。`feishu.ResultRecord` 只需要传入采集时间戳、`DeviceSerial`、`App`、`Query`、`TaskID` 与 `feishu.VideoData`（包括 `CacheKey`/`VideoID`/`URL`/`ShareLink`/`UserName` 等）即可，内部会自动填充 `Scene`、`ItemID`、`ItemURL` 等字段并推送到多维表格。

## 测试

- 默认单元测试：`go test ./feishu`
- 启用真实 API（需 `.env` 与目标多维表格访问权限）：

```bash
FEISHU_LIVE_TEST=1 go test ./feishu -run Live
```

`TestResultRecordCreateLive` 会向结果表写入一行实际数据并回读验证；`TestTargetRecordLifecycleLive` 会创建/更新任务表中的记录。

## 常见链接

- 任务表（读写）示例：`https://bytedance.larkoffice.com/wiki/DKKwwF9XRincITkd0g1c6udUnHe?table=tblLUmsGgp5SECWF`
- 采集结果表：`https://bytedance.larkoffice.com/wiki/DKKwwF9XRincITkd0g1c6udUnHe?table=tblzoZuR6aminfye&view=vewTF27mJQ`

在新环境中部署时，只需将上述链接替换为对应租户的多维表格地址，或通过 `override` 参数调整列名，即可复用该模块。
