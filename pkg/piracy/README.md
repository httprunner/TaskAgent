# 盗版检测包（pkg/piracy）

提供短剧盗版检测与（可选）上报的核心能力：
- **检测引擎**：比对采集结果（Feishu/SQLite）与剧单总时长，找出超阈值可疑组合
- **上报封装**：将可疑结果写回飞书任务状态表，或生成 webhook/CSV 汇总

## 能力概览
- 核心检测：`Detect` / `DetectCommon` / `analyzeRows`
- Reporter：`NewReporter` 读取环境配置，封装检测 + 写表
- 数据源支持：
  - Feishu Bitable（结果表、剧单表）
  - 本地 SQLite（采集结果、剧单镜像）
- Webhook 汇总：`SendSummaryWebhook` 支持 Feishu / SQLite 混合取数

## 主要特性
- 自动检测：聚合 `(Params, UserID)` 时长，对比剧单全剧时长
- 阈值可配：默认 0.5（50%），环境变量 `THRESHOLD` 或代码传参覆盖
- 飞书集成：结果表/剧单表/任务状态表字段均可通过环境覆盖
- 灵活取数：支持 Feishu/SQLite 双源，SQLite 优先（如本地镜像）
- 批量处理：`auto` 场景可遍历全剧单
- 汇总输出：支持 CSV、Webhook（含采集记录与剧单元数据）

## 架构要点

### 核心类型

```go
// Config - Configuration for piracy detection
type Config struct {
    ParamsField   string  // Field name for drama parameters
    UserIDField   string  // Field name for user ID
    DurationField string  // Field name for video duration
    Threshold     float64 // Ratio threshold (default 0.5 = 50%)
    // ... other field mappings
}

// Match - Represents a suspicious Params+UserID combination
type Match struct {
    Params        string  // Drama name/params
    UserID        string  // User ID
    UserName      string  // User name
    SumDuration   float64 // Total duration of user's videos
    TotalDuration float64 // Original drama total duration
    Ratio         float64 // SumDuration/TotalDuration ratio
    RecordCount   int     // Number of video records
}

// Report - Aggregated detection results
type Report struct {
    Matches       []Match  // Suspicious combinations
    ResultRows    int      // Number of result rows processed
    TaskRows    int      // Number of target rows processed
    MissingParams []string // Params without corresponding drama data
    Threshold     float64  // Applied threshold
}
```

### 组件分层

1. 检测引擎（`detect.go`）
   - `Detect`：Feishu 取数 + 分析
   - `DetectCommon` / `analyzeRows`：核心聚合与比对
   - `ApplyDefaults`：字段/阈值默认配置

2. Reporter（`reporter.go`）
   - `NewReporter`：环境变量驱动的 Reporter
   - `DetectWithFilters` / `ReportMatches`：检测 + 条件写表

3. 工具（`helpers.go` 等）
   - 字段解析、Filter 组合、SQLite 取数 (`sqlite_source.go` / `task_params.go`)

## 使用示例

### 单次检测（Feishu 结果表 + 剧单表）

```go
import "github.com/httprunner/TaskAgent/pkg/piracy"

// Configure detection options
opts := piracy.Options{
    ResultTable: piracy.TableConfig{
        URL:    "https://example.feishu.cn/base/xxx",
        Filter: `{"conjunction":"and","conditions":[{"field_name":"Params","operator":"is","value":["短剧名称"]}]}`,
    },
    DramaTable: piracy.TableConfig{
        URL: "https://example.feishu.cn/base/yyy",
    },
    Config: piracy.Config{
        Threshold: 0.5, // 50% threshold
    },
}

// Run detection
report, err := piracy.Detect(ctx, opts)
if err != nil {
    return err
}

// Process results
for _, match := range report.Matches {
    fmt.Printf("Suspicious: %s by %s (ratio: %.2f)\n",
        match.Params, match.UserID, match.Ratio)
}
```

### 通过 Reporter 进行检测并上报

```go
// Create reporter (reads config from environment)
reporter := piracy.NewReporter()
if !reporter.IsConfigured() {
    log.Fatal("Reporter not configured")
}

// Report piracy for specific drama params
params := []string{"短剧A", "短剧B", "短剧C"}
err := reporter.ReportPiracyForParams(ctx, "com.smile.gifmaker", params)
if err != nil {
    return err
}

```

### 发送 Webhook 汇总

`SendSummaryWebhook` aggregates drama metadata together with capture records (from either Feishu Bitables or the local tracking SQLite database) and POSTs a normalized JSON payload to a webhook URL. The payload flattens every column described by `feishu.DramaFields` at the top level and adds a `records` slice where each element contains the columns defined by `feishu.ResultFields`.

```go
payload, err := piracy.SendSummaryWebhook(ctx, piracy.WebhookOptions{
    Params:      "真千金她是学霸",
    App:         "kuaishou",
    UserID:      "3618381723",
    WebhookURL:  os.Getenv("SUMMARY_WEBHOOK_URL"),
    Source:      piracy.WebhookSourceSQLite, // or piracy.WebhookSourceFeishu
    RecordLimit: 100,
})
if err != nil {
    log.Fatal().Err(err).Msg("webhook summary failed")
}
records := payload["records"].([]map[string]any)
log.Info().Int("records", len(records)).Msg("summary delivered")
```

#### Webhook 鉴权（VEDEM AGW）

当下游 Webhook 服务位于 VEDEM 网关之后时，`SendSummaryWebhook` 会启用 AGW 签名方案：

- 签名算法：`auth-v2`，使用 HMAC-SHA256 计算签名；
- 签名内容：固定字符串 `"UNSIGNED-PAYLOAD"`（即“unsigned body” 模式）；
- 请求头：
  - `Agw-Auth`：`auth-v2/<AK>/<timestamp>/1800/<HMAC(UNSIGNED-PAYLOAD)>`
  - `Agw-Auth-Content`：`UNSIGNED-PAYLOAD`

所需环境变量：

```bash
VEDEM_IMAGE_AK=your_ak_here
VEDEM_IMAGE_SK=your_sk_here
```

行为说明：

- 当 `VEDEM_IMAGE_AK` / `VEDEM_IMAGE_SK` 均非空时，Webhook 请求将自动携带上述 AGW 鉴权头；
- 任一为空时，鉴权会被显式跳过，`SendSummaryWebhook` 仍会正常发送 **未签名** 的 HTTP 请求（适合本地调试或非 VEDEM 环境）；
- TaskAgent 不负责加载 `.env`，请在启动进程前确保相关环境变量已注入（例如通过 shell 导出或上游进程配置）。

Key environment knobs for this workflow:

- `DRAMA_FIELD_PRIORITY` 与 `DRAMA_FIELD_RIGHTS_SCENARIO` 可自定义短剧优先级与维权场景字段，避免表结构差异导致查询失败。
- `RESULT_FIELD_APP` and `RESULT_FIELD_USERNAME` override the result-table App/UserName columns.
- `DRAMA_SQLITE_TABLE` and `RESULT_SQLITE_TABLE` change the SQLite table names (defaults: `drama_catalog`, `capture_results`).
- `TRACKING_STORAGE_DB_PATH` overrides the SQLite database path when using the local source; otherwise the helper reuses the same location as the tracking storage manager.
- All drama columns are flattened into the webhook JSON root level, so downstream services can access source-specific fields directly.
```

### CLI Usage

```bash
# Detection only
piracy detect --result-filter '{"conjunction":"and","conditions":[{"field_name":"Params","operator":"is","value":["短剧名称"]}]}'

# Detection and reporting
piracy report --app com.smile.gifmaker --params "短剧A,短剧B"

# Full automation
piracy auto --app com.smile.gifmaker --output results/piracy_auto.csv

# Increase concurrency
piracy auto --app com.smile.gifmaker --concurrency 20
```

## Configuration

### Environment Variables

```bash
# Required for both detection and reporting
FEISHU_APP_ID=cli_xxxxxxxxxxxxxx
FEISHU_APP_SECRET=xxxxxxxxxxxxxxxx

# Table URLs
RESULT_BITABLE_URL=https://example.feishu.cn/base/xxx  # Video data
DRAMA_BITABLE_URL=https://example.feishu.cn/base/yyy   # Drama durations
TASK_BITABLE_URL=https://example.feishu.cn/base/zzz  # Where to write reports

# Optional field mappings (defaults shown)
RESULT_FIELD_PARAMS=Params
RESULT_FIELD_USERID=UserID
RESULT_FIELD_DURATION=ItemDuration
DRAMA_FIELD_NAME=短剧名称
DRAMA_FIELD_DURATION=全剧时长（秒）
THRESHOLD=0.5
# Webhook summary specific overrides
RESULT_FIELD_APP=App
RESULT_FIELD_USERNAME=UserName
DRAMA_FIELD_PRIORITY=Priority
DRAMA_FIELD_RIGHTS_SCENARIO=RightsProtectionScenario
DRAMA_SQLITE_TABLE=drama_catalog
RESULT_SQLITE_TABLE=capture_results
```

### Default Field Mappings

The package uses sensible defaults based on Feishu's standard field names:

- **Result Table**: `Params`, `UserID`, `ItemDuration`
- **Drama Table**: `Params`, `TotalDuration`
- **Task Table**: Fields for `App`, `Scene`, `Params`, `User`, `Status`

## Detection Logic

1. **Data Fetching**: Retrieves video records and drama duration information
2. **Aggregation**: Groups video durations by `(Params, UserID)` combinations
3. **Ratio Calculation**: Calculates `sum_duration / drama_total_duration`
4. **Threshold Filtering**: Flags combinations exceeding the threshold (default 50%)
5. **Reporting**: Outputs suspicious combinations with detailed metrics

## Integration

The package is integrated into EvalAgent's search functionality:

```go
// In search agent initialization
agent.PiracyReporter = piracy.NewReporter()
if agent.PiracyReporter.IsConfigured() {
    // Piracy detection will run automatically
}
```

## Testing

Run the comprehensive test suite:

```bash
go test ./pkg/piracy/...
```

Tests cover:
- Core detection logic with various scenarios
- Threshold filtering
- Missing data handling
- Configuration management
- Helper functions
