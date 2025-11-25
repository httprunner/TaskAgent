# Piracy Detection Package

This package provides comprehensive piracy detection and reporting functionality for drama content monitoring. It combines the core detection engine with high-level reporting capabilities.

## Overview

The `pkg/piracy` package merges the functionality of two packages:
- **Core Detection Engine**: Analyzes video data to identify potential piracy by comparing user video durations against original drama durations
- **Reporter**: High-level wrapper that performs detection and automatically reports findings to task tables

## Features

- **Automated Piracy Detection**: Compares aggregated video durations against original drama durations
- **Configurable Thresholds**: Default 50% threshold (configurable via environment or code)
- **Feishu Integration**: Seamless integration with Feishu bitable for data sources and reporting
- **Flexible Field Mapping**: Configurable field names for different table schemas
- **Batch Processing**: Process multiple drama parameters in a single operation
- **Comprehensive Reporting**: Detailed reports with suspicious combinations, ratios, and statistics
- **Webhook Summaries**: Query Feishu or SQLite, aggregate drama metadata + capture records, and forward the payload to custom webhook endpoints

## Architecture

### Core Types

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

### Main Components

1. **Detection Engine** (`detect.go`):
   - `Detect(ctx, opts) (*Report, error)` - Main detection function
   - `analyzeRows(resultRows, dramaRows, cfg) *Report` - Core analysis logic
   - `ApplyDefaults()` - Configuration management

2. **Reporter** (`reporter.go`):
   - `NewReporter() *Reporter` - Creates reporter from environment
   - `ReportPiracyForParams(ctx, app, paramsList) error` - Main reporting function
   - `IsConfigured() bool` - Configuration validation

3. **Utilities** (`helpers.go`):
   - `getString(fields, name) string` - Safe field extraction
   - `getFloat(fields, name) (float64, bool)` - Numeric field extraction

## Usage

### Basic Detection

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

### Using the Reporter

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

### Sending Webhook Summaries

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
DRAMA_FIELD_DURATION=TotalDuration
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
