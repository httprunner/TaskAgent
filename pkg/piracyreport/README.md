# Piracy Report Package

This package provides functionality for detecting piracy content and reporting suspicious combinations to a Feishu bitable drama table.

## Overview

The `piracyreport` package builds on top of the `piracydetect` package to not only detect piracy but also automatically report findings to the drama table.

### Features

- Detect piracy based on video duration ratios
- Filter by specific params (drama names)
- Automatically report suspicious combinations to drama table
- Configurable threshold (default: 50%)

## Installation

This package is part of the TaskAgent module. No separate installation needed.

## Usage

### As a Library

```go
package main

import (
    "context"
    "github.com/httprunner/TaskAgent/pkg/piracyreport"
)

func main() {
    // Create reporter (reads config from environment)
    reporter := piracyreport.NewReporter()

    if !reporter.IsConfigured() {
        // Handle configuration error
        return
    }

    // Detect and report piracy for specific params
    ctx := context.Background()
    app := "com.smile.gifmaker"
    paramsList := []string{"短剧A", "短剧B", "短剧C"}

    if err := reporter.ReportPiracyForParams(ctx, app, paramsList); err != nil {
        // Handle error
    }
}
```

### As a Command-Line Tool

Use the `piracy` root command with the `report` subcommand to detect and report piracy.

#### Install

```bash
cd cmd/piracy
go install
```

#### Usage Examples

**Single drama:**
```bash
piracy report --app com.smile.gifmaker --params "80姐妹的交换人生"
```

**Multiple dramas (comma-separated):**
```bash
piracy report --app com.tencent.mm --params "短剧A,短剧B,短剧C"
```

**Read from file (one drama per line):**
```bash
cat dramas.txt
# 短剧A
# 短剧B
# 短剧C

piracy-report --app com.smile.gifmaker --params-file dramas.txt
```

#### Command Line Flags

- `--app` (required): App package name (e.g., `com.smile.gifmaker`, `com.tencent.mm`)
- `--params`: Comma-separated list of drama params
- `--params-file`: Path to a file containing drama params (one per line, supports comments with #)

#### Environment Variables Required

```bash
# Feishu credentials
FEISHU_APP_ID=cli_xxxxxxxxxxxxxx
FEISHU_APP_SECRET=xxxxxxxxxxxxxxxx

# Bitable URLs (three tables)
DRAMA_BITABLE_URL=https://bytedance.larkoffice.com/wiki/xxx?table=xxx&view=xxx
TARGET_BITABLE_URL=https://bytedance.larkoffice.com/wiki/xxx?table=xxx&view=xxx
RESULT_BITABLE_URL=https://bytedance.larkoffice.com/wiki/xxx?table=xxx&view=xxx
```

**Three Tables Required:**

1. **RESULT_BITABLE_URL** (Result Table - Source)
   - Contains captured video data
   - Fields: Params, UserID, UserName, ItemDuration, etc.

2. **DRAMA_BITABLE_URL** (Original Drama Table - Source)
   - Contains original drama information and durations
   - Key fields: Params, TotalDuration
   - Used as reference to calculate duration ratios

3. **TARGET_BITABLE_URL** (Target Table - Destination)
   - Task scheduling table where piracy reports are written
   - Fields: TaskID, App, Scene, Params, User, Status, etc.
   - Detection results are written here with Status="PiracyDetected"

## Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `FEISHU_APP_ID` | Feishu application ID | Yes |
| `FEISHU_APP_SECRET` | Feishu application secret | Yes |
| `FEISHU_TENANT_KEY` | Feishu tenant key (optional) | No |
| `RESULT_BITABLE_URL` | URL to the result table containing video data (source) | Yes |
| `DRAMA_BITABLE_URL` | URL to the original drama table with durations (source) | Yes |
| `TARGET_BITABLE_URL` | URL to the target table for writing piracy reports (destination) | Yes |

### Threshold Configuration

The default ratio threshold is 50%. To change this, modify the `threshold` field in the `Reporter` struct:

```go
reporter := piracyreport.NewReporter()
// Note: Currently threshold is not exposed via environment variable
// You would need to modify the source code to change it
```

## Data Flow

1. **Input**: List of params (drama names) and app package name
2. **Fetch Result Data**: Query result table for videos matching the params (Source: `RESULT_BITABLE_URL`)
3. **Fetch Drama Data**: Query original drama table for drama durations (Source: `DRAMA_BITABLE_URL`)
4. **Analysis**: Calculate duration ratios for each (params, userID) combination
5. **Filter**: Keep only combinations where ratio ≥ threshold (default 50%)
6. **Report**: Write suspicious combinations to target table with status "PiracyDetected" (Destination: `TARGET_BITABLE_URL`)

## Table Schemas

### Target Table Schema (Destination)

The target table (specified by `TARGET_BITABLE_URL`) should have at least these fields:

- `TaskID` (optional): Auto-incrementing task identifier (auto-generated if omitted)
- `Params` (required): Drama name/params
- `App` (required): App package name
- `Scene` (required): Scene name (will be set to "个人页搜索")
- `User` (required): User ID of the suspected pirate
- `Status` (optional): Task status (will be set to "PiracyDetected")
- `Datetime` (optional): Creation timestamp

### Drama Table Schema (Source)

The drama table (specified by `DRAMA_BITABLE_URL`) should contain at least:

- `Params` (required): Drama name/params
- `TotalDuration` (required): Total drama duration in seconds

This table is used as a reference to calculate duration ratios.

## Example Output

### Command Line

```bash
$ piracy-report --app com.smile.gifmaker --params "80姐妹的交换人生"
11:56PM INF Reporter initialized
11:56PM INF Starting piracy detection for params app=com.smile.gifmaker params_count=1
11:56PM INF fetching bitable rows options={"Filter":"AND(CurrentValue.[Params]=\"80姐妹的交换人生\")","Limit":0,"ViewID":""} url=https://...result_table...
11:56PM DBG listing bitable records limit=0 page_size=200 table_id=tblzoZuR6aminfye
11:56PM DBG listing bitable records limit=0 page_size=200 table_id=tblzoZuR6aminfye
11:56PM INF fetched bitable records count=203
11:56PM INF fetching bitable rows options={"Filter":"","Limit":0,"ViewID":""} url=https://...drama_table...
11:56PM DBG listing bitable records limit=0 page_size=200 table_id=tblovfFFSi1vNIrA
11:56PM DBG listing bitable records limit=0 page_size=200 table_id=tblovfFFSi1vNIrA
11:56PM INF fetched bitable records count=336
11:56PM INF analyzing result rows with drama rows
11:56PM INF piracy detection completed params_missing_duration=0 result_rows=203 suspicious_combos=1 target_rows=336 threshold=0.5
11:56PM INF Writing piracy matches to drama table match_count=1
11:56PM INF Writing records to drama table record_count=1 table_url=https://...drama_table...
11:56PM INF Successfully wrote piracy report records to drama table record_count=1
11:56PM INF Piracy report completed successfully
```

### Reported Data in Drama Table

| App | Scene | Params | User | Status |
|-----|-------|--------|------|--------|
| com.smile.gifmaker | 个人页搜索 | 80姐妹的交换人生 | 141857908 | PiracyDetected |

## Error Handling

- If environment variables are missing, `IsConfigured()` returns `false`
- If piracy detection fails, an error is returned with details
- If writing to drama table fails, an error is returned with details
- The reporter logs warnings instead of errors for non-critical issues

## Implementation Details

### Core Functions

- `NewReporter()`: Creates a new reporter instance, reads config from environment
- `IsConfigured()`: Checks if required environment variables are set
- `ReportPiracyForParams()`: Main function that orchestrates detection and reporting
- `buildParamsFilter()`: Builds Feishu filter for querying result table
- `writeMatchesToDramaTable()`: Writes detection results to drama table

### Filter Construction

The filter for querying the result table is constructed as:

- Single param: `CurrentValue.[Params]="param"`
- Multiple params: `OR(CurrentValue.[Params]="param1", CurrentValue.[Params]="param2", ...)`

## Testing

### Using the piracy-detect subcommand (lower level)

If you just want to detect piracy without reporting:

```bash
go run ./cmd/piracy detect \
  --result-filter 'AND(CurrentValue.[Params]="80姐妹的交换人生")'
```

### Using the piracy-report subcommand (this package)

```bash
go run ./cmd/piracy report \
  --app com.smile.gifmaker \
  --params "80姐妹的交换人生"
```

## Related Packages

- `github.com/httprunner/TaskAgent/pkg/piracydetect`: Core piracy detection logic
- `github.com/httprunner/TaskAgent/feishu`: Feishu API client

## License

Part of TaskAgent project
