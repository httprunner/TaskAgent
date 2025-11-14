# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

TaskAgent is a Go 1.24 module that orchestrates Android capture devices by polling Feishu Bitable tasks and dispatching them through a pluggable job runner system. The project uses a modular architecture with interfaces for extensibility.

### Key Design Principles
- **Interface-driven architecture** for maximum extensibility
- **Lean dependency stack** - only essential libraries
- **Context-aware operations** throughout the codebase
- **Structured logging** with zerolog for observability
- **Table-driven tests** following Go best practices
- **Environment-based configuration** with sensible defaults

## Essential Commands

### Build & Development
```bash
# Format code
go fmt ./...

# Run static analysis
go vet ./...

# Build all packages
go build ./...

# Run unit tests
go test ./...

# Run integration tests (requires Feishu credentials)
FEISHU_LIVE_TEST=1 go test ./feishu -run Live

# Run tests for a specific package
go test ./feishu
go test ./providers/adb
go test ./cmd/piracy
```

### Piracy Detection Tool
```bash
# Build and run piracy detection
go run ./cmd/piracy detect \
  --result-filter 'AND(CurrentValue.[Params]="keyword")' \
  --drama-filter "<filter>" \
  --output-csv result.csv
```

## Architecture & Key Components

### Core Architecture Pattern: Interface-Based Plugin System

**Main Components:**
- **`DevicePoolAgent`** (`pool.go`): Central orchestrator that coordinates device discovery, task fetching, and job dispatch
- **Core Interfaces** (defined in `pool.go`):
  - `JobRunner`: Implement this interface to execute tasks (`pool.JobRunner`)
  - `DeviceProvider`: Implement this to discover devices (default: ADB provider)
  - `TaskManager`: Implement this to manage task lifecycle (default: Feishu Bitable)

### Core Workflow
1. **Device Discovery**: `DeviceProvider.ListDevices()` → ADB serial numbers
2. **Task Fetching**: `TaskManager.FetchAvailableTasks()` → Feishu Bitable queries
3. **Task Assignment**: Smart dispatch (targeted vs general tasks)
4. **Job Execution**: `JobRunner.RunJob()` → User implementation
5. **Status Updates**: `TaskManager.OnTasksCompleted()` → Feishu status sync

### Main Entry Points
```go
// Continuous polling mode
agent, _ := pool.NewDevicePoolAgent(cfg, runner)
agent.Start(ctx, "app-name")

// Single execution mode
agent.RunOnce(ctx, "app-name")
```

### Package Structure
- `pool.go` / `tasks.go`: Core orchestration logic
- `feishu/`: Feishu API client and Bitable operations
- `providers/adb/`: Android device discovery via ADB
- `cmd/piracy/`: Unified piracy CLI (`detect` + `report` subcommands)
- `pkg/piracydetect/`: Reusable piracy detection library
- `internal/`: Environment loading utilities

### Key Types
- `DevicePoolAgent`: Main orchestrator that polls devices and dispatches jobs
- `JobRequest`: Contains device serial and task payload for job execution
- `FeishuTaskConfig`: Configuration for Feishu Bitable integration

## Environment Configuration

Required environment variables (create `.env` file):
- `FEISHU_APP_ID`: Feishu app ID
- `FEISHU_APP_SECRET`: Feishu app secret
- `FEISHU_TENANT_KEY`: Optional tenant key
- `FEISHU_BASE_URL`: Optional custom base URL
- `FEISHU_LIVE_TEST=1`: Enable live integration tests

### Table Configuration (New Field Naming)
```bash
# Original drama table
DRAMA_BITABLE_URL=https://bytedance.larkoffice.com/wiki/xxx
DRAMA_NAME_FIELD=短剧名称
DRAMA_DURATION_FIELD=全剧时长（秒）

# Target collection table
TARGET_BITABLE_URL=https://bytedance.larkoffice.com/wiki/xxx
TARGET_PARAMS_FIELD=Params

# Result table
RESULT_BITABLE_URL=https://bytedance.larkoffice.com/wiki/xxx
RESULT_PARAMS_FIELD=Params
RESULT_USERID_FIELD=UserID
RESULT_DURATION_FIELD=ItemDuration

# Piracy detection threshold
THRESHOLD=0.5
```

**Note:** All configuration fields use table-specific prefixes for clarity. Use `RESULT_*` for result table, `TARGET_*` for target table, and `DRAMA_*` for drama table fields.

## Testing Guidelines

- Use table-driven tests following existing patterns in `*_test.go` files
- Target ≥80% statement coverage for new packages
- Integration tests require `FEISHU_LIVE_TEST=1` and real Feishu tables
- Mock external dependencies in unit tests
- Use `Test<Component><Scenario>` naming pattern

## Development Patterns

### Adding New Features
1. Follow existing package structure (providers for device discovery, plugins for specialized logic)
2. Implement appropriate interfaces rather than modifying core logic
3. Add comprehensive tests including edge cases
4. Update relevant README files

### Feishu Integration
- Use `feishu.NewClientFromEnv()` for API client creation
- Bitable operations use `TargetFields`/`TargetRecordInput` for task tables
- Result tables use `ResultFields`/`ResultRecordInput`
- Always handle context cancellation properly

### Configuration Management
- Environment variables loaded automatically from `.env` file
- All fields use table-specific prefixes (RESULT_*, TARGET_*, DRAMA_*)
- Use `feishu.DefaultResultFields` for standard field mappings
- Custom field mappings supported via environment variables

### Error Handling
- Use structured logging with `zerolog`
- Return meaningful errors that can be handled upstream
- Implement retry logic with exponential backoff for transient failures
- Always handle context cancellation properly
- Never commit sensitive data (API keys, tokens) to source control
