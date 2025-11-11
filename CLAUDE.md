# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

TaskAgent is a Go 1.24 module that orchestrates Android capture devices by polling Feishu Bitable tasks and dispatching them through a pluggable job runner system. The project uses a modular architecture with interfaces for extensibility.

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
go test ./cmd/piracy_detect
```

### Piracy Detection Tool
```bash
# Build and run piracy detection
go run ./cmd/piracy_detect \
  --result-filter 'AND(CurrentValue.[Params]="keyword")' \
  --drama-filter "<filter>" \
  --output-csv result.csv
```

## Architecture & Key Components

### Core Interfaces
- **JobRunner**: Implement this interface to execute tasks (`pool.JobRunner`)
- **DeviceProvider**: Implement this to discover devices (default: ADB provider)
- **TaskManager**: Implement this to manage task lifecycle (default: Feishu Bitable)

### Package Structure
- `pool.go` / `tasks.go`: Core orchestration logic
- `feishu/`: Feishu API client and Bitable operations
- `providers/adb/`: Android device discovery via ADB
- `cmd/piracy_detect/`: Piracy detection CLI with plugin architecture
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
- `DRAMA_BITABLE_URL`: URL for original drama information table

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

### Error Handling
- Use structured logging with `zerolog`
- Return meaningful errors that can be handled upstream
- Never commit sensitive data (API keys, tokens) to source control