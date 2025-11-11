# Piracy Detection CLI

A simplified CLI tool to detect suspicious (Params + UserID) combinations where the sum of captured item duration exceeds a ratio of the original drama total duration.

## Overview

The CLI uses a straightforward approach:
1. Fetches rows from result and original drama Feishu Bitables
2. Aggregates item durations by Params+UserID combinations from result table
3. Compares against original drama durations from the drama table
4. Reports combinations exceeding the configured threshold

## Configuration

All configuration is done through environment variables in your `.env` file:

```bash
# Required: Table URLs
RESULT_BITABLE_URL="https://example.larkoffice.com/wiki/..."
DRAMA_BITABLE_URL="https://example.larkoffice.com/wiki/..."

# Optional: Field names (use defaults if not specified)
PARAMS_FIELD="Params"             # Column in result table containing drama identifier
USERID_FIELD="UserID"             # Column in result table containing user ID
DURATION_FIELD="ItemDuration"     # Column in result table containing item duration (seconds)

# Optional: Original drama table field names
DRAMA_PARAMS_FIELD="Params"       # Column in drama table containing drama identifier
DRAMA_DURATION_FIELD="TotalDuration"  # Column in drama table containing total duration (seconds)

# Optional: Detection threshold ratio (0-1), default is 0.5 (50%)
THRESHOLD="0.5"

# Standard Feishu credentials (required)
FEISHU_APP_ID="cli_xxxxxxxxxxxxxxxx"
FEISHU_APP_SECRET="your-secret-here"
```

## Usage

### Basic Usage

With environment variables configured in `.env`:

```bash
# Run detection with all rows
go run ./cmd/piracy_detect

# Filter result rows
go run ./cmd/piracy_detect --result-filter 'AND(CurrentValue.[Params]="少女嗨翻系统")'

# Save results to CSV
go run ./cmd/piracy_detect --output-csv result.csv

# Combine filters and output
go run ./cmd/piracy_detect \
  --result-filter 'AND(CurrentValue.[Params]="少女嗨翻系统")' \
  --output-csv suspicious.csv
```

### Available Flags

Command-line flags:

- `--result-filter`: Filter to apply to result table rows (optional)
- `--drama-filter`: Filter to apply to original drama table rows (optional)
- `--output-csv`: Path to save CSV output (optional)

All other configuration (table URLs, field names, threshold) must be set via environment variables.

## Development

- Build: `go build ./cmd/piracy_detect`
- Lint: `go vet ./cmd/piracy_detect`
- Test: `go test ./pkg/piracydetect`

See the repository `AGENTS.md` for broader project guidelines.
