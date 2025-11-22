# Piracy CLI

A unified CLI that exposes both piracy detection and reporting workflows through `piracy detect` and `piracy report`. The shared command root configures logging and environment loading before command-specific logic executes.

## Subcommands

### detect
- Fetches rows from the result and drama Feishu Bitables (default)
- Alternatively consumes local files via `--use-files` (JSONL capture data + CSV drama catalog)
- Aggregates durations for `(Params, UserID)` combinations
- Compares aggregated totals against original drama durations
- Prints suspicious combos and optionally dumps a CSV

The command relies on the same environment variables described below and adds a few query-specific flags:

```bash
go run ./cmd/piracy detect \
  --result-filter 'AND(CurrentValue.[Params]="少女嗨翻系统")' \
  --drama-filter '<drama filter>' \
  --output-csv suspicious.csv
```

Available flags:
- `--result-filter`: Filter for result table rows
- `--target-filter`: (currently unused) Filter for target table rows
- `--drama-filter`: Filter for drama table rows
- `--output-csv`: Optional path to persist suspicious combos as CSV
- `--use-files`: Toggle local-file mode instead of Feishu Bitables
- `--data-file`: JSONL capture file consumed when `--use-files` is set
- `--drama-file`: CSV drama catalog used in file mode
- `--filter-rate`: Percentage threshold for filtering printed matches in file mode (default 50)

### report
- Runs detection for the supplied drama params
- Writes suspicious matches into the Feishu target table via `pkg/piracy`

```bash
go run ./cmd/piracy report \
  --app com.smile.gifmaker \
  --params "80姐妹的交换人生,短剧B" \
  --params-file dramas.txt
```

Available flags:
- `--app` (required): App package name, e.g., `com.smile.gifmaker`
- `--params`: Comma-separated list of drama params
- `--params-file`: File with one param per line (ignores empty lines and `#` comments)

### auto
- Fetches the entire drama list and durations from the drama table
- Iterates each drama, fetching matching capture results and running detection immediately
- Reports suspicious matches per drama before moving to the next and writes a consolidated CSV at the end
- Supports configurable concurrency (default 10) to process multiple dramas in parallel

```bash
go run ./cmd/piracy auto \
  --app com.smile.gifmaker \
  --output results/piracy_auto.csv
```

Available flags:
- `--app` (required): App package name used for reporting
- `--output`: Optional CSV output path (`./piracy_auto_<timestamp>.csv` by default)
- `--result-filter`: Extra filter for the result table query
- `--drama-filter`: Extra filter for the drama table query
- `--concurrency`: Number of dramas processed in parallel (default 10)

## Environment configuration

All table URLs, field names, and thresholds are configured through environment variables (typically stored in `.env`). The CLI expects:

```bash
RESULT_BITABLE_URL="https://example.larkoffice.com/wiki/..."
DRAMA_BITABLE_URL="https://example.larkoffice.com/wiki/..."
TASK_BITABLE_URL="https://example.larkoffice.com/wiki/..."
FEISHU_APP_ID="cli_xxxxx"
FEISHU_APP_SECRET="secret"
THRESHOLD="0.5"
```

Field names for each table can be overridden using the prefixed variables explained in the repository `AGENTS.md`.

## Development commands
- Build: `go build ./cmd/piracy`
- Lint: `go vet ./cmd/piracy`
- Detection/reporting tests: `go test ./pkg/piracy`
