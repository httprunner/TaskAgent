# Piracy CLI（盗版检测辅助工具）

基于 Feishu 多维表格 + 本地 sqlite 采集结果的检测与（可选）上报工具。当前仅保留 `piracy detect` 命令。

## Subcommands

### detect（检测 + 可选上报）
- 参数来源：默认从飞书“任务状态”表按 `App` 过滤出 Params（Scene 固定“综合页搜索”，Status 固定 `success`）；也可用 `--params` 直接指定单点验证。
- 数据来源：本地 sqlite `capture_results` 表（采集结果）；飞书剧单表（全剧时长）。
- 行为：比对 `(Params, UserID)` 聚合时长与全剧时长，打印结果，可导出 CSV；`--report` 时把可疑项写回任务状态表。

The command relies on the same environment variables described below and adds a few query-specific flags:

#### 示例 1：从任务状态表批量取参，只看结果（不写回）
```bash
go run ./cmd/piracy detect \
  --sqlite /path/to/capture.sqlite \
  --app com.smile.gifmaker \
  --output-csv suspicious.csv
```

#### 示例 2：从任务状态表取参并写回任务表
```bash
go run ./cmd/piracy detect \
  --sqlite /path/to/capture.sqlite \
  --app com.smile.gifmaker \
  --report
```

#### 示例 3：指定少量剧名做单点验证（不依赖任务表）
```bash
go run ./cmd/piracy detect \
  --sqlite /path/to/capture.sqlite \
  --params "剧A,剧B"
```

#### 示例 4：单点验证并写回任务表（不依赖任务表）
```bash
go run ./cmd/piracy detect \
  --sqlite /path/to/capture.sqlite \
  --params "剧A,剧B" \
  --report \
  --app com.smile.gifmaker
```

常用参数：
- `--sqlite`：采集结果 sqlite 路径，默认 `~/.eval/records.sqlite`
- `--params`：逗号分隔的剧名，单点验证用；不提供则从任务状态表取参（Scene 固定综合页搜索，Status 固定 success）
- `--app`：任务状态表的 App 过滤（必填）
- `--result-filter` / `--drama-filter`：对结果表 / 剧单表附加 Feishu FilterInfo JSON
- `--output-csv`：导出 CSV 路径
- `--report`：写回任务状态表

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
