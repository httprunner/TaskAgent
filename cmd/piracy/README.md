# Piracy CLI（盗版检测辅助工具）

基于 Feishu 多维表格 + 本地 sqlite 采集结果的检测与（可选）上报工具，提供 `detect`、`replay`、`backfill`、`webhook-worker` 四个子命令。

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
  --report \
  --threshold 0.5  # 若不指定，使用环境 THRESHOLD 或默认值
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
  --app com.smile.gifmaker \
  --threshold 0.7  # 覆盖阈值
```

常用参数：
- `--sqlite`：采集结果 sqlite 路径，默认 `~/.eval/records.sqlite`
- `--params`：逗号分隔的剧名，单点验证用；不提供则从任务状态表取参（Scene 固定综合页搜索，Status 固定 success）
- `--app`：任务状态表的 App 过滤（必填）
- `--result-filter` / `--drama-filter`：对结果表 / 剧单表附加 Feishu FilterInfo JSON
- `--output-csv`：导出 CSV 路径
- `--report`：写回任务状态表
- `--threshold`：覆盖阈值（小数，0 表示使用环境 THRESHOLD 或默认 0.5）

### replay（重放单个任务并生成子任务）
- 场景：采集流程完成后，需要针对某个 TaskID 重试盗版检测/子任务创建（例如排查 DatetimeFieldConvFail）。
- 数据来源：`capture_tasks` sqlite（默认 `~/.eval/records.sqlite`，可用 `--db-path` 覆盖），以及线上 `RESULT_BITABLE_URL` / `DRAMA_BITABLE_URL`。
- 行为：读取指定 TaskID 的 Params/App/Datetime，调用 `DetectMatchesWithDetails` + `CreateGroupTasksForPiracyMatches`，对结果完全等价于线上自动流程。

示例：
```bash
go run ./cmd/piracy replay \
  --task-id 1234 \
  --app com.smile.gifmaker \
  --db-path /Users/me/.eval/records.sqlite
```

常用参数：
- `--task-id`（必填）：要重放的 TaskID。
- `--db-path`：自定义 sqlite 路径；留空时自动解析 tracking DB（`storage.ResolveDatabasePath`）。
- 全局 `--app`：覆盖任务行里的 App 值（当任务表缺失 App 或需要临时切换包名时使用）。

### backfill（批量扫描综合页成功任务，按需补写子任务）
- 场景：综合页任务已成功但未生成个人页/合集/锚点子任务，可离线重新检测并（在确认后）写回。
- 数据来源：本地 `capture_tasks` sqlite（默认 `~/.eval/records.sqlite`），Scene 固定综合页搜（`SceneGeneralSearch`），Status 固定 `success`。可用 `--date` 按 `YYYY-MM-DD` 过滤。
- 行为：对每个父任务调用 `DetectMatchesWithDetails`，默认只打印匹配结果；传入 `--sync` 时会调用 `CreateGroupTasksForPiracyMatches` 写回任务表，`--skip-existing` 可避免重复写入（通过 `GroupID = {TaskID}_1` 判断）。

示例：
```bash
# 仅检测（不写表）
go run ./cmd/piracy backfill --date 2025-12-01

# 过滤指定日期并写回，写前若检测到已有子任务则跳过
go run ./cmd/piracy backfill \
  --date 2025-12-01 \
  --sync \
  --skip-existing

# 使用自定义 sqlite 路径
go run ./cmd/piracy backfill --db-path /path/to/records.sqlite --sync
```

常用参数：
- `--date`：仅处理指定日期（本地时区）的综合页成功任务。
- `--sync`：开启写表；不指定时仅检测不写入。
- `--skip-existing`：与 `--sync` 一起使用，若已存在 `GroupID={TaskID}_1` 的子任务则跳过。
- `--db-path`：自定义 `capture_tasks` sqlite 路径，默认自动解析。
- 全局 `--task-url` / `--app`：覆盖任务表 URL 与 App 过滤/回填（缺省时使用环境变量）。

### webhook-worker（重试 webhook 汇总）
- 功能：轮询任务状态表中挂起/失败的 webhook 汇总任务，重新发送到 SUMMARY_WEBHOOK_URL。
- 默认数据来源：`TASK_BITABLE_URL`（可用 `--task-url` 覆盖），`SUMMARY_WEBHOOK_URL`（可用 `--webhook-url` 覆盖）。
- 可选过滤：`--app`（默认取 BUNDLE_ID 环境变量）。
- 调度参数：`--poll-interval`（默认 30s），`--batch-limit`（默认 20 条/次）。

示例：
```bash
# 读取环境变量 + 采用默认配置
go run ./cmd/piracy webhook-worker

# 命令行传参
go run ./cmd/piracy webhook-worker \
  --task-url "https://bytedance.larkoffice.com/wiki/..." \
  --webhook-url "https://example.com/webhook" \
  --app com.smile.gifmaker \
  --poll-interval 1m \
  --batch-limit 50
```

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
