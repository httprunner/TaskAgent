# Environment Variables

TaskAgent automatically loads the first `.env` file it finds above the current
working directory (see `internal/envload.Ensure`). The tables below capture
every environment variable referenced anywhere in the project, along with
defaults and where they take effect.

## Feishu Credentials & API

| Variable | Required | Default | Purpose / Usage |
| --- | --- | --- | --- |
| `FEISHU_APP_ID` | Yes | – | App ID for Feishu Open Platform. Needed by `pkg/feishu` client, all Feishu-backed sinks, and CLI helpers. |
| `FEISHU_APP_SECRET` | Yes | – | App secret paired with `FEISHU_APP_ID`. |
| `FEISHU_TENANT_KEY` | Optional | empty | Optional tenant key for self-built apps. Passed through to the Feishu SDK. |
| `FEISHU_BASE_URL` | Optional | `https://open.feishu.cn` | Override for Feishu Open Platform base URL (useful for sandbox clusters). |

## Feishu Table Links

| Variable | Required | Default | Purpose / Usage |
| --- | --- | --- | --- |
| `RESULT_BITABLE_URL` | Yes for Feishu reporting | – | Capture-result table URL. Consumed by `pkg/piracy` reporter/detector, webhook summaries (Feishu mode), and `pkg/storage`'s Feishu sink. |
| `DRAMA_BITABLE_URL` | Yes when reading drama metadata from Feishu | – | Drama catalog table URL. Used by `pkg/piracy` detection/reporting and webhook summaries (Feishu mode). |
| `TARGET_BITABLE_URL` | Required for piracy reporting | – | Target table used to write piracy alerts. Required by `piracy.Reporter`. |

## Field Mapping & Thresholds

These variables allow the Feishu/SQLite helpers to adapt to custom column
names.

| Variable | Default | Purpose |
| --- | --- | --- |
| `RESULT_APP_FIELD` | `App` | Result-table column storing the app/package (used when filtering webhook summaries). |
| `RESULT_PARAMS_FIELD` | `Params` | Result-table column containing drama params/name. |
| `RESULT_USERID_FIELD` | `UserID` | Result-table column containing the uploader ID. |
| `RESULT_USERNAME_FIELD` | `UserName` | Result-table column storing uploader name. |
| `RESULT_DURATION_FIELD` | `ItemDuration` | Result-table duration column (seconds). |
| `TARGET_PARAMS_FIELD` | `Params` | Field name inside the target table for drama params. |
| `TARGET_DURATION_FIELD` | `TotalDuration` | Field name for the drama duration stored in the target table. |
| `DRAMA_ID_FIELD` | `DramaID` | Drama table column that stores the drama identifier. |
| `DRAMA_NAME_FIELD` | `短剧名称` | Drama table column for short-drama names (used by webhook summaries and filters). |
| `DRAMA_DURATION_FIELD` | `TotalDuration` | Drama table column for full duration (seconds). |
| `DRAMA_PRIORITY_FIELD` | `Priority` | Drama table column for internal priority grading (webhook summaries). |
| `DRAMA_RIGHTS_SCENARIO_FIELD` | `RightsProtectionScenario` | Drama table column for the rights-protection scenario text (webhook summaries). |
| `THRESHOLD` | `0.5` | Float ratio (0–1) used by piracy detection/reporting when filtering suspicious matches. |

## Local Storage & SQLite

| Variable | Default | Purpose / Usage |
| --- | --- | --- |
| `TRACKING_STORAGE_DISABLE_JSONL` | unset | When set (any non-empty value), disables JSONL file output for capture records. |
| `TRACKING_STORAGE_ENABLE_SQLITE` | unset (`false`) | Forces the SQLite sink on even if not configured via code. Recognizes `1`/`true`. |
| `RESULT_STORAGE_ENABLE_FEISHU` | unset (`false`) | Forces the Feishu result-table sink on (requires Feishu credentials + `RESULT_BITABLE_URL`). |
| `TRACKING_STORAGE_DB_PATH` | `$HOME/.eval/records.sqlite` | Custom path for the tracking SQLite database. Used by `pkg/storage` and webhook summaries (SQLite source). |
| `DRAMA_SQLITE_TABLE` | `drama_catalog` | Table name for drama metadata inside the local SQLite DB (webhook summaries, if Source=sqlite). |
| `RESULT_SQLITE_TABLE` | `capture_results` | Table name for capture records inside the local SQLite DB (webhook summaries, if Source=sqlite). |

## Testing & Tooling Toggles

| Variable | Default | Purpose |
| --- | --- | --- |
| `FEISHU_LIVE_TEST` | unset (`0`) | When set to `1`, enables live Feishu integration tests (executed via `go test ./feishu -run Live`). Keep unset during normal CI to avoid hitting production tables. |

## Quick Reference

- All Feishu credentials/URLs should live in `.env` so `godotenv` can populate
  them for CLI runs (`piracy detect/report/auto`) and agents.
- Field overrides are optional; leaving them unset keeps the defaults that
  match the standard Feishu templates provided in `pkg/feishu`.
- Storage-related toggles let you enable/disable JSONL, SQLite, and Feishu
  sinks without code changes, which is helpful when debugging or running the
  agent on constrained machines.
