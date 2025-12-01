package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	pool "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/pkg/feishu"
	"github.com/httprunner/TaskAgent/pkg/piracy"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func newDetectCmd() *cobra.Command {
	var (
		flagResultFilter string
		flagDramaFilter  string
		flagOutputCSV    string
		flagSQLitePath   string
		flagThreshold    float64
		flagParams       string
		flagReport       bool
	)

	cmd := &cobra.Command{
		Use:   "detect",
		Short: "Detect piracy using sqlite results + Feishu drama table",
		Long: `Detect possible piracy by comparing captured video data in sqlite
with original drama durations stored in a Feishu bitable. The command reads
Params can come from the Feishu 任务状态表 (固定 Scene=综合页搜索，Status=success，按 App 过滤) 或
be provided directly via the --params flag. The command fetches matching dramas,
looks up capture results from sqlite, evaluates ratios, optionally dumps a CSV,
and can optionally write suspicious combos back to the 任务状态表。

Example:
  piracy detect --sqlite /path/to/capture.sqlite --app com.smile.gifmaker`,
		RunE: func(cmd *cobra.Command, args []string) error {
			dbPath := strings.TrimSpace(flagSQLitePath)
			cfg := piracy.Config{}
			cfg.ApplyDefaults()

			reporter := piracy.NewReporter()
			if globalTask := strings.TrimSpace(cmd.Flag("task-url").Value.String()); globalTask != "" {
				reporter.OverrideTaskTableURL(globalTask)
			}
			if reporter.DramaTableURL() == "" {
				return fmt.Errorf("original drama table url is required ($DRAMA_BITABLE_URL)")
			}
			if reporter.TaskTableURL() == "" {
				return fmt.Errorf("task status table url is required ($TASK_BITABLE_URL)")
			}

			app := strings.TrimSpace(cmd.Flag("app").Value.String())

			params := getParams(flagParams)

			if len(params) == 0 {
				if app == "" {
					return fmt.Errorf("--app is required to fetch params from task table")
				}

				client, err := feishu.NewClientFromEnv()
				if err != nil {
					return err
				}

				params, err = piracy.FetchParamsFromTaskTable(cmd.Context(),
					client, reporter.TaskTableURL(),
					app, pool.SceneGeneralSearch, "success")
				if err != nil {
					return err
				}
				if len(params) == 0 {
					log.Warn().Msg("no params found in task table with given filters; nothing to detect")
					return nil
				}
			}

			// Ensure downstream sqlite consumers read the same db file
			if dbPath != "" {
				os.Setenv("TRACKING_STORAGE_DB_PATH", dbPath)
			}

			resultFilter, err := piracy.ParseFilterJSON(flagResultFilter)
			if err != nil {
				return fmt.Errorf("invalid result-filter: %w", err)
			}
			dramaFilter, err := piracy.ParseFilterJSON(flagDramaFilter)
			if err != nil {
				return fmt.Errorf("invalid drama-filter: %w", err)
			}

			if flagReport {
				if reporter.TaskTableURL() == "" {
					return fmt.Errorf("task status table url is required ($TASK_BITABLE_URL)")
				}
				if app == "" {
					return fmt.Errorf("--app is required when --report is set")
				}
			}

			threshold := reporter.Threshold()
			if flagThreshold > 0 {
				threshold = flagThreshold
			}

			log.Info().
				Int("params", len(params)).
				Str("sqlite_path", dbPath).
				Str("drama_table", reporter.DramaTableURL()).
				Str("task_table", reporter.TaskTableURL()).
				Float64("threshold", threshold).
				Msg("starting piracy detection from sqlite results")
			report, err := reporter.DetectWithFiltersThreshold(cmd.Context(),
				params, resultFilter, dramaFilter, threshold)
			if err != nil {
				return err
			}
			report = piracy.FilterReportByRatio(report, threshold)

			printReport(report)
			if flagOutputCSV != "" {
				if err := dumpCSV(flagOutputCSV, report); err != nil {
					return err
				}
			}

			if !flagReport {
				return nil
			}

			return reporter.ReportMatches(cmd.Context(), app, report.Matches)
		},
	}

	// Query flags
	cmd.Flags().StringVar(&flagResultFilter, "result-filter", "", "Feishu FilterInfo JSON for result rows")
	cmd.Flags().StringVar(&flagDramaFilter, "drama-filter", "", "Feishu FilterInfo JSON for drama rows")
	cmd.Flags().StringVar(&flagParams, "params", "", "Comma-separated list of drama params (optional override)")

	// SQLite mode flags
	cmd.Flags().StringVar(&flagSQLitePath, "sqlite", "", "Path to sqlite result database; enables sqlite mode when set")
	cmd.Flags().Float64Var(&flagThreshold, "threshold", 0, "覆盖阈值（小数，0.5 表示 50%）；默认读取 THRESHOLD 或配置")
	cmd.Flags().BoolVar(&flagReport, "report", false, "Write suspicious combos to task status table")

	// Common flags
	cmd.Flags().StringVar(&flagOutputCSV, "output-csv", "", "Optional output CSV file path")

	return cmd
}

func printReport(report *piracy.Report) {
	log.Info().
		Int("task_rows", report.TaskRows).
		Int("result_rows", report.ResultRows).
		Float64("threshold_percent", report.Threshold*100).
		Int("suspicious_combos", len(report.Matches)).
		Int("params_missing_duration", len(report.MissingParams)).
		Msg("piracy detection completed")

	if len(report.Matches) == 0 {
		log.Info().Msg("No suspicious combos found")
		if len(report.MissingParams) > 0 {
			log.Info().
				Int("count", len(report.MissingParams)).
				Msg("params without duration were skipped")
		}
		return
	}

	if len(report.MissingParams) > 0 {
		validCount := 0
		for _, key := range report.MissingParams {
			if !strings.HasPrefix(key, "{") && key != "" {
				validCount++
			}
		}
		if validCount > 0 {
			log.Info().
				Int("count", validCount).
				Msg("params without duration were skipped")
		}
	}

	if len(report.Matches) > 0 {
		jsonData, err := json.MarshalIndent(report.Matches, "", "  ")
		if err != nil {
			log.Error().Err(err).Msg("failed to marshal results to JSON")
		} else {
			fmt.Println(string(jsonData))
		}
	}
}

func dumpCSV(path string, report *piracy.Report) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create csv failed: %w", err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{"Params", "UserID", "UserName", "SumDurationSec", "TotalDurationSec", "RatioPct", "RecordCount"}); err != nil {
		return err
	}
	for _, match := range report.Matches {
		row := []string{
			match.Params,
			match.UserID,
			match.UserName,
			fmt.Sprintf("%.1f", match.SumDuration),
			fmt.Sprintf("%.1f", match.TotalDuration),
			fmt.Sprintf("%.1f", match.Ratio*100),
			fmt.Sprintf("%d", match.RecordCount),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	log.Info().
		Int("row_count", len(report.Matches)).
		Str("path", path).
		Msg("saved results to csv")
	return w.Error()
}
