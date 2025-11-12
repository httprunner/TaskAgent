package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/httprunner/TaskAgent/pkg/feishu"
	"github.com/httprunner/TaskAgent/pkg/piracy"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func newDetectCmd() *cobra.Command {
	var (
		flagResultFilter string
		flagTargetFilter string
		flagDramaFilter  string
		flagOutputCSV    string
		flagDataFile     string
		flagDramaFile    string
		flagFilterRate   float64
		flagUseFiles     bool
	)

	cmd := &cobra.Command{
		Use:   "detect",
		Short: "Detect possible piracy from Feishu tables or local files",
		Long: `Detect possible piracy by comparing video data with drama information.

Mode 1: Feishu Tables (default)
  Uses Feishu bitable as data source. Requires RESULT_BITABLE_URL and DRAMA_BITABLE_URL.

Mode 2: Local Files (--use-files)
  Uses local JSONL and CSV files as data source. Requires --data-file and --drama-file.

Examples:
  # Feishu mode (default)
  piracy detect

  # Local file mode
  piracy detect --use-files --data-file records.jsonl --drama-file dramas.csv`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Check if using local file mode
			if flagUseFiles {
				// Validate required file parameters
				if flagDataFile == "" {
					return fmt.Errorf("--data-file is required for file mode")
				}
				if flagDramaFile == "" {
					return fmt.Errorf("--drama-file is required for file mode")
				}

				log.Info().
					Str("data_file", flagDataFile).
					Str("drama_file", flagDramaFile).
					Float64("filter_rate", flagFilterRate).
					Msg("starting piracy detection from files")

				// Run piracy detection from files
				return piracy.DetectFromFiles(flagDataFile, flagDramaFile, flagFilterRate, flagOutputCSV)
			}

			// Default: Feishu mode
			resultURL := pickOrEnv("", feishu.EnvResultBitableURL)
			dramaURL := pickOrEnv("", "DRAMA_BITABLE_URL")
			if resultURL == "" {
				return fmt.Errorf("result table url is required ($%s)", feishu.EnvResultBitableURL)
			}
			if dramaURL == "" {
				return fmt.Errorf("original drama table url is required ($DRAMA_BITABLE_URL)")
			}

			opts := piracy.Options{
				ResultTable: piracy.TableConfig{
					URL:    resultURL,
					Filter: flagResultFilter,
				},
				DramaTable: piracy.TableConfig{
					URL:    dramaURL,
					Filter: flagDramaFilter,
				},
				// Config fields will be read from environment variables
			}

			log.Info().
				Str("result_table", resultURL).
				Str("drama_table", dramaURL).
				Msg("starting piracy detection from Feishu tables")

			report, err := piracy.Detect(cmd.Context(), opts)
			if err != nil {
				return err
			}

			printReport(report)
			if flagOutputCSV != "" {
				return dumpCSV(flagOutputCSV, report)
			}
			return nil
		},
	}

	// Feishu mode flags
	cmd.Flags().StringVar(&flagResultFilter, "result-filter", "", "Feishu filter for result rows")
	cmd.Flags().StringVar(&flagTargetFilter, "target-filter", "", "Feishu filter for target rows")
	cmd.Flags().StringVar(&flagDramaFilter, "drama-filter", "", "Feishu filter for original drama rows")

	// File mode flags
	cmd.Flags().BoolVar(&flagUseFiles, "use-files", false, "Use local files instead of Feishu tables")
	cmd.Flags().StringVar(&flagDataFile, "data-file", "", "Input JSONL data file path (for file mode)")
	cmd.Flags().StringVar(&flagDramaFile, "drama-file", "", "Input CSV drama file path (for file mode)")
	cmd.Flags().Float64Var(&flagFilterRate, "filter-rate", 50.0, "Filter rate threshold for display in percentage (default 50, for file mode)")

	// Common flags
	cmd.Flags().StringVar(&flagOutputCSV, "output-csv", "", "Optional output CSV file path")

	return cmd
}

func pickOrEnv(flagVal, envKey string) string {
	if trimmed := strings.TrimSpace(flagVal); trimmed != "" {
		return trimmed
	}
	return strings.TrimSpace(os.Getenv(envKey))
}

func printReport(report *piracy.Report) {
	log.Info().
		Int("target_rows", report.TargetRows).
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
