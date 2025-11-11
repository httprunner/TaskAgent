package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"github.com/httprunner/TaskAgent/feishu"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "feishu-piracy",
	Short: "Run Feishu piracy detection against result/target bitables",
	RunE: func(cmd *cobra.Command, args []string) error {
		resultURL := pickOrEnv(flagResultTable, feishu.EnvResultBitableURL)
		targetURL := pickOrEnv(flagTargetTable, feishu.EnvTargetBitableURL)
		if resultURL == "" {
			return fmt.Errorf("result table url is required (flag or $%s)", feishu.EnvResultBitableURL)
		}
		if targetURL == "" {
			return fmt.Errorf("target table url is required (flag or $%s)", feishu.EnvTargetBitableURL)
		}
		threshold := normalizeThreshold(flagThreshold)
		opts := PiracyOptions{
			ResultTableURL:      resultURL,
			TargetTableURL:      targetURL,
			ResultViewID:        flagResultView,
			TargetViewID:        flagTargetView,
			ResultFilter:        flagResultFilter,
			TargetFilter:        flagTargetFilter,
			ResultLimit:         flagResultLimit,
			TargetLimit:         flagTargetLimit,
			ParamsField:         strings.TrimSpace(flagParamsField),
			UserIDField:         strings.TrimSpace(flagUserField),
			DurationField:       strings.TrimSpace(flagDurationField),
			TargetDurationField: strings.TrimSpace(flagTargetDurationField),
			ThresholdRatio:      threshold,
		}
		report, err := RunPiracyDetection(cmd.Context(), opts)
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

var (
	flagResultTable         string
	flagTargetTable         string
	flagResultView          string
	flagTargetView          string
	flagResultFilter        string
	flagTargetFilter        string
	flagResultLimit         int
	flagTargetLimit         int
	flagParamsField         string
	flagUserField           string
	flagDurationField       string
	flagTargetDurationField string
	flagThreshold           float64
	flagOutputCSV           string
)

func init() {
	rootCmd.Flags().StringVar(&flagResultTable, "result-table", "", "result table URL or set $RESULT_BITABLE_URL")
	rootCmd.Flags().StringVar(&flagTargetTable, "target-table", "", "target table URL or set $TARGET_BITABLE_URL")
	rootCmd.Flags().StringVar(&flagResultView, "result-view", "", "optional view id for result table")
	rootCmd.Flags().StringVar(&flagTargetView, "target-view", "", "optional view id for target table")
	rootCmd.Flags().StringVar(&flagResultFilter, "result-filter", "", "Feishu filter for result rows")
	rootCmd.Flags().StringVar(&flagTargetFilter, "target-filter", "", "Feishu filter for target rows")
	rootCmd.Flags().IntVar(&flagResultLimit, "result-limit", 0, "maximum number of result rows to fetch (0=all)")
	rootCmd.Flags().IntVar(&flagTargetLimit, "target-limit", 0, "maximum number of target rows to fetch (0=all)")
	rootCmd.Flags().StringVar(&flagParamsField, "params-field", "", "column name that holds the drama identifier")
	rootCmd.Flags().StringVar(&flagUserField, "user-field", "", "column name that holds the author ID")
	rootCmd.Flags().StringVar(&flagDurationField, "duration-field", "", "column name that holds ItemDuration")
	rootCmd.Flags().StringVar(&flagTargetDurationField, "target-duration-field", "", "column name that holds the drama total duration")
	rootCmd.Flags().Float64Var(&flagThreshold, "threshold", 50, "minimum ratio to mark piracy (50 means 50%%)")
	rootCmd.Flags().StringVar(&flagOutputCSV, "output-csv", "", "optional csv to persist suspicious combos")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func pickOrEnv(flagVal, envKey string) string {
	if trimmed := strings.TrimSpace(flagVal); trimmed != "" {
		return trimmed
	}
	return strings.TrimSpace(os.Getenv(envKey))
}

func normalizeThreshold(value float64) float64 {
	if value <= 0 {
		return 0.5
	}
	if value > 1 {
		return value / 100
	}
	return value
}

func printReport(report PiracyReport) {
	fmt.Printf("Fetched %d target rows and %d result rows (threshold %.1f%%)\n",
		report.TargetRows, report.ResultRows, report.ThresholdRatio*100)
	if len(report.Matches) == 0 {
		fmt.Println("No suspicious combos found")
		return
	}
	fmt.Printf("Found %d suspicious combos (params missing duration: %d)\n", len(report.Matches), len(report.MissingTargetParams))
	for _, match := range report.Matches {
		fmt.Printf("- Params=%s | UserID=%s | ItemDuration=%.1f | Target=%.1f | Ratio=%.1f%% | Records=%d\n",
			match.Params, match.UserID, match.ItemDuration, match.TargetDuration, match.Ratio*100, match.RecordCount)
	}
	if len(report.MissingTargetParams) > 0 {
		fmt.Printf("Skipped %d params without duration: %s\n", len(report.MissingTargetParams), strings.Join(report.MissingTargetParams, ","))
	}
}

func dumpCSV(path string, report PiracyReport) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create csv failed: %w", err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{"Params", "UserID", "ItemDurationSec", "TargetDurationSec", "RatioPct", "RecordCount"}); err != nil {
		return err
	}
	for _, match := range report.Matches {
		row := []string{
			match.Params,
			match.UserID,
			fmt.Sprintf("%.1f", match.ItemDuration),
			fmt.Sprintf("%.1f", match.TargetDuration),
			fmt.Sprintf("%.1f", match.Ratio*100),
			fmt.Sprintf("%d", match.RecordCount),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	fmt.Printf("Saved %d rows to %s\n", len(report.Matches), path)
	return w.Error()
}
