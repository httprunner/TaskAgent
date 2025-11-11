package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/httprunner/TaskAgent/feishu"
	"github.com/httprunner/TaskAgent/pkg/piracydetect"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func init() {
	// Configure zerolog to use console writer with colors (non-JSON format)
	output := zerolog.ConsoleWriter{Out: os.Stderr}
	log.Logger = zerolog.New(output).With().Timestamp().Logger()
}

var rootCmd = &cobra.Command{
	Use:   "piracy",
	Short: "Run piracy detection against result and drama bitables",
	RunE: func(cmd *cobra.Command, args []string) error {
		resultURL := pickOrEnv("", feishu.EnvResultBitableURL)
		dramaURL := pickOrEnv("", "DRAMA_BITABLE_URL")
		if resultURL == "" {
			return fmt.Errorf("result table url is required ($%s)", feishu.EnvResultBitableURL)
		}
		if dramaURL == "" {
			return fmt.Errorf("original drama table url is required ($DRAMA_BITABLE_URL)")
		}

		opts := piracydetect.Options{
			ResultTable: piracydetect.TableConfig{
				URL:    resultURL,
				Filter: flagResultFilter,
			},
			DramaTable: piracydetect.TableConfig{
				URL:    dramaURL,
				Filter: flagDramaFilter,
			},
			// Config fields will be read from environment variables
		}

		log.Info().
			Str("result_table", resultURL).
			Str("drama_table", dramaURL).
			Msg("starting piracy detection")

		report, err := piracydetect.Detect(cmd.Context(), opts)
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
	flagResultFilter string
	flagTargetFilter string
	flagDramaFilter  string
	flagOutputCSV    string
)

func init() {
	rootCmd.Flags().StringVar(&flagResultFilter, "result-filter", "", "Feishu filter for result rows")
	rootCmd.Flags().StringVar(&flagTargetFilter, "target-filter", "", "Feishu filter for target rows")
	rootCmd.Flags().StringVar(&flagDramaFilter, "drama-filter", "", "Feishu filter for original drama rows")
	rootCmd.Flags().StringVar(&flagOutputCSV, "output-csv", "", "optional csv to persist suspicious combos")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("piracy detect failed")
	}
}

func pickOrEnv(flagVal, envKey string) string {
	if trimmed := strings.TrimSpace(flagVal); trimmed != "" {
		return trimmed
	}
	return strings.TrimSpace(os.Getenv(envKey))
}

func printReport(report *piracydetect.Report) {
	// Use structured logging for statistics
	log.Info().
		Int("target_rows", report.TargetRows).
		Int("result_rows", report.ResultRows).
		Float64("threshold_percent", report.Threshold*100).
		Int("suspicious_combos", len(report.Matches)).
		Int("params_missing_duration", len(report.MissingParams)).
		Msg("piracy detection completed")

	// Only print detailed results if there are matches
	if len(report.Matches) == 0 {
		log.Info().Msg("No suspicious combos found")
		// Also log missing params info if there were missing durations
		if len(report.MissingParams) > 0 {
			log.Info().
				Int("count", len(report.MissingParams)).
				Msg("params without duration were skipped")
		}
		return
	}

	// Log missing params (simplified, just count)
	if len(report.MissingParams) > 0 {
		// Only log valid keys, skip malformed ones with JSON
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

	// Print suspicious combos as JSON list
	if len(report.Matches) > 0 {
		jsonData, err := json.MarshalIndent(report.Matches, "", "  ")
		if err != nil {
			log.Error().Err(err).Msg("failed to marshal results to JSON")
		} else {
			fmt.Println(string(jsonData))
		}
	}
}

func dumpCSV(path string, report *piracydetect.Report) error {
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
