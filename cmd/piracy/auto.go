package main

import (
	"fmt"

	"github.com/httprunner/TaskAgent/pkg/piracy"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func newAutoCmd() *cobra.Command {
	var (
		flagApp          string
		flagOutput       string
		flagResultFilter string
		flagDramaFilter  string
		flagConcurrency  int
	)

	cmd := &cobra.Command{
		Use:   "auto",
		Short: "Run detection and reporting for all dramas",
		RunE: func(cmd *cobra.Command, args []string) error {
			resultFilter, err := piracy.ParseFilterJSON(flagResultFilter)
			if err != nil {
				return fmt.Errorf("invalid result-filter: %w", err)
			}
			dramaFilter, err := piracy.ParseFilterJSON(flagDramaFilter)
			if err != nil {
				return fmt.Errorf("invalid drama-filter: %w", err)
			}

			opts := piracy.AutoOptions{
				App:          flagApp,
				OutputCSV:    flagOutput,
				ResultFilter: resultFilter,
				DramaFilter:  dramaFilter,
				Concurrency:  flagConcurrency,
			}

			summary, err := piracy.RunAuto(cmd.Context(), opts)
			if err != nil {
				return err
			}

			log.Info().
				Int("matches", summary.MatchCount).
				Int("dramas", summary.DramaCount).
				Int("concurrency", summary.Concurrency).
				Float64("threshold", summary.Threshold).
				Str("csv", summary.CSVPath).
				Msg("piracy auto completed")
			return nil
		},
	}

	cmd.Flags().StringVar(&flagApp, "app", "", "App package name (e.g., com.smile.gifmaker)")
	cmd.Flags().StringVar(&flagOutput, "output", "", "Output CSV file path (defaults to ./piracy_auto_<timestamp>.csv)")
	cmd.Flags().StringVar(&flagResultFilter, "result-filter", "", "Optional FilterInfo JSON applied to the result table")
	cmd.Flags().StringVar(&flagDramaFilter, "drama-filter", "", "Optional FilterInfo JSON applied to the drama table")
	cmd.Flags().IntVar(&flagConcurrency, "concurrency", 10, "Number of dramas processed in parallel (default 10)")

	if err := cmd.MarkFlagRequired("app"); err != nil {
		// Cobra only returns error when flag name is invalid. Panic highlights programmer error.
		panic(fmt.Sprintf("failed to mark --app as required: %v", err))
	}

	return cmd
}
