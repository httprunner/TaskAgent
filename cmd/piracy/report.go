package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/httprunner/TaskAgent/pkg/piracyreport"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func newReportCmd() *cobra.Command {
	var (
		flagApp        string
		flagParams     string
		flagParamsFile string
	)

	cmd := &cobra.Command{
		Use:   "report",
		Short: "Detect piracy for specific drama params and report to drama table",
		Long: `Detect piracy for specified drama params and write suspicious matches back to the drama table.
The command reuses the same drama and result tables that the detection workflow targets.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if flagApp == "" {
				return fmt.Errorf("app package name is required (e.g., com.smile.gifmaker)")
			}

			paramsList, err := getParamsList(flagParams, flagParamsFile)
			if err != nil {
				return fmt.Errorf("failed to get params: %w", err)
			}
			if len(paramsList) == 0 {
				return fmt.Errorf("no params provided. Use --params or --params-file flag")
			}

			log.Info().
				Str("app", flagApp).
				Int("params_count", len(paramsList)).
				Msg("starting piracy report")

			reporter := piracyreport.NewReporter()
			if !reporter.IsConfigured() {
				return fmt.Errorf("reporter not configured. Please check environment variables: FEISHU_APP_ID, FEISHU_APP_SECRET, RESULT_BITABLE_URL, DRAMA_BITABLE_URL")
			}

			ctx := context.Background()
			if err := reporter.ReportPiracyForParams(ctx, flagApp, paramsList); err != nil {
				return fmt.Errorf("piracy report failed: %w", err)
			}

			log.Info().Msg("Piracy report completed successfully")
			return nil
		},
	}

	cmd.Flags().StringVar(&flagApp, "app", "", "App package name (e.g., com.smile.gifmaker)")
	cmd.Flags().StringVar(&flagParams, "params", "", "Comma-separated list of drama params (e.g., \"短剧A,短剧B\")")
	cmd.Flags().StringVar(&flagParamsFile, "params-file", "", "Path to file containing drama params (one per line)")
	_ = cmd.MarkFlagRequired("app")

	return cmd
}

func getParamsList(cliParams, cliFile string) ([]string, error) {
	if cliFile != "" {
		content, err := os.ReadFile(cliFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read params file: %w", err)
		}

		lines := strings.Split(string(content), "\n")
		paramsList := make([]string, 0, len(lines))
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line != "" && !strings.HasPrefix(line, "#") {
				paramsList = append(paramsList, line)
			}
		}
		return paramsList, nil
	}

	if cliParams == "" {
		return []string{}, nil
	}

	params := strings.Split(cliParams, ",")
	result := make([]string, 0, len(params))
	for _, param := range params {
		param = strings.TrimSpace(param)
		if param != "" {
			result = append(result, param)
		}
	}
	return result, nil
}
