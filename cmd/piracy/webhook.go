package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/httprunner/TaskAgent/pkg/feishu"
	"github.com/httprunner/TaskAgent/pkg/piracy"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func newWebhookCmd() *cobra.Command {
	var (
		flagTargetURL  string
		flagWebhookURL string
		flagApp        string
		flagPoll       time.Duration
		flagBatchLimit int
	)

	cmd := &cobra.Command{
		Use:   "webhook-worker",
		Short: "Retry pending/failed summary webhooks stored in the task bitable",
		RunE: func(cmd *cobra.Command, args []string) error {
			target := firstNonEmpty(flagTargetURL, os.Getenv(feishu.EnvTaskBitableURL))
			if strings.TrimSpace(target) == "" {
				return fmt.Errorf("--task-url or %s must be provided", feishu.EnvTaskBitableURL)
			}
			summary := firstNonEmpty(flagWebhookURL, os.Getenv("SUMMARY_WEBHOOK_URL"))
			if strings.TrimSpace(summary) == "" {
				return fmt.Errorf("--webhook-url or SUMMARY_WEBHOOK_URL must be provided")
			}
			app := firstNonEmpty(flagApp, os.Getenv("BUNDLE_ID"))

			cfg := piracy.WebhookWorkerConfig{
				TaskBitableURL:    strings.TrimSpace(target),
				SummaryWebhookURL: strings.TrimSpace(summary),
				App:               strings.TrimSpace(app),
				PollInterval:      flagPoll,
				BatchLimit:        flagBatchLimit,
			}
			worker, err := piracy.NewWebhookWorker(cfg)
			if err != nil {
				return err
			}
			log.Info().
				Str("task_bitable", cfg.TaskBitableURL).
				Str("webhook_url", cfg.SummaryWebhookURL).
				Str("app_filter", cfg.App).
				Dur("poll_interval", cfg.PollInterval).
				Int("batch_limit", cfg.BatchLimit).
				Msg("starting piracy webhook worker")
			return worker.Run(cmd.Context())
		},
	}

	cmd.Flags().StringVar(&flagTargetURL, "task-url", "", "Feishu task bitable URL (default from TASK_BITABLE_URL)")
	cmd.Flags().StringVar(&flagWebhookURL, "webhook-url", "", "Summary webhook URL (default from SUMMARY_WEBHOOK_URL)")
	cmd.Flags().StringVar(&flagApp, "app", "", "Optional App filter (defaults to BUNDLE_ID env)")
	cmd.Flags().DurationVar(&flagPoll, "poll-interval", 30*time.Second, "Interval between webhook scans")
	cmd.Flags().IntVar(&flagBatchLimit, "batch-limit", 20, "Maximum number of webhook tasks processed per scan")

	return cmd
}

func firstNonEmpty(values ...string) string {
	for _, val := range values {
		if trimmed := strings.TrimSpace(val); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
