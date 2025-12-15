package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/httprunner/TaskAgent/internal/env"
	"github.com/httprunner/TaskAgent/internal/feishusdk"
	"github.com/httprunner/TaskAgent/pkg/webhook"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func newWebhookWorkerCmd() *cobra.Command {
	var (
		flagTaskURL          string
		flagWebhookBitable   string
		flagSummaryWebhook   string
		flagPollInterval     time.Duration
		flagBatchLimit       int
		flagGroupCooldownDur time.Duration
	)

	cmd := &cobra.Command{
		Use:   "webhook-worker",
		Short: "Process webhook result rows in WEBHOOK_BITABLE_URL",
		RunE: func(cmd *cobra.Command, args []string) error {
			taskURL := firstNonEmpty(flagTaskURL, rootTaskURL, env.String(feishusdk.EnvTaskBitableURL, ""))
			if strings.TrimSpace(taskURL) == "" {
				return fmt.Errorf("--task-url or %s must be provided", feishusdk.EnvTaskBitableURL)
			}
			webhookBitable := firstNonEmpty(flagWebhookBitable, env.String(feishusdk.EnvWebhookBitableURL, ""), env.String("PUSH_RESULT_BITABLE_URL", ""))
			if strings.TrimSpace(webhookBitable) == "" {
				return fmt.Errorf("--webhook-bitable-url or %s must be provided", feishusdk.EnvWebhookBitableURL)
			}
			webhookURL := firstNonEmpty(flagSummaryWebhook, env.String("SUMMARY_WEBHOOK_URL", ""))
			if strings.TrimSpace(webhookURL) == "" {
				return fmt.Errorf("--webhook-url or SUMMARY_WEBHOOK_URL must be provided")
			}

			worker, err := webhook.NewWebhookResultWorker(webhook.WebhookResultWorkerConfig{
				TaskBitableURL:    strings.TrimSpace(taskURL),
				WebhookBitableURL: strings.TrimSpace(webhookBitable),
				SummaryWebhookURL: strings.TrimSpace(webhookURL),
				PollInterval:      flagPollInterval,
				BatchLimit:        flagBatchLimit,
				GroupCooldown:     flagGroupCooldownDur,
			})
			if err != nil {
				return err
			}
			log.Info().
				Str("task_bitable", strings.TrimSpace(taskURL)).
				Str("webhook_bitable", strings.TrimSpace(webhookBitable)).
				Str("webhook_url", strings.TrimSpace(webhookURL)).
				Msg("starting webhook result worker")
			return worker.Run(cmd.Context())
		},
	}

	cmd.Flags().StringVar(&flagTaskURL, "task-url", "", "Feishu task bitable URL (default from TASK_BITABLE_URL)")
	cmd.Flags().StringVar(&flagWebhookBitable, "webhook-bitable-url", "", "Webhook result bitable URL (default from WEBHOOK_BITABLE_URL)")
	cmd.Flags().StringVar(&flagSummaryWebhook, "webhook-url", "", "Summary webhook URL (default from SUMMARY_WEBHOOK_URL)")
	cmd.Flags().DurationVar(&flagPollInterval, "poll-interval", 30*time.Second, "Interval between scans")
	cmd.Flags().IntVar(&flagBatchLimit, "batch-limit", 20, "Maximum number of webhook result rows processed per scan")
	cmd.Flags().DurationVar(&flagGroupCooldownDur, "group-cooldown", 2*time.Minute, "Cooldown duration before rechecking an incomplete row")

	return cmd
}
