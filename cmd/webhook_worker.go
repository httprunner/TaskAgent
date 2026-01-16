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
		flagPollInterval     time.Duration
		flagBatchLimit       int
		flagGroupCooldownDur time.Duration
		flagFilterGroupID    string
		flagFilterDate       string
	)

	cmd := &cobra.Command{
		Use:   "webhook-worker",
		Short: "Process webhook result rows in WEBHOOK_BITABLE_URL",
		RunE: func(cmd *cobra.Command, args []string) error {
			taskURL := firstNonEmpty(flagTaskURL, rootTaskURL, env.String(feishusdk.EnvTaskBitableURL, ""))
			if strings.TrimSpace(taskURL) == "" {
				return fmt.Errorf("--task-url or %s must be provided", feishusdk.EnvTaskBitableURL)
			}
			webhookBitable := firstNonEmpty(flagWebhookBitable, env.String(feishusdk.EnvWebhookBitableURL, ""))
			if strings.TrimSpace(webhookBitable) == "" {
				return fmt.Errorf("--webhook-bitable-url or %s must be provided", feishusdk.EnvWebhookBitableURL)
			}
			baseURL := strings.TrimSpace(env.String("CRAWLER_SERVICE_BASE_URL", ""))
			if baseURL == "" {
				return fmt.Errorf("CRAWLER_SERVICE_BASE_URL must be provided")
			}

			worker, err := webhook.NewWebhookResultWorker(webhook.WebhookResultWorkerConfig{
				TaskBitableURL:    strings.TrimSpace(taskURL),
				WebhookBitableURL: strings.TrimSpace(webhookBitable),
				PollInterval:      flagPollInterval,
				BatchLimit:        flagBatchLimit,
				GroupCooldown:     flagGroupCooldownDur,
				TargetGroupID:     strings.TrimSpace(flagFilterGroupID),
				TargetDate:        strings.TrimSpace(flagFilterDate),
			})
			if err != nil {
				return err
			}
			log.Info().
				Str("task_bitable", strings.TrimSpace(taskURL)).
				Str("webhook_bitable", strings.TrimSpace(webhookBitable)).
				Str("crawler_service_base_url", baseURL).
				Str("filter_group_id", strings.TrimSpace(flagFilterGroupID)).
				Str("filter_date", strings.TrimSpace(flagFilterDate)).
				Msg("starting webhook result worker")
			return worker.Run(cmd.Context())
		},
	}

	cmd.Flags().StringVar(&flagTaskURL, "task-url", "", "Feishu task bitable URL (default from TASK_BITABLE_URL)")
	cmd.Flags().StringVar(&flagWebhookBitable, "webhook-bitable-url", "", "Webhook result bitable URL (default from WEBHOOK_BITABLE_URL)")
	cmd.Flags().DurationVar(&flagPollInterval, "poll-interval", 30*time.Second, "Interval between scans")
	cmd.Flags().IntVar(&flagBatchLimit, "batch-limit", 20, "Maximum number of webhook result rows processed per scan")
	cmd.Flags().DurationVar(&flagGroupCooldownDur, "group-cooldown", 2*time.Minute, "Cooldown duration before rechecking an incomplete row")
	cmd.Flags().StringVar(&flagFilterGroupID, "group-id", "", "Limit processing to the specified GroupID (optional)")
	cmd.Flags().StringVar(&flagFilterDate, "date", "", "Limit processing to the specified logical date (YYYY-MM-DD, optional)")

	return cmd
}
