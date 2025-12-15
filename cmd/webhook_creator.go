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

func newWebhookCreatorCmd() *cobra.Command {
	var (
		flagTaskURL        string
		flagWebhookBitable string
		flagApp            string
		flagPollInterval   time.Duration
		flagBatchLimit     int
		flagTodayOnly      bool
	)

	cmd := &cobra.Command{
		Use:   "webhook-creator",
		Short: "Create webhook result rows for external tasks (video screen capture)",
		RunE: func(cmd *cobra.Command, args []string) error {
			taskURL := firstNonEmpty(flagTaskURL, rootTaskURL, env.String(feishusdk.EnvTaskBitableURL, ""))
			if strings.TrimSpace(taskURL) == "" {
				return fmt.Errorf("--task-url or %s must be provided", feishusdk.EnvTaskBitableURL)
			}
			webhookBitable := firstNonEmpty(flagWebhookBitable, env.String(feishusdk.EnvWebhookBitableURL, ""), env.String("PUSH_RESULT_BITABLE_URL", ""))
			if strings.TrimSpace(webhookBitable) == "" {
				return fmt.Errorf("--webhook-bitable-url or %s must be provided", feishusdk.EnvWebhookBitableURL)
			}
			app := firstNonEmpty(flagApp, rootApp, env.String("BUNDLE_ID", ""))

			creator, err := webhook.NewWebhookResultCreator(webhook.WebhookResultCreatorConfig{
				TaskBitableURL:    strings.TrimSpace(taskURL),
				WebhookBitableURL: strings.TrimSpace(webhookBitable),
				AppFilter:         strings.TrimSpace(app),
				PollInterval:      flagPollInterval,
				BatchLimit:        flagBatchLimit,
				ScanTodayOnly:     flagTodayOnly,
			})
			if err != nil {
				return err
			}
			log.Info().
				Str("task_bitable", strings.TrimSpace(taskURL)).
				Str("webhook_bitable", strings.TrimSpace(webhookBitable)).
				Str("app_filter", strings.TrimSpace(app)).
				Msg("starting webhook result creator")
			return creator.Run(cmd.Context())
		},
	}

	cmd.Flags().StringVar(&flagTaskURL, "task-url", "", "Feishu task bitable URL (default from TASK_BITABLE_URL)")
	cmd.Flags().StringVar(&flagWebhookBitable, "webhook-bitable-url", "", "Webhook result bitable URL (default from WEBHOOK_BITABLE_URL)")
	cmd.Flags().StringVar(&flagApp, "app", "", "Optional App filter (defaults to root --app or BUNDLE_ID env)")
	cmd.Flags().DurationVar(&flagPollInterval, "poll-interval", 30*time.Second, "Interval between scans")
	cmd.Flags().IntVar(&flagBatchLimit, "batch-limit", 50, "Maximum number of tasks processed per scan")
	cmd.Flags().BoolVar(&flagTodayOnly, "today-only", true, "Filter tasks by Datetime=Today when scanning the task table")

	return cmd
}
