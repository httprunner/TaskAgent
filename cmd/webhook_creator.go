package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/httprunner/TaskAgent/internal/env"
	"github.com/httprunner/TaskAgent/internal/feishusdk"
	"github.com/httprunner/TaskAgent/pkg/webhook"
	"github.com/spf13/cobra"
)

func newWebhookCreatorCmd() *cobra.Command {
	var (
		flagTaskURL        string
		flagWebhookBitable string
		flagApp            string
		flagPollInterval   time.Duration
		flagBatchLimit     int
		flagDate           string
		flagBizType        string
		flagSkipExisting   bool
	)

	cmd := &cobra.Command{
		Use:   "webhook-creator",
		Short: "Create webhook result rows for external tasks (video screen capture)",
		RunE: func(cmd *cobra.Command, args []string) error {
			taskURL := firstNonEmpty(flagTaskURL, rootTaskURL, env.String(feishusdk.EnvTaskBitableURL, ""))
			if strings.TrimSpace(taskURL) == "" {
				return fmt.Errorf("--task-url or %s must be provided", feishusdk.EnvTaskBitableURL)
			}
			webhookBitable := firstNonEmpty(flagWebhookBitable, env.String(feishusdk.EnvWebhookBitableURL, ""))
			if strings.TrimSpace(webhookBitable) == "" {
				return fmt.Errorf("--webhook-bitable-url or %s must be provided", feishusdk.EnvWebhookBitableURL)
			}
			app := firstNonEmpty(flagApp, rootApp, env.String("BUNDLE_ID", ""))

			scanDate := strings.TrimSpace(flagDate)
			if scanDate == "" {
				scanDate = time.Now().In(time.Local).Format("2006-01-02")
			}
			if _, err := time.ParseInLocation("2006-01-02", scanDate, time.Local); err != nil {
				return fmt.Errorf("--date must be in YYYY-MM-DD format, got=%q", scanDate)
			}

			pollEnabled := cmd.Flags().Lookup("poll-interval").Changed
			if pollEnabled && flagPollInterval <= 0 {
				return fmt.Errorf("--poll-interval must be greater than 0 when enabled, got=%s", flagPollInterval)
			}

			bizType := strings.TrimSpace(flagBizType)
			if bizType == "" {
				bizType = webhook.WebhookBizTypePiracyGeneralSearch
			}

			creator, err := webhook.NewWebhookResultCreator(webhook.WebhookResultCreatorConfig{
				TaskBitableURL:    strings.TrimSpace(taskURL),
				WebhookBitableURL: strings.TrimSpace(webhookBitable),
				AppFilter:         strings.TrimSpace(app),
				PollInterval:      flagPollInterval,
				BatchLimit:        flagBatchLimit,
				ScanDate:          scanDate,
				BizType:           bizType,
				SkipExisting:      flagSkipExisting,
			})
			if err != nil {
				return err
			}
			if pollEnabled {
				return creator.Run(cmd.Context())
			}
			return creator.RunOnce(cmd.Context())
		},
	}

	cmd.Flags().StringVar(&flagTaskURL, "task-url", "", "Feishu task bitable URL (default from TASK_BITABLE_URL)")
	cmd.Flags().StringVar(&flagWebhookBitable, "webhook-bitable-url", "", "Webhook result bitable URL (default from WEBHOOK_BITABLE_URL)")
	cmd.Flags().StringVar(&flagApp, "app", "", "Optional App filter (defaults to root --app or BUNDLE_ID env)")
	cmd.Flags().DurationVar(&flagPollInterval, "poll-interval", 0, "Enable polling with the given interval (e.g. 30s); default runs once")
	cmd.Flags().IntVar(&flagBatchLimit, "batch-limit", 50, "Maximum number of tasks processed per scan")
	cmd.Flags().StringVar(&flagDate, "date", "", "Filter tasks by Datetime=ExactDate (YYYY-MM-DD); defaults to today")
	cmd.Flags().StringVar(&flagBizType, "biz-type", "", "BizType to process: piracy_general_search, video_screen_capture, or single_url_capture (default piracy_general_search)")
	cmd.Flags().BoolVar(&flagSkipExisting, "skip-existing", false, "Skip webhook rows when <BizType, GroupID, Date> already exists")

	return cmd
}
