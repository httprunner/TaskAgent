package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pool "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/pkg/feishu"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func newSingleURLCmd() *cobra.Command {
	var (
		flagTaskURL        string
		flagCrawlerBaseURL string
		flagPollInterval   time.Duration
		flagFetchLimit     int
		flagOnce           bool
	)

	cmd := &cobra.Command{
		Use:   "singleurl",
		Short: "Run the SingleURLWorker",
		Long:  "Dedicated worker for single URL capture tasks; polls the task table and calls the crawler service.",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			taskURL := strings.TrimSpace(flagTaskURL)
			if taskURL == "" {
				taskURL = strings.TrimSpace(os.Getenv(feishu.EnvTaskBitableURL))
			}
			if taskURL == "" {
				return fmt.Errorf("--task-url or $%s is required", feishu.EnvTaskBitableURL)
			}
			if strings.TrimSpace(flagCrawlerBaseURL) != "" {
				if err := os.Setenv("CRAWLER_SERVICE_BASE_URL", strings.TrimSpace(flagCrawlerBaseURL)); err != nil {
					return fmt.Errorf("set CRAWLER_SERVICE_BASE_URL failed: %w", err)
				}
			}
			poll := flagPollInterval
			if poll <= 0 {
				poll = 30 * time.Second
			}
			limit := flagFetchLimit
			if limit <= 0 {
				limit = pool.DefaultSingleURLWorkerLimit
			}
			worker, err := pool.NewSingleURLWorkerFromEnv(taskURL, limit, poll)
			if err != nil {
				return err
			}
			if flagOnce {
				log.Info().
					Str("task_url", taskURL).
					Bool("once", true).
					Int("fetch_limit", limit).
					Msg("singleurl worker processing once")
				return worker.ProcessOnce(ctx)
			}
			sigCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
			defer stop()
			log.Info().
				Str("task_url", taskURL).
				Dur("poll_interval", poll).
				Int("fetch_limit", limit).
				Msg("singleurl worker running")
			return worker.Run(sigCtx)
		},
	}

	cmd.Flags().StringVar(&flagTaskURL, "task-url", "", "Task status table URL overriding $TASK_BITABLE_URL")
	cmd.Flags().StringVar(&flagCrawlerBaseURL, "crawler-base-url", "", "Crawler service endpoint overriding $CRAWLER_SERVICE_BASE_URL")
	cmd.Flags().DurationVar(&flagPollInterval, "poll-interval", 30*time.Second, "Polling interval when running continuously")
	cmd.Flags().IntVar(&flagFetchLimit, "fetch-limit", pool.DefaultSingleURLWorkerLimit, "Maximum tasks to fetch per cycle")
	cmd.Flags().BoolVar(&flagOnce, "once", false, "Run only one ProcessOnce cycle and exit")

	return cmd
}
