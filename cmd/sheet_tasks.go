package main

import (
	"fmt"
	"strings"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/internal/env"
	"github.com/spf13/cobra"
)

func newSheetTasksCmd() *cobra.Command {
	var (
		flagSourceURL string
		flagTaskURL   string
		flagBatchSize int
		flagLimit     int
		flagPoll      time.Duration
	)

	cmd := &cobra.Command{
		Use:   "create-tasks",
		Short: "Create task records from a Feishu spreadsheet",
		Long:  "读取飞书表格中的原始任务信息，筛选未传行并创建多维表格任务，同时回写已传状态。",
		RunE: func(cmd *cobra.Command, args []string) error {
			sourceURLs := taskagent.ParseSheetSourceURLs(firstNonEmpty(flagSourceURL, env.String(taskagent.EnvSourceSheetURL, "")))
			taskURL := firstNonEmpty(flagTaskURL, rootTaskURL, env.String(taskagent.EnvTaskBitableURL, ""))
			if len(sourceURLs) == 0 {
				return fmt.Errorf("--source-url or $%s is required", taskagent.EnvSourceSheetURL)
			}
			if strings.TrimSpace(taskURL) == "" {
				return fmt.Errorf("--task-url or $%s is required", taskagent.EnvTaskBitableURL)
			}
			return taskagent.RunSheetTasks(cmd.Context(), taskagent.SheetTaskOptions{
				SourceURLs:   sourceURLs,
				TaskURL:      taskURL,
				BatchSize:    flagBatchSize,
				Limit:        flagLimit,
				PollInterval: flagPoll,
			})
		},
	}

	cmd.Flags().StringVar(&flagSourceURL, "source-url", "", "Source Feishu sheet URL overriding $SOURCE_SHEET_URL")
	cmd.Flags().StringVar(&flagTaskURL, "task-url", "", "Task status table URL overriding $TASK_BITABLE_URL")
	cmd.Flags().IntVar(&flagBatchSize, "batch-size", 500, "Batch size for task creation and sheet updates")
	cmd.Flags().IntVar(&flagLimit, "limit", 2000, "Maximum number of tasks to create in this run (0 means no limit)")
	cmd.Flags().DurationVar(&flagPoll, "poll-interval", 0, "Polling interval for continuous task creation (0 means run once)")

	return cmd
}
