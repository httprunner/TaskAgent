package main

import (
	"fmt"
	"strings"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/internal/env"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

const (
	createSourceSheet   = "sheet"
	createSourceBitable = "bitable"

	createTaskSingleURL     = "single-url"
	createTaskGeneralSearch = "general-search"
	createTaskProfileSearch = "profile-search"
)

func newCreateTasksCmd() *cobra.Command {
	var (
		flagSource       string
		flagTaskType     string
		flagSourceURL    string
		flagTaskURL      string
		flagDate         string
		flagKeywordSeps  []string
		flagBatchSize    int
		flagSkipExisting bool
		flagLimit        int
		flagPoll         time.Duration
	)

	cmd := &cobra.Command{
		Use:   "create-tasks",
		Short: "Create tasks from Feishu sources",
		Long:  "统一创建任务入口：支持飞书表格（单链采集）与多维表（综合页搜索/个人页搜索）。",
		RunE: func(cmd *cobra.Command, args []string) error {
			source := strings.TrimSpace(flagSource)
			taskType := strings.TrimSpace(flagTaskType)
			if source == "" {
				return fmt.Errorf("--source is required (sheet|bitable)")
			}
			if taskType == "" {
				return fmt.Errorf("--task-type is required (single-url|general-search|profile-search)")
			}

			switch source {
			case createSourceSheet:
				if taskType != createTaskSingleURL {
					return fmt.Errorf("source=%s requires --task-type=%s", createSourceSheet, createTaskSingleURL)
				}
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
			case createSourceBitable:
				if taskType != createTaskGeneralSearch && taskType != createTaskProfileSearch {
					return fmt.Errorf("source=%s requires --task-type=%s or %s", createSourceBitable, createTaskGeneralSearch, createTaskProfileSearch)
				}
				if strings.TrimSpace(flagDate) == "" {
					return fmt.Errorf("--date is required for search tasks (YYYY-MM-DD)")
				}
				taskURL := firstNonEmpty(flagTaskURL, rootTaskURL, env.String(taskagent.EnvTaskBitableURL, ""))
				if strings.TrimSpace(taskURL) == "" {
					return fmt.Errorf("--task-url or $%s is required", taskagent.EnvTaskBitableURL)
				}
				scene := taskagent.SceneGeneralSearch
				sourceURL := strings.TrimSpace(flagSourceURL)
				if taskType == createTaskProfileSearch {
					scene = taskagent.SceneProfileSearch
					if sourceURL == "" {
						sourceURL = env.String(taskagent.EnvAccountBitableURL, "")
					}
				} else if sourceURL == "" {
					sourceURL = env.String(taskagent.EnvDramaBitableURL, "")
				}
				cfg := taskagent.SearchTaskConfig{
					Date:              flagDate,
					App:               firstNonEmpty(rootApp, taskagent.DefaultSearchTaskApp),
					Scene:             scene,
					TaskTableURL:      taskURL,
					SourceTableURL:    sourceURL,
					KeywordSeparators: flagKeywordSeps,
					BatchSize:         flagBatchSize,
					SkipExisting:      flagSkipExisting,
				}
				res, err := taskagent.CreateSearchTasks(cmd.Context(), cfg)
				if err != nil {
					return err
				}
				if taskType == createTaskProfileSearch {
					log.Info().
						Str("date", res.Date).
						Int("accounts", res.SourceCount).
						Int("params", res.TotalParams).
						Int("created", res.CreatedCount).
						Msg("profile search tasks created")
					for _, detail := range res.Details {
						log.Debug().
							Str("biz_task_id", detail.BizTaskID).
							Str("account_id", detail.AccountID).
							Str("book_id", detail.BookID).
							Int("params", detail.ExpandedParams).
							Msg("profile search params expanded")
					}
					return nil
				}
				log.Info().
					Str("date", res.Date).
					Int("dramas", res.SourceCount).
					Int("params", res.TotalParams).
					Int("created", res.CreatedCount).
					Msg("drama tasks created")
				for _, detail := range res.Details {
					log.Debug().
						Str("drama", detail.DramaName).
						Str("book_id", detail.BookID).
						Int("params", detail.ExpandedParams).
						Msg("drama params expanded")
				}
				return nil
			default:
				return fmt.Errorf("unsupported source %q (supported: sheet, bitable)", source)
			}
		},
	}

	cmd.Flags().StringVar(&flagSource, "source", "", "Task source: sheet|bitable")
	cmd.Flags().StringVar(&flagTaskType, "task-type", "", "Task type: single-url|general-search|profile-search")
	cmd.Flags().StringVar(&flagSourceURL, "source-url", "", "Source table/sheet URL overriding env")
	cmd.Flags().StringVar(&flagTaskURL, "task-url", "", "Task status table URL overriding $TASK_BITABLE_URL")
	cmd.Flags().StringVar(&flagDate, "date", "", "采集日期 (YYYY-MM-DD), required for search tasks")
	cmd.Flags().StringSliceVar(&flagKeywordSeps, "keyword-sep", taskagent.DefaultSearchKeywordSeparators(), "搜索词分隔符列表（默认支持 | 和 ｜）")
	cmd.Flags().IntVar(&flagBatchSize, "batch-size", 500, "写入任务时的批大小 (<=500)")
	cmd.Flags().BoolVar(&flagSkipExisting, "skip-existing", false, "若源表中 TaskID 非空则跳过创建（仅适用于搜索任务）")
	cmd.Flags().IntVar(&flagLimit, "limit", 2000, "Maximum number of tasks to create in this run (0 means no limit, sheet only)")
	cmd.Flags().DurationVar(&flagPoll, "poll-interval", 0, "Polling interval for continuous task creation (0 means run once, sheet only)")

	return cmd
}
