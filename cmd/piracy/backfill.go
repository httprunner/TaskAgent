package main

import (
	"strings"

	"github.com/httprunner/TaskAgent/pkg/piracy"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func newBackfillCmd() *cobra.Command {
	var (
		flagDate         string
		flagSync         bool
		flagSkipExisting bool
		flagDBPath       string
		flagBookIDs      string
	)

	cmd := &cobra.Command{
		Use:   "backfill",
		Short: "重新扫描综合页成功任务，按需补写个人页/合集/锚点任务",
		RunE: func(cmd *cobra.Command, args []string) error {
			bookIDs := parseCSV(flagBookIDs)
			cfg := piracy.BackfillConfig{
				Date:         flagDate,
				Sync:         flagSync,
				SkipExisting: flagSkipExisting,
				DBPath:       flagDBPath,
				TaskTableURL: rootTaskURL,
				AppOverride:  rootApp,
				BookIDs:      bookIDs,
			}
			stats, err := piracy.BackfillTasks(cmd.Context(), cfg)
			if stats != nil {
				log.Info().
					Int("scanned", stats.Scanned).
					Int("matched", stats.Matched).
					Int("created", stats.Created).
					Int("skipped_existing", stats.SkippedExisting).
					Int("failed", len(stats.Failures)).
					Msg("piracy backfill 完成")
			}
			return err
		},
	}

	cmd.Flags().StringVar(&flagDate, "date", "", "仅处理指定日期 (YYYY-MM-DD) 的综合页任务")
	cmd.Flags().BoolVar(&flagSync, "sync", false, "开启写表；默认仅检测不写入")
	cmd.Flags().BoolVar(&flagSkipExisting, "skip-existing", false, "写表前若检测到已有 Group 任务则跳过")
	cmd.Flags().StringVar(&flagDBPath, "db-path", "", "自定义 capture_tasks sqlite 路径")
	cmd.Flags().StringVar(&flagBookIDs, "book-id", "", "仅处理指定 BookID（逗号分隔）")
	return cmd
}

func parseCSV(raw string) []string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	parts := strings.Split(trimmed, ",")
	result := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		v := strings.TrimSpace(part)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		result = append(result, v)
	}
	return result
}
