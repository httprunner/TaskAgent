package main

import (
	"github.com/httprunner/TaskAgent/pkg/piracy"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func newDramaTasksCmd() *cobra.Command {
	var (
		flagDate      string
		flagDramaURL  string
		flagAliasSeps []string
		flagBatchSize int
	)

	cmd := &cobra.Command{
		Use:   "drama-tasks",
		Short: "Create 综合页搜索 tasks from drama catalog aliases",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := piracy.DramaTaskConfig{
				Date:            flagDate,
				App:             firstNonEmpty(rootApp, ""),
				Scene:           "",
				TaskTableURL:    firstNonEmpty(rootTaskURL, ""),
				DramaTableURL:   flagDramaURL,
				AliasSeparators: flagAliasSeps,
				BatchSize:       flagBatchSize,
			}
			if cfg.App == "" {
				cfg.App = "com.smile.gifmaker"
			}

			res, err := piracy.CreateDramaSearchTasks(cmd.Context(), cfg)
			if err != nil {
				return err
			}
			log.Info().
				Str("date", res.Date).
				Int("dramas", res.DramaCount).
				Int("params", res.ParamCount).
				Int("created", res.CreatedCount).
				Msg("drama tasks created")
			for _, detail := range res.Details {
				log.Debug().
					Str("drama", detail.DramaName).
					Str("book_id", detail.BookID).
					Int("params", detail.ParamCount).
					Msg("drama params expanded")
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&flagDate, "date", "", "采集日期 (YYYY-MM-DD)")
	cmd.Flags().StringVar(&flagDramaURL, "drama-url", "", "覆盖 $DRAMA_BITABLE_URL 的剧单表 URL")
	cmd.Flags().StringSliceVar(&flagAliasSeps, "alias-sep", []string{"|", "｜"}, "搜索别名分隔符列表（默认支持 | 和 ｜）")
	cmd.Flags().IntVar(&flagBatchSize, "batch-size", 500, "写入任务时的批大小 (<=500)")
	_ = cmd.MarkFlagRequired("date")

	return cmd
}
