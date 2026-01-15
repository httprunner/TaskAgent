package main

import (
	"os"

	"github.com/httprunner/TaskAgent/internal/env"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "taskagent",
	Short: "Infra helpers for Feishu-based workflows",
	Long:  `taskagent CLI 提供基于 Feishu 多维表格的基础设施工具（webhook 重试、单链采集、剧单转任务等），统一加载环境并输出结构化日志；具体业务检测逻辑由外部 agent 实现。`,
}

var (
	rootApp     string
	rootTaskURL string
)

func init() {
	output := zerolog.ConsoleWriter{Out: os.Stderr}
	log.Logger = zerolog.New(output).With().Timestamp().Logger()
	rootCmd.PersistentFlags().StringVar(&rootApp, "app", "", "App 包名覆盖 BUNDLE_ID")
	rootCmd.PersistentFlags().StringVar(&rootTaskURL, "task-url", "", "任务状态表 URL 覆盖 TASK_BITABLE_URL")
	rootCmd.AddCommand(
		newWebhookWorkerCmd(),
		newWebhookCreatorCmd(),
		newSingleURLCmd(),
		newDramaTasksCmd(),
		newSheetTasksCmd(),
	)
	_ = env.Ensure()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("taskagent command failed")
	}
}
