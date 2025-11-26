package main

import (
	"os"

	"github.com/httprunner/TaskAgent/internal"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "piracy",
	Short: "Piracy detection helpers",
	Long: `piracy 提供短剧盗版检测与（可选）上报的 CLI，统一加载环境并输出结构化日志。`,
}

func init() {
	output := zerolog.ConsoleWriter{Out: os.Stderr}
	log.Logger = zerolog.New(output).With().Timestamp().Logger()

	rootCmd.AddCommand(newDetectCmd(), newWebhookCmd())
	_ = internal.Ensure()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("piracy command failed")
	}
}
