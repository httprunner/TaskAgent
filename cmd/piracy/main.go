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
	Short: "Piracy detection and reporting helpers",
	Long: `piracy combines the detection and reporting workflows for drama piracy
monitoring. The shared command sets up structured logging and environment loading
before delegating to subcommands.`,
}

func init() {
	output := zerolog.ConsoleWriter{Out: os.Stderr}
	log.Logger = zerolog.New(output).With().Timestamp().Logger()

	rootCmd.AddCommand(newDetectCmd(), newReportCmd(), newAutoCmd())
	_ = internal.Ensure()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("piracy command failed")
	}
}
