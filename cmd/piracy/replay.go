package main

import (
	"strings"

	"github.com/httprunner/TaskAgent/pkg/piracy"
	"github.com/spf13/cobra"
)

func newReplayCmd() *cobra.Command {
	var taskID int64
	var dbPath string
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Re-run piracy workflow for a cached sqlite task",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := piracy.ReplayConfig{
				TaskID: taskID,
				DBPath: dbPath,
				App:    strings.TrimSpace(rootApp),
			}
			return piracy.ReplayTask(cmd.Context(), cfg)
		},
	}
	cmd.Flags().Int64Var(&taskID, "task-id", 0, "TaskID to replay from sqlite cache")
	cmd.Flags().StringVar(&dbPath, "db-path", "", "optional sqlite path (defaults to tracking db)")
	return cmd
}
