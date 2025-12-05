package piracy

import (
	"context"
	"fmt"
	"strings"

	pool "github.com/httprunner/TaskAgent"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// WorkflowConfig encapsulates the dependencies required to run the piracy workflow.
type WorkflowConfig struct {
	// TargetBitableURL points to the Feishu table that stores target tasks.
	TargetBitableURL string
	// ProfileScene identifies tasks that should follow the profile-search branch.
	// Defaults to "个人页搜索" if empty.
	ProfileScene string
	// Reporter allows callers to inject a pre-configured piracy reporter.
	Reporter *Reporter
	// FeishuClient allows dependency injection for testing; if nil TargetBitableURL must be set.
	FeishuClient *pool.FeishuTaskClient
}

// Workflow orchestrates piracy ratio detection, extra-field updates, and optional webhook syncs.
type Workflow struct {
	reporter     *Reporter
	feishuClient *pool.FeishuTaskClient
	profileScene string
}

// NewWorkflow builds a workflow from the provided configuration. If the target table URL
// or reporter configuration is missing, the function returns (nil, nil) so callers can gracefully skip it.
func NewWorkflow(cfg WorkflowConfig) (*Workflow, error) {
	trimmedURL := strings.TrimSpace(cfg.TargetBitableURL)
	client := cfg.FeishuClient
	if client == nil && trimmedURL == "" {
		return nil, nil
	}

	reporter := cfg.Reporter
	if reporter == nil {
		reporter = NewReporter()
	}
	if reporter == nil || !reporter.IsConfigured() {
		log.Warn().Msg("piracy reporter not configured; skip piracy workflow")
		return nil, nil
	}

	if client == nil {
		var err error
		client, err = pool.NewFeishuTaskClient(trimmedURL)
		if err != nil {
			return nil, errors.Wrap(err, "init feishu task client for piracy workflow failed")
		}
	}

	workflow := &Workflow{
		reporter:     reporter,
		feishuClient: client,
		profileScene: strings.TrimSpace(cfg.ProfileScene),
	}
	return workflow, nil
}

// Process dispatches search tasks to the appropriate piracy handling branch based on the configured scenes.
func (w *Workflow) Process(ctx context.Context, pkg string, tasks []*pool.FeishuTask) error {
	if w == nil || len(tasks) == 0 {
		return nil
	}
	var general []*pool.FeishuTask
	var personal []*pool.FeishuTask
	for _, task := range tasks {
		if task == nil {
			continue
		}
		if w.isProfileSearchTask(task) {
			personal = append(personal, task)
		} else {
			general = append(general, task)
		}
	}

	var errs []string
	if len(general) > 0 {
		if err := w.handleGeneralSearch(ctx, pkg, general); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(personal) > 0 {
		if err := w.handleProfileSearch(ctx, pkg, personal); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (w *Workflow) handleGeneralSearch(ctx context.Context, pkg string, tasks []*pool.FeishuTask) error {
	if w.reporter == nil {
		return nil
	}

	// Process each general search task individually to use its TaskID as parentTaskID
	var errs []string
	for _, task := range tasks {
		if task == nil {
			continue
		}
		params := strings.TrimSpace(task.Params)
		if params == "" {
			continue
		}

		log.Info().
			Int64("task_id", task.TaskID).
			Str("params", params).
			Msg("piracy workflow running detection with video details for general task")

		// Detect piracy matches with video details (ItemID, Tags, AnchorPoint)
		details, err := w.reporter.DetectMatchesWithDetails(ctx, []string{params})
		if err != nil {
			errs = append(errs, fmt.Sprintf("task %d detection failed: %v", task.TaskID, err))
			continue
		}

		if len(details) == 0 {
			log.Info().
				Int64("task_id", task.TaskID).
				Str("params", params).
				Msg("no piracy matches found for task")
			continue
		}

		log.Info().
			Int64("task_id", task.TaskID).
			Str("params", params).
			Int("match_details", len(details)).
			Msg("piracy matches found with video details, creating child tasks")

		// Create group tasks based on video details:
		// - 1 "个人页搜索" task per match
		// - 1 "合集视频采集" task if any video has collection tag
		// - N "视频锚点采集" tasks for each video with appLink
		if err := w.reporter.CreateGroupTasksForPiracyMatches(ctx, pkg, task.TaskID, task.Datetime, task.DatetimeRaw, task.BookID, details); err != nil {
			errs = append(errs, fmt.Sprintf("task %d report failed: %v", task.TaskID, err))
			continue
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (w *Workflow) handleProfileSearch(ctx context.Context, pkg string, tasks []*pool.FeishuTask) error {
	if w.reporter == nil || w.feishuClient == nil {
		return nil
	}
	if err := w.updateTaskExtras(ctx, tasks); err != nil {
		return err
	}
	return nil
}

func (w *Workflow) detectExtraForTask(ctx context.Context, task *pool.FeishuTask) string {
	if w.reporter == nil || task == nil {
		return ""
	}
	params := strings.TrimSpace(task.Params)
	if params == "" {
		return ""
	}
	report, err := w.reporter.DetectWithFiltersThreshold(ctx, []string{params}, nil, nil, 0)
	if err != nil {
		log.Error().Err(err).Str("params", params).Msg("piracy ratio detection failed")
		return ""
	}
	match := w.matchReportForTask(report, task)
	if match != nil {
		return fmt.Sprintf("ratio=%.2f%%", match.Ratio*100)
	}
	return fmt.Sprintf("ratio<%.0f%%", w.reporter.Threshold()*100)
}

func (w *Workflow) updateTaskExtras(ctx context.Context, tasks []*pool.FeishuTask) error {
	if w == nil || w.feishuClient == nil || len(tasks) == 0 {
		return nil
	}
	updates := w.collectExtraUpdates(ctx, tasks)
	if len(updates) == 0 {
		return nil
	}
	if err := w.feishuClient.UpdateTaskExtras(ctx, updates); err != nil {
		return errors.Wrap(err, "update extra field failed")
	}
	return nil
}

func (w *Workflow) collectExtraUpdates(ctx context.Context, tasks []*pool.FeishuTask) []pool.TaskExtraUpdate {
	cache := make(map[string]string)
	updates := make([]pool.TaskExtraUpdate, 0, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		extra := w.cachedExtraForTask(ctx, task, cache)
		if extra == "" {
			continue
		}
		updates = append(updates, pool.TaskExtraUpdate{Task: task, Extra: extra})
	}
	return updates
}

func (w *Workflow) cachedExtraForTask(ctx context.Context, task *pool.FeishuTask, cache map[string]string) string {
	if task == nil {
		return ""
	}
	if cache == nil {
		cache = make(map[string]string)
	}
	keyParts := []string{
		strings.TrimSpace(task.Params),
		strings.TrimSpace(task.UserID),
		strings.TrimSpace(task.UserName),
	}
	key := strings.Join(keyParts, "|")
	if val, ok := cache[key]; ok {
		return val
	}
	val := w.detectExtraForTask(ctx, task)
	cache[key] = val
	return val
}

func (w *Workflow) matchReportForTask(report *Report, task *pool.FeishuTask) *Match {
	if report == nil || len(report.Matches) == 0 || task == nil {
		return nil
	}
	param := strings.TrimSpace(task.Params)
	userID := strings.TrimSpace(task.UserID)
	userName := strings.TrimSpace(task.UserName)
	for idx := range report.Matches {
		match := report.Matches[idx]
		if strings.TrimSpace(match.Params) != param {
			continue
		}
		if userID != "" && strings.TrimSpace(match.UserID) == userID {
			return &report.Matches[idx]
		}
		if userID == "" && userName != "" && strings.TrimSpace(match.UserName) == userName {
			return &report.Matches[idx]
		}
	}
	return nil
}

func (w *Workflow) isProfileSearchTask(task *pool.FeishuTask) bool {
	if task == nil {
		return false
	}
	if trimmed := strings.TrimSpace(task.Scene); trimmed != "" {
		return trimmed == w.profileScene
	}
	return false
}
