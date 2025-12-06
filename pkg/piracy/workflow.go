package piracy

import (
	"context"
	"fmt"
	"strings"
	"time"

	pool "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/pkg/feishu"
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

type bucketKey struct {
	bookID string
	app    string
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

	grouped := make(map[bucketKey][]*pool.FeishuTask)
	for _, task := range tasks {
		if task == nil {
			continue
		}
		bookID := strings.TrimSpace(task.BookID)
		app := strings.TrimSpace(task.App)
		if bookID == "" || app == "" {
			continue
		}
		key := bucketKey{bookID: bookID, app: app}
		grouped[key] = append(grouped[key], task)
	}

	var errs []string
	for key, bucket := range grouped {
		if len(bucket) == 0 {
			continue
		}
		parent := firstFeishuTask(bucket)
		params, ready := w.collectBookParams(ctx, key, parent, bucket)
		if !ready {
			log.Debug().
				Str("book_id", key.bookID).
				Str("app", key.app).
				Msg("piracy workflow skipping bucket: tasks not ready for detection")
			continue
		}
		if len(params) == 0 || parent == nil {
			continue
		}

		log.Info().
			Str("book_id", key.bookID).
			Str("app", key.app).
			Int("task_count", len(bucket)).
			Int("params", len(params)).
			Msg("piracy workflow running detection for completed bucket")

		details, err := w.reporter.DetectMatchesWithDetails(ctx, params)
		if err != nil {
			errs = append(errs, fmt.Sprintf("book %s detection failed: %v", key.bookID, err))
			continue
		}
		if len(details) == 0 {
			continue
		}
		filtered := filterDetailsByBookID(details, key.bookID)
		if len(filtered) == 0 {
			continue
		}

		if err := w.reporter.CreateGroupTasksForPiracyMatches(ctx, pkg, parent.TaskID, parent.Datetime, parent.DatetimeRaw, key.bookID, filtered); err != nil {
			errs = append(errs, fmt.Sprintf("book %s report failed: %v", key.bookID, err))
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (w *Workflow) collectBookParams(ctx context.Context, key bucketKey, parent *pool.FeishuTask, bucket []*pool.FeishuTask) ([]string, bool) {
	date := taskDayFromFeishuTask(parent)
	rows, err := fetchBookCaptureTasks(ctx, key.bookID, key.app, pool.SceneGeneralSearch, date)
	if err != nil {
		log.Warn().
			Err(err).
			Str("book_id", key.bookID).
			Str("app", key.app).
			Msg("piracy workflow: capture_tasks lookup failed; falling back to in-memory bucket")
		return paramsFromFeishuTasks(bucket)
	}
	if len(rows) == 0 {
		log.Debug().
			Str("book_id", key.bookID).
			Str("app", key.app).
			Msg("piracy workflow: no capture_tasks rows found; falling back to bucket statuses")
		return paramsFromFeishuTasks(bucket)
	}
	if !captureTasksAllSuccess(rows) {
		return nil, false
	}
	params := paramsFromCaptureTasks(rows)
	if len(params) == 0 {
		return nil, false
	}
	return params, true
}

func paramsFromFeishuTasks(tasks []*pool.FeishuTask) ([]string, bool) {
	seen := make(map[string]struct{}, len(tasks))
	params := make([]string, 0, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		if strings.TrimSpace(task.Status) != feishu.StatusSuccess {
			return nil, false
		}
		if trimmed := strings.TrimSpace(task.Params); trimmed != "" {
			if _, ok := seen[trimmed]; ok {
				continue
			}
			seen[trimmed] = struct{}{}
			params = append(params, trimmed)
		}
	}
	if len(params) == 0 {
		return nil, false
	}
	return params, true
}

func firstFeishuTask(tasks []*pool.FeishuTask) *pool.FeishuTask {
	for _, task := range tasks {
		if task != nil {
			return task
		}
	}
	return nil
}

func taskDayFromFeishuTask(task *pool.FeishuTask) *time.Time {
	if task == nil {
		return nil
	}
	if task.Datetime != nil {
		local := task.Datetime.In(time.Local)
		return &local
	}
	trimmed := strings.TrimSpace(task.DatetimeRaw)
	if trimmed == "" {
		return nil
	}
	if parsed, _ := parseBackfillDatetime(trimmed); parsed != nil {
		return parsed
	}
	return nil
}

func filterDetailsByBookID(details []MatchDetail, bookID string) []MatchDetail {
	if len(details) == 0 {
		return nil
	}
	trimmed := strings.TrimSpace(bookID)
	if trimmed == "" {
		return details
	}
	filtered := make([]MatchDetail, 0, len(details))
	for _, detail := range details {
		if strings.TrimSpace(detail.Match.BookID) == trimmed {
			filtered = append(filtered, detail)
		}
	}
	return filtered
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
	bookID := strings.TrimSpace(task.BookID)
	userID := strings.TrimSpace(task.UserID)
	userName := strings.TrimSpace(task.UserName)
	for idx := range report.Matches {
		match := report.Matches[idx]
		if bookID != "" {
			if strings.TrimSpace(match.BookID) != bookID {
				continue
			}
		} else if strings.TrimSpace(match.Params) != param {
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
