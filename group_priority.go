package taskagent

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

func NewGroupTaskPrioritizer(base TaskManager, taskTableURL string, prioritizerOpts GroupTaskPrioritizerOptions) (*GroupTaskPrioritizer, error) {
	if base == nil {
		return nil, errors.New("base task manager is nil")
	}
	url := strings.TrimSpace(taskTableURL)
	if url == "" {
		return nil, errors.New("task table url is empty")
	}
	prioritizerOpts = normalizeGroupTaskPrioritizerOptions(prioritizerOpts)
	counter, err := NewGroupTaskRemainingCounter(url, prioritizerOpts.CountCap)
	if err != nil {
		return nil, err
	}
	return &GroupTaskPrioritizer{
		Base:    base,
		Counter: counter,
		Opts:    prioritizerOpts,
	}, nil
}

type GroupTaskRemainingCounter struct {
	Client       *FeishuClient
	TaskTableURL string
	// CountCap caps how many pending/failed rows are scanned for a single group.
	CountCap int
}

func NewGroupTaskRemainingCounter(taskTableURL string, countCap int) (*GroupTaskRemainingCounter, error) {
	client, err := NewFeishuClientFromEnv()
	if err != nil {
		return nil, err
	}
	return &GroupTaskRemainingCounter{
		Client:       client,
		TaskTableURL: taskTableURL,
		CountCap:     countCap,
	}, nil
}

func (c *GroupTaskRemainingCounter) CountRemaining(ctx context.Context, app string, key GroupKey) (int, error) {
	if c == nil || c.Client == nil {
		return 0, errors.New("group remaining counter client is nil")
	}
	tableURL := strings.TrimSpace(c.TaskTableURL)
	if tableURL == "" {
		return 0, errors.New("group remaining counter task table url is empty")
	}
	key = normalizeGroupKey(key)
	if key.GroupID == "" || key.BizType == "" || key.Day == "" {
		return 0, nil
	}
	fields := DefaultTaskFields()
	groupField := strings.TrimSpace(fields.GroupID)
	sceneField := strings.TrimSpace(fields.Scene)
	dateField := strings.TrimSpace(fields.Date)
	statusField := strings.TrimSpace(fields.Status)
	appField := strings.TrimSpace(fields.App)
	if groupField == "" || sceneField == "" || dateField == "" || statusField == "" {
		return 0, errors.New("task table field mapping is missing (GroupID/Scene/Date/Status)")
	}

	dayCond := ExactDateCondition(dateField, key.Day)
	if dayCond == nil {
		return 0, nil
	}

	filter := NewFeishuFilterInfo("and")
	filter.Conditions = append(filter.Conditions,
		NewFeishuCondition(groupField, "is", key.GroupID),
		dayCond,
	)
	if appField != "" && strings.TrimSpace(app) != "" {
		filter.Conditions = append(filter.Conditions, NewFeishuCondition(appField, "is", strings.TrimSpace(app)))
	}

	statusOr := NewFeishuFilterInfo("or")
	statusOr.Conditions = append(statusOr.Conditions,
		NewFeishuCondition(statusField, "is", StatusPending),
		NewFeishuCondition(statusField, "is", StatusFailed),
	)

	scenes := scenesForBizType(key.BizType)
	if len(scenes) == 0 {
		return 0, nil
	}
	sceneConds := make([]*FeishuCondition, 0, len(scenes))
	for _, s := range scenes {
		if trimmed := strings.TrimSpace(s); trimmed != "" {
			sceneConds = append(sceneConds, NewFeishuCondition(sceneField, "is", trimmed))
		}
	}
	sceneOr := NewFeishuFilterInfo("or")
	sceneOr.Conditions = append(sceneOr.Conditions, sceneConds...)

	final := MergeFeishuFiltersAND(filter, statusOr, sceneOr)
	limit := c.CountCap
	if limit <= 0 {
		limit = defaultGroupPriorityCountCap
	}
	table, err := c.Client.FetchTaskTableWithOptions(ctx, tableURL, nil, &FeishuTaskQueryOptions{
		Filter:     final,
		Limit:      limit,
		IgnoreView: true,
	})
	if err != nil {
		return 0, err
	}
	if table == nil {
		return 0, nil
	}
	return len(table.Rows), nil
}

func normalizeGroupKey(key GroupKey) GroupKey {
	key.BizType = strings.TrimSpace(key.BizType)
	key.GroupID = strings.TrimSpace(key.GroupID)
	key.Day = strings.TrimSpace(key.Day)
	return key
}

const (
	TaskBizTypePiracyGeneralSearch = "piracy_general_search"
	TaskBizTypeVideoScreenCapture  = "video_screen_capture"
	TaskBizTypeSingleURLCapture    = "single_url_capture"

	defaultGroupPriorityOversample        = 10
	defaultGroupPriorityCountTTL          = 20 * time.Second
	defaultGroupPriorityMaxGroupsPerFetch = 40
	defaultGroupPriorityCountCap          = 200
	defaultGroupPriorityLargeRemaining    = 1 << 30
	defaultGroupPriorityFocusGroups       = 2
)

func normalizeGroupTaskPrioritizerOptions(opts GroupTaskPrioritizerOptions) GroupTaskPrioritizerOptions {
	if opts.Oversample <= 0 {
		opts.Oversample = defaultGroupPriorityOversample
	}
	if opts.CountTTL <= 0 {
		opts.CountTTL = defaultGroupPriorityCountTTL
	}
	if opts.MaxGroupsPerFetch <= 0 {
		opts.MaxGroupsPerFetch = defaultGroupPriorityMaxGroupsPerFetch
	}
	if opts.DefaultLargeRemaining <= 0 {
		opts.DefaultLargeRemaining = defaultGroupPriorityLargeRemaining
	}
	if opts.CountCap <= 0 {
		opts.CountCap = defaultGroupPriorityCountCap
	}
	return opts
}

type GroupKey struct {
	BizType string
	GroupID string
	Day     string
}

func (k GroupKey) String() string {
	return fmt.Sprintf("%s|%s|%s", strings.TrimSpace(k.BizType), strings.TrimSpace(k.GroupID), strings.TrimSpace(k.Day))
}

type GroupRemainingCounter interface {
	CountRemaining(ctx context.Context, app string, key GroupKey) (int, error)
}

type GroupTaskPrioritizerOptions struct {
	// Oversample controls how many candidates are fetched from the base task manager
	// before re-ordering and truncating to the requested limit.
	Oversample int
	// CountTTL controls how long group remaining counts are cached in memory.
	CountTTL time.Duration
	// MaxGroupsPerFetch caps how many distinct groups can be counted per fetch call.
	MaxGroupsPerFetch int
	// DefaultLargeRemaining is used when group counting fails or is unavailable.
	DefaultLargeRemaining int
	// Clock is used for cache TTL decisions.
	Clock func() time.Time
	// CountCap caps how many pending/failed rows are scanned for a single group.
	CountCap int
	// FocusGroups caps how many top-ranked groups can be selected per fetch.
	// When >0, tasks are first selected from the remaining-minimum K groups, then filled from the rest.
	FocusGroups int
}

type groupCountEntry struct {
	remaining int
	at        time.Time
}

// GroupTaskPrioritizer re-orders tasks by group remaining count (ascending) so that
// "almost done" groups are preferentially completed and can be pushed downstream earlier.
//
// Remaining counts are computed as the number of tasks with Status in {pending, failed}
// under the same <BizType, GroupID, Day> key.
type GroupTaskPrioritizer struct {
	Base    TaskManager
	Counter GroupRemainingCounter
	Opts    GroupTaskPrioritizerOptions

	once  sync.Once
	mu    sync.Mutex
	cache map[string]groupCountEntry
}

func (p *GroupTaskPrioritizer) FetchAvailableTasks(ctx context.Context, app string, limit int) ([]*Task, error) {
	if p == nil || p.Base == nil {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if limit <= 0 {
		return nil, nil
	}
	p.ensureDefaults()
	oversample := p.Opts.Oversample
	candidateLimit := limit
	if oversample > 1 {
		if next := limit * oversample; next > limit {
			candidateLimit = next
		}
	}
	candidates, err := p.Base.FetchAvailableTasks(ctx, app, candidateLimit)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 || len(candidates) <= 1 {
		if len(candidates) > limit {
			return candidates[:limit], nil
		}
		return candidates, nil
	}

	remainingByTask := p.resolveRemaining(ctx, strings.TrimSpace(app), candidates)
	if len(remainingByTask) == 0 {
		if len(candidates) > limit {
			return candidates[:limit], nil
		}
		return candidates, nil
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		a := candidates[i]
		b := candidates[j]
		ra, oka := remainingByTask[a]
		rb, okb := remainingByTask[b]
		if !oka {
			ra = p.defaultLargeRemaining()
		}
		if !okb {
			rb = p.defaultLargeRemaining()
		}
		if ra != rb {
			return ra < rb
		}
		ka := groupKeyString(a)
		kb := groupKeyString(b)
		if ka != kb {
			return ka < kb
		}
		return false
	})

	if p.Opts.FocusGroups > 0 {
		candidates = focusCandidates(candidates, remainingByTask, p.Opts.FocusGroups, limit)
	} else if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	return candidates, nil
}

func focusCandidates(candidates []*Task, remainingByTask map[*Task]int, focusGroups int, limit int) []*Task {
	if limit <= 0 || len(candidates) == 0 {
		return candidates
	}
	if focusGroups <= 0 {
		if len(candidates) > limit {
			return candidates[:limit]
		}
		return candidates
	}
	remainingByGroup := make(map[string]int)
	for task, remaining := range remainingByTask {
		if task == nil {
			continue
		}
		key := groupKeyString(task)
		if key == "" {
			continue
		}
		if prev, ok := remainingByGroup[key]; !ok || remaining < prev {
			remainingByGroup[key] = remaining
		}
	}
	type groupRank struct {
		key       string
		remaining int
	}
	ranks := make([]groupRank, 0, len(remainingByGroup))
	for key, remaining := range remainingByGroup {
		ranks = append(ranks, groupRank{key: key, remaining: remaining})
	}
	sort.Slice(ranks, func(i, j int) bool {
		if ranks[i].remaining != ranks[j].remaining {
			return ranks[i].remaining < ranks[j].remaining
		}
		return ranks[i].key < ranks[j].key
	})
	if len(ranks) > focusGroups {
		ranks = ranks[:focusGroups]
	}
	focusSet := make(map[string]struct{}, len(ranks))
	for _, r := range ranks {
		focusSet[r.key] = struct{}{}
	}

	out := make([]*Task, 0, minInt(limit, len(candidates)))
	used := make(map[*Task]struct{})
	for _, t := range candidates {
		if len(out) >= limit {
			return out
		}
		if t == nil {
			continue
		}
		if _, ok := focusSet[groupKeyString(t)]; !ok {
			continue
		}
		out = append(out, t)
		used[t] = struct{}{}
	}
	for _, t := range candidates {
		if len(out) >= limit {
			break
		}
		if t == nil {
			continue
		}
		if _, ok := used[t]; ok {
			continue
		}
		out = append(out, t)
	}
	return out
}

func groupKeyString(task *Task) string {
	key, ok := groupKeyFromTask(task)
	if !ok {
		return ""
	}
	return key.String()
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (p *GroupTaskPrioritizer) OnTasksDispatched(ctx context.Context, deviceSerial string, tasks []*Task) error {
	if p == nil || p.Base == nil {
		return nil
	}
	return p.Base.OnTasksDispatched(ctx, deviceSerial, tasks)
}

func (p *GroupTaskPrioritizer) OnTaskStarted(ctx context.Context, deviceSerial string, task *Task) error {
	if p == nil || p.Base == nil {
		return nil
	}
	return p.Base.OnTaskStarted(ctx, deviceSerial, task)
}

func (p *GroupTaskPrioritizer) OnTaskResult(ctx context.Context, deviceSerial string, task *Task, runErr error) error {
	if p == nil || p.Base == nil {
		return nil
	}
	if err := p.Base.OnTaskResult(ctx, deviceSerial, task, runErr); err != nil {
		return err
	}
	if runErr != nil {
		return nil
	}
	p.decrementGroupIfPossible(task)
	return nil
}

func (p *GroupTaskPrioritizer) OnTasksCompleted(ctx context.Context, deviceSerial string, tasks []*Task, jobErr error) error {
	if p == nil || p.Base == nil {
		return nil
	}
	return p.Base.OnTasksCompleted(ctx, deviceSerial, tasks, jobErr)
}

func (p *GroupTaskPrioritizer) defaultLargeRemaining() int {
	if p == nil {
		return defaultGroupPriorityLargeRemaining
	}
	p.ensureDefaults()
	if p.Opts.DefaultLargeRemaining > 0 {
		return p.Opts.DefaultLargeRemaining
	}
	return defaultGroupPriorityLargeRemaining
}

func (p *GroupTaskPrioritizer) now() time.Time {
	if p != nil && p.Opts.Clock != nil {
		return p.Opts.Clock()
	}
	return time.Now()
}

func (p *GroupTaskPrioritizer) resolveRemaining(ctx context.Context, app string, tasks []*Task) map[*Task]int {
	if p == nil || p.Counter == nil || len(tasks) == 0 {
		return nil
	}
	p.ensureDefaults()
	keys := make([]GroupKey, 0, len(tasks))
	keyByTask := make(map[*Task]GroupKey, len(tasks))
	seen := make(map[string]struct{})
	for _, t := range tasks {
		key, ok := groupKeyFromTask(t)
		if !ok {
			continue
		}
		k := key.String()
		keyByTask[t] = key
		if _, exists := seen[k]; exists {
			continue
		}
		seen[k] = struct{}{}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return nil
	}

	maxGroups := p.Opts.MaxGroupsPerFetch
	if len(keys) > maxGroups {
		keys = keys[:maxGroups]
	}

	remainingByKey := make(map[string]int, len(keys))
	for _, key := range keys {
		k := key.String()
		remaining, ok := p.cachedRemaining(k)
		if ok {
			remainingByKey[k] = remaining
			continue
		}
		remaining, err := p.Counter.CountRemaining(ctx, app, key)
		if err != nil {
			remainingByKey[k] = p.defaultLargeRemaining()
			continue
		}
		if remaining <= 0 {
			p.deleteCache(k)
			remainingByKey[k] = 0
			continue
		}
		p.setCache(k, remaining)
		remainingByKey[k] = remaining
	}

	remainingByTask := make(map[*Task]int, len(tasks))
	for task, key := range keyByTask {
		if task == nil {
			continue
		}
		if remaining, ok := remainingByKey[key.String()]; ok {
			remainingByTask[task] = remaining
		}
	}
	return remainingByTask
}

func (p *GroupTaskPrioritizer) cachedRemaining(key string) (int, bool) {
	if p == nil {
		return 0, false
	}
	p.ensureDefaults()
	ttl := p.Opts.CountTTL
	now := p.now()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cache == nil {
		return 0, false
	}
	entry, ok := p.cache[key]
	if !ok {
		return 0, false
	}
	if now.Sub(entry.at) > ttl {
		delete(p.cache, key)
		return 0, false
	}
	return entry.remaining, true
}

func (p *GroupTaskPrioritizer) ensureDefaults() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		p.Opts = normalizeGroupTaskPrioritizerOptions(p.Opts)
	})
}

func (p *GroupTaskPrioritizer) setCache(key string, remaining int) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cache == nil {
		p.cache = make(map[string]groupCountEntry)
	}
	p.cache[key] = groupCountEntry{remaining: remaining, at: p.now()}
}

func (p *GroupTaskPrioritizer) deleteCache(key string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cache == nil {
		return
	}
	delete(p.cache, key)
}

func (p *GroupTaskPrioritizer) decrementGroupIfPossible(task *Task) {
	key, ok := groupKeyFromTask(task)
	if !ok {
		return
	}
	k := key.String()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cache == nil {
		return
	}
	entry, ok := p.cache[k]
	if !ok {
		return
	}
	entry.remaining--
	if entry.remaining <= 0 {
		delete(p.cache, k)
		return
	}
	entry.at = p.now()
	p.cache[k] = entry
}

func groupKeyFromTask(task *Task) (GroupKey, bool) {
	if task == nil {
		return GroupKey{}, false
	}
	ft, ok := task.Payload.(*FeishuTask)
	if !ok || ft == nil {
		return GroupKey{}, false
	}
	groupID := strings.TrimSpace(ft.GroupID)
	if groupID == "" {
		return GroupKey{}, false
	}
	day := feishuTaskDay(ft)
	if day == "" {
		return GroupKey{}, false
	}
	biz := BizTypeForScene(strings.TrimSpace(ft.Scene))
	if biz == "" {
		return GroupKey{}, false
	}
	return GroupKey{
		BizType: biz,
		GroupID: groupID,
		Day:     day,
	}, true
}

func feishuTaskDay(task *FeishuTask) string {
	if task == nil {
		return ""
	}
	if task.Datetime != nil {
		return task.Datetime.In(time.Local).Format("2006-01-02")
	}
	return ""
}

func scenesForBizType(bizType string) []string {
	switch strings.TrimSpace(bizType) {
	case TaskBizTypeVideoScreenCapture:
		return []string{SceneVideoScreenCapture}
	case TaskBizTypeSingleURLCapture:
		return []string{SceneSingleURLCapture}
	case TaskBizTypePiracyGeneralSearch:
		return []string{SceneGeneralSearch, SceneProfileSearch, SceneCollection, SceneAnchorCapture}
	default:
		return nil
	}
}

func BizTypeForScene(scene string) string {
	switch strings.TrimSpace(scene) {
	case SceneVideoScreenCapture:
		return TaskBizTypeVideoScreenCapture
	case SceneSingleURLCapture:
		return TaskBizTypeSingleURLCapture
	case SceneGeneralSearch, SceneProfileSearch, SceneCollection, SceneAnchorCapture:
		return TaskBizTypePiracyGeneralSearch
	default:
		return ""
	}
}
