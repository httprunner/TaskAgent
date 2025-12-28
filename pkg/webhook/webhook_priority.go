package webhook

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/rs/zerolog/log"
)

// WebhookTaskPrioritizer caches TaskIDs from pending/failed webhook rows.
type WebhookTaskPrioritizer struct {
	tableURL   string
	batchLimit int
	ttl        time.Duration
	nodeIndex  int
	nodeTotal  int
	fetcher    TaskFetcher

	mu        sync.Mutex
	lastFetch time.Time
	cacheList []int64
}

// TaskFetcher provides task fetching methods needed for webhook prioritization.
type TaskFetcher interface {
	FetchAvailableTasks(ctx context.Context, app string, limit int) ([]*taskagent.Task, error)
	FetchTasksByIDs(ctx context.Context, app string, taskIDs []int64, limit int) ([]*taskagent.Task, error)
}

type shardConfigProvider interface {
	ShardConfig() (int, int, bool)
}

// WebhookTaskPrioritizerOptions customizes prioritizer behavior.
type WebhookTaskPrioritizerOptions struct {
	BatchLimit int
	TTL        time.Duration
	NodeIndex  int
	NodeTotal  int
	Fetcher    TaskFetcher
}

// NewWebhookTaskPrioritizer creates a prioritizer based on webhook results.
// When tableURL is empty, the prioritizer is disabled and will no-op.
func NewWebhookTaskPrioritizer(tableURL string) *WebhookTaskPrioritizer {
	return NewWebhookTaskPrioritizerWithOptions(tableURL, WebhookTaskPrioritizerOptions{})
}

// NewWebhookTaskPrioritizerWithOptions creates a prioritizer with custom options.
func NewWebhookTaskPrioritizerWithOptions(tableURL string, opts WebhookTaskPrioritizerOptions) *WebhookTaskPrioritizer {
	batchLimit := opts.BatchLimit
	ttl := opts.TTL
	nodeIndex := opts.NodeIndex
	nodeTotal := opts.NodeTotal
	fetcher := opts.Fetcher
	if batchLimit <= 0 {
		batchLimit = 40
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	nodeIndex, nodeTotal = taskagent.NormalizeShardConfig(nodeIndex, nodeTotal)
	return &WebhookTaskPrioritizer{
		tableURL:   strings.TrimSpace(firstNonEmpty(tableURL, taskagent.EnvString(taskagent.EnvWebhookBitableURL, ""))),
		batchLimit: batchLimit,
		ttl:        ttl,
		nodeIndex:  nodeIndex,
		nodeTotal:  nodeTotal,
		fetcher:    fetcher,
	}
}

// ListTaskIDs returns ordered TaskIDs collected from pending/failed webhook rows.
// When limit > 0, it truncates the list to the given size.
func (p *WebhookTaskPrioritizer) ListTaskIDs(ctx context.Context, limit int) ([]int64, error) {
	if p == nil || strings.TrimSpace(p.tableURL) == "" {
		return nil, errors.New("webhook task prioritizer is not configured")
	}
	ordered, err := p.snapshot(ctx)
	if err != nil {
		log.Error().Err(err).Msg("failed to snapshot webhook prioritized task IDs")
		return nil, err
	}
	if len(ordered) == 0 {
		log.Warn().Msg("no webhook tasks available for prioritization")
		return ordered, nil
	}
	if limit > 0 && len(ordered) > limit {
		ordered = ordered[:limit]
	}
	log.Info().Ints64("tasks", ordered).Msg("fetched webhook prioritized task IDs")
	return ordered, err
}

// FetchAvailableTasks prioritizes webhook task IDs before falling back to normal fetch.
// It applies shard filtering when nodeTotal > 1.
func (p *WebhookTaskPrioritizer) FetchAvailableTasks(
	ctx context.Context,
	app string,
	limit int,
) ([]*taskagent.Task, error) {
	if p == nil || p.fetcher == nil {
		return nil, errors.New("webhook task prioritizer fetcher is nil")
	}
	fetcher := p.fetcher
	app = strings.TrimSpace(app)
	if limit <= 0 {
		limit = 1
	}
	nodeIndex, nodeTotal := taskagent.NormalizeShardConfig(p.nodeIndex, p.nodeTotal)
	fetcherShards := false
	if provider, ok := fetcher.(shardConfigProvider); ok {
		idx, total, enabled := provider.ShardConfig()
		if enabled {
			nodeIndex, nodeTotal = taskagent.NormalizeShardConfig(idx, total)
			fetcherShards = true
		}
	}
	expandedLimit := limit
	if nodeTotal > 1 {
		expandedLimit = limit * nodeTotal
		if expandedLimit < limit {
			expandedLimit = limit
		}
	}
	planFetchLimit := expandedLimit
	fallbackLimit := expandedLimit
	if fetcherShards {
		fallbackLimit = limit
	}

	log.Info().
		Str("app", app).
		Int("node_index", nodeIndex).
		Int("node_total", nodeTotal).
		Int("fetch_limit", planFetchLimit).
		Msg("attempting to load webhook prioritized task IDs")
	planIDs, planErr := p.ListTaskIDs(ctx, planFetchLimit)
	if planErr != nil {
		log.Warn().Err(planErr).Msg("failed to load webhook plans; fallback to normal task fetching")
	}
	if len(planIDs) > 0 {
		tasks, err := fetcher.FetchTasksByIDs(ctx, app, planIDs, planFetchLimit)
		if err != nil {
			return nil, err
		}
		filtered := taskagent.FilterTasksByShard(tasks, nodeIndex, nodeTotal, limit)
		if len(filtered) == 0 {
			log.Warn().
				Str("app", app).
				Int("node_index", nodeIndex).
				Int("node_total", nodeTotal).
				Int("plan_ids", len(planIDs)).
				Msg("webhook plan tasks filtered out by shard; fallback to normal task fetching")
		} else {
			log.Info().
				Str("app", app).
				Int("node_index", nodeIndex).
				Int("node_total", nodeTotal).
				Int("plan_ids", len(planIDs)).
				Int("selected", len(filtered)).
				Msg("sharded task manager selected tasks from webhook plan")
			return filtered, nil
		}
	}

	tasks, err := fetcher.FetchAvailableTasks(ctx, app, fallbackLimit)
	if err != nil {
		return nil, err
	}
	if fetcherShards {
		return tasks, nil
	}
	return taskagent.FilterTasksByShard(tasks, nodeIndex, nodeTotal, limit), nil
}

func (p *WebhookTaskPrioritizer) snapshot(ctx context.Context) ([]int64, error) {
	now := time.Now()
	p.mu.Lock()
	if !p.lastFetch.IsZero() && now.Sub(p.lastFetch) < p.ttl && len(p.cacheList) > 0 {
		cachedList := copyTaskIDList(p.cacheList)
		p.mu.Unlock()
		return cachedList, nil
	}
	cachedList := copyTaskIDList(p.cacheList)
	p.mu.Unlock()

	taskIDs, err := listWebhookResultTaskIDs(ctx, p.tableURL, p.batchLimit)
	if err != nil {
		if len(cachedList) > 0 {
			return cachedList, err
		}
		return nil, err
	}
	nextList := make([]int64, 0, len(taskIDs))
	nextSet := make(map[int64]struct{})
	for _, id := range taskIDs {
		if id <= 0 {
			continue
		}
		if _, exists := nextSet[id]; exists {
			continue
		}
		nextSet[id] = struct{}{}
		nextList = append(nextList, id)
	}

	p.mu.Lock()
	p.cacheList = nextList
	p.lastFetch = now
	p.mu.Unlock()

	return copyTaskIDList(nextList), nil
}

func copyTaskIDList(src []int64) []int64 {
	if len(src) == 0 {
		return nil
	}
	out := make([]int64, len(src))
	copy(out, src)
	return out
}

func listWebhookResultTaskIDs(ctx context.Context, tableURL string, batchLimit int) ([]int64, error) {
	url := strings.TrimSpace(firstNonEmpty(tableURL, taskagent.EnvString(taskagent.EnvWebhookBitableURL, "")))
	if url == "" {
		return nil, nil
	}
	store, err := newWebhookResultStore(url)
	if err != nil {
		return nil, err
	}
	if store == nil {
		return nil, nil
	}
	rows, err := store.listCandidates(ctx, listCandidatesOptions{
		BatchLimit: batchLimit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]int64, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.TaskIDs...)
	}
	return out, nil
}
