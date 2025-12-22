package webhook

import (
	"context"
	"strings"
	"sync"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
)

// WebhookTaskPrioritizer caches TaskIDs from pending/failed webhook rows.
type WebhookTaskPrioritizer struct {
	tableURL   string
	batchLimit int
	ttl        time.Duration

	mu        sync.Mutex
	lastFetch time.Time
	cacheList []int64
}

// NewWebhookTaskPrioritizer creates a prioritizer based on webhook results.
// When tableURL is empty, the prioritizer is disabled and will no-op.
func NewWebhookTaskPrioritizer(tableURL string, batchLimit int, ttl time.Duration) *WebhookTaskPrioritizer {
	if batchLimit <= 0 {
		batchLimit = 40
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &WebhookTaskPrioritizer{
		tableURL:   strings.TrimSpace(firstNonEmpty(tableURL, taskagent.EnvString(taskagent.EnvWebhookBitableURL, ""))),
		batchLimit: batchLimit,
		ttl:        ttl,
	}
}

// ListTaskIDs returns ordered TaskIDs collected from pending/failed webhook rows.
// When limit > 0, it truncates the list to the given size.
func (p *WebhookTaskPrioritizer) ListTaskIDs(ctx context.Context, limit int) ([]int64, error) {
	if p == nil || strings.TrimSpace(p.tableURL) == "" {
		return nil, nil
	}
	ordered, err := p.snapshot(ctx)
	if len(ordered) == 0 {
		return ordered, err
	}
	if limit > 0 && len(ordered) > limit {
		ordered = ordered[:limit]
	}
	return ordered, err
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
	rows, err := store.listCandidates(ctx, batchLimit)
	if err != nil {
		return nil, err
	}
	out := make([]int64, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.TaskIDs...)
	}
	return out, nil
}
