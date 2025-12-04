package pool

import (
	"context"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/httprunner/TaskAgent/pkg/feishu"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

const (
	defaultCookiePlatform = "快手"
	defaultCookieCacheTTL = 5 * time.Minute
)

// CookieRecord captures a cookie entry fetched from the Feishu table.
type CookieRecord struct {
	RecordID string
	Value    string
}

// CookieProvider exposes rotation-aware cookie selection.
type CookieProvider interface {
	PickCookie(ctx context.Context) (*CookieRecord, error)
}

type cookieProvider struct {
	client      *feishu.Client
	tableURL    string
	platform    string
	cacheTTL    time.Duration
	rand        *rand.Rand
	fetchFn     func(ctx context.Context, client *feishu.Client, tableURL, platform string) ([]feishu.CookieRow, error)
	validate    func(ctx context.Context, value string) error
	markInvalid func(ctx context.Context, recordID string) error

	mu         sync.Mutex
	cache      []CookieRecord
	next       int
	lastReload time.Time
}

// NewCookieProvider builds a rotation-aware cookie provider backed by the Feishu cookies table.
func NewCookieProvider(client *feishu.Client, tableURL, platform string, ttl time.Duration) (CookieProvider, error) {
	if client == nil {
		return nil, errors.New("cookie provider: client is nil")
	}
	trimmed := strings.TrimSpace(tableURL)
	if trimmed == "" {
		return nil, errors.New("cookie provider: table url is empty")
	}
	if strings.TrimSpace(platform) == "" {
		platform = defaultCookiePlatform
	}
	if ttl <= 0 {
		ttl = defaultCookieCacheTTL
	}
	p := &cookieProvider{
		client:   client,
		tableURL: trimmed,
		platform: platform,
		cacheTTL: ttl,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	p.fetchFn = p.defaultFetch
	p.validate = p.defaultValidate
	p.markInvalid = p.defaultMarkInvalid
	return p, nil
}

func (p *cookieProvider) PickCookie(ctx context.Context) (*CookieRecord, error) {
	if p == nil {
		return nil, nil
	}
	for {
		rec, err := p.nextRecord(ctx)
		if err != nil || rec == nil {
			return rec, err
		}
		if err := p.validate(ctx, rec.Value); err != nil {
			log.Warn().Err(err).
				Str("record_id", rec.RecordID).
				Msg("cookie provider: validation failed; marking invalid")
			if err := p.markInvalid(ctx, rec.RecordID); err != nil {
				log.Warn().Err(err).
					Str("record_id", rec.RecordID).
					Msg("cookie provider: failed to mark cookie invalid")
			}
			p.removeCachedRecord(rec.RecordID)
			continue
		}
		return rec, nil
	}
}

func (p *cookieProvider) nextRecord(ctx context.Context) (*CookieRecord, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.cache) == 0 || time.Since(p.lastReload) >= p.cacheTTL {
		if err := p.refreshCacheLocked(ctx); err != nil {
			return nil, err
		}
	}
	if len(p.cache) == 0 {
		return nil, nil
	}
	rec := p.cache[p.next]
	p.next = (p.next + 1) % len(p.cache)
	copy := rec
	return &copy, nil
}

func (p *cookieProvider) refreshCacheLocked(ctx context.Context) error {
	if p.fetchFn == nil {
		return errors.New("cookie provider: fetch function is nil")
	}
	rows, err := p.fetchFn(ctx, p.client, p.tableURL, p.platform)
	if err != nil {
		return err
	}
	cache := make([]CookieRecord, 0, len(rows))
	for _, row := range rows {
		value := strings.TrimSpace(row.Cookies)
		if value == "" {
			continue
		}
		cache = append(cache, CookieRecord{RecordID: row.RecordID, Value: value})
	}
	if len(cache) > 1 && p.rand != nil {
		p.rand.Shuffle(len(cache), func(i, j int) {
			cache[i], cache[j] = cache[j], cache[i]
		})
	}
	p.cache = cache
	p.next = 0
	p.lastReload = time.Now()
	return nil
}

func (p *cookieProvider) removeCachedRecord(recordID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	idx := -1
	for i, rec := range p.cache {
		if rec.RecordID == recordID {
			idx = i
			break
		}
	}
	if idx == -1 {
		return
	}
	p.cache = append(p.cache[:idx], p.cache[idx+1:]...)
	if len(p.cache) == 0 {
		p.next = 0
		return
	}
	if p.next >= len(p.cache) {
		p.next = 0
	}
}

func (p *cookieProvider) defaultFetch(ctx context.Context, client *feishu.Client, tableURL, platform string) ([]feishu.CookieRow, error) {
	return client.FetchCookieRows(ctx, tableURL, nil, platform)
}

func (p *cookieProvider) defaultValidate(context.Context, string) error {
	// TODO: integrate downstream validation API.
	return nil
}

func (p *cookieProvider) defaultMarkInvalid(context.Context, string) error {
	// TODO: update cookie status to invalid via Feishu when validation fails.
	return nil
}
