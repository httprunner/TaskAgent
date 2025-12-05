package pool

import (
	"bytes"
	"context"
	"encoding/json"
	stdErrors "errors"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/httprunner/TaskAgent/pkg/feishu"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

const (
	defaultCookiePlatform            = "快手"
	defaultCookieCacheTTL            = 5 * time.Minute
	defaultKuaishouHomepageURL       = "https://www.kuaishou.com/?isHome=1"
	defaultKuaishouHomepageUserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
	visionProfileResultNeedLogin     = 2
)

var (
	errCookieInvalid               = stdErrors.New("cookie invalid")
	errCookieValidationUnavailable = stdErrors.New("cookie validation unavailable")
	errApolloStateNotFound         = stdErrors.New("kuaishou homepage: apollo state not found")
	errVisionProfileMissing        = stdErrors.New("kuaishou homepage: vision profile missing")

	cookieValidationOnce sync.Once
	cookieValidationFlag bool
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
	ksValidator *kuaishouCookieValidator

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
	if ctx == nil {
		ctx = context.Background()
	}
	tried := make(map[string]struct{})
	for {
		rec, err := p.nextRecord(ctx)
		if err != nil || rec == nil {
			return rec, err
		}
		if err := p.validate(ctx, rec.Value); err != nil {
			if stdErrors.Is(err, errCookieValidationUnavailable) {
				log.Warn().Err(err).
					Str("record_id", rec.RecordID).
					Msg("cookie provider: validation unavailable; skipping cookie")
				tried[rec.RecordID] = struct{}{}
				if len(tried) >= p.cacheSize() {
					return nil, err
				}
				continue
			}
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

func (p *cookieProvider) defaultValidate(ctx context.Context, value string) error {
	if p == nil {
		return nil
	}
	if !isCookieValidationEnabled() {
		return nil
	}
	if !isKuaishouPlatform(p.platform) {
		return nil
	}
	validator := p.getKuaishouValidator()
	if validator == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return validator.Validate(ctx, value)
}

func (p *cookieProvider) defaultMarkInvalid(ctx context.Context, recordID string) error {
	if p == nil {
		return errors.New("cookie provider: nil instance")
	}
	if p.client == nil {
		return errors.New("cookie provider: client is nil")
	}
	if strings.TrimSpace(p.tableURL) == "" {
		return errors.New("cookie provider: table url is empty")
	}
	return p.client.UpdateCookieStatus(ctx, p.tableURL, recordID, feishu.CookieStatusInvalid, nil)
}

func (p *cookieProvider) cacheSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.cache)
}

func (p *cookieProvider) getKuaishouValidator() *kuaishouCookieValidator {
	if p == nil {
		return nil
	}
	if p.ksValidator == nil {
		p.ksValidator = newKuaishouCookieValidator(nil)
	}
	return p.ksValidator
}

func isKuaishouPlatform(name string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(name))
	if trimmed == "" {
		return false
	}
	return strings.Contains(trimmed, "kuaishou") || strings.Contains(trimmed, "快手")
}

type kuaishouCookieValidator struct {
	client      *http.Client
	homepageURL string
	userAgent   string
}

func newKuaishouCookieValidator(client *http.Client) *kuaishouCookieValidator {
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	return &kuaishouCookieValidator{
		client:      client,
		homepageURL: defaultKuaishouHomepageURL,
		userAgent:   defaultKuaishouHomepageUserAgent,
	}
}

func (v *kuaishouCookieValidator) Validate(ctx context.Context, rawCookie string) error {
	if v == nil {
		return nil
	}
	trimmed := strings.TrimSpace(rawCookie)
	if trimmed == "" {
		return errCookieInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return v.validateViaHomepage(ctx, trimmed)
}

func (v *kuaishouCookieValidator) validateViaHomepage(ctx context.Context, cookie string) error {
	if strings.TrimSpace(v.homepageURL) == "" {
		return errCookieValidationUnavailable
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.homepageURL, nil)
	if err != nil {
		return errCookieValidationUnavailable
	}
	req.Header.Set("Cookie", cookie)
	if ua := strings.TrimSpace(v.userAgent); ua != "" {
		req.Header.Set("User-Agent", ua)
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return errCookieValidationUnavailable
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return errCookieInvalid
	}
	if resp.StatusCode >= http.StatusInternalServerError {
		return errCookieValidationUnavailable
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 5<<20))
	if err != nil {
		return errCookieValidationUnavailable
	}
	stateJSON, err := extractApolloStateJSON(body)
	if err != nil {
		if stdErrors.Is(err, errApolloStateNotFound) {
			return errCookieInvalid
		}
		return errCookieValidationUnavailable
	}
	profile, err := parseVisionProfileFromApolloState(stateJSON)
	if err != nil {
		if stdErrors.Is(err, errVisionProfileMissing) {
			return errCookieInvalid
		}
		return errCookieValidationUnavailable
	}
	if profile.profileUserID() != "" {
		return nil
	}
	if profile.requiresLogin() {
		return errCookieInvalid
	}
	if profile.missingUserProfile() {
		return errCookieInvalid
	}
	return errCookieValidationUnavailable
}

func extractApolloStateJSON(body []byte) ([]byte, error) {
	const marker = "window.__APOLLO_STATE__="
	idx := bytes.Index(body, []byte(marker))
	if idx == -1 {
		return nil, errApolloStateNotFound
	}
	segment := body[idx+len(marker):]
	start := bytes.IndexByte(segment, '{')
	if start == -1 {
		return nil, errCookieValidationUnavailable
	}
	data := segment[start:]
	depth := 0
	inString := false
	escaped := false
	for i := 0; i < len(data); i++ {
		b := data[i]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			switch b {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}
			continue
		}
		switch b {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return bytes.TrimSpace(data[:i+1]), nil
			}
		}
	}
	return nil, errCookieValidationUnavailable
}

func parseVisionProfileFromApolloState(payload []byte) (*visionProfileState, error) {
	var state struct {
		DefaultClient map[string]json.RawMessage `json:"defaultClient"`
	}
	if err := json.Unmarshal(payload, &state); err != nil {
		return nil, err
	}
	if len(state.DefaultClient) == 0 {
		return nil, errVisionProfileMissing
	}
	raw, ok := state.DefaultClient["$ROOT_QUERY.visionProfile({})"]
	if !ok || len(bytes.TrimSpace(raw)) == 0 {
		return nil, errVisionProfileMissing
	}
	var profile visionProfileState
	if err := json.Unmarshal(raw, &profile); err != nil {
		return nil, err
	}
	return &profile, nil
}

type visionProfileState struct {
	Result      int `json:"result"`
	UserProfile *struct {
		Profile *struct {
			UserID string `json:"user_id"`
		} `json:"profile"`
	} `json:"userProfile"`
}

func (s visionProfileState) profileUserID() string {
	if s.UserProfile == nil || s.UserProfile.Profile == nil {
		return ""
	}
	return strings.TrimSpace(s.UserProfile.Profile.UserID)
}

func (s visionProfileState) requiresLogin() bool {
	return s.Result == visionProfileResultNeedLogin
}

func (s visionProfileState) missingUserProfile() bool {
	if s.UserProfile == nil || s.UserProfile.Profile == nil {
		return true
	}
	return strings.TrimSpace(s.UserProfile.Profile.UserID) == ""
}

func isCookieValidationEnabled() bool {
	cookieValidationOnce.Do(func() {
		val := strings.TrimSpace(os.Getenv("ENABLE_COOKIE_VALIDATION"))
		switch strings.ToLower(val) {
		case "1", "true", "yes", "on":
			cookieValidationFlag = true
		default:
			cookieValidationFlag = false
		}
	})
	return cookieValidationFlag
}
