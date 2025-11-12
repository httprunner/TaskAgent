package feishu

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	envload "github.com/httprunner/TaskAgent/internal"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkauth "github.com/larksuite/oapi-sdk-go/v3/service/auth/v3"
)

func init() {
	_ = envload.Ensure()
}

const (
	defaultBaseURL      = "https://open.feishu.cn"
	defaultHTTPTimeout  = 30 * time.Second
	tokenExpiryFallback = 60 * time.Minute
)

// Client wraps interactions with Feishu APIs required by the batch workflow.
type Client struct {
	appID     string
	appSecret string
	tenantKey string

	baseURL    string
	larkClient *lark.Client
	httpClient *http.Client

	// used for mock test
	doJSONRequestFunc func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error)

	tokenMu       sync.Mutex
	tenantToken   string
	tokenExpireAt time.Time
}

// NewClientFromEnv constructs a Client using environment variables.
//
// Required variables:
//   - FEISHU_APP_ID
//   - FEISHU_APP_SECRET
//
// Optional variables:
//   - FEISHU_TENANT_KEY
//   - FEISHU_BASE_URL (defaults to https://open.feishu.cn)
func NewClientFromEnv() (*Client, error) {
	appID := strings.TrimSpace(os.Getenv("FEISHU_APP_ID"))
	appSecret := strings.TrimSpace(os.Getenv("FEISHU_APP_SECRET"))
	tenantKey := strings.TrimSpace(os.Getenv("FEISHU_TENANT_KEY"))
	baseURL := strings.TrimSpace(os.Getenv("FEISHU_BASE_URL"))

	if appID == "" || appSecret == "" {
		return nil, errors.New("feishu: FEISHU_APP_ID and FEISHU_APP_SECRET must be set in environment")
	}

	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	baseURL = strings.TrimRight(baseURL, "/")

	opts := []lark.ClientOptionFunc{
		lark.WithLogLevel(larkcore.LogLevelError),
	}
	if baseURL != "" && baseURL != lark.FeishuBaseUrl {
		opts = append(opts, lark.WithOpenBaseUrl(baseURL))
	}

	client := lark.NewClient(appID, appSecret, opts...)

	return &Client{
		appID:      appID,
		appSecret:  appSecret,
		tenantKey:  tenantKey,
		baseURL:    baseURL,
		larkClient: client,
		httpClient: &http.Client{Timeout: defaultHTTPTimeout},
	}, nil
}

// getTenantAccessToken retrieves (and caches) a tenant_access_token for direct HTTP calls.
func (c *Client) getTenantAccessToken(ctx context.Context) (string, error) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()

	if c.tenantToken != "" && time.Now().Before(c.tokenExpireAt.Add(-30*time.Second)) {
		return c.tenantToken, nil
	}

	body := larkauth.NewInternalTenantAccessTokenReqBodyBuilder().
		AppId(c.appID).
		AppSecret(c.appSecret).
		Build()

	req := larkauth.NewInternalTenantAccessTokenReqBuilder().
		Body(body).
		Build()

	resp, err := c.larkClient.Auth.V3.TenantAccessToken.Internal(ctx, req)
	if err != nil {
		return "", fmt.Errorf("feishu: request tenant access token failed: %w", err)
	}
	if resp == nil || resp.ApiResp == nil {
		return "", errors.New("feishu: empty response when fetching tenant access token")
	}

	var parsed struct {
		Code              int    `json:"code"`
		Msg               string `json:"msg"`
		TenantAccessToken string `json:"tenant_access_token"`
		Expire            int    `json:"expire"`
	}
	if err := json.Unmarshal(resp.ApiResp.RawBody, &parsed); err != nil {
		return "", fmt.Errorf("feishu: decode tenant access token response: %w", err)
	}
	if parsed.Code != 0 {
		return "", fmt.Errorf("feishu: tenant access token error code=%d msg=%s", parsed.Code, parsed.Msg)
	}
	if parsed.TenantAccessToken == "" {
		return "", errors.New("feishu: tenant access token missing in response")
	}

	ttl := time.Duration(parsed.Expire) * time.Second
	if ttl <= 0 {
		ttl = tokenExpiryFallback
	}

	c.tenantToken = parsed.TenantAccessToken
	c.tokenExpireAt = time.Now().Add(ttl)

	return c.tenantToken, nil
}

func (c *Client) apiBase() string {
	if c.baseURL != "" {
		return c.baseURL
	}
	return defaultBaseURL
}

func (c *Client) doJSONRequest(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
	if c.doJSONRequestFunc != nil {
		return c.doJSONRequestFunc(ctx, method, path, payload)
	}
	return c.doJSONRequestInternal(ctx, method, path, payload)
}

func (c *Client) doJSONRequestInternal(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
	token, err := c.getTenantAccessToken(ctx)
	if err != nil {
		return nil, nil, err
	}

	var req *http.Request
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, nil, fmt.Errorf("feishu: marshal request payload: %w", err)
		}
		req, err = http.NewRequestWithContext(ctx, method, c.apiBase()+path, bytes.NewReader(raw))
		if err != nil {
			return nil, nil, fmt.Errorf("feishu: build request: %w", err)
		}
	} else {
		req, err = http.NewRequestWithContext(ctx, method, c.apiBase()+path, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("feishu: build request: %w", err)
		}
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("feishu: execute request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp, nil, fmt.Errorf("feishu: read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return resp, rawBody, fmt.Errorf("feishu: http %d response: %s", resp.StatusCode, strings.TrimSpace(string(rawBody)))
	}

	return resp, rawBody, nil
}
