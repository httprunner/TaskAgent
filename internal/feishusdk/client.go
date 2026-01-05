package feishusdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/httprunner/TaskAgent/internal/env"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkauth "github.com/larksuite/oapi-sdk-go/v3/service/auth/v3"
	larkbitable "github.com/larksuite/oapi-sdk-go/v3/service/bitable/v1"
	larkwiki "github.com/larksuite/oapi-sdk-go/v3/service/wiki/v2"
	"golang.org/x/sync/singleflight"
)

const (
	defaultBaseURL      = "https://open.feishu.cn"
	defaultHTTPTimeout  = 60 * time.Second
	tokenExpiryFallback = 60 * time.Minute

	defaultTransport = "sdk"
)

type bitableAppTableRecordAPI interface {
	Search(ctx context.Context, appToken, tableID string, pageSize int, pageToken string, body *larkbitable.SearchAppTableRecordReqBody, options ...larkcore.RequestOptionFunc) (*larkbitable.SearchAppTableRecordResp, error)
	Create(ctx context.Context, appToken, tableID string, record *larkbitable.AppTableRecord, options ...larkcore.RequestOptionFunc) (*larkbitable.CreateAppTableRecordResp, error)
	Update(ctx context.Context, appToken, tableID, recordID string, record *larkbitable.AppTableRecord, options ...larkcore.RequestOptionFunc) (*larkbitable.UpdateAppTableRecordResp, error)
	BatchCreate(ctx context.Context, appToken, tableID string, body *larkbitable.BatchCreateAppTableRecordReqBody, options ...larkcore.RequestOptionFunc) (*larkbitable.BatchCreateAppTableRecordResp, error)
	BatchUpdate(ctx context.Context, appToken, tableID string, body *larkbitable.BatchUpdateAppTableRecordReqBody, options ...larkcore.RequestOptionFunc) (*larkbitable.BatchUpdateAppTableRecordResp, error)
	BatchGet(ctx context.Context, appToken, tableID string, body *larkbitable.BatchGetAppTableRecordReqBody, options ...larkcore.RequestOptionFunc) (*larkbitable.BatchGetAppTableRecordResp, error)
}

type larkAppTableRecordService interface {
	Search(ctx context.Context, req *larkbitable.SearchAppTableRecordReq, options ...larkcore.RequestOptionFunc) (*larkbitable.SearchAppTableRecordResp, error)
	Create(ctx context.Context, req *larkbitable.CreateAppTableRecordReq, options ...larkcore.RequestOptionFunc) (*larkbitable.CreateAppTableRecordResp, error)
	Update(ctx context.Context, req *larkbitable.UpdateAppTableRecordReq, options ...larkcore.RequestOptionFunc) (*larkbitable.UpdateAppTableRecordResp, error)
	BatchCreate(ctx context.Context, req *larkbitable.BatchCreateAppTableRecordReq, options ...larkcore.RequestOptionFunc) (*larkbitable.BatchCreateAppTableRecordResp, error)
	BatchUpdate(ctx context.Context, req *larkbitable.BatchUpdateAppTableRecordReq, options ...larkcore.RequestOptionFunc) (*larkbitable.BatchUpdateAppTableRecordResp, error)
	BatchGet(ctx context.Context, req *larkbitable.BatchGetAppTableRecordReq, options ...larkcore.RequestOptionFunc) (*larkbitable.BatchGetAppTableRecordResp, error)
}

type sdkBitableAppTableRecordAPI struct {
	svc larkAppTableRecordService
}

type wikiSpaceAPI interface {
	GetNode(ctx context.Context, token string, options ...larkcore.RequestOptionFunc) (*larkwiki.GetNodeSpaceResp, error)
}

type larkWikiSpaceService interface {
	GetNode(ctx context.Context, req *larkwiki.GetNodeSpaceReq, options ...larkcore.RequestOptionFunc) (*larkwiki.GetNodeSpaceResp, error)
}

type sdkWikiSpaceAPI struct {
	svc larkWikiSpaceService
}

func (w sdkWikiSpaceAPI) GetNode(ctx context.Context, token string, options ...larkcore.RequestOptionFunc) (*larkwiki.GetNodeSpaceResp, error) {
	req := larkwiki.NewGetNodeSpaceReqBuilder().
		Token(token).
		Build()
	return w.svc.GetNode(ctx, req, options...)
}

func (a sdkBitableAppTableRecordAPI) Search(ctx context.Context, appToken, tableID string, pageSize int, pageToken string, body *larkbitable.SearchAppTableRecordReqBody, options ...larkcore.RequestOptionFunc) (*larkbitable.SearchAppTableRecordResp, error) {
	builder := larkbitable.NewSearchAppTableRecordReqBuilder().
		AppToken(appToken).
		TableId(tableID).
		PageSize(pageSize)
	if strings.TrimSpace(pageToken) != "" {
		builder.PageToken(strings.TrimSpace(pageToken))
	}
	if body != nil {
		builder.Body(body)
	}
	return a.svc.Search(ctx, builder.Build(), options...)
}

func (a sdkBitableAppTableRecordAPI) Create(ctx context.Context, appToken, tableID string, record *larkbitable.AppTableRecord, options ...larkcore.RequestOptionFunc) (*larkbitable.CreateAppTableRecordResp, error) {
	req := larkbitable.NewCreateAppTableRecordReqBuilder().
		AppToken(appToken).
		TableId(tableID).
		AppTableRecord(record).
		Build()
	return a.svc.Create(ctx, req, options...)
}

func (a sdkBitableAppTableRecordAPI) Update(ctx context.Context, appToken, tableID, recordID string, record *larkbitable.AppTableRecord, options ...larkcore.RequestOptionFunc) (*larkbitable.UpdateAppTableRecordResp, error) {
	req := larkbitable.NewUpdateAppTableRecordReqBuilder().
		AppToken(appToken).
		TableId(tableID).
		RecordId(recordID).
		AppTableRecord(record).
		Build()
	return a.svc.Update(ctx, req, options...)
}

func (a sdkBitableAppTableRecordAPI) BatchCreate(ctx context.Context, appToken, tableID string, body *larkbitable.BatchCreateAppTableRecordReqBody, options ...larkcore.RequestOptionFunc) (*larkbitable.BatchCreateAppTableRecordResp, error) {
	req := larkbitable.NewBatchCreateAppTableRecordReqBuilder().
		AppToken(appToken).
		TableId(tableID).
		Body(body).
		Build()
	return a.svc.BatchCreate(ctx, req, options...)
}

func (a sdkBitableAppTableRecordAPI) BatchUpdate(ctx context.Context, appToken, tableID string, body *larkbitable.BatchUpdateAppTableRecordReqBody, options ...larkcore.RequestOptionFunc) (*larkbitable.BatchUpdateAppTableRecordResp, error) {
	req := larkbitable.NewBatchUpdateAppTableRecordReqBuilder().
		AppToken(appToken).
		TableId(tableID).
		Body(body).
		Build()
	return a.svc.BatchUpdate(ctx, req, options...)
}

func (a sdkBitableAppTableRecordAPI) BatchGet(ctx context.Context, appToken, tableID string, body *larkbitable.BatchGetAppTableRecordReqBody, options ...larkcore.RequestOptionFunc) (*larkbitable.BatchGetAppTableRecordResp, error) {
	req := larkbitable.NewBatchGetAppTableRecordReqBuilder().
		AppToken(appToken).
		TableId(tableID).
		Body(body).
		Build()
	return a.svc.BatchGet(ctx, req, options...)
}

// Client wraps interactions with Feishu APIs required by the batch workflow.
type Client struct {
	appID     string
	appSecret string
	tenantKey string

	baseURL    string
	larkClient *lark.Client
	httpClient *http.Client

	transport string

	coreConfigOnce sync.Once
	coreConfig     *larkcore.Config
	coreConfigErr  error

	bitableAPI bitableAppTableRecordAPI
	wikiAPI    wikiSpaceAPI

	// used for mock test
	doJSONRequestFunc func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error)
	doSDKRequestFunc  func(ctx context.Context, req *larkcore.ApiReq, config *larkcore.Config, options ...larkcore.RequestOptionFunc) (*larkcore.ApiResp, error)

	tokenMu       sync.Mutex
	tenantToken   string
	tokenExpireAt time.Time

	appTokenMu    sync.RWMutex
	appTokenCache map[string]string
	appTokenGroup singleflight.Group
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
//   - FEISHU_TRANSPORT (sdk/http, defaults to sdk)
func NewClientFromEnv() (*Client, error) {
	appID := env.String("FEISHU_APP_ID", "")
	appSecret := env.String("FEISHU_APP_SECRET", "")
	tenantKey := env.String("FEISHU_TENANT_KEY", "")
	baseURL := env.String("FEISHU_BASE_URL", "")
	transport := env.String("FEISHU_TRANSPORT", "")

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

	normalize := func(raw, fallback string) string {
		mode := strings.ToLower(strings.TrimSpace(raw))
		if mode == "" {
			mode = fallback
		}
		switch mode {
		case "sdk", "http":
			return mode
		default:
			return fallback
		}
	}
	transportMode := normalize(transport, defaultTransport)

	return &Client{
		appID:      appID,
		appSecret:  appSecret,
		tenantKey:  tenantKey,
		baseURL:    baseURL,
		larkClient: client,
		httpClient: &http.Client{Timeout: defaultHTTPTimeout},
		transport:  transportMode,
		bitableAPI: sdkBitableAppTableRecordAPI{
			svc: client.Bitable.V1.AppTableRecord,
		},
		wikiAPI: sdkWikiSpaceAPI{
			svc: client.Wiki.V2.Space,
		},
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

func (c *Client) useHTTP() bool {
	if c == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(c.transport), "http")
}

func (c *Client) bitableAppTableRecord() bitableAppTableRecordAPI {
	if c == nil {
		return nil
	}
	if c.bitableAPI != nil {
		return c.bitableAPI
	}
	if c.larkClient == nil {
		return nil
	}
	return sdkBitableAppTableRecordAPI{svc: c.larkClient.Bitable.V1.AppTableRecord}
}

func (c *Client) wikiSpace() wikiSpaceAPI {
	if c == nil {
		return nil
	}
	if c.wikiAPI != nil {
		return c.wikiAPI
	}
	if c.larkClient == nil {
		return nil
	}
	return sdkWikiSpaceAPI{svc: c.larkClient.Wiki.V2.Space}
}

func (c *Client) tenantRequestOptions(token string) []larkcore.RequestOptionFunc {
	opts := []larkcore.RequestOptionFunc{larkcore.WithTenantAccessToken(token)}
	if strings.TrimSpace(c.tenantKey) != "" {
		opts = append(opts, larkcore.WithTenantKey(strings.TrimSpace(c.tenantKey)))
	}
	return opts
}

func (c *Client) wikiSDK(ctx context.Context) (wikiSpaceAPI, []larkcore.RequestOptionFunc, error) {
	if c == nil {
		return nil, nil, errors.New("feishu: client is nil")
	}
	api := c.wikiSpace()
	if api == nil {
		return nil, nil, errors.New("feishu: wiki sdk client is nil")
	}
	token, err := c.getTenantAccessToken(ctx)
	if err != nil {
		return nil, nil, err
	}
	return api, c.tenantRequestOptions(token), nil
}

func (c *Client) apiBase() string {
	if c.baseURL != "" {
		return c.baseURL
	}
	return defaultBaseURL
}

func (c *Client) sdkCoreConfig() (*larkcore.Config, error) {
	if c == nil {
		return nil, errors.New("feishu: client is nil")
	}
	c.coreConfigOnce.Do(func() {
		if strings.TrimSpace(c.appID) == "" || strings.TrimSpace(c.appSecret) == "" {
			c.coreConfigErr = errors.New("feishu: app credentials are missing")
			return
		}
		cfg := &larkcore.Config{
			BaseUrl:    c.apiBase(),
			AppId:      c.appID,
			AppSecret:  c.appSecret,
			ReqTimeout: defaultHTTPTimeout,
			LogLevel:   larkcore.LogLevelError,
			AppType:    larkcore.AppTypeSelfBuilt,
			HttpClient: c.httpClient,
		}
		larkcore.NewLogger(cfg)
		larkcore.NewHttpClient(cfg)
		larkcore.NewSerialization(cfg)
		c.coreConfig = cfg
	})
	if c.coreConfigErr != nil {
		return nil, c.coreConfigErr
	}
	if c.coreConfig == nil {
		return nil, errors.New("feishu: sdk core config is nil")
	}
	return c.coreConfig, nil
}

func (c *Client) doSDKOpenAPIRequest(ctx context.Context, req *larkcore.ApiReq, options ...larkcore.RequestOptionFunc) (*larkcore.ApiResp, error) {
	if c == nil {
		return nil, errors.New("feishu: client is nil")
	}
	if req == nil {
		return nil, errors.New("feishu: openapi request is nil")
	}
	cfg, err := c.sdkCoreConfig()
	if err != nil {
		return nil, err
	}
	if c.doSDKRequestFunc != nil {
		return c.doSDKRequestFunc(ctx, req, cfg, options...)
	}
	return larkcore.Request(ctx, req, cfg, options...)
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
