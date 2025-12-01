package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// HTTPClient 轻量级飞书API客户端
type HTTPClient struct {
	baseURL    string
	httpClient *http.Client
	creds      struct {
		appID     string
		appSecret string
	}
	tokenCache struct {
		token  string
		expiry time.Time
	}
}

// NewHTTPClient 创建客户端
func NewHTTPClient() (*HTTPClient, error) {
	c := &HTTPClient{
		baseURL:    strings.TrimSuffix(os.Getenv("FEISHU_BASE_URL"), "/"),
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}

	c.creds.appID = strings.TrimSpace(os.Getenv("FEISHU_APP_ID"))
	c.creds.appSecret = strings.TrimSpace(os.Getenv("FEISHU_APP_SECRET"))

	if c.baseURL == "" {
		c.baseURL = "https://open.feishu.cn"
	}
	if c.creds.appID == "" || c.creds.appSecret == "" {
		return nil, errors.New("FEISHU_APP_ID and FEISHU_APP_SECRET must be set")
	}
	return c, nil
}

// getTenantAccessToken 获取访问令牌
func (c *HTTPClient) getTenantAccessToken(ctx context.Context) (string, error) {
	if c.tokenCache.token != "" && time.Now().Before(c.tokenCache.expiry.Add(-30*time.Second)) {
		return c.tokenCache.token, nil
	}

	reqBody, _ := json.Marshal(map[string]string{
		"app_id":     c.creds.appID,
		"app_secret": c.creds.appSecret,
	})

	req, _ := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/open-apis/auth/v3/tenant_access_token/internal", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("auth failed: %d", resp.StatusCode)
	}

	var authResp struct {
		Code              int    `json:"code"`
		TenantAccessToken string `json:"tenant_access_token"`
		Expire            int    `json:"expire"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&authResp); err != nil {
		return "", err
	}
	if authResp.Code != 0 || authResp.TenantAccessToken == "" {
		return "", fmt.Errorf("auth error: %d", authResp.Code)
	}

	c.tokenCache.token = authResp.TenantAccessToken
	c.tokenCache.expiry = time.Now().Add(time.Duration(authResp.Expire) * time.Second)
	return c.tokenCache.token, nil
}

// parseBitableURL 解析Bitable链接
func parseBitableURL(rawURL string) (appToken, tableID, wikiToken string, err error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", "", err
	}

	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) < 2 {
		return "", "", "", errors.New("invalid URL format")
	}

	switch parts[0] {
	case "wiki":
		wikiToken = parts[1]
	case "base":
		appToken = parts[1]
	default:
		return "", "", "", errors.New("unsupported URL format")
	}

	tableID = u.Query().Get("table")
	if tableID == "" {
		return "", "", "", errors.New("missing table_id")
	}

	return appToken, tableID, wikiToken, nil
}

// getWikiNode 获取wiki对应的bitable token
func (c *HTTPClient) getWikiNode(ctx context.Context, wikiToken string) (string, error) {
	token, err := c.getTenantAccessToken(ctx)
	if err != nil {
		return "", err
	}

	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s/open-apis/wiki/v2/spaces/get_node?token=%s", c.baseURL, url.QueryEscape(wikiToken)), nil)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("wiki request failed: %d", resp.StatusCode)
	}

	var wikiResp struct {
		Code int `json:"code"`
		Data struct {
			Node struct {
				ObjToken string `json:"obj_token"`
				ObjType  string `json:"obj_type"`
			} `json:"node"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&wikiResp); err != nil {
		return "", err
	}
	if wikiResp.Code != 0 || wikiResp.Data.Node.ObjToken == "" || wikiResp.Data.Node.ObjType != "bitable" {
		return "", fmt.Errorf("invalid wiki response: %d", wikiResp.Code)
	}

	return wikiResp.Data.Node.ObjToken, nil
}

// CreateTaskRecord 创建任务记录
func (c *HTTPClient) CreateTaskRecord(ctx context.Context, tableURL string, record TaskRecordInput) (string, error) {
	appToken, tableID, wikiToken, err := parseBitableURL(tableURL)
	if err != nil {
		return "", fmt.Errorf("parse URL: %w", err)
	}
	if appToken == "" && wikiToken != "" {
		appToken, err = c.getWikiNode(ctx, wikiToken)
		if err != nil {
			return "", fmt.Errorf("wiki to app token: %w", err)
		}
	}

	token, err := c.getTenantAccessToken(ctx)
	if err != nil {
		return "", err
	}

	fields := map[string]interface{}{
		"App":    record.App,
		"Scene":  record.Scene,
		"Params": record.Params,
		"Status": record.Status,
	}
	// 使用结构体初始化避免map操作
	if record.UserID != "" {
		fields["UserID"] = record.UserID
	}
	if record.UserName != "" {
		fields["UserName"] = record.UserName
	}
	if record.Extra != "" {
		fields["Extra"] = record.Extra
	}
	if record.DeviceSerial != "" {
		fields["DeviceSerial"] = record.DeviceSerial
	}

	body, _ := json.Marshal(map[string]interface{}{"fields": fields})
	req, _ := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("%s/open-apis/bitable/v1/apps/%s/tables/%s/records", c.baseURL, appToken, tableID),
		bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("create failed: %d", resp.StatusCode)
	}

	var createResp struct {
		Code int `json:"code"`
		Data struct {
			Record struct {
				RecordID string `json:"record_id"`
			} `json:"record"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&createResp); err != nil {
		return "", err
	}
	if createResp.Code != 0 || createResp.Data.Record.RecordID == "" {
		return "", fmt.Errorf("create error: %d", createResp.Code)
	}

	return createResp.Data.Record.RecordID, nil
}

// TaskRecordInput 简化的任务记录结构
type TaskRecordInput struct {
	App, Scene, Params      string
	Status, Webhook         string
	UserID, UserName, Extra string
	DeviceSerial            string
	ItemID                  string
}

func main() {
	aID := flag.String("aid", "", "aid value")
	eID := flag.String("eid", "", "eid value")
	flag.Parse()

	tableURL := os.Getenv("TASK_BITABLE_URL")
	// 参数验证
	if tableURL == "" || *aID == "" || *eID == "" {
		log.Fatal("missing required parameters: -table, -aid, -eid")
	}

	client, err := NewHTTPClient()
	if err != nil {
		log.Fatal(err)
	}

	itemID := strings.TrimSpace(*eID)
	params, _ := json.Marshal(map[string]string{
		"type": "auto_additional_crawl",
		"aid":  strings.TrimSpace(*aID),
		"eid":  itemID,
	})

	record := TaskRecordInput{
		App:     "com.smile.gifmaker",
		Scene:   "视频录屏采集",
		ItemID:  itemID,
		Status:  "pending",
		Webhook: "pending",
		Params:  string(params),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id, err := client.CreateTaskRecord(ctx, tableURL, record)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Created task record %s\n", id)
}
