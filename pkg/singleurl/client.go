package singleurl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type crawlerTaskClient interface {
	CreateTask(ctx context.Context, url string, cookies []string, meta map[string]string) (string, error)
	GetTask(ctx context.Context, taskID string) (*crawlerTaskStatus, error)
	SendTaskSummary(ctx context.Context, payload TaskSummaryPayload) error
}

type crawlerTaskStatus struct {
	TaskID    string
	AID       string
	ItemID    string
	Status    string
	VID       string
	Error     string
	CreatedAt any
	UpdatedAt any
}

// TaskSummaryCombination captures a unique bid/account pair for the finished task payload.
type TaskSummaryCombination struct {
	Bid       string `json:"bid"`
	AccountID string `json:"account_id"`
}

// TaskSummaryPayload mirrors the downloader service finish_message schema.
type TaskSummaryPayload struct {
	Status             string                   `json:"status"`
	Total              int                      `json:"total"`
	Done               int                      `json:"done"`
	UniqueCombinations []TaskSummaryCombination `json:"unique_combinations"`
	UniqueCount        int                      `json:"unique_count"`
	TaskName           string                   `json:"task_name"`
	Email              string                   `json:"email"`
}

var errCrawlerTaskNotFound = errors.New("crawler task not found")

type restCrawlerTaskClient struct {
	baseURL    string
	httpClient *http.Client
}

func newRESTCrawlerTaskClient(baseURL string, httpClient *http.Client) (crawlerTaskClient, error) {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return nil, errors.New("crawler base url is empty")
	}
	baseURL = strings.TrimSuffix(baseURL, "/")
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 60 * time.Second}
	}
	return &restCrawlerTaskClient{baseURL: baseURL, httpClient: httpClient}, nil
}

func (c *restCrawlerTaskClient) CreateTask(ctx context.Context, url string, cookies []string, meta map[string]string) (string, error) {
	logEvent := log.Debug().Str("url", url).Int("cookies", len(cookies))
	cleanMeta := make(map[string]string, len(meta))
	for k, v := range meta {
		trimmedKey := strings.TrimSpace(k)
		trimmedVal := strings.TrimSpace(v)
		if trimmedKey == "" || trimmedVal == "" {
			continue
		}
		cleanMeta[trimmedKey] = trimmedVal
		logEvent = logEvent.Str(trimmedKey, trimmedVal)
	}
	logEvent.Msg("creating crawler task")

	type requestExtra struct {
		Cookies []string `json:"cookies,omitempty"`
	}
	type requestBody struct {
		Platform string        `json:"platform"`
		Bid      string        `json:"bid"`
		UID      string        `json:"uid"`
		URL      string        `json:"url"`
		CDNURL   string        `json:"cdn_url,omitempty"`
		Extra    *requestExtra `json:"extra,omitempty"`
	}

	payload := requestBody{
		Platform: strings.TrimSpace(cleanMeta["platform"]),
		Bid:      strings.TrimSpace(cleanMeta["bid"]),
		UID:      strings.TrimSpace(cleanMeta["uid"]),
		URL:      strings.TrimSpace(url),
		CDNURL:   strings.TrimSpace(cleanMeta["cdn_url"]),
	}
	if payload.Platform == "" {
		payload.Platform = defaultCookiePlatform
	}
	if payload.Bid == "" {
		return "", errors.New("crawler create task payload missing bid")
	}
	if payload.UID == "" {
		return "", errors.New("crawler create task payload missing uid")
	}
	if payload.URL == "" {
		return "", errors.New("crawler create task payload missing url")
	}
	if normalized := normalizeCookies(cookies); len(normalized) > 0 {
		payload.Extra = &requestExtra{Cookies: normalized}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", errors.Wrap(err, "encode create task payload")
	}
	endpoint := fmt.Sprintf("%s/download/tasks", c.baseURL)
	log.Debug().Str("url", endpoint).Any("payload", payload).Msg("create task request to crawler")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", errors.Wrap(err, "build create task request")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", errors.Wrap(err, "call crawler create task")
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		return "", c.errorFromResponse(resp)
	}
	var parsed struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			TaskID any `json:"task_id"`
		} `json:"data"`
	}
	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&parsed); err != nil {
		return "", errors.Wrap(err, "decode create task response")
	}
	if parsed.Code != 0 {
		return "", errors.Errorf("crawler create task failed: code=%d msg=%s", parsed.Code, strings.TrimSpace(parsed.Msg))
	}
	taskID := stringifyJSONID(parsed.Data.TaskID)
	if taskID == "" {
		return "", errors.New("crawler create task returned empty task_id")
	}
	return taskID, nil
}

func (c *restCrawlerTaskClient) GetTask(ctx context.Context, taskID string) (*crawlerTaskStatus, error) {
	requestedTaskID := strings.TrimSpace(taskID)
	log.Debug().Str("task_id", requestedTaskID).Msg("getting crawler task status")
	endpoint := fmt.Sprintf("%s/download/tasks/%s", c.baseURL, requestedTaskID)
	log.Debug().Str("url", endpoint).Str("task_id", requestedTaskID).Msg("get task from crawler")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, errors.Wrap(err, "build get task request")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "call crawler get task")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, errCrawlerTaskNotFound
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, c.errorFromResponse(resp)
	}
	var parsed struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			TaskID     any    `json:"task_id"`
			AID        string `json:"a_id"`
			ItemID     string `json:"item_id"`
			TaskStatus string `json:"task_status"`
			VID        string `json:"vid"`
			DetailMsg  string `json:"msg"`
			CreatedAt  any    `json:"created_at"`
			UpdatedAt  any    `json:"updated_at"`
		} `json:"data"`
	}
	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&parsed); err != nil {
		return nil, errors.Wrap(err, "decode get task response")
	}
	if parsed.Code != 0 {
		return nil, errors.Errorf("crawler get task failed: code=%d msg=%s", parsed.Code, strings.TrimSpace(parsed.Msg))
	}
	parsedTaskID := stringifyJSONID(parsed.Data.TaskID)
	if parsedTaskID == "" {
		parsedTaskID = requestedTaskID
	}
	return &crawlerTaskStatus{
		TaskID:    parsedTaskID,
		AID:       strings.TrimSpace(parsed.Data.AID),
		ItemID:    strings.TrimSpace(parsed.Data.ItemID),
		Status:    strings.TrimSpace(parsed.Data.TaskStatus),
		VID:       strings.TrimSpace(parsed.Data.VID),
		Error:     strings.TrimSpace(parsed.Data.DetailMsg),
		CreatedAt: parsed.Data.CreatedAt,
		UpdatedAt: parsed.Data.UpdatedAt,
	}, nil
}

func (c *restCrawlerTaskClient) errorFromResponse(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	return errors.Errorf("crawler request %s %s failed: status=%d body=%s", resp.Request.Method, resp.Request.URL.Path, resp.StatusCode, strings.TrimSpace(string(body)))
}

func (c *restCrawlerTaskClient) SendTaskSummary(ctx context.Context, payload TaskSummaryPayload) error {
	if c == nil {
		return errors.New("crawler client is nil")
	}
	normalized, err := normalizeTaskSummaryPayload(payload)
	if err != nil {
		return err
	}
	log.Debug().
		Str("status", normalized.Status).
		Str("task_name", normalized.TaskName).
		Int("total", normalized.Total).
		Int("done", normalized.Done).
		Int("unique_count", normalized.UniqueCount).
		Msg("sending crawler task summary")
	body, err := json.Marshal(normalized)
	if err != nil {
		return errors.Wrap(err, "encode task summary payload")
	}
	endpoint := fmt.Sprintf("%s/download/tasks/finish", c.baseURL)
	log.Debug().Str("url", endpoint).Any("payload", normalized).Msg("send task summary request to crawler")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return errors.Wrap(err, "build task summary request")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "call crawler task summary")
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		return c.errorFromResponse(resp)
	}
	var parsed struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&parsed); err != nil && !errors.Is(err, io.EOF) {
		return errors.Wrap(err, "decode task summary response")
	}
	if parsed.Code != 0 {
		return errors.Errorf("crawler task summary failed: code=%d msg=%s", parsed.Code, strings.TrimSpace(parsed.Msg))
	}
	return nil
}

// SendTaskSummaryToCrawler is a convenience wrapper that normalizes the
// given payload and forwards it to the downloader service's
// /download/tasks/finish endpoint using a short‑lived REST client. It is
// intended for grouped single_url_capture flows driven by webhook workers.
func SendTaskSummaryToCrawler(ctx context.Context, baseURL string, payload TaskSummaryPayload) error {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return errors.New("crawler base url is empty")
	}
	client, err := newRESTCrawlerTaskClient(baseURL, nil)
	if err != nil {
		return err
	}
	return client.SendTaskSummary(ctx, payload)
}

func normalizeCookies(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	out := make([]string, 0, len(values))
	for _, v := range values {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func normalizeTaskSummaryPayload(payload TaskSummaryPayload) (TaskSummaryPayload, error) {
	normalized := payload
	normalized.Status = strings.TrimSpace(normalized.Status)
	if normalized.Status == "" {
		normalized.Status = "finished"
	}
	normalized.TaskName = strings.TrimSpace(normalized.TaskName)
	normalized.Email = strings.TrimSpace(normalized.Email)
	if normalized.Total < 0 {
		normalized.Total = 0
	}
	if normalized.Done < 0 {
		normalized.Done = 0
	}
	if normalized.Done == 0 && normalized.Total > 0 {
		normalized.Done = normalized.Total
	}
	normalized.UniqueCombinations = sanitizeSummaryCombinations(normalized.UniqueCombinations)
	if normalized.UniqueCount <= 0 {
		normalized.UniqueCount = len(normalized.UniqueCombinations)
	}
	return normalized, nil
}

func sanitizeSummaryCombinations(combos []TaskSummaryCombination) []TaskSummaryCombination {
	if len(combos) == 0 {
		return combos
	}
	result := make([]TaskSummaryCombination, 0, len(combos))
	seen := make(map[string]struct{}, len(combos))
	for _, combo := range combos {
		bid := strings.TrimSpace(combo.Bid)
		uid := strings.TrimSpace(combo.AccountID)
		if bid == "" || uid == "" {
			continue
		}
		key := bid + "|" + uid
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, TaskSummaryCombination{Bid: bid, AccountID: uid})
	}
	return result
}

func stringifyJSONID(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(v)
	case json.Number:
		return strings.TrimSpace(v.String())
	case float64:
		if v == float64(int64(v)) {
			return fmt.Sprintf("%d", int64(v))
		}
		return strings.TrimSpace(fmt.Sprintf("%v", v))
	case int:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case uint64:
		return fmt.Sprintf("%d", v)
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", v))
	}
}
