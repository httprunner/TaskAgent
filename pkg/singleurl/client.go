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
	GetTask(ctx context.Context, jobID string) (*crawlerTaskStatus, error)
	SendTaskSummary(ctx context.Context, payload TaskSummaryPayload) error
}

type crawlerTaskStatus struct {
	JobID  string
	Status string
	VID    string
	Error  string
}

// TaskSummaryCombination captures a unique bid/account pair for the finished task payload.
type TaskSummaryCombination struct {
	Bid       string `json:"bid"`
	AccountID string `json:"account_id"`
}

// TaskSummaryPayload mirrors the downloader service finish_message schema.
type TaskSummaryPayload struct {
	TaskID             string                   `json:"task_id"`
	Total              int                      `json:"total"`
	Done               int                      `json:"done"`
	UniqueCombinations []TaskSummaryCombination `json:"unique_combinations"`
	UniqueCount        int                      `json:"unique_count"`
	CreatedAt          int64                    `json:"created_at"`
	TaskName           string                   `json:"task_name"`
	Email              string                   `json:"email"`
}

var errCrawlerJobNotFound = errors.New("crawler task not found")

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
		JobID string `json:"job_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", errors.Wrap(err, "decode create task response")
	}
	if strings.TrimSpace(parsed.JobID) == "" {
		return "", errors.New("crawler create task returned empty job_id")
	}
	return parsed.JobID, nil
}

func (c *restCrawlerTaskClient) GetTask(ctx context.Context, jobID string) (*crawlerTaskStatus, error) {
	log.Debug().Str("job_id", jobID).Msg("getting crawler task status")
	endpoint := fmt.Sprintf("%s/download/tasks/%s", c.baseURL, strings.TrimSpace(jobID))
	log.Debug().Str("url", endpoint).Str("jobID", jobID).Msg("get task job from crawler")
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
		return nil, errCrawlerJobNotFound
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, c.errorFromResponse(resp)
	}
	var parsed struct {
		JobID  string `json:"job_id"`
		Status string `json:"status"`
		VID    string `json:"vid"`
		Error  string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, errors.Wrap(err, "decode get task response")
	}
	return &crawlerTaskStatus{
		JobID:  jobID,
		Status: parsed.Status,
		VID:    parsed.VID,
		Error:  parsed.Error,
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
	normalized, err := normalizeTaskSummaryPayload(payload, time.Now())
	if err != nil {
		return err
	}
	log.Debug().
		Str("task_id", normalized.TaskID).
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

func normalizeTaskSummaryPayload(payload TaskSummaryPayload, now time.Time) (TaskSummaryPayload, error) {
	normalized := payload
	normalized.TaskID = strings.TrimSpace(normalized.TaskID)
	if normalized.TaskID == "" {
		return TaskSummaryPayload{}, errors.New("task summary payload missing task_id")
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
	if normalized.CreatedAt <= 0 {
		normalized.CreatedAt = now.UTC().Unix()
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
