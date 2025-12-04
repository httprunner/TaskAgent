package pool

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
	CreateTask(ctx context.Context, url string, cookies []string) (string, error)
	GetTask(ctx context.Context, jobID string) (*crawlerTaskStatus, error)
}

type crawlerTaskStatus struct {
	JobID  string
	Status string
	VID    string
	Error  string
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
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &restCrawlerTaskClient{baseURL: baseURL, httpClient: httpClient}, nil
}

func (c *restCrawlerTaskClient) CreateTask(ctx context.Context, url string, cookies []string) (string, error) {
	log.Debug().Str("url", url).Int("cookies", len(cookies)).Msg("creating crawler task")
	payload := map[string]any{
		"url":          strings.TrimSpace(url),
		"cookies":      normalizeCookies(cookies),
		"sync_to_hive": true,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", errors.Wrap(err, "encode create task payload")
	}
	endpoint := fmt.Sprintf("%s/download/tasks", c.baseURL)
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
