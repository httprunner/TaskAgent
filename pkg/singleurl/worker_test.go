package singleurl

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/httprunner/TaskAgent/internal/feishusdk"
)

func TestSingleURLWorkerDefaultLimit(t *testing.T) {
	worker, err := NewSingleURLWorker(SingleURLWorkerConfig{
		Client:        &singleURLTestClient{},
		CrawlerClient: &stubCrawlerClient{createJobID: "noop"},
		BitableURL:    "https://bitable.example",
		Limit:         0,
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	if worker.limit != DefaultSingleURLWorkerLimit {
		t.Fatalf("expected limit %d, got %d", DefaultSingleURLWorkerLimit, worker.limit)
	}
}

func TestSingleURLWorkerQueuesTaskAfterCreatingCrawlerJob(t *testing.T) {
	client := &singleURLTestClient{
		rows: map[string][]feishusdk.TaskRow{
			feishusdk.StatusPending: {
				{
					TaskID: 1,
					Scene:  SceneSingleURLCapture,
					Status: feishusdk.StatusPending,
					Params: "capture",
					BookID: "B001",
					UserID: "U001",
					App:    "kuaishou",
					URL:    "https://example.com/video",
				},
			},
		},
	}
	crawler := &stubCrawlerClient{createJobID: "job-123"}
	worker, err := NewSingleURLWorker(SingleURLWorkerConfig{
		Client:        client,
		CrawlerClient: crawler,
		BitableURL:    "https://bitable.example",
		Limit:         5,
		PollInterval:  time.Second,
		Clock:         func() time.Time { return time.Unix(100, 0) },
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	if err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("process once: %v", err)
	}
	if len(crawler.createdURLs) != 1 {
		t.Fatalf("expected crawler create call")
	}
	if got := crawler.createdURLs[0]; got != "https://example.com/video" {
		t.Fatalf("unexpected url %s", got)
	}
	meta := crawler.createdMeta[0]
	if got := meta["platform"]; got != "kuaishou" {
		t.Fatalf("unexpected platform %s", got)
	}
	if got := meta["bid"]; got != "B001" {
		t.Fatalf("unexpected bid %s", got)
	}
	if got := meta["uid"]; got != "U001" {
		t.Fatalf("unexpected uid %s", got)
	}
	if len(client.updateCalls) == 0 {
		t.Fatalf("no update calls recorded")
	}
	last := client.updateCalls[len(client.updateCalls)-1]
	if status := last.fields[feishusdk.DefaultTaskFields.Status]; status != singleURLStatusQueued {
		t.Fatalf("expected status queued, got %#v", status)
	}
	extra := last.fields[feishusdk.DefaultTaskFields.Extra]
	if extraStr, _ := extra.(string); extraStr == "" || !strings.Contains(extraStr, "job-123") {
		t.Fatalf("extra missing job id: %v", extra)
	}
}

func TestSingleURLWorkerUsesCookiesWhenAvailable(t *testing.T) {
	client := &singleURLTestClient{
		rows: map[string][]feishusdk.TaskRow{
			feishusdk.StatusPending: {
				{
					TaskID: 10,
					Scene:  SceneSingleURLCapture,
					Status: feishusdk.StatusPending,
					Params: "capture",
					BookID: "B010",
					UserID: "U010",
					App:    "kuaishou",
					URL:    "https://example.com/cookie",
				},
			},
		},
	}
	crawler := &stubCrawlerClient{createJobID: "job-cookie"}
	provider := &stubCookieProvider{
		values: []*CookieRecord{{RecordID: "rec-1", Value: "cookie=value"}},
	}
	worker, err := NewSingleURLWorker(SingleURLWorkerConfig{
		Client:         client,
		CrawlerClient:  crawler,
		CookieProvider: provider,
		BitableURL:     "https://bitable.example",
		Limit:          5,
		PollInterval:   time.Second,
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	if err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("process once: %v", err)
	}
	if len(crawler.createdCookies) != 1 {
		t.Fatalf("expected cookies to be forwarded")
	}
	got := crawler.createdCookies[0]
	if len(got) != 1 || got[0] != "cookie=value" {
		t.Fatalf("unexpected cookies payload: %#v", got)
	}
}

func TestSingleURLWorkerPollsSuccessAndWritesVid(t *testing.T) {
	meta := singleURLMetadata{Attempts: []singleURLAttempt{{JobID: "job-success"}}}
	client := &singleURLTestClient{
		rows: map[string][]feishusdk.TaskRow{
			singleURLStatusQueued: {
				{
					TaskID: 2,
					Scene:  SceneSingleURLCapture,
					Status: singleURLStatusQueued,
					Params: "capture",
					BookID: "B002",
					UserID: "U002",
					URL:    "https://example.com/video2",
					Extra:  encodeSingleURLMetadata(meta),
				},
			},
		},
	}
	crawler := &stubCrawlerClient{
		statuses: map[string]*crawlerTaskStatus{
			"job-success": {JobID: "job-success", Status: "done", VID: "vid-999"},
		},
	}
	worker, err := NewSingleURLWorker(SingleURLWorkerConfig{
		Client:        client,
		CrawlerClient: crawler,
		BitableURL:    "https://bitable.example",
		Limit:         5,
		PollInterval:  time.Second,
		Clock:         func() time.Time { return time.Unix(200, 0) },
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	if err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("process once: %v", err)
	}
	if len(client.updateCalls) < 2 {
		t.Fatalf("expected at least 2 updates, got %d", len(client.updateCalls))
	}
	extraUpdated := client.updateCalls[len(client.updateCalls)-2]
	if extraStr, _ := extraUpdated.fields[feishusdk.DefaultTaskFields.Extra].(string); !strings.Contains(extraStr, "vid-999") {
		t.Fatalf("vid missing in extra: %v", extraStr)
	}
	statusUpdate := client.updateCalls[len(client.updateCalls)-1]
	if statusUpdate.fields[feishusdk.DefaultTaskFields.Status] != feishusdk.StatusSuccess {
		t.Fatalf("expected success status, got %#v", statusUpdate.fields[feishusdk.DefaultTaskFields.Status])
	}
}

func TestSingleURLWorkerMarksCrawlerFailure(t *testing.T) {
	crawler := &stubCrawlerClient{createErr: errors.New("boom")}
	client := &singleURLTestClient{
		rows: map[string][]feishusdk.TaskRow{
			feishusdk.StatusPending: {
				{TaskID: 3, Scene: SceneSingleURLCapture, Status: feishusdk.StatusPending, Params: "capture", BookID: "B003", UserID: "U003", App: "kuaishou", URL: "https://example.com/3"},
			},
		},
	}
	worker, err := NewSingleURLWorker(SingleURLWorkerConfig{
		Client:        client,
		CrawlerClient: crawler,
		BitableURL:    "https://bitable.example",
		Limit:         5,
		PollInterval:  time.Second,
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	if err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("process once: %v", err)
	}
	if len(client.updateCalls) == 0 {
		t.Fatalf("expected failure update")
	}
	call := client.updateCalls[len(client.updateCalls)-1]
	if call.fields[feishusdk.DefaultTaskFields.Status] != feishusdk.StatusFailed {
		t.Fatalf("expected failed status, got %#v", call.fields)
	}
	if extraStr, _ := call.fields[feishusdk.DefaultTaskFields.Extra].(string); !strings.Contains(extraStr, "boom") {
		t.Fatalf("missing error in extra: %v", extraStr)
	}
}

func TestSingleURLWorkerSendsGroupSummaryWhenAllSuccess(t *testing.T) {
	groupID := buildSingleURLGroupID("com.smile.gifmaker", "B001", "U001")
	client := &singleURLTestClient{
		rows: map[string][]feishusdk.TaskRow{
			singleURLStatusQueued: {
				{
					TaskID:  20,
					Scene:   SceneSingleURLCapture,
					Status:  singleURLStatusQueued,
					Params:  "capture",
					BookID:  "B001",
					UserID:  "U001",
					GroupID: groupID,
					Extra:   encodeSingleURLMetadata(singleURLMetadata{Attempts: []singleURLAttempt{{JobID: "job-sum-1"}}}),
				},
				{
					TaskID:  21,
					Scene:   SceneSingleURLCapture,
					Status:  singleURLStatusQueued,
					Params:  "capture",
					BookID:  "B001",
					UserID:  "U001",
					GroupID: groupID,
					Extra:   encodeSingleURLMetadata(singleURLMetadata{Attempts: []singleURLAttempt{{JobID: "job-sum-2"}}}),
				},
			},
		},
		groupRows: map[string][]feishusdk.TaskRow{
			groupID: {
				{TaskID: 20, Scene: SceneSingleURLCapture, Status: singleURLStatusQueued, Webhook: feishusdk.WebhookPending, BookID: "B001", UserID: "U001", GroupID: groupID, Params: "drama-A"},
				{TaskID: 21, Scene: SceneSingleURLCapture, Status: singleURLStatusQueued, Webhook: feishusdk.WebhookPending, BookID: "B001", UserID: "U001", GroupID: groupID, Params: "drama-A"},
			},
		},
	}
	crawler := &stubCrawlerClient{
		statuses: map[string]*crawlerTaskStatus{
			"job-sum-1": {JobID: "job-sum-1", Status: "done", VID: "vid-1"},
			"job-sum-2": {JobID: "job-sum-2", Status: "done", VID: "vid-2"},
		},
	}
	worker, err := NewSingleURLWorker(SingleURLWorkerConfig{
		Client:        client,
		CrawlerClient: crawler,
		BitableURL:    "https://bitable.example",
		Limit:         5,
		PollInterval:  time.Second,
		Clock:         func() time.Time { return time.Unix(900, 0) },
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	if err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("process once: %v", err)
	}
	if len(crawler.summaryPayloads) != 1 {
		t.Fatalf("expected 1 summary payload, got %d", len(crawler.summaryPayloads))
	}
	payload := crawler.summaryPayloads[0]
	if payload.TaskID != groupID {
		t.Fatalf("unexpected task_id %s", payload.TaskID)
	}
	if payload.Total != 2 || payload.Done != 2 {
		t.Fatalf("unexpected totals: %+v", payload)
	}
	if payload.UniqueCount != 1 {
		t.Fatalf("expected unique count 1, got %d", payload.UniqueCount)
	}
	if payload.TaskName != "drama-A" {
		t.Fatalf("unexpected task name %s", payload.TaskName)
	}
	if got := client.groupRows[groupID][0].Webhook; got != feishusdk.WebhookSuccess {
		t.Fatalf("expected webhook success, got %s", got)
	}
}

func TestSingleURLWorkerSkipsGroupSummaryWhenNotAllSuccess(t *testing.T) {
	groupID := buildSingleURLGroupID("com.smile.gifmaker", "B010", "U010")
	client := &singleURLTestClient{
		rows: map[string][]feishusdk.TaskRow{
			singleURLStatusQueued: {
				{
					TaskID:  30,
					Scene:   SceneSingleURLCapture,
					Status:  singleURLStatusQueued,
					Params:  "capture",
					BookID:  "B010",
					UserID:  "U010",
					GroupID: groupID,
					Extra:   encodeSingleURLMetadata(singleURLMetadata{Attempts: []singleURLAttempt{{JobID: "job-mixed"}}}),
				},
			},
		},
		groupRows: map[string][]feishusdk.TaskRow{
			groupID: {
				{TaskID: 30, Scene: SceneSingleURLCapture, Status: singleURLStatusQueued, Webhook: feishusdk.WebhookPending, BookID: "B010", UserID: "U010", GroupID: groupID, Params: "drama-B"},
				{TaskID: 31, Scene: SceneSingleURLCapture, Status: feishusdk.StatusRunning, Webhook: feishusdk.WebhookPending, BookID: "B010", UserID: "U010", GroupID: groupID, Params: "drama-B"},
			},
		},
	}
	crawler := &stubCrawlerClient{
		statuses: map[string]*crawlerTaskStatus{
			"job-mixed": {JobID: "job-mixed", Status: "done", VID: "vid-mixed"},
		},
	}
	worker, err := NewSingleURLWorker(SingleURLWorkerConfig{
		Client:        client,
		CrawlerClient: crawler,
		BitableURL:    "https://bitable.example",
		Limit:         5,
		PollInterval:  time.Second,
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	if err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("process once: %v", err)
	}
	if len(crawler.summaryPayloads) != 0 {
		t.Fatalf("expected no summary payload, got %d", len(crawler.summaryPayloads))
	}
}

func TestSingleURLWorkerRetriesFailedTaskWithExistingJobID(t *testing.T) {
	legacyExtra := `{"job_id":"job-old","error":"boom"}`
	client := &singleURLTestClient{
		rows: map[string][]feishusdk.TaskRow{
			feishusdk.StatusFailed: {
				{
					TaskID: 40,
					Scene:  SceneSingleURLCapture,
					Status: feishusdk.StatusFailed,
					Params: "capture",
					BookID: "B040",
					UserID: "U040",
					URL:    "https://example.com/retry",
					Extra:  legacyExtra,
				},
			},
		},
	}
	crawler := &stubCrawlerClient{createJobID: "job-new"}
	worker, err := NewSingleURLWorker(SingleURLWorkerConfig{
		Client:        client,
		CrawlerClient: crawler,
		BitableURL:    "https://bitable.example",
		Limit:         5,
		PollInterval:  time.Second,
		Clock:         func() time.Time { return time.Unix(500, 0) },
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	if err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("process once: %v", err)
	}
	if len(crawler.createdURLs) != 1 {
		t.Fatalf("expected retry to create crawler job, got %d", len(crawler.createdURLs))
	}
	var encoded string
	for _, call := range client.updateCalls {
		if val, ok := call.fields[feishusdk.DefaultTaskFields.Extra]; ok {
			if s, ok := val.(string); ok {
				encoded = s
			}
		}
	}
	if encoded == "" {
		t.Fatalf("expected extra to be updated")
	}
	meta := decodeSingleURLMetadata(encoded)
	if len(meta.Attempts) != 2 {
		t.Fatalf("expected 2 attempts recorded, got %d", len(meta.Attempts))
	}
	if meta.Attempts[0].JobID != "job-old" || meta.Attempts[1].JobID != "job-new" {
		t.Fatalf("unexpected job history: %#v", meta.Attempts)
	}
}

func TestSingleURLWorkerStopsAfterMaxAttempts(t *testing.T) {
	meta := singleURLMetadata{Attempts: []singleURLAttempt{{JobID: "job-1"}, {JobID: "job-2"}, {JobID: "job-3"}}}
	client := &singleURLTestClient{
		rows: map[string][]feishusdk.TaskRow{
			feishusdk.StatusFailed: {
				{
					TaskID: 70,
					Scene:  SceneSingleURLCapture,
					Status: feishusdk.StatusFailed,
					Params: "capture",
					BookID: "B070",
					UserID: "U070",
					URL:    "https://example.com/stop",
					Extra:  encodeSingleURLMetadata(meta),
				},
			},
		},
	}
	crawler := &stubCrawlerClient{}
	worker, err := NewSingleURLWorker(SingleURLWorkerConfig{
		Client:        client,
		CrawlerClient: crawler,
		BitableURL:    "https://bitable.example",
		Limit:         5,
		PollInterval:  time.Second,
		Clock:         func() time.Time { return time.Unix(600, 0) },
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	if err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("process once: %v", err)
	}
	if len(crawler.createdURLs) != 0 {
		t.Fatalf("expected no new crawler jobs, got %d", len(crawler.createdURLs))
	}
	var encoded string
	for _, call := range client.updateCalls {
		if val, ok := call.fields[feishusdk.DefaultTaskFields.Extra]; ok {
			if s, ok := val.(string); ok {
				encoded = s
			}
		}
	}
	if encoded == "" {
		t.Fatalf("expected extra to be updated")
	}
	decoded := decodeSingleURLMetadata(encoded)
	if decoded.attemptsWithJobID() != singleURLMaxAttempts {
		t.Fatalf("expected metadata to retain %d attempts, got %d", singleURLMaxAttempts, decoded.attemptsWithJobID())
	}
}

type singleURLTestClient struct {
	rows        map[string][]feishusdk.TaskRow
	groupRows   map[string][]feishusdk.TaskRow
	updateCalls []singleURLUpdateCall
}

type singleURLUpdateCall struct {
	taskID int64
	fields map[string]any
}

func (c *singleURLTestClient) FetchTaskTableWithOptions(_ context.Context, _ string, _ *feishusdk.TaskFields, opts *feishusdk.TaskQueryOptions) (*feishusdk.TaskTable, error) {
	groupID := suExtractConditionValue(opts.Filter, feishusdk.DefaultTaskFields.GroupID)
	if strings.TrimSpace(groupID) != "" {
		rows := cloneTaskRows(c.groupRows[groupID])
		return &feishusdk.TaskTable{Fields: feishusdk.DefaultTaskFields, Rows: rows}, nil
	}
	status := suExtractConditionValue(opts.Filter, feishusdk.DefaultTaskFields.Status)
	scene := suExtractConditionValue(opts.Filter, feishusdk.DefaultTaskFields.Scene)
	if scene != SceneSingleURLCapture {
		return &feishusdk.TaskTable{Fields: feishusdk.DefaultTaskFields}, nil
	}
	rows := cloneTaskRows(c.rows[status])
	limit := 0
	if opts != nil {
		limit = opts.Limit
	}
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return &feishusdk.TaskTable{Fields: feishusdk.DefaultTaskFields, Rows: rows}, nil
}

func (c *singleURLTestClient) UpdateTaskStatus(context.Context, *feishusdk.TaskTable, int64, string) error {
	return nil
}

func (c *singleURLTestClient) UpdateTaskFields(_ context.Context, _ *feishusdk.TaskTable, taskID int64, fields map[string]any) error {
	copyFields := make(map[string]any, len(fields))
	for k, v := range fields {
		copyFields[k] = v
	}
	c.updateCalls = append(c.updateCalls, singleURLUpdateCall{taskID: taskID, fields: copyFields})
	c.applyFieldUpdates(taskID, fields)
	return nil
}

func (c *singleURLTestClient) applyFieldUpdates(taskID int64, fields map[string]any) {
	if len(c.groupRows) == 0 {
		return
	}
	for groupID, rows := range c.groupRows {
		updated := false
		for idx, row := range rows {
			if row.TaskID != taskID {
				continue
			}
			for field, value := range fields {
				switch field {
				case feishusdk.DefaultTaskFields.Status:
					rows[idx].Status = fmt.Sprint(value)
				case feishusdk.DefaultTaskFields.Webhook:
					rows[idx].Webhook = fmt.Sprint(value)
				case feishusdk.DefaultTaskFields.Extra:
					if str, ok := value.(string); ok {
						rows[idx].Extra = str
					}
				case feishusdk.DefaultTaskFields.GroupID:
					rows[idx].GroupID = fmt.Sprint(value)
				}
			}
			updated = true
		}
		if updated {
			c.groupRows[groupID] = rows
		}
	}
}

func cloneTaskRows(rows []feishusdk.TaskRow) []feishusdk.TaskRow {
	if len(rows) == 0 {
		return nil
	}
	dup := make([]feishusdk.TaskRow, len(rows))
	copy(dup, rows)
	return dup
}

func suExtractConditionValue(filter *feishusdk.FilterInfo, field string) string {
	if filter == nil {
		return ""
	}
	for _, cond := range filter.Conditions {
		if cond == nil || cond.FieldName == nil || *cond.FieldName != field {
			continue
		}
		if len(cond.Value) > 0 {
			return cond.Value[0]
		}
	}
	for _, child := range filter.Children {
		for _, cond := range child.Conditions {
			if cond == nil || cond.FieldName == nil || *cond.FieldName != field {
				continue
			}
			if len(cond.Value) > 0 {
				return cond.Value[0]
			}
		}
	}
	return ""
}

type stubCrawlerClient struct {
	createJobID     string
	createErr       error
	statuses        map[string]*crawlerTaskStatus
	createdURLs     []string
	createdCookies  [][]string
	createdMeta     []map[string]string
	summaryPayloads []TaskSummaryPayload
	summaryErr      error
	queriedJobID    []string
}

func (c *stubCrawlerClient) CreateTask(_ context.Context, url string, cookies []string, meta map[string]string) (string, error) {
	c.createdURLs = append(c.createdURLs, url)
	copyCookies := append([]string(nil), cookies...)
	c.createdCookies = append(c.createdCookies, copyCookies)
	copyMeta := make(map[string]string, len(meta))
	for k, v := range meta {
		copyMeta[k] = v
	}
	c.createdMeta = append(c.createdMeta, copyMeta)
	if c.createErr != nil {
		return "", c.createErr
	}
	if c.createJobID == "" {
		c.createJobID = "job-default"
	}
	return c.createJobID, nil
}

func (c *stubCrawlerClient) GetTask(_ context.Context, jobID string) (*crawlerTaskStatus, error) {
	c.queriedJobID = append(c.queriedJobID, jobID)
	if status, ok := c.statuses[jobID]; ok {
		return status, nil
	}
	return nil, errCrawlerJobNotFound
}

func (c *stubCrawlerClient) SendTaskSummary(_ context.Context, payload TaskSummaryPayload) error {
	c.summaryPayloads = append(c.summaryPayloads, payload)
	return c.summaryErr
}

type stubCookieProvider struct {
	values []*CookieRecord
	idx    int
	err    error
}

func (s *stubCookieProvider) PickCookie(context.Context) (*CookieRecord, error) {
	if s.err != nil {
		return nil, s.err
	}
	if len(s.values) == 0 {
		return nil, nil
	}
	rec := s.values[s.idx%len(s.values)]
	s.idx++
	copy := *rec
	return &copy, nil
}
