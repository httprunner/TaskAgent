package pool

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	feishusvc "github.com/httprunner/TaskAgent/pkg/feishu"
)

func TestSingleURLWorkerQueuesTaskAfterCreatingCrawlerJob(t *testing.T) {
	client := &singleURLTestClient{
		rows: map[string][]feishusvc.TaskRow{
			feishusvc.StatusPending: {
				{
					TaskID: 1,
					Scene:  SceneSingleURLCapture,
					Status: feishusvc.StatusPending,
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
	if status := last.fields[feishusvc.DefaultTaskFields.Status]; status != singleURLStatusQueued {
		t.Fatalf("expected status queued, got %#v", status)
	}
	extra := last.fields[feishusvc.DefaultTaskFields.Extra]
	if extraStr, _ := extra.(string); extraStr == "" || !strings.Contains(extraStr, "job-123") {
		t.Fatalf("extra missing job id: %v", extra)
	}
}

func TestSingleURLWorkerUsesCookiesWhenAvailable(t *testing.T) {
	client := &singleURLTestClient{
		rows: map[string][]feishusvc.TaskRow{
			feishusvc.StatusPending: {
				{
					TaskID: 10,
					Scene:  SceneSingleURLCapture,
					Status: feishusvc.StatusPending,
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
	meta := singleURLMetadata{JobID: "job-success"}
	client := &singleURLTestClient{
		rows: map[string][]feishusvc.TaskRow{
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
	if extraStr, _ := extraUpdated.fields[feishusvc.DefaultTaskFields.Extra].(string); !strings.Contains(extraStr, "vid-999") {
		t.Fatalf("vid missing in extra: %v", extraStr)
	}
	statusUpdate := client.updateCalls[len(client.updateCalls)-1]
	if statusUpdate.fields[feishusvc.DefaultTaskFields.Status] != feishusvc.StatusSuccess {
		t.Fatalf("expected success status, got %#v", statusUpdate.fields[feishusvc.DefaultTaskFields.Status])
	}
}

func TestSingleURLWorkerMarksCrawlerFailure(t *testing.T) {
	crawler := &stubCrawlerClient{createErr: errors.New("boom")}
	client := &singleURLTestClient{
		rows: map[string][]feishusvc.TaskRow{
			feishusvc.StatusPending: {
				{TaskID: 3, Scene: SceneSingleURLCapture, Status: feishusvc.StatusPending, Params: "capture", BookID: "B003", UserID: "U003", App: "kuaishou", URL: "https://example.com/3"},
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
	if call.fields[feishusvc.DefaultTaskFields.Status] != feishusvc.StatusFailed {
		t.Fatalf("expected failed status, got %#v", call.fields)
	}
	if extraStr, _ := call.fields[feishusvc.DefaultTaskFields.Extra].(string); !strings.Contains(extraStr, "boom") {
		t.Fatalf("missing error in extra: %v", extraStr)
	}
}

type singleURLTestClient struct {
	rows        map[string][]feishusvc.TaskRow
	updateCalls []singleURLUpdateCall
}

type singleURLUpdateCall struct {
	taskID int64
	fields map[string]any
}

func (c *singleURLTestClient) FetchTaskTableWithOptions(_ context.Context, _ string, _ *feishusvc.TaskFields, opts *feishusvc.TaskQueryOptions) (*feishusvc.TaskTable, error) {
	status := suExtractConditionValue(opts.Filter, feishusvc.DefaultTaskFields.Status)
	scene := suExtractConditionValue(opts.Filter, feishusvc.DefaultTaskFields.Scene)
	if scene != SceneSingleURLCapture {
		return &feishusvc.TaskTable{Fields: feishusvc.DefaultTaskFields}, nil
	}
	rows := cloneTaskRows(c.rows[status])
	limit := 0
	if opts != nil {
		limit = opts.Limit
	}
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return &feishusvc.TaskTable{Fields: feishusvc.DefaultTaskFields, Rows: rows}, nil
}

func (c *singleURLTestClient) UpdateTaskStatus(context.Context, *feishusvc.TaskTable, int64, string) error {
	return nil
}

func (c *singleURLTestClient) UpdateTaskFields(_ context.Context, _ *feishusvc.TaskTable, taskID int64, fields map[string]any) error {
	copyFields := make(map[string]any, len(fields))
	for k, v := range fields {
		copyFields[k] = v
	}
	c.updateCalls = append(c.updateCalls, singleURLUpdateCall{taskID: taskID, fields: copyFields})
	return nil
}

func cloneTaskRows(rows []feishusvc.TaskRow) []feishusvc.TaskRow {
	if len(rows) == 0 {
		return nil
	}
	dup := make([]feishusvc.TaskRow, len(rows))
	copy(dup, rows)
	return dup
}

func suExtractConditionValue(filter *feishusvc.FilterInfo, field string) string {
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
	createJobID    string
	createErr      error
	statuses       map[string]*crawlerTaskStatus
	createdURLs    []string
	createdCookies [][]string
	createdMeta    []map[string]string
	queriedJobID   []string
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
