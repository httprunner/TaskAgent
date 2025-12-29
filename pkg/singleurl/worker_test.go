package singleurl

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/internal/feishusdk"
)

func TestSingleURLWorkerAutoLimitPreservesZero(t *testing.T) {
	worker, err := NewSingleURLWorker(SingleURLWorkerConfig{
		Client:        &singleURLTestClient{},
		CrawlerClient: &stubCrawlerClient{createTaskID: "noop"},
		BitableURL:    "https://bitable.example",
		Limit:         0,
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	if worker.limit != 0 {
		t.Fatalf("expected auto limit to preserve 0, got %d", worker.limit)
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
	crawler := &stubCrawlerClient{createTaskID: "task-123"}
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
	if got := meta["platform"]; got != defaultCookiePlatform {
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
	if status := last.fields[feishusdk.DefaultTaskFields.Status]; status != feishusdk.StatusDownloaderQueued {
		t.Fatalf("expected status %q, got %#v", feishusdk.StatusDownloaderQueued, status)
	}
	logsVal := last.fields[feishusdk.DefaultTaskFields.Logs]
	if logsStr, _ := logsVal.(string); logsStr == "" || !strings.Contains(logsStr, "task-123") {
		t.Fatalf("logs missing task id: %v", logsVal)
	}
}

func TestSingleURLWorkerForwardsCDNURLFromExtra(t *testing.T) {
	client := &singleURLTestClient{
		rows: map[string][]feishusdk.TaskRow{
			feishusdk.StatusPending: {
				{
					TaskID: 11,
					Scene:  SceneSingleURLCapture,
					Status: feishusdk.StatusPending,
					Params: "capture",
					BookID: "B011",
					UserID: "U011",
					App:    "com.smile.gifmaker",
					URL:    "https://example.com/share",
					Extra:  `{"cdn_url":"https://cdn.example/video.m3u8"}`,
				},
			},
		},
	}
	crawler := &stubCrawlerClient{createTaskID: "task-cdn"}
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
	if len(crawler.createdMeta) != 1 {
		t.Fatalf("expected crawler create call")
	}
	meta := crawler.createdMeta[0]
	if got := meta["cdn_url"]; got != "https://cdn.example/video.m3u8" {
		t.Fatalf("unexpected cdn_url %s", got)
	}
}

func TestSingleURLReadyWorkerMarksFailedWhenCDNURLMissing(t *testing.T) {
	client := &singleURLTestClient{
		rows: map[string][]feishusdk.TaskRow{
			feishusdk.StatusReady: {
				{
					TaskID: 12,
					Scene:  SceneSingleURLCapture,
					Status: feishusdk.StatusReady,
					Params: "capture",
					BookID: "B012",
					UserID: "U012",
					App:    "com.smile.gifmaker",
					URL:    "https://example.com/share",
					Extra:  `{}`,
				},
			},
		},
	}
	worker, err := NewSingleURLWorker(SingleURLWorkerConfig{
		Client:        client,
		CrawlerClient: &stubCrawlerClient{createTaskID: "noop"},
		BitableURL:    "https://bitable.example",
		Limit:         5,
		PollInterval:  time.Second,
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	worker.newTaskStatuses = []string{feishusdk.StatusReady}
	if err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("process once: %v", err)
	}
	if len(client.updateCalls) == 0 {
		t.Fatalf("expected task status update")
	}
	call := client.updateCalls[len(client.updateCalls)-1]
	if call.fields[feishusdk.DefaultTaskFields.Status] != feishusdk.StatusFailed {
		t.Fatalf("expected status %q, got %#v", feishusdk.StatusFailed, call.fields[feishusdk.DefaultTaskFields.Status])
	}
}

func TestSingleURLWorkerPollsSuccessAndWritesVid(t *testing.T) {
	meta := singleURLMetadata{Attempts: []singleURLAttempt{{TaskID: "job-success"}}}
	client := &singleURLTestClient{
		rows: map[string][]feishusdk.TaskRow{
			feishusdk.StatusDownloaderQueued: {
				{
					TaskID: 2,
					Scene:  SceneSingleURLCapture,
					Status: feishusdk.StatusDownloaderQueued,
					Params: "capture",
					BookID: "B002",
					UserID: "U002",
					URL:    "https://example.com/video2",
					Logs:   encodeSingleURLMetadata(meta),
				},
			},
		},
	}
	crawler := &stubCrawlerClient{
		statuses: map[string]*crawlerTaskStatus{
			"job-success": {TaskID: "job-success", Status: "COMPLETED", VID: "vid-999"},
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
	logsUpdated := client.updateCalls[len(client.updateCalls)-2]
	if logsStr, _ := logsUpdated.fields[feishusdk.DefaultTaskFields.Logs].(string); !strings.Contains(logsStr, "vid-999") {
		t.Fatalf("vid missing in logs: %v", logsStr)
	}
	statusUpdate := client.updateCalls[len(client.updateCalls)-1]
	if statusUpdate.fields[feishusdk.DefaultTaskFields.Status] != feishusdk.StatusSuccess {
		t.Fatalf("expected success status, got %#v", statusUpdate.fields[feishusdk.DefaultTaskFields.Status])
	}
}

func TestSingleURLWorkerDoesNotMarkSuccessWhenVidMissing(t *testing.T) {
	meta := singleURLMetadata{Attempts: []singleURLAttempt{{TaskID: "job-missing-vid"}}}
	client := &singleURLTestClient{
		rows: map[string][]feishusdk.TaskRow{
			feishusdk.StatusDownloaderQueued: {
				{
					TaskID: 4,
					Scene:  SceneSingleURLCapture,
					Status: feishusdk.StatusDownloaderQueued,
					Params: "capture",
					BookID: "B004",
					UserID: "U004",
					URL:    "https://example.com/video4",
					Logs:   encodeSingleURLMetadata(meta),
				},
			},
		},
	}
	crawler := &stubCrawlerClient{
		statuses: map[string]*crawlerTaskStatus{
			"job-missing-vid": {TaskID: "job-missing-vid", Status: "COMPLETED", VID: ""},
		},
	}
	worker, err := NewSingleURLWorker(SingleURLWorkerConfig{
		Client:        client,
		CrawlerClient: crawler,
		BitableURL:    "https://bitable.example",
		Limit:         5,
		PollInterval:  time.Second,
		Clock:         func() time.Time { return time.Unix(300, 0) },
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	if err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("process once: %v", err)
	}
	if len(client.updateCalls) == 0 {
		t.Fatalf("expected updates")
	}
	last := client.updateCalls[len(client.updateCalls)-1]
	if last.fields[feishusdk.DefaultTaskFields.Status] == feishusdk.StatusSuccess {
		t.Fatalf("should not mark success when vid missing")
	}
	if last.fields[feishusdk.DefaultTaskFields.Status] != feishusdk.StatusDownloaderProcessing {
		t.Fatalf("expected processing status when vid missing, got %#v", last.fields[feishusdk.DefaultTaskFields.Status])
	}
	var logsStr string
	for _, call := range client.updateCalls {
		if v, ok := call.fields[feishusdk.DefaultTaskFields.Logs]; ok {
			if s, ok := v.(string); ok {
				logsStr = s
			}
		}
	}
	if strings.Contains(logsStr, "\"vid\"") {
		t.Fatalf("vid should not be written when missing, logs=%s", logsStr)
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
	if call.fields[feishusdk.DefaultTaskFields.Status] != feishusdk.StatusDownloaderFailed {
		t.Fatalf("expected failed status, got %#v", call.fields)
	}
	logsField := feishusdk.DefaultTaskFields.Logs
	logsStr, _ := call.fields[logsField].(string)
	if !strings.Contains(logsStr, "boom") {
		t.Fatalf("missing error in logs: %v", logsStr)
	}
}

func TestSingleURLWorkerMovesDownloaderFailedToDeviceStage(t *testing.T) {
	meta := singleURLMetadata{Attempts: []singleURLAttempt{{TaskID: "job-1", Error: "boom"}}}
	client := &singleURLTestClient{
		rows: map[string][]feishusdk.TaskRow{
			feishusdk.StatusDownloaderFailed: {
				{
					TaskID: 50,
					Scene:  SceneSingleURLCapture,
					Status: feishusdk.StatusDownloaderFailed,
					Params: "capture",
					BookID: "B050",
					UserID: "U050",
					URL:    "https://example.com/retry-dl",
					Logs:   encodeSingleURLMetadata(meta),
				},
			},
		},
	}
	crawler := &stubCrawlerClient{createTaskID: "task-should-not-create"}
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
	if len(crawler.createdURLs) != 0 {
		t.Fatalf("expected no crawler task when dl-failed is reset, got %d", len(crawler.createdURLs))
	}
	if len(client.updateCalls) == 0 {
		t.Fatalf("expected update calls")
	}
	last := client.updateCalls[len(client.updateCalls)-1]
	if status := last.fields[feishusdk.DefaultTaskFields.Status]; status != feishusdk.StatusFailed {
		t.Fatalf("expected status %q, got %#v", feishusdk.StatusFailed, status)
	}
	if logs, ok := last.fields[feishusdk.DefaultTaskFields.Logs].(string); !ok || strings.TrimSpace(logs) != "[]" {
		t.Fatalf("expected logs to be reset, got %#v", last.fields[feishusdk.DefaultTaskFields.Logs])
	}
}

func TestSingleURLWorkerSendsGroupSummaryWhenAllSuccess(t *testing.T) {
	// Legacy group summary + Webhook column updates have been removed.
	// This test now verifies that SingleURLWorker completes tasks without
	// triggering any crawler summary calls or webhook field mutations.
	groupID := buildSingleURLGroupID("com.smile.gifmaker", "B001", "U001")
	client := &singleURLTestClient{
		rows: map[string][]feishusdk.TaskRow{
			feishusdk.StatusDownloaderQueued: {
				{
					TaskID:  20,
					Scene:   SceneSingleURLCapture,
					Status:  feishusdk.StatusDownloaderQueued,
					Params:  "capture",
					BookID:  "B001",
					UserID:  "U001",
					GroupID: groupID,
					Logs:    encodeSingleURLMetadata(singleURLMetadata{Attempts: []singleURLAttempt{{TaskID: "job-sum-1"}}}),
				},
				{
					TaskID:  21,
					Scene:   SceneSingleURLCapture,
					Status:  feishusdk.StatusDownloaderQueued,
					Params:  "capture",
					BookID:  "B001",
					UserID:  "U001",
					GroupID: groupID,
					Logs:    encodeSingleURLMetadata(singleURLMetadata{Attempts: []singleURLAttempt{{TaskID: "job-sum-2"}}}),
				},
			},
		},
		groupRows: map[string][]feishusdk.TaskRow{
			groupID: {
				{TaskID: 20, Scene: SceneSingleURLCapture, Status: feishusdk.StatusDownloaderQueued, Webhook: feishusdk.WebhookPending, BookID: "B001", UserID: "U001", GroupID: groupID, Params: "drama-A"},
				{TaskID: 21, Scene: SceneSingleURLCapture, Status: feishusdk.StatusDownloaderQueued, Webhook: feishusdk.WebhookPending, BookID: "B001", UserID: "U001", GroupID: groupID, Params: "drama-A"},
			},
		},
	}
	crawler := &stubCrawlerClient{
		statuses: map[string]*crawlerTaskStatus{
			"job-sum-1": {TaskID: "job-sum-1", Status: "COMPLETED", VID: "vid-1"},
			"job-sum-2": {TaskID: "job-sum-2", Status: "COMPLETED", VID: "vid-2"},
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
}

func TestSingleURLWorkerSkipsGroupSummaryWhenNotAllSuccess(t *testing.T) {
	// Group summary is no longer emitted by SingleURLWorker. This test just
	// ensures that processing does not error when group tasks are mixed.
	groupID := buildSingleURLGroupID("com.smile.gifmaker", "B010", "U010")
	client := &singleURLTestClient{
		rows: map[string][]feishusdk.TaskRow{
			feishusdk.StatusDownloaderQueued: {
				{
					TaskID:  30,
					Scene:   SceneSingleURLCapture,
					Status:  feishusdk.StatusDownloaderQueued,
					Params:  "capture",
					BookID:  "B010",
					UserID:  "U010",
					GroupID: groupID,
					Logs:    encodeSingleURLMetadata(singleURLMetadata{Attempts: []singleURLAttempt{{TaskID: "job-mixed"}}}),
				},
			},
		},
		groupRows: map[string][]feishusdk.TaskRow{
			groupID: {
				{TaskID: 30, Scene: SceneSingleURLCapture, Status: feishusdk.StatusDownloaderQueued, Webhook: feishusdk.WebhookPending, BookID: "B010", UserID: "U010", GroupID: groupID, Params: "drama-B"},
				{TaskID: 31, Scene: SceneSingleURLCapture, Status: feishusdk.StatusDownloaderProcessing, Webhook: feishusdk.WebhookPending, BookID: "B010", UserID: "U010", GroupID: groupID, Params: "drama-B"},
			},
		},
	}
	crawler := &stubCrawlerClient{
		statuses: map[string]*crawlerTaskStatus{
			"job-mixed": {TaskID: "job-mixed", Status: "COMPLETED", VID: "vid-mixed"},
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
}

func TestSingleURLWorkerRetriesFailedTaskWithExistingTaskID(t *testing.T) {
	meta := singleURLMetadata{Attempts: []singleURLAttempt{{TaskID: "task-old", Error: "boom"}}}
	client := &singleURLTestClient{}
	// Seed the initial failed task row with metadata in Logs.
	client.rows = map[string][]feishusdk.TaskRow{
		feishusdk.StatusFailed: {
			{
				TaskID: 40,
				Scene:  SceneSingleURLCapture,
				Status: feishusdk.StatusFailed,
				Params: "capture",
				BookID: "B040",
				UserID: "U040",
				URL:    "https://example.com/retry",
				Logs:   encodeSingleURLMetadata(meta),
			},
		},
	}
	crawler := &stubCrawlerClient{createTaskID: "task-new"}
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
		t.Fatalf("expected retry to create crawler task, got %d", len(crawler.createdURLs))
	}
	var encoded string
	for _, call := range client.updateCalls {
		if val, ok := call.fields[feishusdk.DefaultTaskFields.Logs]; ok {
			if s, ok := val.(string); ok {
				encoded = s
			}
		}
	}
	if encoded == "" {
		t.Fatalf("expected extra to be updated")
	}
	decoded := decodeSingleURLMetadata(encoded)
	if len(decoded.Attempts) != 2 {
		t.Fatalf("expected 2 attempts recorded, got %d", len(decoded.Attempts))
	}
	if decoded.Attempts[0].TaskID != "task-old" || decoded.Attempts[1].TaskID != "task-new" {
		t.Fatalf("unexpected task history: %#v", decoded.Attempts)
	}
}

func TestSingleURLWorkerIgnoresAttemptCap(t *testing.T) {
	meta := singleURLMetadata{Attempts: []singleURLAttempt{{TaskID: "job-1"}, {TaskID: "job-2"}, {TaskID: "job-3"}}}
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
					Logs:   encodeSingleURLMetadata(meta),
				},
			},
		},
	}
	crawler := &stubCrawlerClient{createTaskID: "job-4"}
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
	if len(crawler.createdURLs) != 1 {
		t.Fatalf("expected new crawler job despite attempts, got %d", len(crawler.createdURLs))
	}
}

func TestSingleURLReadyWorkerFetchesYesterdayTasks(t *testing.T) {
	client := &singleURLTestClient{
		rowsByDatePreset: map[string]map[string][]feishusdk.TaskRow{
			taskagent.TaskDateYesterday: {
				feishusdk.StatusReady: {
					{
						TaskID: 21,
						Scene:  SceneSingleURLCapture,
						Status: feishusdk.StatusReady,
						Params: "capture",
						BookID: "B021",
						UserID: "U021",
						App:    "kuaishou",
						URL:    "https://example.com/share/21",
						Extra:  `{"cdn_url":"https://cdn.example/21.m3u8"}`,
					},
				},
			},
		},
	}
	crawler := &stubCrawlerClient{createTaskID: "task-yesterday"}
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
	worker.newTaskStatuses = []string{feishusdk.StatusReady}
	worker.datePresets = []string{taskagent.TaskDateToday, taskagent.TaskDateYesterday}

	if err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("process once: %v", err)
	}
	if len(client.updateCalls) == 0 {
		t.Fatalf("expected update calls")
	}
	last := client.updateCalls[len(client.updateCalls)-1]
	if status := last.fields[feishusdk.DefaultTaskFields.Status]; status != feishusdk.StatusDownloaderQueued {
		t.Fatalf("expected status %q, got %#v", feishusdk.StatusDownloaderQueued, status)
	}
}

func TestSingleURLReadyWorkerReconcilesWhenLogsHasCrawlerTaskID(t *testing.T) {
	client := &singleURLTestClient{
		rows: map[string][]feishusdk.TaskRow{
			feishusdk.StatusReady: {
				{
					TaskID: 31,
					Scene:  SceneSingleURLCapture,
					Status: feishusdk.StatusReady,
					Params: "capture",
					BookID: "B031",
					UserID: "U031",
					App:    "kuaishou",
					URL:    "https://example.com/share/31",
					Extra:  `{"cdn_url":"https://cdn.example/31.m3u8"}`,
					Logs:   `[{"task_id":"task-31","status":"dl-queued"}]`,
				},
			},
		},
	}
	crawler := &stubCrawlerClient{
		statuses: map[string]*crawlerTaskStatus{
			"task-31": {Status: "WAITING"},
		},
	}
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
	worker.newTaskStatuses = []string{feishusdk.StatusReady}
	worker.activeTaskStatuses = nil

	if err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("process once: %v", err)
	}
	found := false
	for _, call := range client.updateCalls {
		if call.taskID != 31 {
			continue
		}
		if v, ok := call.fields[feishusdk.DefaultTaskFields.Status]; ok && v == feishusdk.StatusDownloaderQueued {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected task to be marked %q", feishusdk.StatusDownloaderQueued)
	}
	if len(crawler.queriedTaskID) == 0 || crawler.queriedTaskID[0] != "task-31" {
		t.Fatalf("expected crawler task to be queried, got %#v", crawler.queriedTaskID)
	}
}

func TestSingleURLWorkerConcurrencySerialFeishuUpdates(t *testing.T) {
	client := &singleURLConcurrencyClient{
		base: &singleURLTestClient{
			rows: map[string][]feishusdk.TaskRow{
				feishusdk.StatusPending: {
					{TaskID: 1, Scene: SceneSingleURLCapture, Status: feishusdk.StatusPending, BookID: "B1", UserID: "U1", App: "kuaishou", URL: "https://example.com/1"},
					{TaskID: 2, Scene: SceneSingleURLCapture, Status: feishusdk.StatusPending, BookID: "B2", UserID: "U2", App: "kuaishou", URL: "https://example.com/2"},
					{TaskID: 3, Scene: SceneSingleURLCapture, Status: feishusdk.StatusPending, BookID: "B3", UserID: "U3", App: "kuaishou", URL: "https://example.com/3"},
					{TaskID: 4, Scene: SceneSingleURLCapture, Status: feishusdk.StatusPending, BookID: "B4", UserID: "U4", App: "kuaishou", URL: "https://example.com/4"},
					{TaskID: 5, Scene: SceneSingleURLCapture, Status: feishusdk.StatusPending, BookID: "B5", UserID: "U5", App: "kuaishou", URL: "https://example.com/5"},
					{TaskID: 6, Scene: SceneSingleURLCapture, Status: feishusdk.StatusPending, BookID: "B6", UserID: "U6", App: "kuaishou", URL: "https://example.com/6"},
					{TaskID: 7, Scene: SceneSingleURLCapture, Status: feishusdk.StatusPending, BookID: "B7", UserID: "U7", App: "kuaishou", URL: "https://example.com/7"},
					{TaskID: 8, Scene: SceneSingleURLCapture, Status: feishusdk.StatusPending, BookID: "B8", UserID: "U8", App: "kuaishou", URL: "https://example.com/8"},
				},
			},
		},
		updateDelay: 30 * time.Millisecond,
	}
	crawler := &concurrencyCrawlerClient{
		delay: 50 * time.Millisecond,
	}

	worker, err := NewSingleURLWorker(SingleURLWorkerConfig{
		Client:        client,
		CrawlerClient: crawler,
		BitableURL:    "https://bitable.example",
		Limit:         8,
		PollInterval:  time.Second,
		Concurrency:   4,
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	if err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("process once: %v", err)
	}
	if got := crawler.maxConcurrentCreate.Load(); got <= 1 {
		t.Fatalf("expected concurrent create tasks, max=%d", got)
	}
	if got := client.maxConcurrentUpdate.Load(); got != 1 {
		t.Fatalf("expected serial feishu updates, max=%d", got)
	}
}

func TestSingleURLWorkerPollsProcessingTasksBeforeQueued(t *testing.T) {
	client := &singleURLTestClient{
		rows: map[string][]feishusdk.TaskRow{
			feishusdk.StatusDownloaderQueued: {
				{TaskID: 100, Scene: SceneSingleURLCapture, Status: feishusdk.StatusDownloaderQueued, BookID: "B100", UserID: "U100", App: "kuaishou", URL: "https://example.com/q1", Logs: `[{"task_id":"queued-1"}]`},
				{TaskID: 101, Scene: SceneSingleURLCapture, Status: feishusdk.StatusDownloaderQueued, BookID: "B101", UserID: "U101", App: "kuaishou", URL: "https://example.com/q2", Logs: `[{"task_id":"queued-2"}]`},
				{TaskID: 102, Scene: SceneSingleURLCapture, Status: feishusdk.StatusDownloaderQueued, BookID: "B102", UserID: "U102", App: "kuaishou", URL: "https://example.com/q3", Logs: `[{"task_id":"queued-3"}]`},
				{TaskID: 103, Scene: SceneSingleURLCapture, Status: feishusdk.StatusDownloaderQueued, BookID: "B103", UserID: "U103", App: "kuaishou", URL: "https://example.com/q4", Logs: `[{"task_id":"queued-4"}]`},
				{TaskID: 104, Scene: SceneSingleURLCapture, Status: feishusdk.StatusDownloaderQueued, BookID: "B104", UserID: "U104", App: "kuaishou", URL: "https://example.com/q5", Logs: `[{"task_id":"queued-5"}]`},
				{TaskID: 105, Scene: SceneSingleURLCapture, Status: feishusdk.StatusDownloaderQueued, BookID: "B105", UserID: "U105", App: "kuaishou", URL: "https://example.com/q6", Logs: `[{"task_id":"queued-6"}]`},
			},
			feishusdk.StatusDownloaderProcessing: {
				{TaskID: 200, Scene: SceneSingleURLCapture, Status: feishusdk.StatusDownloaderProcessing, BookID: "B200", UserID: "U200", App: "kuaishou", URL: "https://example.com/p1", Logs: `[{"task_id":"processing-1"}]`},
			},
		},
	}
	crawler := &stubCrawlerClient{
		statuses: map[string]*crawlerTaskStatus{
			"queued-1":     {TaskID: "queued-1", Status: "WAITING"},
			"queued-2":     {TaskID: "queued-2", Status: "WAITING"},
			"queued-3":     {TaskID: "queued-3", Status: "WAITING"},
			"queued-4":     {TaskID: "queued-4", Status: "WAITING"},
			"queued-5":     {TaskID: "queued-5", Status: "WAITING"},
			"queued-6":     {TaskID: "queued-6", Status: "WAITING"},
			"processing-1": {TaskID: "processing-1", Status: "PROCESSING"},
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
	// No pending tasks in the stub client; only active tasks will be polled.
	if err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("process once: %v", err)
	}
	found := false
	for _, id := range crawler.queriedTaskID {
		if id == "processing-1" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected processing task to be polled; queried=%v", crawler.queriedTaskID)
	}
}

func TestSingleURLWorkerPollsActiveTasksBeforeDispatchingNewTasks(t *testing.T) {
	client := &singleURLTestClient{
		rows: map[string][]feishusdk.TaskRow{
			feishusdk.StatusPending: {
				{TaskID: 1, Scene: SceneSingleURLCapture, Status: feishusdk.StatusPending, BookID: "B001", UserID: "U001", App: "kuaishou", URL: "https://example.com/video"},
			},
			feishusdk.StatusDownloaderProcessing: {
				{TaskID: 2, Scene: SceneSingleURLCapture, Status: feishusdk.StatusDownloaderProcessing, BookID: "B002", UserID: "U002", App: "kuaishou", URL: "https://example.com/processing", Logs: `[{"task_id":"processing-1"}]`},
			},
		},
	}
	crawler := &orderedCrawlerClient{
		statuses: map[string]*crawlerTaskStatus{
			"processing-1": {TaskID: "processing-1", Status: "PROCESSING"},
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
	if len(crawler.events) == 0 {
		t.Fatalf("expected crawler calls, got none")
	}
	if crawler.events[0] != "get:processing-1" {
		t.Fatalf("expected active polling first, got events=%v", crawler.events)
	}
}

func TestSingleURLWorkerPollsActiveTasksWithoutDatetimeFilter(t *testing.T) {
	client := &singleURLTestClient{
		rows: map[string][]feishusdk.TaskRow{
			feishusdk.StatusDownloaderProcessing: {
				{TaskID: 2, Scene: SceneSingleURLCapture, Status: feishusdk.StatusDownloaderProcessing, BookID: "B002", UserID: "U002", App: "kuaishou", URL: "https://example.com/processing", Logs: `[{"task_id":"processing-1"}]`},
			},
		},
	}
	crawler := &stubCrawlerClient{
		statuses: map[string]*crawlerTaskStatus{
			"processing-1": {TaskID: "processing-1", Status: "PROCESSING"},
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
	if !client.sawActiveWithoutDatetime {
		t.Fatalf("expected active query without Datetime filter")
	}
	if !client.sawIgnoreView {
		t.Fatalf("expected IgnoreView for singleurl fetch")
	}
}

func TestSingleURLWorkerRotatesActivePagesAcrossStatuses(t *testing.T) {
	client := newPagedSingleURLClient(t, map[string]pagedSingleURLStatus{
		feishusdk.StatusDownloaderProcessing: {
			page1: []feishusdk.TaskRow{
				{TaskID: 1, Scene: SceneSingleURLCapture, Status: feishusdk.StatusDownloaderProcessing, BookID: "B001", UserID: "U001", App: "kuaishou", URL: "https://example.com/processing-1", Logs: `[{"task_id":"processing-1"}]`},
			},
			page2: []feishusdk.TaskRow{
				{TaskID: 11, Scene: SceneSingleURLCapture, Status: feishusdk.StatusDownloaderProcessing, BookID: "B011", UserID: "U011", App: "kuaishou", URL: "https://example.com/processing-2", Logs: `[{"task_id":"processing-2"}]`},
			},
			nextToken: "p2",
		},
		feishusdk.StatusDownloaderQueued: {
			page1: []feishusdk.TaskRow{
				{TaskID: 2, Scene: SceneSingleURLCapture, Status: feishusdk.StatusDownloaderQueued, BookID: "B002", UserID: "U002", App: "kuaishou", URL: "https://example.com/queued-1", Logs: `[{"task_id":"queued-1"}]`},
			},
			page2: []feishusdk.TaskRow{
				{TaskID: 22, Scene: SceneSingleURLCapture, Status: feishusdk.StatusDownloaderQueued, BookID: "B022", UserID: "U022", App: "kuaishou", URL: "https://example.com/queued-2", Logs: `[{"task_id":"queued-2"}]`},
			},
			nextToken: "q2",
		},
	})
	crawler := &stubCrawlerClient{
		statuses: map[string]*crawlerTaskStatus{
			"processing-1": {TaskID: "processing-1", Status: "PROCESSING"},
			"processing-2": {TaskID: "processing-2", Status: "PROCESSING"},
			"queued-1":     {TaskID: "queued-1", Status: "WAITING"},
			"queued-2":     {TaskID: "queued-2", Status: "WAITING"},
		},
	}
	worker, err := NewSingleURLWorker(SingleURLWorkerConfig{
		Client:        client,
		CrawlerClient: crawler,
		BitableURL:    "https://bitable.example",
		Limit:         10,
		PollInterval:  time.Second,
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}

	if err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("process once: %v", err)
	}
	if got := client.pageTokens(feishusdk.StatusDownloaderProcessing); len(got) < 2 || got[0] != "" || got[1] != "p2" {
		t.Fatalf("expected dl-processing to scan first two pages in one pass, got %v", got)
	}
	if got := client.pageTokens(feishusdk.StatusDownloaderQueued); len(got) < 2 || got[0] != "" || got[1] != "q2" {
		t.Fatalf("expected dl-queued to scan first two pages in one pass, got %v", got)
	}

	if err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("process once: %v", err)
	}
	if got := client.pageTokens(feishusdk.StatusDownloaderProcessing); len(got) < 4 || got[2] != "" {
		t.Fatalf("expected dl-processing to restart from first page on next pass, got %v", got)
	}
	if got := client.pageTokens(feishusdk.StatusDownloaderQueued); len(got) < 4 || got[2] != "" {
		t.Fatalf("expected dl-queued to restart from first page on next pass, got %v", got)
	}
}

type alwaysWaitingCrawlerClient struct{}

func (alwaysWaitingCrawlerClient) CreateTask(context.Context, string, map[string]string) (string, error) {
	return "task-default", nil
}

func (alwaysWaitingCrawlerClient) GetTask(context.Context, string) (*crawlerTaskStatus, error) {
	return &crawlerTaskStatus{Status: "WAITING"}, nil
}

func (alwaysWaitingCrawlerClient) SendTaskSummary(context.Context, TaskSummaryPayload) error {
	return nil
}

func TestSingleURLWorkerActiveFetchScansMultiplePagesPerPass(t *testing.T) {
	makeRows := func(taskStatus, prefix string, base, count int) []feishusdk.TaskRow {
		rows := make([]feishusdk.TaskRow, 0, count)
		for i := 0; i < count; i++ {
			rows = append(rows, feishusdk.TaskRow{
				TaskID: int64(base + i),
				Scene:  SceneSingleURLCapture,
				Status: taskStatus,
				BookID: "B",
				UserID: "U",
				App:    "kuaishou",
				URL:    "https://example.com/" + prefix,
				Logs:   fmt.Sprintf(`[{"task_id":"%s-%d"}]`, prefix, i),
			})
		}
		return rows
	}

	client := newPagedSingleURLClient(t, map[string]pagedSingleURLStatus{
		feishusdk.StatusDownloaderProcessing: {
			page1:     makeRows(feishusdk.StatusDownloaderProcessing, "processing", 100000, 120),
			page2:     makeRows(feishusdk.StatusDownloaderProcessing, "processing2", 200000, 120),
			nextToken: "p2",
		},
		feishusdk.StatusDownloaderQueued: {
			page1:     makeRows(feishusdk.StatusDownloaderQueued, "queued", 300000, 120),
			page2:     makeRows(feishusdk.StatusDownloaderQueued, "queued2", 400000, 120),
			nextToken: "q2",
		},
	})
	worker, err := NewSingleURLWorker(SingleURLWorkerConfig{
		Client:        client,
		CrawlerClient: alwaysWaitingCrawlerClient{},
		BitableURL:    "https://bitable.example",
		Limit:         100,
		PollInterval:  time.Second,
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	worker.newTaskStatuses = nil

	if err := worker.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("process once: %v", err)
	}
	if got := client.pageTokens(feishusdk.StatusDownloaderProcessing); len(got) < 2 {
		t.Fatalf("expected dl-processing to scan multiple pages in one pass, got %v", got)
	}
	if got := client.pageTokens(feishusdk.StatusDownloaderQueued); len(got) < 1 {
		t.Fatalf("expected dl-queued to be scanned after dl-processing, got %v", got)
	}
}

type singleURLTestClient struct {
	rows                     map[string][]feishusdk.TaskRow
	rowsByDatePreset         map[string]map[string][]feishusdk.TaskRow
	groupRows                map[string][]feishusdk.TaskRow
	updateCalls              []singleURLUpdateCall
	sawActiveWithoutDatetime bool
	sawIgnoreView            bool
	pageCalls                map[string][]string
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
	if opts != nil && opts.IgnoreView {
		c.sawIgnoreView = true
	}
	if opts != nil {
		if c.pageCalls == nil {
			c.pageCalls = make(map[string][]string)
		}
		c.pageCalls[status] = append(c.pageCalls[status], strings.TrimSpace(opts.PageToken))
	}
	datePreset := suExtractConditionValue(opts.Filter, feishusdk.DefaultTaskFields.Datetime)
	if (status == feishusdk.StatusDownloaderProcessing || status == feishusdk.StatusDownloaderQueued) && strings.TrimSpace(datePreset) == "" {
		c.sawActiveWithoutDatetime = true
	}
	var rows []feishusdk.TaskRow
	if len(c.rowsByDatePreset) > 0 {
		rows = cloneTaskRows(c.rowsByDatePreset[datePreset][status])
	} else {
		rows = cloneTaskRows(c.rows[status])
	}
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
				case feishusdk.DefaultTaskFields.Logs:
					if str, ok := value.(string); ok {
						rows[idx].Logs = str
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
	createTaskID    string
	createErr       error
	statuses        map[string]*crawlerTaskStatus
	createdURLs     []string
	createdMeta     []map[string]string
	summaryPayloads []TaskSummaryPayload
	summaryErr      error
	queriedTaskID   []string
}

type pagedSingleURLStatus struct {
	page1     []feishusdk.TaskRow
	page2     []feishusdk.TaskRow
	nextToken string
}

type pagedSingleURLClient struct {
	t *testing.T

	statuses map[string]pagedSingleURLStatus

	callsMu sync.Mutex
	calls   map[string][]string
}

func newPagedSingleURLClient(t *testing.T, statuses map[string]pagedSingleURLStatus) *pagedSingleURLClient {
	return &pagedSingleURLClient{
		t:        t,
		statuses: statuses,
		calls:    make(map[string][]string),
	}
}

func (c *pagedSingleURLClient) pageTokens(status string) []string {
	c.callsMu.Lock()
	defer c.callsMu.Unlock()
	out := make([]string, len(c.calls[status]))
	copy(out, c.calls[status])
	return out
}

func (c *pagedSingleURLClient) FetchTaskTableWithOptions(_ context.Context, _ string, _ *feishusdk.TaskFields, opts *feishusdk.TaskQueryOptions) (*feishusdk.TaskTable, error) {
	status := suExtractConditionValue(opts.Filter, feishusdk.DefaultTaskFields.Status)
	scene := suExtractConditionValue(opts.Filter, feishusdk.DefaultTaskFields.Scene)
	if scene != SceneSingleURLCapture {
		return &feishusdk.TaskTable{Fields: feishusdk.DefaultTaskFields}, nil
	}
	c.callsMu.Lock()
	c.calls[status] = append(c.calls[status], strings.TrimSpace(opts.PageToken))
	c.callsMu.Unlock()

	cfg, ok := c.statuses[status]
	if !ok {
		return &feishusdk.TaskTable{Fields: feishusdk.DefaultTaskFields}, nil
	}
	limit := 0
	if opts != nil {
		limit = opts.Limit
	}
	token := strings.TrimSpace(opts.PageToken)
	if token == "" {
		rows := cloneTaskRows(cfg.page1)
		if limit > 0 && len(rows) > limit {
			rows = rows[:limit]
		}
		table := &feishusdk.TaskTable{Fields: feishusdk.DefaultTaskFields, Rows: rows, HasMore: true, NextPageToken: cfg.nextToken, Pages: 1}
		return table, nil
	}
	if token == strings.TrimSpace(cfg.nextToken) {
		rows := cloneTaskRows(cfg.page2)
		if limit > 0 && len(rows) > limit {
			rows = rows[:limit]
		}
		table := &feishusdk.TaskTable{Fields: feishusdk.DefaultTaskFields, Rows: rows, HasMore: false, NextPageToken: "", Pages: 1}
		return table, nil
	}
	return &feishusdk.TaskTable{Fields: feishusdk.DefaultTaskFields}, nil
}

func (c *pagedSingleURLClient) UpdateTaskStatus(context.Context, *feishusdk.TaskTable, int64, string) error {
	return nil
}

func (c *pagedSingleURLClient) UpdateTaskFields(context.Context, *feishusdk.TaskTable, int64, map[string]any) error {
	return nil
}

func (c *stubCrawlerClient) CreateTask(_ context.Context, url string, meta map[string]string) (string, error) {
	c.createdURLs = append(c.createdURLs, url)
	copyMeta := make(map[string]string, len(meta))
	for k, v := range meta {
		copyMeta[k] = v
	}
	c.createdMeta = append(c.createdMeta, copyMeta)
	if c.createErr != nil {
		return "", c.createErr
	}
	if c.createTaskID == "" {
		c.createTaskID = "task-default"
	}
	return c.createTaskID, nil
}

func (c *stubCrawlerClient) GetTask(_ context.Context, taskID string) (*crawlerTaskStatus, error) {
	c.queriedTaskID = append(c.queriedTaskID, taskID)
	if status, ok := c.statuses[taskID]; ok {
		return status, nil
	}
	return nil, errCrawlerTaskNotFound
}

func (c *stubCrawlerClient) SendTaskSummary(_ context.Context, payload TaskSummaryPayload) error {
	c.summaryPayloads = append(c.summaryPayloads, payload)
	return c.summaryErr
}

type singleURLConcurrencyClient struct {
	base        *singleURLTestClient
	mu          sync.Mutex
	inUpdate    atomic.Int32
	updateDelay time.Duration

	maxConcurrentUpdate atomic.Int32
}

func (c *singleURLConcurrencyClient) FetchTaskTableWithOptions(ctx context.Context, rawURL string, override *feishusdk.TaskFields, opts *feishusdk.TaskQueryOptions) (*feishusdk.TaskTable, error) {
	return c.base.FetchTaskTableWithOptions(ctx, rawURL, override, opts)
}

func (c *singleURLConcurrencyClient) UpdateTaskStatus(ctx context.Context, table *feishusdk.TaskTable, taskID int64, newStatus string) error {
	return c.base.UpdateTaskStatus(ctx, table, taskID, newStatus)
}

func (c *singleURLConcurrencyClient) UpdateTaskFields(ctx context.Context, table *feishusdk.TaskTable, taskID int64, fields map[string]any) error {
	cur := c.inUpdate.Add(1)
	for {
		prev := c.maxConcurrentUpdate.Load()
		if cur <= prev || c.maxConcurrentUpdate.CompareAndSwap(prev, cur) {
			break
		}
	}
	if c.updateDelay > 0 {
		time.Sleep(c.updateDelay)
	}
	c.mu.Lock()
	err := c.base.UpdateTaskFields(ctx, table, taskID, fields)
	c.mu.Unlock()
	c.inUpdate.Add(-1)
	return err
}

type concurrencyCrawlerClient struct {
	delay time.Duration

	nextID              atomic.Int64
	inCreate            atomic.Int32
	maxConcurrentCreate atomic.Int32
}

func (c *concurrencyCrawlerClient) CreateTask(ctx context.Context, url string, meta map[string]string) (string, error) {
	cur := c.inCreate.Add(1)
	for {
		prev := c.maxConcurrentCreate.Load()
		if cur <= prev || c.maxConcurrentCreate.CompareAndSwap(prev, cur) {
			break
		}
	}
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	c.inCreate.Add(-1)
	id := c.nextID.Add(1)
	return fmt.Sprintf("task-%d", id), nil
}

func (c *concurrencyCrawlerClient) GetTask(context.Context, string) (*crawlerTaskStatus, error) {
	return nil, errCrawlerTaskNotFound
}

func (c *concurrencyCrawlerClient) SendTaskSummary(context.Context, TaskSummaryPayload) error {
	return nil
}

type orderedCrawlerClient struct {
	statuses map[string]*crawlerTaskStatus
	events   []string
}

func (c *orderedCrawlerClient) CreateTask(_ context.Context, _ string, _ map[string]string) (string, error) {
	c.events = append(c.events, "create")
	return "task-1", nil
}

func (c *orderedCrawlerClient) GetTask(_ context.Context, taskID string) (*crawlerTaskStatus, error) {
	c.events = append(c.events, "get:"+strings.TrimSpace(taskID))
	if status, ok := c.statuses[strings.TrimSpace(taskID)]; ok {
		return status, nil
	}
	return nil, errCrawlerTaskNotFound
}

func (c *orderedCrawlerClient) SendTaskSummary(context.Context, TaskSummaryPayload) error {
	return nil
}
