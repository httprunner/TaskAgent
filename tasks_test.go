package pool

import (
	"context"
	"strings"
	"testing"
	"time"

	feishusvc "github.com/httprunner/TaskAgent/pkg/feishu"
)

func TestFetchFeishuTasksWithStrategyFiltersInvalidTasks(t *testing.T) {
	ctx := context.Background()
	loc := time.FixedZone("UTC+8", 8*3600)

	valid := timePtr(time.Date(2025, 11, 9, 9, 30, 0, 0, loc))
	future := timePtr(time.Date(2025, 11, 9, 21, 0, 0, 0, loc))
	past := timePtr(time.Date(2025, 11, 8, 23, 0, 0, 0, loc))

	client := &stubTargetClient{
		tables: []*feishusvc.TaskTable{
			{
				Rows: []feishusvc.TaskRow{
					{TaskID: 1, Params: "foo", App: "com.app", Datetime: valid},
					{TaskID: 2, Params: "bar", App: "com.app", Datetime: future},
					{TaskID: 3, Params: "baz", App: "com.app", Datetime: past},
					{TaskID: 4, Params: "qux", App: "com.app"},
				},
			},
		},
	}

	tasks, err := fetchFeishuTasksWithStrategy(ctx, client, "https://example.com/bitable/abc", feishusvc.DefaultTaskFields, "com.app", []string{""}, 5, "")
	if err != nil {
		t.Fatalf("fetchFeishuTasksWithStrategy returned error: %v", err)
	}
	expectedIDs := []int64{1, 2, 3, 4}
	if len(tasks) != len(expectedIDs) {
		t.Fatalf("expected %d tasks, got %d", len(expectedIDs), len(tasks))
	}
	for i, id := range expectedIDs {
		if tasks[i].TaskID != id {
			t.Fatalf("unexpected task order: got %d at position %d, want %d", tasks[i].TaskID, i, id)
		}
	}
}

type stubTargetClient struct {
	tables []*feishusvc.TaskTable
	index  int
}

func (s *stubTargetClient) FetchTaskTableWithOptions(ctx context.Context, rawURL string, override *feishusvc.TaskFields, opts *feishusvc.TaskQueryOptions) (*feishusvc.TaskTable, error) {
	if s.index >= len(s.tables) {
		return &feishusvc.TaskTable{}, nil
	}
	tbl := s.tables[s.index]
	s.index++
	return tbl, nil
}

func (s *stubTargetClient) UpdateTaskStatus(ctx context.Context, table *feishusvc.TaskTable, taskID int64, newStatus string) error {
	return nil
}

func (s *stubTargetClient) UpdateTaskFields(ctx context.Context, table *feishusvc.TaskTable, taskID int64, fields map[string]any) error {
	return nil
}

func timePtr(t time.Time) *time.Time {
	v := t
	return &v
}

func TestFetchTodayPendingFeishuTasksSceneStatusPriorityStopsAfterLimit(t *testing.T) {
	ctx := context.Background()

	client := &sceneStatusTargetClient{
		rows: map[string][]feishusvc.TaskRow{
			"个人页搜索|pending|with": {
				{TaskID: 11, Params: "A", App: "com.app", Scene: "个人页搜索"},
				{TaskID: 12, Params: "B", App: "com.app", Scene: "个人页搜索"},
				{TaskID: 13, Params: "C", App: "com.app", Scene: "个人页搜索"},
			},
			"个人页搜索|failed|with": {
				{TaskID: 14, Params: "D", App: "com.app", Scene: "个人页搜索"},
			},
			"综合页搜索|pending|with": {
				{TaskID: 21, Params: "E", App: "com.app", Scene: "综合页搜索"},
			},
		},
	}

	tasks, err := fetchTodayPendingFeishuTasks(ctx, client, "https://example.com/bitable/foo", "com.app", 3)
	if err != nil {
		t.Fatalf("fetchTodayPendingFeishuTasks returned error: %v", err)
	}
	got := collectTaskIDs(tasks)
	want := []int64{11, 12, 21}
	if !equalIDs(got, want) {
		t.Fatalf("unexpected ids: got %v, want %v", got, want)
	}
}

type sceneStatusTargetClient struct {
	rows map[string][]feishusvc.TaskRow
}

func (c *sceneStatusTargetClient) FetchTaskTableWithOptions(ctx context.Context, rawURL string, override *feishusvc.TaskFields, opts *feishusvc.TaskQueryOptions) (*feishusvc.TaskTable, error) {
	scene := extractConditionValue(opts.Filter, feishusvc.DefaultTaskFields.Scene)
	status := extractConditionValue(opts.Filter, feishusvc.DefaultTaskFields.Status)
	dt := "without"
	if filterHasDatetime(opts.Filter, feishusvc.DefaultTaskFields.Datetime) {
		dt = "with"
	}
	key := strings.TrimSpace(scene) + "|" + strings.TrimSpace(status) + "|" + dt
	rows := c.rows[key]
	clone := make([]feishusvc.TaskRow, len(rows))
	copy(clone, rows)
	return &feishusvc.TaskTable{
		Rows:   clone,
		Fields: feishusvc.DefaultTaskFields,
	}, nil
}

func (c *sceneStatusTargetClient) UpdateTaskStatus(ctx context.Context, table *feishusvc.TaskTable, taskID int64, newStatus string) error {
	return nil
}

func (c *sceneStatusTargetClient) UpdateTaskFields(ctx context.Context, table *feishusvc.TaskTable, taskID int64, fields map[string]any) error {
	return nil
}

func extractConditionValue(filter *feishusvc.FilterInfo, field string) string {
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

func filterHasDatetime(filter *feishusvc.FilterInfo, field string) bool {
	if filter == nil {
		return false
	}
	for _, cond := range filter.Conditions {
		if cond != nil && cond.FieldName != nil && *cond.FieldName == field {
			return true
		}
	}
	for _, child := range filter.Children {
		for _, cond := range child.Conditions {
			if cond != nil && cond.FieldName != nil && *cond.FieldName == field {
				return true
			}
		}
	}
	return false
}

func collectTaskIDs(tasks []*FeishuTask) []int64 {
	result := make([]int64, 0, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		result = append(result, task.TaskID)
	}
	return result
}

func equalIDs(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type recordingTargetClient struct {
	updates []map[string]any
}

func (r *recordingTargetClient) FetchTaskTableWithOptions(ctx context.Context, rawURL string, override *feishusvc.TaskFields, opts *feishusvc.TaskQueryOptions) (*feishusvc.TaskTable, error) {
	return nil, nil
}

func (r *recordingTargetClient) UpdateTaskStatus(ctx context.Context, table *feishusvc.TaskTable, taskID int64, newStatus string) error {
	return nil
}

func (r *recordingTargetClient) UpdateTaskFields(ctx context.Context, table *feishusvc.TaskTable, taskID int64, fields map[string]any) error {
	cp := make(map[string]any, len(fields))
	for k, v := range fields {
		cp[k] = v
	}
	r.updates = append(r.updates, cp)
	return nil
}

func TestUpdateFeishuTaskStatusesAssignsDispatchedDevice(t *testing.T) {
	client := &recordingTargetClient{}
	table := &feishusvc.TaskTable{
		Ref:    feishusvc.BitableRef{AppToken: "app", TableID: "tbl"},
		Fields: feishusvc.DefaultTaskFields,
	}
	task := &FeishuTask{
		TaskID: 1,
		source: &feishuTaskSource{
			client: client,
			table:  table,
		},
	}

	dispatchedAt := time.Date(2025, 11, 10, 10, 0, 0, 0, time.UTC)
	if err := updateFeishuTaskStatuses(context.Background(), []*FeishuTask{task}, "dispatched", "device-xyz", &taskStatusMeta{dispatchedAt: &dispatchedAt}); err != nil {
		t.Fatalf("updateFeishuTaskStatuses returned error: %v", err)
	}
	if len(client.updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(client.updates))
	}
	statusField := feishusvc.DefaultTaskFields.Status
	serialField := feishusvc.DefaultTaskFields.DispatchedDevice
	dispatchedField := feishusvc.DefaultTaskFields.DispatchedAt
	update := client.updates[0]
	if got := update[statusField]; got != "dispatched" {
		t.Fatalf("expected status field=%s got=%v", statusField, got)
	}
	if got := update[serialField]; got != "device-xyz" {
		t.Fatalf("expected device serial field=%s got=%v", serialField, got)
	}
	expectedMillis := dispatchedAt.UTC().UnixMilli()
	gotAny, ok := update[dispatchedField]
	if !ok {
		t.Fatalf("expected dispatched time field=%s to be set", dispatchedField)
	}
	gotMillis, ok := gotAny.(int64)
	if !ok || gotMillis != expectedMillis {
		t.Fatalf("expected dispatched time field=%s to be %d, got %v", dispatchedField, expectedMillis, gotAny)
	}
	if task.DispatchedAt == nil || !task.DispatchedAt.Equal(dispatchedAt) {
		t.Fatalf("task dispatched time not recorded: %#v", task.DispatchedAt)
	}
}

func TestUpdateFeishuTaskStatusesAssignsElapsedSeconds(t *testing.T) {
	client := &recordingTargetClient{}
	table := &feishusvc.TaskTable{
		Ref:    feishusvc.BitableRef{AppToken: "app", TableID: "tbl"},
		Fields: feishusvc.DefaultTaskFields,
	}
	dispatchedAt := time.Date(2025, 11, 10, 9, 0, 0, 0, time.UTC)
	task := &FeishuTask{
		TaskID:       2,
		DispatchedAt: &dispatchedAt,
		source: &feishuTaskSource{
			client: client,
			table:  table,
		},
	}
	completedAt := dispatchedAt.Add(95 * time.Second)
	if err := updateFeishuTaskStatuses(context.Background(), []*FeishuTask{task}, "success", "", &taskStatusMeta{completedAt: &completedAt}); err != nil {
		t.Fatalf("updateFeishuTaskStatuses returned error: %v", err)
	}
	if len(client.updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(client.updates))
	}
	elapsedField := feishusvc.DefaultTaskFields.ElapsedSeconds
	update := client.updates[0]
	if got := update[elapsedField]; got == nil {
		t.Fatalf("expected elapsed seconds field=%s to be set", elapsedField)
	}
	if task.ElapsedSeconds != 95 {
		t.Fatalf("expected task elapsed seconds to be 95, got %d", task.ElapsedSeconds)
	}
}

func TestUpdateStatusesSkipsDuplicateStatusWrites(t *testing.T) {
	client := &FeishuTaskClient{}
	recorder := &recordingTargetClient{}
	table := &feishusvc.TaskTable{Fields: feishusvc.DefaultTaskFields}
	source := &feishuTaskSource{client: recorder, table: table}
	tasks := []*Task{
		{Payload: &FeishuTask{TaskID: 11, Status: feishusvc.StatusSuccess, source: source}},
		{Payload: &FeishuTask{TaskID: 12, Status: feishusvc.StatusSuccess, source: source}},
	}
	updated, err := client.updateStatuses(context.Background(), tasks, feishusvc.StatusSuccess, "device-1")
	if err != nil {
		t.Fatalf("updateStatuses returned error: %v", err)
	}
	if len(recorder.updates) != 0 {
		t.Fatalf("expected no updates when desired status already set, got %d", len(recorder.updates))
	}
	for _, task := range updated {
		if task.Status != feishusvc.StatusSuccess {
			t.Fatalf("task %d status changed unexpectedly: %s", task.TaskID, task.Status)
		}
	}
}

func TestUpdateStatusesStillUpdatesPendingTasks(t *testing.T) {
	client := &FeishuTaskClient{}
	recorder := &recordingTargetClient{}
	table := &feishusvc.TaskTable{Fields: feishusvc.DefaultTaskFields}
	source := &feishuTaskSource{client: recorder, table: table}
	task := &FeishuTask{TaskID: 21, Status: feishusvc.StatusPending, source: source}
	updated, err := client.updateStatuses(context.Background(), []*Task{{Payload: task}}, feishusvc.StatusSuccess, "device-1")
	if err != nil {
		t.Fatalf("updateStatuses returned error: %v", err)
	}
	if len(updated) != 1 {
		t.Fatalf("expected 1 task in response, got %d", len(updated))
	}
	if len(recorder.updates) != 1 {
		t.Fatalf("expected 1 update for pending task, got %d", len(recorder.updates))
	}
	if task.Status != feishusvc.StatusSuccess {
		t.Fatalf("expected task status to update to success, got %s", task.Status)
	}
}
