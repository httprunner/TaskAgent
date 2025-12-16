package taskagent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/httprunner/TaskAgent/internal/feishusdk"
)

func TestFetchFeishuTasksWithStrategyFiltersInvalidTasks(t *testing.T) {
	ctx := context.Background()
	loc := time.FixedZone("UTC+8", 8*3600)

	valid := timePtr(time.Date(2025, 11, 9, 9, 30, 0, 0, loc))
	future := timePtr(time.Date(2025, 11, 9, 21, 0, 0, 0, loc))
	past := timePtr(time.Date(2025, 11, 8, 23, 0, 0, 0, loc))

	client := &stubTargetClient{
		tables: []*feishusdk.TaskTable{
			{
				Rows: []feishusdk.TaskRow{
					{TaskID: 1, Params: "foo", App: "com.app", Datetime: valid},
					{TaskID: 2, Params: "bar", App: "com.app", Datetime: future},
					{TaskID: 3, Params: "baz", App: "com.app", Datetime: past},
					{TaskID: 4, Params: "qux", App: "com.app"},
				},
			},
		},
	}

	tasks, err := FetchFeishuTasksWithStrategy(ctx, client, "https://example.com/bitable/abc", feishusdk.DefaultTaskFields, "com.app", []string{""}, 5, "")
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

func TestFetchFeishuTasksWithFilterAllowsBookOrURLOnlyRows(t *testing.T) {
	ctx := context.Background()
	client := &stubTargetClient{
		tables: []*feishusdk.TaskTable{
			{
				Fields: feishusdk.DefaultTaskFields,
				Rows: []feishusdk.TaskRow{
					{TaskID: 101, BookID: "book-only"},
					{TaskID: 102, URL: "https://example.com/item"},
					{TaskID: 103, UserID: "uid-only"},
					{TaskID: 104},
				},
			},
		},
	}
	tasks, err := FetchFeishuTasksWithFilter(ctx, client, "https://example.com/bitable/rows", nil, 10)
	if err != nil {
		t.Fatalf("fetchFeishuTasksWithFilter returned error: %v", err)
	}
	got := collectTaskIDs(tasks)
	want := []int64{101, 102, 103}
	if !equalIDs(got, want) {
		t.Fatalf("unexpected ids: got %v want %v", got, want)
	}
}

type stubTargetClient struct {
	tables []*feishusdk.TaskTable
	index  int
}

func (s *stubTargetClient) FetchTaskTableWithOptions(ctx context.Context, rawURL string, override *feishusdk.TaskFields, opts *feishusdk.TaskQueryOptions) (*feishusdk.TaskTable, error) {
	if s.index >= len(s.tables) {
		return &feishusdk.TaskTable{}, nil
	}
	tbl := s.tables[s.index]
	s.index++
	return tbl, nil
}

func (s *stubTargetClient) UpdateTaskStatus(ctx context.Context, table *feishusdk.TaskTable, taskID int64, newStatus string) error {
	return nil
}

func (s *stubTargetClient) UpdateTaskFields(ctx context.Context, table *feishusdk.TaskTable, taskID int64, fields map[string]any) error {
	return nil
}

func timePtr(t time.Time) *time.Time {
	v := t
	return &v
}

func TestFetchTodayPendingFeishuTasksSceneStatusPriorityStopsAfterLimit(t *testing.T) {
	ctx := context.Background()

	client := &sceneStatusTargetClient{
		rows: map[string][]feishusdk.TaskRow{
			"个人页搜索|pending|with": {
				{TaskID: 11, Params: "A", App: "com.app", Scene: SceneProfileSearch},
				{TaskID: 12, Params: "B", App: "com.app", Scene: SceneProfileSearch},
				{TaskID: 13, Params: "C", App: "com.app", Scene: SceneProfileSearch},
			},
			"个人页搜索|failed|with": {
				{TaskID: 14, Params: "D", App: "com.app", Scene: SceneProfileSearch},
			},
			"综合页搜索|pending|with": {
				{TaskID: 1, Params: "E", App: "com.app", Scene: SceneGeneralSearch},
			},
		},
	}

	tasks, err := fetchTodayPendingFeishuTasks(ctx, client, "https://example.com/bitable/foo", "com.app", 3, nil)
	if err != nil {
		t.Fatalf("fetchTodayPendingFeishuTasks returned error: %v", err)
	}
	got := collectTaskIDs(tasks)
	want := []int64{1, 11, 12}
	if !equalIDs(got, want) {
		t.Fatalf("unexpected ids: got %v, want %v", got, want)
	}
}

func TestFetchTodayPendingFeishuTasksRespectsAllowedScenes(t *testing.T) {
	ctx := context.Background()
	client := &sceneStatusTargetClient{
		rows: map[string][]feishusdk.TaskRow{
			"单个链接采集|pending|with": {
				{TaskID: 50, Params: "S1", App: "com.app", Scene: SceneSingleURLCapture},
				{TaskID: 51, Params: "S2", App: "com.app", Scene: SceneSingleURLCapture},
			},
			"综合页搜索|pending|with": {
				{TaskID: 60, Params: "E1", App: "com.app", Scene: SceneGeneralSearch},
			},
		},
	}
	allowed := map[string]struct{}{SceneGeneralSearch: {}}
	tasks, err := fetchTodayPendingFeishuTasks(ctx, client, "https://example.com/bitable/foo", "com.app", 2, allowed)
	if err != nil {
		t.Fatalf("fetchTodayPendingFeishuTasks returned error: %v", err)
	}
	got := collectTaskIDs(tasks)
	want := []int64{60}
	if !equalIDs(got, want) {
		t.Fatalf("unexpected ids: got %v want %v", got, want)
	}
}

func TestFetchTodayPendingFeishuTasksReturnsEmptyWhenSceneNotAllowed(t *testing.T) {
	ctx := context.Background()
	client := &sceneStatusTargetClient{
		rows: map[string][]feishusdk.TaskRow{
			"单个链接采集|pending|with": {
				{TaskID: 70, Params: "S1", App: "com.app", Scene: SceneSingleURLCapture},
			},
		},
	}
	allowed := map[string]struct{}{SceneGeneralSearch: {}}
	tasks, err := fetchTodayPendingFeishuTasks(ctx, client, "https://example.com/bitable/foo", "com.app", 5, allowed)
	if err != nil {
		t.Fatalf("fetchTodayPendingFeishuTasks returned error: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("expected no tasks when scene not allowed, got %v", collectTaskIDs(tasks))
	}
}

func TestFetchTodayPendingFeishuTasksAllowedScenesOrderPreserved(t *testing.T) {
	ctx := context.Background()
	client := &sceneStatusTargetClient{
		rows: map[string][]feishusdk.TaskRow{
			"综合页搜索|pending|with": {
				{TaskID: 201, Params: "G1", App: "com.app", Scene: SceneGeneralSearch},
			},
			"个人页搜索|pending|with": {
				{TaskID: 301, Params: "P1", App: "com.app", Scene: SceneProfileSearch},
				{TaskID: 302, Params: "P2", App: "com.app", Scene: SceneProfileSearch},
			},
		},
	}
	allowed := map[string]struct{}{SceneGeneralSearch: {}, SceneProfileSearch: {}}
	tasks, err := fetchTodayPendingFeishuTasks(ctx, client, "https://example.com/bitable/foo", "com.app", 3, allowed)
	if err != nil {
		t.Fatalf("fetchTodayPendingFeishuTasks returned error: %v", err)
	}
	got := collectTaskIDs(tasks)
	want := []int64{201, 301, 302}
	if !equalIDs(got, want) {
		t.Fatalf("unexpected ids: got %v want %v", got, want)
	}
}

func TestNormalizeAllowedScenesDefaultsExcludeSingleURL(t *testing.T) {
	set := normalizeAllowedScenes(nil)
	if set == nil {
		t.Fatalf("expected default scene set")
	}
	if _, ok := set[SceneSingleURLCapture]; ok {
		t.Fatalf("default scenes should not include single url capture")
	}
	if len(set) != len(defaultDeviceScenes) {
		t.Fatalf("expected %d default scenes, got %d", len(defaultDeviceScenes), len(set))
	}
}

func TestNormalizeAllowedScenesRespectsExplicitList(t *testing.T) {
	custom := []string{SceneSingleURLCapture}
	set := normalizeAllowedScenes(custom)
	if len(set) != len(custom) {
		t.Fatalf("expected len %d got %d", len(custom), len(set))
	}
	if _, ok := set[SceneSingleURLCapture]; !ok {
		t.Fatalf("expected single url scene to be allowed when explicitly provided")
	}
}

type sceneStatusTargetClient struct {
	rows map[string][]feishusdk.TaskRow
}

func (c *sceneStatusTargetClient) FetchTaskTableWithOptions(ctx context.Context, rawURL string, override *feishusdk.TaskFields, opts *feishusdk.TaskQueryOptions) (*feishusdk.TaskTable, error) {
	scene := extractConditionValue(opts.Filter, feishusdk.DefaultTaskFields.Scene)
	status := extractConditionValue(opts.Filter, feishusdk.DefaultTaskFields.Status)
	dt := "without"
	if filterHasDatetime(opts.Filter, feishusdk.DefaultTaskFields.Datetime) {
		dt = "with"
	}
	key := strings.TrimSpace(scene) + "|" + strings.TrimSpace(status) + "|" + dt
	rows := c.rows[key]
	clone := make([]feishusdk.TaskRow, len(rows))
	copy(clone, rows)
	return &feishusdk.TaskTable{
		Rows:   clone,
		Fields: feishusdk.DefaultTaskFields,
	}, nil
}

func (c *sceneStatusTargetClient) UpdateTaskStatus(ctx context.Context, table *feishusdk.TaskTable, taskID int64, newStatus string) error {
	return nil
}

func (c *sceneStatusTargetClient) UpdateTaskFields(ctx context.Context, table *feishusdk.TaskTable, taskID int64, fields map[string]any) error {
	return nil
}

func extractConditionValue(filter *feishusdk.FilterInfo, field string) string {
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

func filterHasDatetime(filter *feishusdk.FilterInfo, field string) bool {
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
	uploads []struct {
		appToken  string
		fileName  string
		asImage   bool
		contentSz int
	}
}

func (r *recordingTargetClient) FetchTaskTableWithOptions(ctx context.Context, rawURL string, override *feishusdk.TaskFields, opts *feishusdk.TaskQueryOptions) (*feishusdk.TaskTable, error) {
	return nil, nil
}

func (r *recordingTargetClient) UpdateTaskStatus(ctx context.Context, table *feishusdk.TaskTable, taskID int64, newStatus string) error {
	return nil
}

func (r *recordingTargetClient) UpdateTaskFields(ctx context.Context, table *feishusdk.TaskTable, taskID int64, fields map[string]any) error {
	cp := make(map[string]any, len(fields))
	for k, v := range fields {
		cp[k] = v
	}
	r.updates = append(r.updates, cp)
	return nil
}

func (r *recordingTargetClient) UploadBitableMedia(ctx context.Context, appToken string, fileName string, content []byte, asImage bool) (string, error) {
	r.uploads = append(r.uploads, struct {
		appToken  string
		fileName  string
		asImage   bool
		contentSz int
	}{
		appToken:  appToken,
		fileName:  fileName,
		asImage:   asImage,
		contentSz: len(content),
	})
	return "file-token-123", nil
}

func TestBuildFeishuFilterInfoWithStatusesEmbedsBaseConditions(t *testing.T) {
	fields := feishusdk.DefaultTaskFields
	filter := buildFeishuFilterInfo(fields, "com.app", []string{feishusdk.StatusPending, feishusdk.StatusPending}, SceneSingleURLCapture)
	if filter == nil {
		t.Fatalf("expected filter, got nil")
	}
	if len(filter.Conditions) != 0 {
		t.Fatalf("expected no top-level conditions when statuses present, got %d", len(filter.Conditions))
	}
	if len(filter.Children) != 1 {
		t.Fatalf("expected single child for deduped statuses, got %d", len(filter.Children))
	}
	child := filter.Children[0]
	assertConditionValue(t, child.Conditions, fields.App, "com.app")
	assertConditionValue(t, child.Conditions, fields.Scene, SceneSingleURLCapture)
	assertConditionValue(t, child.Conditions, fields.Datetime, "Today")
	assertConditionValue(t, child.Conditions, fields.Status, feishusdk.StatusPending)
}

func TestBuildFeishuFilterInfoBlankStatusAddsVariants(t *testing.T) {
	fields := feishusdk.DefaultTaskFields
	filter := buildFeishuFilterInfo(fields, "", []string{""}, SceneSingleURLCapture)
	if filter == nil {
		t.Fatalf("expected filter, got nil")
	}
	if len(filter.Children) != 2 {
		t.Fatalf("expected two children for blank status variants, got %d", len(filter.Children))
	}
	opSeen := map[string]struct{}{}
	for _, child := range filter.Children {
		conds := findConditions(child.Conditions, fields.Status)
		if len(conds) != 1 {
			t.Fatalf("expected single status condition per child, got %d", len(conds))
		}
		if cond := conds[0]; cond.Operator == nil {
			t.Fatalf("status condition missing operator")
		} else {
			opSeen[strings.ToLower(strings.TrimSpace(*cond.Operator))] = struct{}{}
		}
		assertConditionValue(t, child.Conditions, fields.Scene, SceneSingleURLCapture)
		assertConditionValue(t, child.Conditions, fields.Datetime, "Today")
	}
	if len(opSeen) != 2 {
		t.Fatalf("expected both isEmpty and is operators, got %v", opSeen)
	}
	if _, ok := opSeen["isempty"]; !ok {
		t.Fatalf("missing isEmpty operator")
	}
	if _, ok := opSeen["is"]; !ok {
		t.Fatalf("missing is operator")
	}
}

func TestBuildFeishuFilterInfoWithoutStatusesUsesBaseConditions(t *testing.T) {
	fields := feishusdk.DefaultTaskFields
	filter := buildFeishuFilterInfo(fields, "com.app", nil, SceneSingleURLCapture)
	if filter == nil {
		t.Fatalf("expected filter, got nil")
	}
	if len(filter.Children) != 0 {
		t.Fatalf("expected no children without statuses, got %d", len(filter.Children))
	}
	if len(filter.Conditions) != 3 {
		t.Fatalf("expected three base conditions, got %d", len(filter.Conditions))
	}
	assertConditionValue(t, filter.Conditions, fields.App, "com.app")
	assertConditionValue(t, filter.Conditions, fields.Scene, SceneSingleURLCapture)
	assertConditionValue(t, filter.Conditions, fields.Datetime, "Today")
}

func assertConditionValue(t *testing.T, conds []*feishusdk.Condition, field, want string) {
	t.Helper()
	matched := findConditions(conds, field)
	if len(matched) != 1 {
		t.Fatalf("expected exactly one condition for field %s, got %d", field, len(matched))
	}
	cond := matched[0]
	if want == "" {
		return
	}
	if len(cond.Value) == 0 {
		t.Fatalf("condition for field %s missing value", field)
	}
	if cond.Value[0] != want {
		t.Fatalf("unexpected value for field %s: got %q want %q", field, cond.Value[0], want)
	}
}

func findConditions(conds []*feishusdk.Condition, field string) []*feishusdk.Condition {
	result := make([]*feishusdk.Condition, 0)
	for _, cond := range conds {
		if cond == nil || cond.FieldName == nil {
			continue
		}
		if *cond.FieldName == field {
			result = append(result, cond)
		}
	}
	return result
}

func TestUpdateFeishuTaskStatusesAssignsDispatchedDevice(t *testing.T) {
	client := &recordingTargetClient{}
	table := &feishusdk.TaskTable{
		Ref:    feishusdk.BitableRef{AppToken: "app", TableID: "tbl"},
		Fields: feishusdk.DefaultTaskFields,
	}
	task := &FeishuTask{
		TaskID: 1,
		source: &feishuTaskSource{
			client: client,
			table:  table,
		},
	}

	dispatchedAt := time.Date(2025, 11, 10, 10, 0, 0, 0, time.UTC)
	if err := UpdateFeishuTaskStatuses(context.Background(), []*FeishuTask{task}, "dispatched", "device-xyz", &TaskStatusMeta{DispatchedAt: &dispatchedAt}); err != nil {
		t.Fatalf("updateFeishuTaskStatuses returned error: %v", err)
	}
	if len(client.updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(client.updates))
	}
	statusField := feishusdk.DefaultTaskFields.Status
	serialField := feishusdk.DefaultTaskFields.DispatchedDevice
	dispatchedField := feishusdk.DefaultTaskFields.DispatchedAt
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
	table := &feishusdk.TaskTable{
		Ref:    feishusdk.BitableRef{AppToken: "app", TableID: "tbl"},
		Fields: feishusdk.DefaultTaskFields,
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
	if err := UpdateFeishuTaskStatuses(context.Background(), []*FeishuTask{task}, "success", "", &TaskStatusMeta{CompletedAt: &completedAt}); err != nil {
		t.Fatalf("updateFeishuTaskStatuses returned error: %v", err)
	}
	if len(client.updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(client.updates))
	}
	elapsedField := feishusdk.DefaultTaskFields.ElapsedSeconds
	update := client.updates[0]
	if got := update[elapsedField]; got == nil {
		t.Fatalf("expected elapsed seconds field=%s to be set", elapsedField)
	}
	if task.ElapsedSeconds != 95 {
		t.Fatalf("expected task elapsed seconds to be 95, got %d", task.ElapsedSeconds)
	}
}

func TestUpdateFeishuTaskStatusesAssignsLogs(t *testing.T) {
	client := &recordingTargetClient{}
	table := &feishusdk.TaskTable{
		Ref:    feishusdk.BitableRef{AppToken: "app", TableID: "tbl"},
		Fields: feishusdk.DefaultTaskFields,
	}
	task := &FeishuTask{
		TaskID: 3,
		source: &feishuTaskSource{
			client: client,
			table:  table,
		},
	}
	logsPath := "/tmp/search/logs"
	if err := UpdateFeishuTaskStatuses(context.Background(), []*FeishuTask{task}, feishusdk.StatusRunning, "", &TaskStatusMeta{Logs: logsPath}); err != nil {
		t.Fatalf("updateFeishuTaskStatuses returned error: %v", err)
	}
	if len(client.updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(client.updates))
	}
	logsField := feishusdk.DefaultTaskFields.Logs
	update := client.updates[0]
	if got := update[logsField]; got != logsPath {
		t.Fatalf("expected logs field=%s got=%v", logsField, got)
	}
	if strings.TrimSpace(task.Logs) != logsPath {
		t.Fatalf("expected task logs to be %q, got %q", logsPath, task.Logs)
	}
}

func TestUpdateFeishuTaskStatusesAssignsLastScreenShot(t *testing.T) {
	t.Setenv("SCREENSHOT_COMPRESS_MIN_BYTES", "1")

	client := &recordingTargetClient{}
	table := &feishusdk.TaskTable{
		Ref:    feishusdk.BitableRef{AppToken: "app", TableID: "tbl"},
		Fields: feishusdk.DefaultTaskFields,
	}
	dir := t.TempDir()
	screenshotPath := filepath.Join(dir, "last.png")
	if err := os.WriteFile(screenshotPath, []byte("pngdata"), 0o644); err != nil {
		t.Fatalf("write screenshot file failed: %v", err)
	}
	task := &FeishuTask{
		TaskID: 4,
		TaskRef: &Task{
			LastScreenShotPath: screenshotPath,
		},
		source: &feishuTaskSource{
			client: client,
			table:  table,
		},
	}
	if err := UpdateFeishuTaskStatuses(context.Background(), []*FeishuTask{task}, feishusdk.StatusSuccess, "", nil); err != nil {
		t.Fatalf("updateFeishuTaskStatuses returned error: %v", err)
	}
	if len(client.uploads) != 1 {
		t.Fatalf("expected 1 upload, got %d", len(client.uploads))
	}
	lastField := feishusdk.DefaultTaskFields.LastScreenShot
	var found bool
	for _, upd := range client.updates {
		if v, ok := upd[lastField]; ok {
			list, ok := v.([]map[string]any)
			if !ok || len(list) != 1 {
				t.Fatalf("expected screenshot field to be attachment list, got %T=%v", v, v)
			}
			if got := list[0]["file_token"]; got != "file-token-123" {
				t.Fatalf("expected file_token to be %q, got %v", "file-token-123", got)
			}
			found = true
		}
	}
	if !found {
		t.Fatalf("expected update payload to contain %s", lastField)
	}
}

func TestUpdateStatusesSkipsDuplicateStatusWrites(t *testing.T) {
	client := &FeishuTaskClient{}
	recorder := &recordingTargetClient{}
	table := &feishusdk.TaskTable{Fields: feishusdk.DefaultTaskFields}
	source := &feishuTaskSource{client: recorder, table: table}
	tasks := []*Task{
		{Payload: &FeishuTask{TaskID: 11, Status: feishusdk.StatusSuccess, source: source}},
		{Payload: &FeishuTask{TaskID: 12, Status: feishusdk.StatusSuccess, source: source}},
	}
	updated, err := client.updateStatuses(context.Background(), tasks, feishusdk.StatusSuccess, "device-1")
	if err != nil {
		t.Fatalf("updateStatuses returned error: %v", err)
	}
	if len(recorder.updates) != 0 {
		t.Fatalf("expected no updates when desired status already set, got %d", len(recorder.updates))
	}
	for _, task := range updated {
		if task.Status != feishusdk.StatusSuccess {
			t.Fatalf("task %d status changed unexpectedly: %s", task.TaskID, task.Status)
		}
	}
}

func TestUpdateStatusesStillUpdatesPendingTasks(t *testing.T) {
	client := &FeishuTaskClient{}
	recorder := &recordingTargetClient{}
	table := &feishusdk.TaskTable{Fields: feishusdk.DefaultTaskFields}
	source := &feishuTaskSource{client: recorder, table: table}
	task := &FeishuTask{TaskID: 21, Status: feishusdk.StatusPending, source: source}
	updated, err := client.updateStatuses(context.Background(), []*Task{{Payload: task}}, feishusdk.StatusSuccess, "device-1")
	if err != nil {
		t.Fatalf("updateStatuses returned error: %v", err)
	}
	if len(updated) != 1 {
		t.Fatalf("expected 1 task in response, got %d", len(updated))
	}
	if len(recorder.updates) != 1 {
		t.Fatalf("expected 1 update for pending task, got %d", len(recorder.updates))
	}
	if task.Status != feishusdk.StatusSuccess {
		t.Fatalf("expected task status to update to success, got %s", task.Status)
	}
}
