package feishu

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	liveReadableBitableURL = "https://bytedance.larkoffice.com/wiki/DKKwwF9XRincITkd0g1c6udUnHe?table=tblLUmsGgp5SECWF&view=vew9Kwl9uR"
	liveWritableBitableURL = "https://bytedance.larkoffice.com/wiki/DKKwwF9XRincITkd0g1c6udUnHe?table=tblLUmsGgp5SECWF"
	liveResultBitableURL   = "https://bytedance.larkoffice.com/wiki/DKKwwF9XRincITkd0g1c6udUnHe?table=tblzoZuR6aminfye&view=vewTF27mJQ"
	liveTargetApp          = "com.smile.gifmaker"
)

func TestParseSpreadsheetURL(t *testing.T) {
	cases := []struct {
		name    string
		url     string
		wantTok string
		wantSID string
		wantErr bool
	}{
		{
			name:    "wiki sheet",
			url:     "https://bytedance.larkoffice.com/wiki/HlauwOB4ZilZDekyEGncpFglnPc?sheet=5eebc2",
			wantTok: "HlauwOB4ZilZDekyEGncpFglnPc",
			wantSID: "5eebc2",
		},
		{
			name:    "sheets link",
			url:     "https://foo.feishu.cn/sheets/shtcn123456?sheet_id=sheet1",
			wantTok: "shtcn123456",
			wantSID: "sheet1",
		},
		{
			name:    "invalid host",
			url:     "https://example.com/sheets/abc",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ref, err := ParseSpreadsheetURL(tc.url)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ref.SpreadsheetToken != tc.wantTok {
				t.Fatalf("token mismatch: want %q got %q", tc.wantTok, ref.SpreadsheetToken)
			}
			if ref.SheetID != tc.wantSID {
				t.Fatalf("sheet id mismatch: want %q got %q", tc.wantSID, ref.SheetID)
			}
		})
	}
}

func TestColumnLabel(t *testing.T) {
	cases := map[int]string{
		1:   "A",
		26:  "Z",
		27:  "AA",
		52:  "AZ",
		53:  "BA",
		702: "ZZ",
		703: "AAA",
	}
	for input, want := range cases {
		got := columnLabel(input)
		if got != want {
			t.Fatalf("columnLabel(%d) = %q, want %q", input, got, want)
		}
	}
}

func TestSanitizeName(t *testing.T) {
	if got := sanitizeName("  测试 Sheet  "); got == "" || got == "feishu_sheet" {
		t.Fatalf("sanitizeName should preserve content, got %q", got)
	}
	if got := sanitizeName("  "); got != "feishu_sheet" {
		t.Fatalf("expected fallback, got %q", got)
	}
}

func TestParseBitableURL(t *testing.T) {
	cases := []struct {
		name     string
		url      string
		wantApp  string
		wantWiki string
		wantTbl  string
		wantVew  string
		wantErr  bool
	}{
		{
			name:    "base link",
			url:     "https://foo.feishu.cn/base/bascnabc123/table?table=tblX1&view=vew123",
			wantApp: "bascnabc123",
			wantTbl: "tblX1",
			wantVew: "vew123",
		},
		{
			name:     "wiki link",
			url:      "https://bytedance.larkoffice.com/wiki/DKKwwF9XRincITkd0g1c6udUnHe?table=tblLUmsGgp5SECWF",
			wantWiki: "DKKwwF9XRincITkd0g1c6udUnHe",
			wantTbl:  "tblLUmsGgp5SECWF",
		},
		{
			name:    "missing table",
			url:     "https://foo.feishu.cn/base/bascnabc123",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ref, err := ParseBitableURL(tc.url)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ref.AppToken != tc.wantApp {
				t.Fatalf("app token mismatch: want %q got %q", tc.wantApp, ref.AppToken)
			}
			if ref.WikiToken != tc.wantWiki {
				t.Fatalf("wiki token mismatch: want %q got %q", tc.wantWiki, ref.WikiToken)
			}
			if ref.TableID != tc.wantTbl {
				t.Fatalf("table mismatch: want %q got %q", tc.wantTbl, ref.TableID)
			}
			if ref.ViewID != tc.wantVew {
				t.Fatalf("view mismatch: want %q got %q", tc.wantVew, ref.ViewID)
			}
		})
	}
}

func TestDecodeTargetRow(t *testing.T) {
	rec := bitableRecord{
		RecordID: "rec123",
		Fields: map[string]any{
			DefaultTargetFields.TaskID:       float64(42),
			DefaultTargetFields.Params:       []any{"{\"song\":\"foo\"}"},
			DefaultTargetFields.App:          "netease",
			DefaultTargetFields.Scene:        "auto",
			DefaultTargetFields.Datetime:     "2025-11-08 10:30:00",
			DefaultTargetFields.Status:       "pending",
			DefaultTargetFields.User:         "tester",
			DefaultTargetFields.DeviceSerial: "dev-123",
		},
	}
	row, err := decodeTargetRow(rec, DefaultTargetFields)
	if err != nil {
		t.Fatalf("decodeTargetRow returned error: %v", err)
	}
	if row.TaskID != 42 {
		t.Fatalf("unexpected TaskID %d", row.TaskID)
	}
	if row.Params == "" || row.App != "netease" || row.Scene != "auto" {
		t.Fatalf("unexpected field values: %+v", row)
	}
	if row.Status != "pending" {
		t.Fatalf("unexpected status %q", row.Status)
	}
	if row.User != "tester" || row.DeviceSerial != "dev-123" {
		t.Fatalf("unexpected user/device: %+v", row)
	}
	if row.Datetime == nil {
		t.Fatalf("expected datetime to be parsed")
	}
	if got := row.Datetime.Format("2006-01-02 15:04:05"); got != "2025-11-08 10:30:00" {
		t.Fatalf("unexpected datetime %q", got)
	}
}

func TestDecodeTargetRowAllowsEmptyStatus(t *testing.T) {
	rec := bitableRecord{
		RecordID: "recEmpty",
		Fields: map[string]any{
			DefaultTargetFields.TaskID:       float64(43),
			DefaultTargetFields.Params:       "{}",
			DefaultTargetFields.App:          "netease",
			DefaultTargetFields.Scene:        "auto",
			DefaultTargetFields.Datetime:     "2025-11-08",
			DefaultTargetFields.Status:       "",
			DefaultTargetFields.User:         "",
			DefaultTargetFields.DeviceSerial: "",
		},
	}
	row, err := decodeTargetRow(rec, DefaultTargetFields)
	if err != nil {
		t.Fatalf("decodeTargetRow should allow empty status: %v", err)
	}
	if row.Status != "" {
		t.Fatalf("expected empty status, got %q", row.Status)
	}
	if row.User != "" || row.DeviceSerial != "" {
		t.Fatalf("expected empty user/device, got %+v", row)
	}
}

func TestDecodeTargetRowMissingStatus(t *testing.T) {
	rec := bitableRecord{
		RecordID: "recMissing",
		Fields: map[string]any{
			DefaultTargetFields.TaskID:   float64(44),
			DefaultTargetFields.Params:   "{}",
			DefaultTargetFields.App:      "netease",
			DefaultTargetFields.Scene:    "auto",
			DefaultTargetFields.Datetime: "2025-11-08",
		},
	}
	if _, err := decodeTargetRow(rec, DefaultTargetFields); err == nil {
		t.Fatalf("expected error when status field missing")
	}
}

func TestRecordIDByTaskID(t *testing.T) {
	table := &TargetTable{
		Rows:      []TargetRow{{RecordID: "rec1", TaskID: 7, Status: "pending"}},
		taskIndex: map[int64]string{7: "rec1"},
	}
	if id, ok := table.RecordIDByTaskID(7); !ok || id != "rec1" {
		t.Fatalf("expected rec1, got ok=%v id=%q", ok, id)
	}
	if _, ok := table.RecordIDByTaskID(8); ok {
		t.Fatalf("expected miss for task 8")
	}
}

func TestParseBitableTimeFallback(t *testing.T) {
	if _, err := parseBitableTime("2025-11-08"); err != nil {
		t.Fatalf("expected date-only format to parse: %v", err)
	}
	if _, err := parseBitableTime(time.Now().Format(time.RFC3339)); err != nil {
		t.Fatalf("expected RFC3339 format to parse: %v", err)
	}
}

func TestFetchTargetTableExampleMock(t *testing.T) {
	ctx := context.Background()
	const wikiResponse = `{"code":0,"msg":"success","data":{"node":{"obj_token":"bascnMockToken","obj_type":"bitable"}}}`
	listResponseData := map[string]any{
		"code": 0,
		"msg":  "success",
		"data": map[string]any{
			"items": []map[string]any{
				{
					"record_id": "recYUOQd9",
					"fields": map[string]any{
						DefaultTargetFields.TaskID:   101,
						DefaultTargetFields.Params:   "{\"song\":\"foo\"}",
						DefaultTargetFields.App:      "netease",
						DefaultTargetFields.Scene:    "batch",
						DefaultTargetFields.Datetime: "2025-11-07 12:30:00",
						DefaultTargetFields.Status:   "pending",
					},
				},
				{
					"record_id": "recTy0283",
					"fields": map[string]any{
						DefaultTargetFields.TaskID:   102,
						DefaultTargetFields.Params:   "{\"song\":\"bar\"}",
						DefaultTargetFields.App:      "qqmusic",
						DefaultTargetFields.Scene:    "batch",
						DefaultTargetFields.Datetime: "2025-11-07 13:00:00",
						DefaultTargetFields.Status:   "done",
					},
				},
			},
			"has_more":   false,
			"page_token": "",
		},
	}
	listResponse, err := json.Marshal(listResponseData)
	if err != nil {
		t.Fatalf("marshal list response: %v", err)
	}
	updateResponse := []byte(`{"code":0,"msg":"success"}`)
	var capturedPayload map[string]any
	wikiCalled := false
	client := &Client{
		doJSONRequestFunc: func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
			switch {
			case method == http.MethodGet && strings.Contains(path, "/wiki/v2/spaces/get_node"):
				wikiCalled = true
				return nil, []byte(wikiResponse), nil
			case method == http.MethodGet && strings.Contains(path, "/bitable/v1/apps/") && strings.Contains(path, "tables/tblLUmsGgp5SECWF/records"):
				if !strings.Contains(path, "bascnMockToken") {
					t.Fatalf("expected resolved app token, got %s", path)
				}
				return nil, listResponse, nil
			case method == http.MethodPut && strings.Contains(path, "/bitable/v1/apps/") && strings.Contains(path, "/records/recYUOQd9"):
				fields, ok := payload.(map[string]any)
				if !ok {
					t.Fatalf("expected map payload, got %T", payload)
				}
				capturedPayload = fields
				return nil, updateResponse, nil
			default:
				t.Fatalf("unexpected request %s %s", method, path)
			}
			return nil, nil, nil
		},
	}

	table, err := client.FetchTargetTable(ctx, liveWritableBitableURL, nil)
	if err != nil {
		t.Fatalf("FetchTargetTable returned error: %v", err)
	}
	if !wikiCalled {
		t.Fatalf("expected wiki resolver call")
	}
	if table == nil || len(table.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %+v", table)
	}
	if table.Ref.TableID != "tblLUmsGgp5SECWF" {
		t.Fatalf("unexpected ref: %+v", table.Ref)
	}
	if id, ok := table.RecordIDByTaskID(101); !ok || id != "recYUOQd9" {
		t.Fatalf("expected record id recYUOQd9 for task 101, got %q ok=%v", id, ok)
	}
	if err := client.UpdateTargetStatus(ctx, table, 101, "processing"); err != nil {
		t.Fatalf("UpdateTargetStatus error: %v", err)
	}
	if table.Rows[0].Status != "processing" {
		t.Fatalf("local table status not updated, got %q", table.Rows[0].Status)
	}
	fields, ok := capturedPayload["fields"].(map[string]any)
	if !ok {
		t.Fatalf("expected captured fields map, got %#v", capturedPayload)
	}
	if fields[DefaultTargetFields.Status] != "processing" {
		t.Fatalf("expected status payload 'processing', got %#v", fields)
	}
	t.Logf("decoded rows from example table (mock): %+v", table.Rows)
}

func TestCreateTargetRecords(t *testing.T) {
	const wikiResponse = `{"code":0,"msg":"success","data":{"node":{"obj_token":"bascnCreateToken","obj_type":"bitable"}}}`
	const batchCreateResponse = `{"code":0,"msg":"success","data":{"records":[{"record_id":"recAAA"},{"record_id":"recBBB"}]}}`

	ctx := context.Background()
	var (
		wikiCalled   bool
		createCalled bool
		captured     map[string]any
	)
	client := &Client{
		doJSONRequestFunc: func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
			switch {
			case method == http.MethodGet && strings.Contains(path, "/wiki/v2/spaces/get_node"):
				wikiCalled = true
				return nil, []byte(wikiResponse), nil
			case method == http.MethodPost && strings.Contains(path, "/records/batch_create"):
				createCalled = true
				m, ok := payload.(map[string]any)
				if !ok {
					t.Fatalf("expected map payload, got %T", payload)
				}
				captured = m
				return nil, []byte(batchCreateResponse), nil
			default:
				t.Fatalf("unexpected request %s %s", method, path)
			}
			return nil, nil, nil
		},
	}

	when := time.Date(2024, 10, 1, 12, 0, 0, 0, time.UTC)
	records := []TargetRecordInput{
		{TaskID: 301, Params: `{"foo":1}`, App: "netease", Scene: "parse", Datetime: &when, Status: "pending", User: "tester"},
		{TaskID: 0, Params: `{"foo":2}`, DatetimeRaw: "2024-10-02 08:00:00", Status: "queued"},
	}
	override := &TargetFields{Status: "biz_status"}
	ids, err := client.CreateTargetRecords(ctx, liveWritableBitableURL, records, override)
	if err != nil {
		t.Fatalf("CreateTargetRecords returned error: %v", err)
	}
	if !wikiCalled || !createCalled {
		t.Fatalf("expected wiki and batch create calls, got wiki=%v create=%v", wikiCalled, createCalled)
	}
	if len(ids) != 2 || ids[0] != "recAAA" || ids[1] != "recBBB" {
		t.Fatalf("unexpected ids %#v", ids)
	}
	recordPayloads, ok := captured["records"].([]map[string]any)
	if !ok {
		t.Fatalf("captured records is %T", captured["records"])
	}
	if len(recordPayloads) != 2 {
		t.Fatalf("expected 2 record payloads, got %d", len(recordPayloads))
	}
	firstFields, ok := recordPayloads[0]["fields"].(map[string]any)
	if !ok {
		t.Fatalf("expected fields map, got %T", recordPayloads[0]["fields"])
	}
	if firstFields[DefaultTargetFields.TaskID] != int64(301) {
		t.Fatalf("unexpected task id payload %#v", firstFields[DefaultTargetFields.TaskID])
	}
	if firstFields["biz_status"] != "pending" {
		t.Fatalf("expected biz_status pending, got %#v", firstFields["biz_status"])
	}
	if _, exists := firstFields[DefaultTargetFields.Status]; exists {
		t.Fatalf("default status field should be absent when override is set")
	}
	if firstFields[DefaultTargetFields.Datetime] != when.Format(time.RFC3339) {
		t.Fatalf("unexpected datetime %v", firstFields[DefaultTargetFields.Datetime])
	}
	secondFields, ok := recordPayloads[1]["fields"].(map[string]any)
	if !ok {
		t.Fatalf("expected fields map for second record, got %T", recordPayloads[1]["fields"])
	}
	if _, exists := secondFields[DefaultTargetFields.TaskID]; exists {
		t.Fatalf("TaskID should be omitted for auto-increment scenario, got %#v", secondFields[DefaultTargetFields.TaskID])
	}
	if secondFields[DefaultTargetFields.Datetime] != "2024-10-02 08:00:00" {
		t.Fatalf("expected raw datetime preserved, got %#v", secondFields[DefaultTargetFields.Datetime])
	}
}

func TestCreateTargetRecordSingle(t *testing.T) {
	const wikiResponse = `{"code":0,"msg":"success","data":{"node":{"obj_token":"bascnSingle","obj_type":"bitable"}}}`
	const createResponse = `{"code":0,"msg":"success","data":{"record":{"record_id":"recSingle"}}}`

	ctx := context.Background()
	var (
		wikiCalled   bool
		createCalled bool
		captured     map[string]any
	)
	client := &Client{
		doJSONRequestFunc: func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
			switch {
			case method == http.MethodGet && strings.Contains(path, "/wiki/v2/spaces/get_node"):
				wikiCalled = true
				return nil, []byte(wikiResponse), nil
			case method == http.MethodPost && strings.Contains(path, "/records") && !strings.Contains(path, "batch_create"):
				createCalled = true
				m, ok := payload.(map[string]any)
				if !ok {
					t.Fatalf("expected map payload, got %T", payload)
				}
				captured = m
				return nil, []byte(createResponse), nil
			default:
				t.Fatalf("unexpected request %s %s", method, path)
			}
			return nil, nil, nil
		},
	}

	rec := TargetRecordInput{Status: "pending"}
	id, err := client.CreateTargetRecord(ctx, liveWritableBitableURL, rec, nil)
	if err != nil {
		t.Fatalf("CreateTargetRecord error: %v", err)
	}
	if id != "recSingle" {
		t.Fatalf("unexpected id %q", id)
	}
	if !wikiCalled || !createCalled {
		t.Fatalf("expected both wiki and create calls")
	}
	fields, ok := captured["fields"].(map[string]any)
	if !ok {
		t.Fatalf("expected fields map, got %T", captured["fields"])
	}
	if _, exists := fields[DefaultTargetFields.TaskID]; exists {
		t.Fatalf("TaskID should be omitted when not provided, got %#v", fields[DefaultTargetFields.TaskID])
	}
	if fields[DefaultTargetFields.Status] != "pending" {
		t.Fatalf("unexpected status %#v", fields[DefaultTargetFields.Status])
	}
}

func TestBuildResultRecordPayloads(t *testing.T) {
	when := time.Date(2024, 11, 8, 12, 0, 0, 0, time.UTC)
	payloads, err := buildResultRecordPayloads([]ResultRecordInput{
		{
			Datetime:     &when,
			DeviceSerial: "dev-001",
			App:          "netease",
			Scene:        "batch",
			Params:       `{"song":"foo"}`,
			ItemID:       "video123",
			ItemCaption:  "Test Video",
			ItemURL:      "https://cdn.example.com/video123.mp4",
			UserName:     "tester",
			UserID:       "author-1",
			Tags:         "tag1,tag2",
			SubTaskID:    "sub-1",
			PayloadJSON:  map[string]any{"foo": "bar"},
		},
	}, DefaultResultFields)
	if err != nil {
		t.Fatalf("buildResultRecordPayloads returned error: %v", err)
	}
	if len(payloads) != 1 {
		t.Fatalf("expected one payload, got %d", len(payloads))
	}
	row := payloads[0]
	ts, ok := row[DefaultResultFields.Datetime].(int64)
	if !ok || ts <= 0 {
		t.Fatalf("expected unix ms int for datetime, got %#v", row[DefaultResultFields.Datetime])
	}
	payloadStr, ok := row[DefaultResultFields.PayloadJSON].(string)
	if !ok || payloadStr == "" {
		t.Fatalf("expected payload json string, got %#v", row[DefaultResultFields.PayloadJSON])
	}
	var decoded map[string]string
	if err := json.Unmarshal([]byte(payloadStr), &decoded); err != nil {
		t.Fatalf("payload json invalid: %v", err)
	}
	if decoded["foo"] != "bar" {
		t.Fatalf("unexpected payload content %#v", decoded)
	}
	if _, err := buildResultRecordPayloads([]ResultRecordInput{
		{DeviceSerial: "dev", PayloadJSON: "{not json}"},
	}, DefaultResultFields); err == nil {
		t.Fatalf("expected error for invalid payload json")
	}
}

func TestCreateResultRecords(t *testing.T) {
	const wikiResponse = `{"code":0,"msg":"success","data":{"node":{"obj_token":"bascnResult","obj_type":"bitable"}}}`
	const batchCreateResponse = `{"code":0,"msg":"success","data":{"records":[{"record_id":"recRes1"},{"record_id":"recRes2"}]}}`

	ctx := context.Background()
	var (
		wikiCalled   bool
		batchCalled  bool
		capturedBody map[string]any
	)
	client := &Client{
		doJSONRequestFunc: func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
			switch {
			case method == http.MethodGet && strings.Contains(path, "/wiki/v2/spaces/get_node"):
				wikiCalled = true
				return nil, []byte(wikiResponse), nil
			case method == http.MethodPost && strings.Contains(path, "/records/batch_create"):
				batchCalled = true
				m, ok := payload.(map[string]any)
				if !ok {
					t.Fatalf("expected map payload, got %T", payload)
				}
				capturedBody = m
				return nil, []byte(batchCreateResponse), nil
			default:
				t.Fatalf("unexpected request %s %s", method, path)
			}
			return nil, nil, nil
		},
	}

	records := []ResultRecordInput{
		{
			DeviceSerial: "dev-1",
			App:          "netease",
			Scene:        "auto",
			Params:       "{}",
			ItemID:       "item-1",
			PayloadJSON:  map[string]any{"id": 1},
		},
		{
			DeviceSerial: "dev-2",
			App:          "douyin",
			Scene:        "manual",
			ItemID:       "item-2",
			PayloadJSON:  json.RawMessage(`{"id":2}`),
		},
	}
	ids, err := client.CreateResultRecords(ctx, liveResultBitableURL, records, nil)
	if err != nil {
		t.Fatalf("CreateResultRecords returned error: %v", err)
	}
	if !wikiCalled || !batchCalled {
		t.Fatalf("expected wiki and batch create calls, got wiki=%v batch=%v", wikiCalled, batchCalled)
	}
	if len(ids) != 2 || ids[0] != "recRes1" || ids[1] != "recRes2" {
		t.Fatalf("unexpected ids %#v", ids)
	}
	payloadRecords, ok := capturedBody["records"].([]map[string]any)
	if !ok {
		t.Fatalf("records payload is %T", capturedBody["records"])
	}
	first, ok := payloadRecords[0]["fields"].(map[string]any)
	if !ok {
		t.Fatalf("fields payload type %T", payloadRecords[0]["fields"])
	}
	if first[DefaultResultFields.DeviceSerial] != "dev-1" {
		t.Fatalf("unexpected device serial %#v", first[DefaultResultFields.DeviceSerial])
	}
	if _, ok := first[DefaultResultFields.PayloadJSON].(string); !ok {
		t.Fatalf("payload json should be string, got %#v", first[DefaultResultFields.PayloadJSON])
	}
}

func TestCreateResultRecordSingle(t *testing.T) {
	const wikiResponse = `{"code":0,"msg":"success","data":{"node":{"obj_token":"bascnResultSingle","obj_type":"bitable"}}}`
	const createResponse = `{"code":0,"msg":"success","data":{"record":{"record_id":"recResSingle"}}}`

	ctx := context.Background()
	var (
		wikiCalled      bool
		createCalled    bool
		capturedPayload map[string]any
	)
	client := &Client{
		doJSONRequestFunc: func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
			switch {
			case method == http.MethodGet && strings.Contains(path, "/wiki/v2/spaces/get_node"):
				wikiCalled = true
				return nil, []byte(wikiResponse), nil
			case method == http.MethodPost && strings.Contains(path, "/records") && !strings.Contains(path, "batch_create"):
				createCalled = true
				m, ok := payload.(map[string]any)
				if !ok {
					t.Fatalf("expected map payload, got %T", payload)
				}
				capturedPayload = m
				return nil, []byte(createResponse), nil
			default:
				t.Fatalf("unexpected request %s %s", method, path)
			}
			return nil, nil, nil
		},
	}

	id, err := client.CreateResultRecord(ctx, liveResultBitableURL, ResultRecordInput{
		DeviceSerial: "dev-only",
		App:          "netease",
		PayloadJSON:  map[string]any{"ok": true},
	}, nil)
	if err != nil {
		t.Fatalf("CreateResultRecord returned error: %v", err)
	}
	if id != "recResSingle" {
		t.Fatalf("unexpected record id %q", id)
	}
	if !wikiCalled || !createCalled {
		t.Fatalf("expected wiki and create calls")
	}
	fields, ok := capturedPayload["fields"].(map[string]any)
	if !ok {
		t.Fatalf("fields payload type %T", capturedPayload["fields"])
	}
	if fields[DefaultResultFields.DeviceSerial] != "dev-only" {
		t.Fatalf("unexpected device serial %#v", fields[DefaultResultFields.DeviceSerial])
	}
	if _, ok := fields[DefaultResultFields.PayloadJSON].(string); !ok {
		t.Fatalf("payload json should be string, got %#v", fields[DefaultResultFields.PayloadJSON])
	}
}

func TestUpdateTargetStatuses(t *testing.T) {
	const wikiResponse = `{"code":0,"msg":"success","data":{"node":{"obj_token":"bascnUpdate","obj_type":"bitable"}}}`
	const customStatusField = "biz_status"
	listResponseData := map[string]any{
		"code": 0,
		"msg":  "success",
		"data": map[string]any{
			"items": []map[string]any{
				{
					"record_id": "recA1",
					"fields": map[string]any{
						DefaultTargetFields.TaskID: 101,
						customStatusField:          "pending",
					},
				},
				{
					"record_id": "recA2",
					"fields": map[string]any{
						DefaultTargetFields.TaskID: 102,
						customStatusField:          "pending",
					},
				},
			},
			"has_more":   false,
			"page_token": "",
		},
	}
	listResponse, err := json.Marshal(listResponseData)
	if err != nil {
		t.Fatalf("marshal list response: %v", err)
	}
	const updateResponse = `{"code":0,"msg":"success"}`

	ctx := context.Background()
	var (
		wikiCalled bool
		listCalled bool
		updates    []map[string]any
	)
	client := &Client{
		doJSONRequestFunc: func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
			switch {
			case method == http.MethodGet && strings.Contains(path, "/wiki/v2/spaces/get_node"):
				wikiCalled = true
				return nil, []byte(wikiResponse), nil
			case method == http.MethodGet && strings.Contains(path, "/bitable/v1/apps/") && strings.Contains(path, "/records"):
				listCalled = true
				return nil, listResponse, nil
			case method == http.MethodPut && strings.Contains(path, "/records/rec"):
				m, ok := payload.(map[string]any)
				if !ok {
					t.Fatalf("expected map payload, got %T", payload)
				}
				updates = append(updates, m)
				return nil, []byte(updateResponse), nil
			default:
				t.Fatalf("unexpected request %s %s", method, path)
			}
			return nil, nil, nil
		},
	}

	changes := []TargetStatusUpdate{{TaskID: 101, NewStatus: "processing"}, {TaskID: 102, NewStatus: "done"}}
	override := &TargetFields{Status: customStatusField}
	if err := client.UpdateTargetStatuses(ctx, liveWritableBitableURL, changes, override); err != nil {
		t.Fatalf("UpdateTargetStatuses returned error: %v", err)
	}
	if !wikiCalled || !listCalled {
		t.Fatalf("expected wiki and list calls")
	}
	if len(updates) != 2 {
		t.Fatalf("expected 2 update calls, got %d", len(updates))
	}
	firstFields, ok := updates[0]["fields"].(map[string]any)
	if !ok {
		t.Fatalf("expected fields map, got %T", updates[0]["fields"])
	}
	if firstFields[customStatusField] != "processing" {
		t.Fatalf("unexpected first status %#v", firstFields[customStatusField])
	}
	secondFields, ok := updates[1]["fields"].(map[string]any)
	if !ok {
		t.Fatalf("expected fields map, got %T", updates[1]["fields"])
	}
	if secondFields[customStatusField] != "done" {
		t.Fatalf("unexpected second status %#v", secondFields[customStatusField])
	}
}

func TestFetchTargetTableExampleLive(t *testing.T) {
	if os.Getenv("FEISHU_LIVE_TEST") != "1" {
		t.Skip("set FEISHU_LIVE_TEST=1 to run live Feishu integration test")
	}
	if os.Getenv("FEISHU_APP_ID") == "" || os.Getenv("FEISHU_APP_SECRET") == "" {
		t.Skip("FEISHU_APP_ID/FEISHU_APP_SECRET not configured")
	}
	ctx := context.Background()
	client, err := NewClientFromEnv()
	if err != nil {
		t.Fatalf("NewClientFromEnv error: %v", err)
	}
	table, err := client.FetchTargetTable(ctx, liveReadableBitableURL, nil)
	if err != nil {
		t.Fatalf("FetchTargetTable live call failed: %v", err)
	}
	if table == nil || len(table.Rows) == 0 {
		t.Fatalf("expected rows from live table, got %+v", table)
	}
	first := table.Rows[0]
	t.Logf("live table row sample: TaskID=%d params=%s app=%s scene=%s datetime=%s status=%s",
		first.TaskID, first.Params, first.App, first.Scene, first.DatetimeRaw, first.Status)
}

func TestFetchTargetTableWithOptionsLive(t *testing.T) {
	if os.Getenv("FEISHU_LIVE_TEST") != "1" {
		t.Skip("set FEISHU_LIVE_TEST=1 to run live Feishu integration test")
	}
	if os.Getenv("FEISHU_APP_ID") == "" || os.Getenv("FEISHU_APP_SECRET") == "" {
		t.Skip("FEISHU_APP_ID/FEISHU_APP_SECRET not configured")
	}
	ctx := context.Background()
	client, err := NewClientFromEnv()
	if err != nil {
		t.Fatalf("NewClientFromEnv error: %v", err)
	}
	appRef := fmt.Sprintf("CurrentValue.[%s]", DefaultTargetFields.App)
	filter := fmt.Sprintf("%s = \"%s\"", appRef, liveTargetApp)
	opts := &TargetQueryOptions{Filter: filter, Limit: 5}
	table, err := client.FetchTargetTableWithOptions(ctx, liveReadableBitableURL, nil, opts)
	if err != nil {
		t.Fatalf("FetchTargetTableWithOptions live call failed: %v", err)
	}
	if table == nil || len(table.Rows) == 0 {
		t.Fatalf("expected rows for %s, got %+v", liveTargetApp, table)
	}
	for i, row := range table.Rows {
		if row.TaskID == 0 {
			t.Fatalf("row %d missing task id", i)
		}
		if strings.TrimSpace(row.Params) == "" {
			t.Fatalf("row %d missing params", i)
		}
		if row.App != liveTargetApp {
			t.Fatalf("row %d app mismatch: got %s", i, row.App)
		}
	}
	t.Logf("fetched %d live rows for %s with filter %s", len(table.Rows), liveTargetApp, filter)
}

func TestTargetRecordLifecycleLive(t *testing.T) {
	if os.Getenv("FEISHU_LIVE_TEST") != "1" {
		t.Skip("set FEISHU_LIVE_TEST=1 to run live Feishu lifecycle test")
	}
	const bitableURL = liveWritableBitableURL
	ctx := context.Background()
	client, err := NewClientFromEnv()
	if err != nil {
		t.Fatalf("NewClientFromEnv error: %v", err)
	}
	baseTaskID := time.Now().Unix()
	records := []TargetRecordInput{
		{
			Params: fmt.Sprintf("{\"test\":\"create_live\",\"id\":%d}", baseTaskID),
			App:    "anygrab-live",
			Scene:  "integration",
			Status: "pending",
			User:   "test-suite",
		},
		{
			Params: fmt.Sprintf("{\"test\":\"create_live\",\"id\":%d}", baseTaskID+1),
			App:    "anygrab-live",
			Scene:  "integration",
			Status: "queued",
			User:   "test-suite",
		},
	}
	ids, err := client.CreateTargetRecords(ctx, bitableURL, records, nil)
	if err != nil {
		t.Fatalf("CreateTargetRecords live error: %v", err)
	}
	if len(ids) != len(records) {
		t.Fatalf("expected %d created ids, got %d", len(records), len(ids))
	}
	ref, err := ParseBitableURL(bitableURL)
	if err != nil {
		t.Fatalf("ParseBitableURL error: %v", err)
	}
	if err := client.ensureBitableAppToken(ctx, &ref); err != nil {
		t.Fatalf("ensureBitableAppToken error: %v", err)
	}
	fields := DefaultTargetFields
	localTable := &TargetTable{
		Ref:       ref,
		Fields:    fields,
		Rows:      make([]TargetRow, 0, len(ids)),
		Invalid:   nil,
		taskIndex: make(map[int64]string, len(ids)),
	}
	for i, recordID := range ids {
		rec, err := client.getBitableRecord(ctx, ref, recordID)
		if err != nil {
			t.Fatalf("getBitableRecord error: %v", err)
		}
		row, err := decodeTargetRow(rec, fields)
		if err != nil {
			t.Fatalf("decodeTargetRow error: %v", err)
		}
		localTable.Rows = append(localTable.Rows, row)
		localTable.taskIndex[row.TaskID] = row.RecordID
		if records[i].TaskID != 0 && row.TaskID != records[i].TaskID {
			t.Logf("server assigned TaskID %d instead of %d", row.TaskID, records[i].TaskID)
		}
		if row.Status != records[i].Status {
			t.Fatalf("unexpected initial status for task %d: got %s", row.TaskID, row.Status)
		}
	}
	updates := []TargetStatusUpdate{
		{TaskID: localTable.Rows[0].TaskID, NewStatus: "processing"},
		{TaskID: localTable.Rows[1].TaskID, NewStatus: "done"},
	}
	for _, upd := range updates {
		if err := client.UpdateTargetStatus(ctx, localTable, upd.TaskID, upd.NewStatus); err != nil {
			t.Fatalf("UpdateTargetStatus live error: %v", err)
		}
	}
	for _, upd := range updates {
		recID, ok := localTable.RecordIDByTaskID(upd.TaskID)
		if !ok {
			t.Fatalf("missing record id for task %d", upd.TaskID)
		}
		rec, err := client.getBitableRecord(ctx, ref, recID)
		if err != nil {
			t.Fatalf("getBitableRecord after update error: %v", err)
		}
		row, err := decodeTargetRow(rec, fields)
		if err != nil {
			t.Fatalf("decodeTargetRow after update error: %v", err)
		}
		if row.Status != upd.NewStatus {
			t.Fatalf("task %d status mismatch after update: want %s got %s", upd.TaskID, upd.NewStatus, row.Status)
		}
	}
	t.Logf("live lifecycle test created records %v and updated statuses via record lookups", ids)
}

func TestResultRecordCreateLive(t *testing.T) {
	if os.Getenv("FEISHU_LIVE_TEST") != "1" {
		t.Skip("set FEISHU_LIVE_TEST=1 to run live result record test")
	}
	if os.Getenv("FEISHU_APP_ID") == "" || os.Getenv("FEISHU_APP_SECRET") == "" {
		t.Skip("FEISHU_APP_ID/FEISHU_APP_SECRET not configured")
	}
	ctx := context.Background()
	client, err := NewClientFromEnv()
	if err != nil {
		t.Fatalf("NewClientFromEnv error: %v", err)
	}
	now := time.Now().UTC()
	record := ResultRecordInput{
		Datetime:     &now,
		DeviceSerial: "cli-live",
		App:          "anygrab-live",
		Scene:        "result-log",
		Params:       fmt.Sprintf("{\"ts\":%d}", now.Unix()),
		ItemID:       fmt.Sprintf("live-result-%d", now.UnixNano()),
		ItemCaption:  "integration test capture",
		ItemURL:      "https://cdn.example.com/live-test.mp4",
		UserName:     "test-suite",
		UserID:       "suite",
		Tags:         "integration,auto",
		SubTaskID:    fmt.Sprintf("sub-%d", now.UnixNano()),
		PayloadJSON: map[string]any{
			"ts":     now.UnixMilli(),
			"status": "ok",
		},
	}
	id, err := client.CreateResultRecord(ctx, liveResultBitableURL, record, nil)
	if err != nil {
		t.Fatalf("CreateResultRecord live error: %v", err)
	}
	ref, err := ParseBitableURL(liveResultBitableURL)
	if err != nil {
		t.Fatalf("ParseBitableURL error: %v", err)
	}
	if err := client.ensureBitableAppToken(ctx, &ref); err != nil {
		t.Fatalf("ensureBitableAppToken error: %v", err)
	}
	got, err := client.getBitableRecord(ctx, ref, id)
	if err != nil {
		t.Fatalf("getBitableRecord error: %v", err)
	}
	if got.Fields == nil {
		t.Fatalf("expected fields in record, got nil")
	}
	if itemID := bitableOptionalString(got.Fields, DefaultResultFields.ItemID); itemID != record.ItemID {
		t.Fatalf("item id mismatch: want %s got %s", record.ItemID, itemID)
	}
	payloadStr := bitableOptionalString(got.Fields, DefaultResultFields.PayloadJSON)
	if payloadStr == "" {
		t.Fatalf("payload json missing in record")
	}
	if !json.Valid([]byte(payloadStr)) {
		t.Fatalf("payload json invalid: %s", payloadStr)
	}
	t.Logf("live result record created id=%s item_id=%s tags=%s", id, record.ItemID, record.Tags)
}
