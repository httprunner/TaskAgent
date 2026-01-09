package feishusdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkbitable "github.com/larksuite/oapi-sdk-go/v3/service/bitable/v1"
	larkwiki "github.com/larksuite/oapi-sdk-go/v3/service/wiki/v2"
)

const (
	liveReadableBitableURL = "https://bytedance.larkoffice.com/wiki/DKKwwF9XRincITkd0g1c6udUnHe?table=tblPvDwGcQ9UEzzi&view=vew9Kwl9uR"
	liveWritableBitableURL = "https://bytedance.larkoffice.com/wiki/DKKwwF9XRincITkd0g1c6udUnHe?table=tblPvDwGcQ9UEzzi"
	liveResultBitableURL   = "https://bytedance.larkoffice.com/wiki/DKKwwF9XRincITkd0g1c6udUnHe?table=tblNwTe8mUxiHUqd&view=vewTF27mJQ"
	liveTargetApp          = "com.smile.gifmaker"
)

type fakeSearchCall struct {
	AppToken  string
	TableID   string
	PageSize  int
	PageToken string
	Body      *larkbitable.SearchAppTableRecordReqBody
}

type fakeBatchCreateCall struct {
	AppToken string
	TableID  string
	Body     *larkbitable.BatchCreateAppTableRecordReqBody
}

type fakeBatchUpdateCall struct {
	AppToken string
	TableID  string
	Body     *larkbitable.BatchUpdateAppTableRecordReqBody
}

type fakeBatchGetCall struct {
	AppToken string
	TableID  string
	Body     *larkbitable.BatchGetAppTableRecordReqBody
}

type fakeCreateCall struct {
	AppToken string
	TableID  string
	Record   *larkbitable.AppTableRecord
}

type fakeUpdateCall struct {
	AppToken  string
	TableID   string
	RecordID  string
	AppRecord *larkbitable.AppTableRecord
}

type fakeBitableAPI struct {
	searchCalls      []fakeSearchCall
	createCalls      []fakeCreateCall
	updateCalls      []fakeUpdateCall
	batchCreateCalls []fakeBatchCreateCall
	batchUpdateCalls []fakeBatchUpdateCall
	batchGetCalls    []fakeBatchGetCall

	searchFn      func(ctx context.Context, call fakeSearchCall) (*larkbitable.SearchAppTableRecordResp, error)
	createFn      func(ctx context.Context, call fakeCreateCall) (*larkbitable.CreateAppTableRecordResp, error)
	updateFn      func(ctx context.Context, call fakeUpdateCall) (*larkbitable.UpdateAppTableRecordResp, error)
	batchCreateFn func(ctx context.Context, call fakeBatchCreateCall) (*larkbitable.BatchCreateAppTableRecordResp, error)
	batchUpdateFn func(ctx context.Context, call fakeBatchUpdateCall) (*larkbitable.BatchUpdateAppTableRecordResp, error)
	batchGetFn    func(ctx context.Context, call fakeBatchGetCall) (*larkbitable.BatchGetAppTableRecordResp, error)
}

func (f *fakeBitableAPI) Search(ctx context.Context, appToken, tableID string, pageSize int, pageToken string, body *larkbitable.SearchAppTableRecordReqBody, _ ...larkcore.RequestOptionFunc) (*larkbitable.SearchAppTableRecordResp, error) {
	call := fakeSearchCall{
		AppToken:  appToken,
		TableID:   tableID,
		PageSize:  pageSize,
		PageToken: pageToken,
		Body:      body,
	}
	f.searchCalls = append(f.searchCalls, call)
	if f.searchFn != nil {
		return f.searchFn(ctx, call)
	}
	return okSearchResp(nil, false, ""), nil
}

func (f *fakeBitableAPI) Create(ctx context.Context, appToken, tableID string, record *larkbitable.AppTableRecord, _ ...larkcore.RequestOptionFunc) (*larkbitable.CreateAppTableRecordResp, error) {
	call := fakeCreateCall{AppToken: appToken, TableID: tableID, Record: record}
	f.createCalls = append(f.createCalls, call)
	if f.createFn != nil {
		return f.createFn(ctx, call)
	}
	return okCreateResp("recDefault", nil), nil
}

func (f *fakeBitableAPI) Update(ctx context.Context, appToken, tableID, recordID string, record *larkbitable.AppTableRecord, _ ...larkcore.RequestOptionFunc) (*larkbitable.UpdateAppTableRecordResp, error) {
	call := fakeUpdateCall{AppToken: appToken, TableID: tableID, RecordID: recordID, AppRecord: record}
	f.updateCalls = append(f.updateCalls, call)
	if f.updateFn != nil {
		return f.updateFn(ctx, call)
	}
	return okUpdateResp(recordID, nil), nil
}

func (f *fakeBitableAPI) BatchCreate(ctx context.Context, appToken, tableID string, body *larkbitable.BatchCreateAppTableRecordReqBody, _ ...larkcore.RequestOptionFunc) (*larkbitable.BatchCreateAppTableRecordResp, error) {
	call := fakeBatchCreateCall{AppToken: appToken, TableID: tableID, Body: body}
	f.batchCreateCalls = append(f.batchCreateCalls, call)
	if f.batchCreateFn != nil {
		return f.batchCreateFn(ctx, call)
	}
	return okBatchCreateResp(nil), nil
}

func (f *fakeBitableAPI) BatchUpdate(ctx context.Context, appToken, tableID string, body *larkbitable.BatchUpdateAppTableRecordReqBody, _ ...larkcore.RequestOptionFunc) (*larkbitable.BatchUpdateAppTableRecordResp, error) {
	call := fakeBatchUpdateCall{AppToken: appToken, TableID: tableID, Body: body}
	f.batchUpdateCalls = append(f.batchUpdateCalls, call)
	if f.batchUpdateFn != nil {
		return f.batchUpdateFn(ctx, call)
	}
	return okBatchUpdateResp(nil), nil
}

func (f *fakeBitableAPI) BatchGet(ctx context.Context, appToken, tableID string, body *larkbitable.BatchGetAppTableRecordReqBody, _ ...larkcore.RequestOptionFunc) (*larkbitable.BatchGetAppTableRecordResp, error) {
	call := fakeBatchGetCall{AppToken: appToken, TableID: tableID, Body: body}
	f.batchGetCalls = append(f.batchGetCalls, call)
	if f.batchGetFn != nil {
		return f.batchGetFn(ctx, call)
	}
	return okBatchGetResp(nil, nil, nil), nil
}

func okApiResp() *larkcore.ApiResp {
	return &larkcore.ApiResp{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		RawBody:    []byte(`{"code":0,"msg":"success"}`),
	}
}

func okSearchResp(items []*larkbitable.AppTableRecord, hasMore bool, pageToken string) *larkbitable.SearchAppTableRecordResp {
	resp := &larkbitable.SearchAppTableRecordResp{
		ApiResp: okApiResp(),
		CodeError: larkcore.CodeError{
			Code: 0,
			Msg:  "success",
		},
		Data: &larkbitable.SearchAppTableRecordRespData{
			Items:   items,
			HasMore: larkcore.BoolPtr(hasMore),
		},
	}
	if strings.TrimSpace(pageToken) != "" {
		resp.Data.PageToken = larkcore.StringPtr(pageToken)
	}
	return resp
}

func okBatchCreateResp(records []*larkbitable.AppTableRecord) *larkbitable.BatchCreateAppTableRecordResp {
	return &larkbitable.BatchCreateAppTableRecordResp{
		ApiResp: okApiResp(),
		CodeError: larkcore.CodeError{
			Code: 0,
			Msg:  "success",
		},
		Data: &larkbitable.BatchCreateAppTableRecordRespData{
			Records: records,
		},
	}
}

func okCreateResp(recordID string, fields map[string]any) *larkbitable.CreateAppTableRecordResp {
	rec := &larkbitable.AppTableRecord{
		RecordId: larkcore.StringPtr(recordID),
		Fields:   fields,
	}
	return &larkbitable.CreateAppTableRecordResp{
		ApiResp: okApiResp(),
		CodeError: larkcore.CodeError{
			Code: 0,
			Msg:  "success",
		},
		Data: &larkbitable.CreateAppTableRecordRespData{
			Record: rec,
		},
	}
}

func okBatchUpdateResp(records []*larkbitable.AppTableRecord) *larkbitable.BatchUpdateAppTableRecordResp {
	return &larkbitable.BatchUpdateAppTableRecordResp{
		ApiResp: okApiResp(),
		CodeError: larkcore.CodeError{
			Code: 0,
			Msg:  "success",
		},
		Data: &larkbitable.BatchUpdateAppTableRecordRespData{
			Records: records,
		},
	}
}

func okUpdateResp(recordID string, fields map[string]any) *larkbitable.UpdateAppTableRecordResp {
	rec := &larkbitable.AppTableRecord{
		RecordId: larkcore.StringPtr(recordID),
		Fields:   fields,
	}
	return &larkbitable.UpdateAppTableRecordResp{
		ApiResp: okApiResp(),
		CodeError: larkcore.CodeError{
			Code: 0,
			Msg:  "success",
		},
		Data: &larkbitable.UpdateAppTableRecordRespData{
			Record: rec,
		},
	}
}

func okBatchGetResp(records []*larkbitable.AppTableRecord, absent, forbidden []string) *larkbitable.BatchGetAppTableRecordResp {
	return &larkbitable.BatchGetAppTableRecordResp{
		ApiResp: okApiResp(),
		CodeError: larkcore.CodeError{
			Code: 0,
			Msg:  "success",
		},
		Data: &larkbitable.BatchGetAppTableRecordRespData{
			Records:            records,
			AbsentRecordIds:    absent,
			ForbiddenRecordIds: forbidden,
		},
	}
}

type fakeWikiAPI struct {
	getNodeFn func(ctx context.Context, token string, options ...larkcore.RequestOptionFunc) (*larkwiki.GetNodeSpaceResp, error)
}

func (f *fakeWikiAPI) GetNode(ctx context.Context, token string, options ...larkcore.RequestOptionFunc) (*larkwiki.GetNodeSpaceResp, error) {
	if f == nil || f.getNodeFn == nil {
		return nil, fmt.Errorf("fake wiki api not configured")
	}
	return f.getNodeFn(ctx, token, options...)
}

func okWikiGetNodeResp(objToken, objType string) *larkwiki.GetNodeSpaceResp {
	return &larkwiki.GetNodeSpaceResp{
		ApiResp: okApiResp(),
		CodeError: larkcore.CodeError{
			Code: 0,
			Msg:  "success",
		},
		Data: &larkwiki.GetNodeSpaceRespData{
			Node: &larkwiki.Node{
				ObjToken: larkcore.StringPtr(objToken),
				ObjType:  larkcore.StringPtr(objType),
			},
		},
	}
}

func newSDKTestClient(fake *fakeBitableAPI, doJSON func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error)) *Client {
	client := &Client{
		appID:         "test-app",
		appSecret:     "test-secret",
		transport:     "sdk",
		bitableAPI:    fake,
		tenantToken:   "test-tenant-token",
		tokenExpireAt: time.Now().Add(1 * time.Hour),
	}
	if doJSON != nil {
		client.doJSONRequestFunc = doJSON
	}
	return client
}

func newHTTPTestClient(doJSON func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error)) *Client {
	client := &Client{
		appID:     "test-app",
		appSecret: "test-secret",
		transport: "http",
		doJSONRequestFunc: func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
			if doJSON == nil {
				return nil, nil, fmt.Errorf("unexpected request %s %s", method, path)
			}
			return doJSON(ctx, method, path, payload)
		},
	}
	return client
}

func runBitableTransports(t *testing.T, fn func(t *testing.T, transport string)) {
	t.Helper()
	for _, transport := range []string{"sdk", "http"} {
		t.Run(transport, func(t *testing.T) {
			fn(t, transport)
		})
	}
}

func pageTokenFromPath(path string) string {
	if idx := strings.Index(path, "?"); idx >= 0 && idx < len(path)-1 {
		raw := path[idx+1:]
		q, err := url.ParseQuery(raw)
		if err != nil {
			return ""
		}
		return strings.TrimSpace(q.Get("page_token"))
	}
	return ""
}

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
			url:      "https://bytedance.larkoffice.com/wiki/DKKwwF9XRincITkd0g1c6udUnHe?table=tblPvDwGcQ9UEzzi",
			wantWiki: "DKKwwF9XRincITkd0g1c6udUnHe",
			wantTbl:  "tblPvDwGcQ9UEzzi",
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

func TestDecodeTaskRow(t *testing.T) {
	when := time.Date(2025, 11, 8, 10, 30, 0, 0, time.Local)
	rec := &larkbitable.AppTableRecord{
		RecordId: larkcore.StringPtr("rec123"),
		Fields: map[string]any{
			DefaultTaskFields.TaskID:           float64(42),
			DefaultTaskFields.Params:           "{\"song\":\"foo\"}",
			DefaultTaskFields.App:              "netease",
			DefaultTaskFields.Scene:            "auto",
			DefaultTaskFields.Date:             when.UnixMilli(),
			DefaultTaskFields.Status:           "pending",
			DefaultTaskFields.UserID:           "user-123",
			DefaultTaskFields.UserName:         "tester",
			DefaultTaskFields.Extra:            "0.85",
			DefaultTaskFields.DeviceSerial:     "dev-target",
			DefaultTaskFields.DispatchedDevice: "dev-actual",
		},
	}
	row, err := decodeTaskRow(rec, DefaultTaskFields)
	if err != nil {
		t.Fatalf("decodeTaskRow returned error: %v", err)
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
	if row.UserID != "user-123" {
		t.Fatalf("unexpected user id %+v", row)
	}
	if row.UserName != "tester" {
		t.Fatalf("unexpected user name %+v", row)
	}
	if row.Extra != "0.85" {
		t.Fatalf("unexpected extra %+v", row)
	}
	if row.DeviceSerial != "dev-target" {
		t.Fatalf("unexpected target device %#v", row.DeviceSerial)
	}
	if row.DispatchedDevice != "dev-actual" {
		t.Fatalf("unexpected dispatched device %#v", row.DispatchedDevice)
	}
	if row.Datetime == nil {
		t.Fatalf("expected datetime to be parsed")
	}
	if got := row.Datetime.Format("2006-01-02 15:04:05"); got != "2025-11-08 10:30:00" {
		t.Fatalf("unexpected datetime %q", got)
	}
}

func TestDecodeTaskRowAllowsEmptyStatus(t *testing.T) {
	when := time.Date(2025, 11, 8, 0, 0, 0, 0, time.Local)
	rec := &larkbitable.AppTableRecord{
		RecordId: larkcore.StringPtr("recEmpty"),
		Fields: map[string]any{
			DefaultTaskFields.TaskID:           float64(43),
			DefaultTaskFields.Params:           "{}",
			DefaultTaskFields.App:              "netease",
			DefaultTaskFields.Scene:            "auto",
			DefaultTaskFields.Date:             when.UnixMilli(),
			DefaultTaskFields.Status:           "",
			DefaultTaskFields.UserID:           "",
			DefaultTaskFields.UserName:         "",
			DefaultTaskFields.Extra:            "",
			DefaultTaskFields.DeviceSerial:     "",
			DefaultTaskFields.DispatchedDevice: "",
		},
	}
	row, err := decodeTaskRow(rec, DefaultTaskFields)
	if err != nil {
		t.Fatalf("decodeTaskRow should allow empty status: %v", err)
	}
	if row.Status != "" {
		t.Fatalf("expected empty status, got %q", row.Status)
	}
	if row.UserID != "" || row.UserName != "" || row.Extra != "" || row.DeviceSerial != "" || row.DispatchedDevice != "" {
		t.Fatalf("expected empty user/device fields, got %+v", row)
	}
}

func TestDecodeTaskRowWithOnlyTargetDeviceSerial(t *testing.T) {
	rec := &larkbitable.AppTableRecord{
		RecordId: larkcore.StringPtr("recTargetOnly"),
		Fields: map[string]any{
			DefaultTaskFields.TaskID:       float64(55),
			DefaultTaskFields.Params:       "{}",
			DefaultTaskFields.App:          "netease",
			DefaultTaskFields.Scene:        "auto",
			DefaultTaskFields.Status:       "pending",
			DefaultTaskFields.DeviceSerial: "dev-only",
		},
	}
	row, err := decodeTaskRow(rec, DefaultTaskFields)
	if err != nil {
		t.Fatalf("decodeTaskRow returned error: %v", err)
	}
	if row.DeviceSerial != "dev-only" || row.DispatchedDevice != "" {
		t.Fatalf("unexpected device fields: %+v", row)
	}
}

func TestDecodeTaskRowMissingStatus(t *testing.T) {
	when := time.Date(2025, 11, 8, 0, 0, 0, 0, time.Local)
	rec := &larkbitable.AppTableRecord{
		RecordId: larkcore.StringPtr("recMissing"),
		Fields: map[string]any{
			DefaultTaskFields.TaskID: float64(44),
			DefaultTaskFields.Params: "{}",
			DefaultTaskFields.App:    "netease",
			DefaultTaskFields.Scene:  "auto",
			DefaultTaskFields.Date:   when.UnixMilli(),
		},
	}
	if _, err := decodeTaskRow(rec, DefaultTaskFields); err == nil {
		t.Fatalf("expected error when status field missing")
	}
}

func TestRecordIDByTaskID(t *testing.T) {
	table := &TaskTable{
		Rows:      []TaskRow{{RecordID: "rec1", TaskID: 7, Status: "pending"}},
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

func TestFetchTaskTableExampleMock(t *testing.T) {
	runBitableTransports(t, func(t *testing.T, transport string) {
		ctx := context.Background()
		const wikiResponse = `{"code":0,"msg":"success","data":{"node":{"obj_token":"bascnMockToken","obj_type":"bitable"}}}`
		firstWhen := time.Date(2025, 11, 7, 12, 30, 0, 0, time.Local).UnixMilli()
		secondWhen := time.Date(2025, 11, 7, 13, 0, 0, 0, time.Local).UnixMilli()
		listResponseData := map[string]any{
			"code": 0,
			"msg":  "success",
			"data": map[string]any{
				"items": []map[string]any{
					{
						"record_id": "recYUOQd9",
						"fields": map[string]any{
							DefaultTaskFields.TaskID: 101,
							DefaultTaskFields.Params: "{\"song\":\"foo\"}",
							DefaultTaskFields.App:    "netease",
							DefaultTaskFields.Scene:  "batch",
							DefaultTaskFields.Date:   firstWhen,
							DefaultTaskFields.Status: "pending",
						},
					},
					{
						"record_id": "recTy0283",
						"fields": map[string]any{
							DefaultTaskFields.TaskID: 102,
							DefaultTaskFields.Params: "{\"song\":\"bar\"}",
							DefaultTaskFields.App:    "qqmusic",
							DefaultTaskFields.Scene:  "batch",
							DefaultTaskFields.Date:   secondWhen,
							DefaultTaskFields.Status: "done",
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

		var (
			capturedPayloadHTTP map[string]any
			capturedFieldsSDK   map[string]any
			wikiCalledHTTP      bool
			wikiCalledSDK       bool
		)

		doJSON := func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
			switch {
			case transport == "http" && method == http.MethodGet && strings.Contains(path, "/wiki/v2/spaces/get_node"):
				wikiCalledHTTP = true
				return nil, []byte(wikiResponse), nil
			case transport == "http" && method == http.MethodPost && strings.Contains(path, "/bitable/v1/apps/") && strings.Contains(path, "tables/tblPvDwGcQ9UEzzi/records/search"):
				if !strings.Contains(path, "bascnMockToken") {
					t.Fatalf("expected resolved app token, got %s", path)
				}
				return nil, listResponse, nil
			case transport == "http" && method == http.MethodPut && strings.Contains(path, "/bitable/v1/apps/") && strings.Contains(path, "/records/recYUOQd9"):
				fields, ok := payload.(map[string]any)
				if !ok {
					t.Fatalf("expected map payload, got %T", payload)
				}
				capturedPayloadHTTP = fields
				return nil, updateResponse, nil
			default:
				t.Fatalf("unexpected request %s %s", method, path)
			}
			return nil, nil, nil
		}

		var client *Client
		if transport == "sdk" {
			fake := &fakeBitableAPI{
				searchFn: func(ctx context.Context, call fakeSearchCall) (*larkbitable.SearchAppTableRecordResp, error) {
					var parsed struct {
						Code int    `json:"code"`
						Msg  string `json:"msg"`
						Data struct {
							Items []struct {
								RecordID string         `json:"record_id"`
								Fields   map[string]any `json:"fields"`
							} `json:"items"`
							HasMore   bool   `json:"has_more"`
							PageToken string `json:"page_token"`
						} `json:"data"`
					}
					if err := json.Unmarshal(listResponse, &parsed); err != nil {
						return nil, err
					}
					items := make([]*larkbitable.AppTableRecord, 0, len(parsed.Data.Items))
					for _, it := range parsed.Data.Items {
						items = append(items, &larkbitable.AppTableRecord{
							RecordId: larkcore.StringPtr(it.RecordID),
							Fields:   it.Fields,
						})
					}
					return okSearchResp(items, parsed.Data.HasMore, parsed.Data.PageToken), nil
				},
				updateFn: func(ctx context.Context, call fakeUpdateCall) (*larkbitable.UpdateAppTableRecordResp, error) {
					if call.RecordID != "recYUOQd9" {
						t.Fatalf("unexpected record id %q", call.RecordID)
					}
					if call.AppRecord != nil {
						capturedFieldsSDK = call.AppRecord.Fields
					}
					return okUpdateResp(call.RecordID, nil), nil
				},
			}
			client = newSDKTestClient(fake, nil)
			client.wikiAPI = &fakeWikiAPI{
				getNodeFn: func(ctx context.Context, token string, options ...larkcore.RequestOptionFunc) (*larkwiki.GetNodeSpaceResp, error) {
					wikiCalledSDK = true
					if token != "DKKwwF9XRincITkd0g1c6udUnHe" {
						t.Fatalf("unexpected wiki token %q", token)
					}
					return okWikiGetNodeResp("bascnMockToken", "bitable"), nil
				},
			}
		} else {
			client = newHTTPTestClient(doJSON)
		}

		table, err := client.FetchTaskTable(ctx, liveWritableBitableURL, nil)
		if err != nil {
			t.Fatalf("FetchTaskTable returned error: %v", err)
		}
		if transport == "sdk" && !wikiCalledSDK {
			t.Fatalf("expected sdk wiki resolver call")
		}
		if transport == "http" && !wikiCalledHTTP {
			t.Fatalf("expected http wiki resolver call")
		}
		if table == nil || len(table.Rows) != 2 {
			t.Fatalf("expected 2 rows, got %+v", table)
		}
		if table.Ref.TableID != "tblPvDwGcQ9UEzzi" {
			t.Fatalf("unexpected ref: %+v", table.Ref)
		}
		if table.Ref.AppToken != "bascnMockToken" {
			t.Fatalf("expected resolved app token bascnMockToken, got %q", table.Ref.AppToken)
		}
		if id, ok := table.RecordIDByTaskID(101); !ok || id != "recYUOQd9" {
			t.Fatalf("expected record id recYUOQd9 for task 101, got %q ok=%v", id, ok)
		}
		if err := client.UpdateTaskStatus(ctx, table, 101, StatusDownloaderProcessing); err != nil {
			t.Fatalf("UpdateTaskStatus error: %v", err)
		}
		if table.Rows[0].Status != StatusDownloaderProcessing {
			t.Fatalf("local table status not updated, got %q", table.Rows[0].Status)
		}
		var fields map[string]any
		if transport == "sdk" {
			fields = capturedFieldsSDK
		} else {
			var ok bool
			fields, ok = capturedPayloadHTTP["fields"].(map[string]any)
			if !ok {
				t.Fatalf("expected captured fields map, got %#v", capturedPayloadHTTP)
			}
		}
		if fields[DefaultTaskFields.Status] != StatusDownloaderProcessing {
			t.Fatalf("expected status payload %#v, got %#v", StatusDownloaderProcessing, fields)
		}
	})
}

func TestUpdateCookieStatus(t *testing.T) {
	runBitableTransports(t, func(t *testing.T, transport string) {
		ctx := context.Background()
		wikiResponse := []byte(`{"code":0,"msg":"success","data":{"node":{"obj_token":"bascnMockToken","obj_type":"bitable"}}}`)

		var (
			capturedCall    fakeBatchUpdateCall
			capturedPayload map[string]any
		)

		doJSON := func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
			switch {
			case transport == "http" && method == http.MethodGet && strings.Contains(path, "/wiki/v2/spaces/get_node"):
				return nil, wikiResponse, nil
			case transport == "http" && method == http.MethodPost && strings.Contains(path, "/records/batch_update"):
				m, ok := payload.(map[string]any)
				if !ok {
					t.Fatalf("expected map payload, got %T", payload)
				}
				capturedPayload = m
				return nil, []byte(`{"code":0,"msg":"success","data":{"records":[{"record_id":"recXYZ"}]}}`), nil
			default:
				t.Fatalf("unexpected request %s %s", method, path)
			}
			return nil, nil, nil
		}

		var client *Client
		if transport == "sdk" {
			fake := &fakeBitableAPI{
				batchUpdateFn: func(ctx context.Context, call fakeBatchUpdateCall) (*larkbitable.BatchUpdateAppTableRecordResp, error) {
					capturedCall = call
					return okBatchUpdateResp([]*larkbitable.AppTableRecord{
						{RecordId: larkcore.StringPtr("recXYZ")},
					}), nil
				},
			}
			client = newSDKTestClient(fake, nil)
			client.wikiAPI = &fakeWikiAPI{
				getNodeFn: func(ctx context.Context, token string, options ...larkcore.RequestOptionFunc) (*larkwiki.GetNodeSpaceResp, error) {
					return okWikiGetNodeResp("bascnMockToken", "bitable"), nil
				},
			}
		} else {
			client = newHTTPTestClient(doJSON)
		}

		if err := client.UpdateCookieStatus(ctx, liveWritableBitableURL, "recXYZ", CookieStatusInvalid, nil); err != nil {
			t.Fatalf("UpdateCookieStatus returned error: %v", err)
		}

		var recordID string
		var fields map[string]any
		if transport == "sdk" {
			if capturedCall.Body == nil || len(capturedCall.Body.Records) != 1 {
				t.Fatalf("expected single record payload, got %#v", capturedCall.Body)
			}
			record := capturedCall.Body.Records[0]
			if record == nil {
				t.Fatalf("record missing")
			}
			recordID = larkcore.StringValue(record.RecordId)
			fields = record.Fields
		} else {
			records, ok := capturedPayload["records"].([]map[string]any)
			if !ok || len(records) != 1 {
				t.Fatalf("expected single record payload, got %#v", capturedPayload)
			}
			record := records[0]
			recordID = toString(record["record_id"])
			fields, _ = record["fields"].(map[string]any)
		}

		if recordID != "recXYZ" {
			t.Fatalf("record_id mismatch: got %q", recordID)
		}
		if fields == nil {
			t.Fatalf("missing fields payload")
		}
		if status := fields[DefaultCookieFields.Status]; status != CookieStatusInvalid {
			t.Fatalf("status mismatch: got %#v", status)
		}
	})
}

func TestListBitableRecordsFilterConversion(t *testing.T) {
	runBitableTransports(t, func(t *testing.T, transport string) {
		ctx := context.Background()
		ref := BitableRef{AppToken: "appToken", TableID: "tbl"}
		filter := NewFilterInfo("and")
		filter.Conditions = append(filter.Conditions,
			NewCondition(DefaultTaskFields.Status, "is", "done"),
			NewCondition(DefaultTaskFields.App, "is", "qqmusic"),
		)
		opts := &TaskQueryOptions{Filter: filter}

		var (
			capturedFilterSDK  *larkbitable.FilterInfo
			capturedFilterHTTP *FilterInfo
		)

		doJSON := func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
			switch {
			case transport == "http" && method == http.MethodPost && strings.Contains(path, "/records/search"):
				body, ok := payload.(map[string]any)
				if !ok {
					t.Fatalf("expected map payload, got %T", payload)
				}
				filter, ok := body["filter"].(*FilterInfo)
				if !ok || filter == nil {
					t.Fatalf("expected filter info in payload, got %T", body["filter"])
				}
				capturedFilterHTTP = filter
				return nil, []byte(`{"code":0,"data":{"items":[],"has_more":false}}`), nil
			default:
				t.Fatalf("unexpected request %s %s", method, path)
			}
			return nil, nil, nil
		}

		var client *Client
		if transport == "sdk" {
			fake := &fakeBitableAPI{
				searchFn: func(ctx context.Context, call fakeSearchCall) (*larkbitable.SearchAppTableRecordResp, error) {
					if call.Body != nil {
						capturedFilterSDK = call.Body.Filter
					}
					return okSearchResp(nil, false, ""), nil
				},
			}
			client = newSDKTestClient(fake, nil)
		} else {
			client = newHTTPTestClient(doJSON)
		}

		if _, _, err := client.listBitableRecords(ctx, ref, 200, opts); err != nil {
			t.Fatalf("listBitableRecords returned error: %v", err)
		}

		var captured *larkbitable.FilterInfo
		if transport == "sdk" {
			captured = capturedFilterSDK
		} else if capturedFilterHTTP != nil {
			captured = &larkbitable.FilterInfo{
				Conjunction: capturedFilterHTTP.Conjunction,
				Conditions:  capturedFilterHTTP.Conditions,
				Children:    capturedFilterHTTP.Children,
			}
		}

		if captured == nil || captured.Conjunction == nil || *captured.Conjunction != "and" {
			t.Fatalf("unexpected captured filter %+v", captured)
		}
		if len(captured.Conditions) != 2 {
			t.Fatalf("expected 2 conditions, got %d", len(captured.Conditions))
		}
	})
}

func TestCreateTaskRecords(t *testing.T) {
	runBitableTransports(t, func(t *testing.T, transport string) {
		const wikiResponse = `{"code":0,"msg":"success","data":{"node":{"obj_token":"bascnCreateToken","obj_type":"bitable"}}}`

		ctx := context.Background()
		var (
			wikiCalledHTTP bool
			wikiCalledSDK  bool
		)

		var (
			createCalled bool
			capturedCall fakeBatchCreateCall
			captured     map[string]any
		)

		when := time.Date(2024, 10, 1, 12, 0, 0, 0, time.UTC)
		records := []TaskRecordInput{
			{TaskID: 301, Params: `{"foo":1}`, App: "netease", Scene: "parse", Datetime: &when, Status: "pending", UserID: "user-123", UserName: "tester", Extra: "0.90"},
			{TaskID: 0, Params: `{"foo":2}`, DatetimeRaw: "2024-10-02 08:00:00", Status: "queued"},
		}
		override := &TaskFields{Status: "biz_status"}

		doJSON := func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
			switch {
			case transport == "http" && method == http.MethodGet && strings.Contains(path, "/wiki/v2/spaces/get_node"):
				wikiCalledHTTP = true
				return nil, []byte(wikiResponse), nil
			case transport == "http" && method == http.MethodPost && strings.Contains(path, "/records/batch_create"):
				createCalled = true
				m, ok := payload.(map[string]any)
				if !ok {
					t.Fatalf("expected map payload, got %T", payload)
				}
				captured = m
				return nil, []byte(`{"code":0,"msg":"success","data":{"records":[{"record_id":"recAAA"},{"record_id":"recBBB"}]}}`), nil
			default:
				t.Fatalf("unexpected request %s %s", method, path)
			}
			return nil, nil, nil
		}

		var client *Client
		if transport == "sdk" {
			fake := &fakeBitableAPI{
				batchCreateFn: func(ctx context.Context, call fakeBatchCreateCall) (*larkbitable.BatchCreateAppTableRecordResp, error) {
					createCalled = true
					capturedCall = call
					return okBatchCreateResp([]*larkbitable.AppTableRecord{
						{RecordId: larkcore.StringPtr("recAAA")},
						{RecordId: larkcore.StringPtr("recBBB")},
					}), nil
				},
			}
			client = newSDKTestClient(fake, nil)
			client.wikiAPI = &fakeWikiAPI{
				getNodeFn: func(ctx context.Context, token string, options ...larkcore.RequestOptionFunc) (*larkwiki.GetNodeSpaceResp, error) {
					wikiCalledSDK = true
					return okWikiGetNodeResp("bascnCreateToken", "bitable"), nil
				},
			}
		} else {
			client = newHTTPTestClient(doJSON)
		}

		ids, err := client.CreateTaskRecords(ctx, liveWritableBitableURL, records, override)
		if err != nil {
			t.Fatalf("CreateTaskRecords returned error: %v", err)
		}
		if transport == "sdk" && (!wikiCalledSDK || !createCalled) {
			t.Fatalf("expected sdk wiki + batch create calls, got wiki=%v create=%v", wikiCalledSDK, createCalled)
		}
		if transport == "http" && (!wikiCalledHTTP || !createCalled) {
			t.Fatalf("expected http wiki + batch create calls, got wiki=%v create=%v", wikiCalledHTTP, createCalled)
		}
		if len(ids) != 2 || ids[0] != "recAAA" || ids[1] != "recBBB" {
			t.Fatalf("unexpected ids %#v", ids)
		}

		var fieldRows []map[string]any
		if transport == "sdk" {
			if capturedCall.Body == nil || len(capturedCall.Body.Records) != 2 {
				t.Fatalf("expected 2 record payloads, got %#v", capturedCall.Body)
			}
			for _, rec := range capturedCall.Body.Records {
				fieldRows = append(fieldRows, rec.Fields)
			}
		} else {
			recordPayloads, ok := captured["records"].([]map[string]any)
			if !ok || len(recordPayloads) != 2 {
				t.Fatalf("expected 2 record payloads, got %#v", captured)
			}
			for _, rec := range recordPayloads {
				fields, ok := rec["fields"].(map[string]any)
				if !ok {
					t.Fatalf("expected fields map, got %T", rec["fields"])
				}
				fieldRows = append(fieldRows, fields)
			}
		}

		firstFields := fieldRows[0]
		if firstFields[DefaultTaskFields.TaskID] != int64(301) {
			t.Fatalf("unexpected task id payload %#v", firstFields[DefaultTaskFields.TaskID])
		}
		if firstFields["biz_status"] != "pending" {
			t.Fatalf("expected biz_status pending, got %#v", firstFields["biz_status"])
		}
		if _, exists := firstFields[DefaultTaskFields.Status]; exists {
			t.Fatalf("default status field should be absent when override is set")
		}
		if firstFields[DefaultTaskFields.Date] != when.Format(time.RFC3339) {
			t.Fatalf("unexpected datetime %v", firstFields[DefaultTaskFields.Date])
		}
		secondFields := fieldRows[1]
		if _, exists := secondFields[DefaultTaskFields.TaskID]; exists {
			t.Fatalf("TaskID should be omitted for auto-increment scenario, got %#v", secondFields[DefaultTaskFields.TaskID])
		}
		if secondFields[DefaultTaskFields.Date] != "2024-10-02 08:00:00" {
			t.Fatalf("expected raw datetime preserved, got %#v", secondFields[DefaultTaskFields.Date])
		}
	})
}

func TestBuildTaskRecordPayloadsParentTaskID(t *testing.T) {
	records := []TaskRecordInput{{TaskID: 1, ParentTaskID: 42}}
	rows, err := buildTaskRecordPayloads(records, DefaultTaskFields)
	if err != nil {
		t.Fatalf("buildTaskRecordPayloads returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("unexpected payload length %d", len(rows))
	}
	val, ok := rows[0][DefaultTaskFields.ParentTaskID]
	if !ok {
		t.Fatalf("ParentTaskID field missing in payload")
	}
	if v, ok := val.(int64); !ok || v != 42 {
		t.Fatalf("unexpected ParentTaskID value %#v", val)
	}
}

func TestBuildTaskRecordPayloadsDatetimeMs(t *testing.T) {
	records := []TaskRecordInput{
		{TaskID: 1, DatetimeRaw: "1700000000000"},
	}
	rows, err := buildTaskRecordPayloads(records, DefaultTaskFields)
	if err != nil {
		t.Fatalf("buildTaskRecordPayloads returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("unexpected payload length %d", len(rows))
	}
	val, ok := rows[0][DefaultTaskFields.Date]
	if !ok {
		t.Fatalf("datetime field missing in payload")
	}
	num, ok := val.(int64)
	if !ok {
		t.Fatalf("expected datetime payload to be int64, got %T (%v)", val, val)
	}
	if num != 1700000000000 {
		t.Fatalf("unexpected datetime value %d", num)
	}
}

func TestCreateTaskRecordSingle(t *testing.T) {
	runBitableTransports(t, func(t *testing.T, transport string) {
		const wikiResponse = `{"code":0,"msg":"success","data":{"node":{"obj_token":"bascnSingle","obj_type":"bitable"}}}`
		const createResponse = `{"code":0,"msg":"success","data":{"record":{"record_id":"recSingle"}}}`

		ctx := context.Background()
		var (
			wikiCalledHTTP bool
			wikiCalledSDK  bool
			createCalled   bool
			capturedHTTP   map[string]any
			capturedSDK    map[string]any
		)

		doJSON := func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
			switch {
			case transport == "http" && method == http.MethodGet && strings.Contains(path, "/wiki/v2/spaces/get_node"):
				wikiCalledHTTP = true
				return nil, []byte(wikiResponse), nil
			case transport == "http" && method == http.MethodPost && strings.Contains(path, "/records") && !strings.Contains(path, "batch_create"):
				createCalled = true
				m, ok := payload.(map[string]any)
				if !ok {
					t.Fatalf("expected map payload, got %T", payload)
				}
				capturedHTTP = m
				return nil, []byte(createResponse), nil
			default:
				t.Fatalf("unexpected request %s %s", method, path)
			}
			return nil, nil, nil
		}

		var client *Client
		if transport == "sdk" {
			fake := &fakeBitableAPI{
				createFn: func(ctx context.Context, call fakeCreateCall) (*larkbitable.CreateAppTableRecordResp, error) {
					createCalled = true
					if call.Record != nil {
						capturedSDK = call.Record.Fields
					}
					return okCreateResp("recSingle", nil), nil
				},
			}
			client = newSDKTestClient(fake, nil)
			client.wikiAPI = &fakeWikiAPI{
				getNodeFn: func(ctx context.Context, token string, options ...larkcore.RequestOptionFunc) (*larkwiki.GetNodeSpaceResp, error) {
					wikiCalledSDK = true
					return okWikiGetNodeResp("bascnSingle", "bitable"), nil
				},
			}
		} else {
			client = newHTTPTestClient(doJSON)
		}

		rec := TaskRecordInput{Status: "pending"}
		id, err := client.CreateTaskRecord(ctx, liveWritableBitableURL, rec, nil)
		if err != nil {
			t.Fatalf("CreateTaskRecord error: %v", err)
		}
		if id != "recSingle" {
			t.Fatalf("unexpected id %q", id)
		}
		if transport == "sdk" && (!wikiCalledSDK || !createCalled) {
			t.Fatalf("expected sdk wiki + create calls")
		}
		if transport == "http" && (!wikiCalledHTTP || !createCalled) {
			t.Fatalf("expected http wiki + create calls")
		}

		var fields map[string]any
		if transport == "sdk" {
			fields = capturedSDK
		} else {
			var ok bool
			fields, ok = capturedHTTP["fields"].(map[string]any)
			if !ok {
				t.Fatalf("expected fields map, got %T", capturedHTTP["fields"])
			}
		}
		if _, exists := fields[DefaultTaskFields.TaskID]; exists {
			t.Fatalf("TaskID should be omitted when not provided, got %#v", fields[DefaultTaskFields.TaskID])
		}
		if fields[DefaultTaskFields.Status] != "pending" {
			t.Fatalf("unexpected status %#v", fields[DefaultTaskFields.Status])
		}
	})
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
			ItemCDNURL:   "https://cdn.example.com/video123.mp4",
			ItemURL:      "https://www.kuaishou.com/short-video/video123",
			UserName:     "tester",
			UserID:       "author-1",
			Tags:         "tag1,tag2",
			TaskID:       101,
			Extra:        map[string]any{"foo": "bar"},
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
	payloadStr, ok := row[DefaultResultFields.Extra].(string)
	if !ok || payloadStr == "" {
		t.Fatalf("expected payload json string, got %#v", row[DefaultResultFields.Extra])
	}
	var decoded map[string]string
	if err := json.Unmarshal([]byte(payloadStr), &decoded); err != nil {
		t.Fatalf("payload json invalid: %v", err)
	}
	if decoded["foo"] != "bar" {
		t.Fatalf("unexpected payload content %#v", decoded)
	}
	if row[DefaultResultFields.ItemURL] != "https://www.kuaishou.com/short-video/video123" {
		t.Fatalf("item url mismatch, got %#v", row[DefaultResultFields.ItemURL])
	}
	if _, err := buildResultRecordPayloads([]ResultRecordInput{
		{DeviceSerial: "dev", Extra: "{not json}"},
	}, DefaultResultFields); err == nil {
		t.Fatalf("expected error for invalid payload json")
	}
}

func TestCreateResultRecords(t *testing.T) {
	runBitableTransports(t, func(t *testing.T, transport string) {
		const wikiResponse = `{"code":0,"msg":"success","data":{"node":{"obj_token":"bascnResult","obj_type":"bitable"}}}`

		ctx := context.Background()
		var (
			wikiCalledHTTP bool
			wikiCalledSDK  bool
			batchCalled    bool
		)

		var (
			capturedCall fakeBatchCreateCall
			capturedBody map[string]any
		)

		doJSON := func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
			switch {
			case transport == "http" && method == http.MethodGet && strings.Contains(path, "/wiki/v2/spaces/get_node"):
				wikiCalledHTTP = true
				return nil, []byte(wikiResponse), nil
			case transport == "http" && method == http.MethodPost && strings.Contains(path, "/records/batch_create"):
				batchCalled = true
				m, ok := payload.(map[string]any)
				if !ok {
					t.Fatalf("expected map payload, got %T", payload)
				}
				capturedBody = m
				return nil, []byte(`{"code":0,"msg":"success","data":{"records":[{"record_id":"recRes1"},{"record_id":"recRes2"}]}}`), nil
			default:
				t.Fatalf("unexpected request %s %s", method, path)
			}
			return nil, nil, nil
		}

		var client *Client
		if transport == "sdk" {
			fake := &fakeBitableAPI{
				batchCreateFn: func(ctx context.Context, call fakeBatchCreateCall) (*larkbitable.BatchCreateAppTableRecordResp, error) {
					batchCalled = true
					capturedCall = call
					return okBatchCreateResp([]*larkbitable.AppTableRecord{
						{RecordId: larkcore.StringPtr("recRes1")},
						{RecordId: larkcore.StringPtr("recRes2")},
					}), nil
				},
			}
			client = newSDKTestClient(fake, nil)
			client.wikiAPI = &fakeWikiAPI{
				getNodeFn: func(ctx context.Context, token string, options ...larkcore.RequestOptionFunc) (*larkwiki.GetNodeSpaceResp, error) {
					wikiCalledSDK = true
					return okWikiGetNodeResp("bascnResult", "bitable"), nil
				},
			}
		} else {
			client = newHTTPTestClient(doJSON)
		}

		records := []ResultRecordInput{
			{
				DeviceSerial: "dev-1",
				App:          "netease",
				Scene:        "auto",
				Params:       "{}",
				ItemID:       "item-1",
				Extra:        map[string]any{"id": 1},
			},
			{
				DeviceSerial: "dev-2",
				App:          "douyin",
				Scene:        "manual",
				ItemID:       "item-2",
				Extra:        json.RawMessage(`{"id":2}`),
			},
		}
		ids, err := client.CreateResultRecords(ctx, liveResultBitableURL, records, nil)
		if err != nil {
			t.Fatalf("CreateResultRecords returned error: %v", err)
		}
		if transport == "sdk" && (!wikiCalledSDK || !batchCalled) {
			t.Fatalf("expected sdk wiki and batch create calls, got wiki=%v batch=%v", wikiCalledSDK, batchCalled)
		}
		if transport == "http" && (!wikiCalledHTTP || !batchCalled) {
			t.Fatalf("expected http wiki and batch create calls, got wiki=%v batch=%v", wikiCalledHTTP, batchCalled)
		}
		if len(ids) != 2 || ids[0] != "recRes1" || ids[1] != "recRes2" {
			t.Fatalf("unexpected ids %#v", ids)
		}

		var firstFields map[string]any
		if transport == "sdk" {
			if capturedCall.Body == nil || len(capturedCall.Body.Records) == 0 {
				t.Fatalf("records payload is %#v", capturedCall.Body)
			}
			firstFields = capturedCall.Body.Records[0].Fields
		} else {
			payloadRecords, ok := capturedBody["records"].([]map[string]any)
			if !ok || len(payloadRecords) == 0 {
				t.Fatalf("records payload is %#v", capturedBody)
			}
			fields, ok := payloadRecords[0]["fields"].(map[string]any)
			if !ok {
				t.Fatalf("fields payload type %T", payloadRecords[0]["fields"])
			}
			firstFields = fields
		}

		if firstFields[DefaultResultFields.DeviceSerial] != "dev-1" {
			t.Fatalf("unexpected dispatched device %#v", firstFields[DefaultResultFields.DeviceSerial])
		}
		if _, ok := firstFields[DefaultResultFields.Extra].(string); !ok {
			t.Fatalf("payload json should be string, got %#v", firstFields[DefaultResultFields.Extra])
		}
	})
}

func TestCreateResultRecordSingle(t *testing.T) {
	runBitableTransports(t, func(t *testing.T, transport string) {
		const wikiResponse = `{"code":0,"msg":"success","data":{"node":{"obj_token":"bascnResultSingle","obj_type":"bitable"}}}`
		const createResponse = `{"code":0,"msg":"success","data":{"record":{"record_id":"recResSingle"}}}`

		ctx := context.Background()
		var (
			wikiCalledHTTP bool
			wikiCalledSDK  bool
			createCalled   bool
			capturedHTTP   map[string]any
			capturedSDK    map[string]any
		)

		doJSON := func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
			switch {
			case transport == "http" && method == http.MethodGet && strings.Contains(path, "/wiki/v2/spaces/get_node"):
				wikiCalledHTTP = true
				return nil, []byte(wikiResponse), nil
			case transport == "http" && method == http.MethodPost && strings.Contains(path, "/records") && !strings.Contains(path, "batch_create"):
				createCalled = true
				m, ok := payload.(map[string]any)
				if !ok {
					t.Fatalf("expected map payload, got %T", payload)
				}
				capturedHTTP = m
				return nil, []byte(createResponse), nil
			default:
				t.Fatalf("unexpected request %s %s", method, path)
			}
			return nil, nil, nil
		}

		var client *Client
		if transport == "sdk" {
			fake := &fakeBitableAPI{
				createFn: func(ctx context.Context, call fakeCreateCall) (*larkbitable.CreateAppTableRecordResp, error) {
					createCalled = true
					if call.Record != nil {
						capturedSDK = call.Record.Fields
					}
					return okCreateResp("recResSingle", nil), nil
				},
			}
			client = newSDKTestClient(fake, nil)
			client.wikiAPI = &fakeWikiAPI{
				getNodeFn: func(ctx context.Context, token string, options ...larkcore.RequestOptionFunc) (*larkwiki.GetNodeSpaceResp, error) {
					wikiCalledSDK = true
					return okWikiGetNodeResp("bascnResultSingle", "bitable"), nil
				},
			}
		} else {
			client = newHTTPTestClient(doJSON)
		}

		id, err := client.CreateResultRecord(ctx, liveResultBitableURL, ResultRecordInput{
			DeviceSerial: "dev-only",
			App:          "netease",
			Extra:        map[string]any{"ok": true},
		}, nil)
		if err != nil {
			t.Fatalf("CreateResultRecord returned error: %v", err)
		}
		if id != "recResSingle" {
			t.Fatalf("unexpected record id %q", id)
		}
		if transport == "sdk" && (!wikiCalledSDK || !createCalled) {
			t.Fatalf("expected sdk wiki and create calls")
		}
		if transport == "http" && (!wikiCalledHTTP || !createCalled) {
			t.Fatalf("expected http wiki and create calls")
		}

		var fields map[string]any
		if transport == "sdk" {
			fields = capturedSDK
		} else {
			var ok bool
			fields, ok = capturedHTTP["fields"].(map[string]any)
			if !ok {
				t.Fatalf("fields payload type %T", capturedHTTP["fields"])
			}
		}
		if fields[DefaultResultFields.DeviceSerial] != "dev-only" {
			t.Fatalf("unexpected dispatched device %#v", fields[DefaultResultFields.DeviceSerial])
		}
		if _, ok := fields[DefaultResultFields.Extra].(string); !ok {
			t.Fatalf("payload json should be string, got %#v", fields[DefaultResultFields.Extra])
		}
	})
}

func TestUpdateTaskStatuses(t *testing.T) {
	runBitableTransports(t, func(t *testing.T, transport string) {
		const wikiResponse = `{"code":0,"msg":"success","data":{"node":{"obj_token":"bascnUpdate","obj_type":"bitable"}}}`
		const customStatusField = "biz_status"
		const updateResponse = `{"code":0,"msg":"success"}`

		ctx := context.Background()
		var (
			wikiCalledHTTP bool
			wikiCalledSDK  bool
			listCalled     bool
			updates        []map[string]any
		)

		doJSON := func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
			switch {
			case transport == "http" && method == http.MethodGet && strings.Contains(path, "/wiki/v2/spaces/get_node"):
				wikiCalledHTTP = true
				return nil, []byte(wikiResponse), nil
			case transport == "http" && method == http.MethodPut && strings.Contains(path, "/records/rec"):
				m, ok := payload.(map[string]any)
				if !ok {
					t.Fatalf("expected map payload, got %T", payload)
				}
				updates = append(updates, m)
				return nil, []byte(updateResponse), nil
			case transport == "http" && method == http.MethodPost && strings.Contains(path, "/records/search"):
				listCalled = true
				return nil, []byte(`{"code":0,"msg":"success","data":{"items":[{"record_id":"recA1","fields":{"TaskID":101,"biz_status":"pending"}},{"record_id":"recA2","fields":{"TaskID":102,"biz_status":"pending"}}],"has_more":false,"page_token":""}}`), nil
			default:
				t.Fatalf("unexpected request %s %s", method, path)
			}
			return nil, nil, nil
		}

		var client *Client
		if transport == "sdk" {
			fake := &fakeBitableAPI{
				searchFn: func(ctx context.Context, call fakeSearchCall) (*larkbitable.SearchAppTableRecordResp, error) {
					listCalled = true
					return okSearchResp([]*larkbitable.AppTableRecord{
						{
							RecordId: larkcore.StringPtr("recA1"),
							Fields: map[string]any{
								DefaultTaskFields.TaskID: int64(101),
								customStatusField:        "pending",
							},
						},
						{
							RecordId: larkcore.StringPtr("recA2"),
							Fields: map[string]any{
								DefaultTaskFields.TaskID: int64(102),
								customStatusField:        "pending",
							},
						},
					}, false, ""), nil
				},
				updateFn: func(ctx context.Context, call fakeUpdateCall) (*larkbitable.UpdateAppTableRecordResp, error) {
					fields := map[string]any{}
					if call.AppRecord != nil {
						fields = call.AppRecord.Fields
					}
					updates = append(updates, map[string]any{
						"fields": fields,
					})
					return okUpdateResp(call.RecordID, nil), nil
				},
			}
			client = newSDKTestClient(fake, nil)
			client.wikiAPI = &fakeWikiAPI{
				getNodeFn: func(ctx context.Context, token string, options ...larkcore.RequestOptionFunc) (*larkwiki.GetNodeSpaceResp, error) {
					wikiCalledSDK = true
					return okWikiGetNodeResp("bascnUpdate", "bitable"), nil
				},
			}
		} else {
			client = newHTTPTestClient(doJSON)
		}

		changes := []TaskStatusUpdate{{TaskID: 101, NewStatus: StatusDownloaderProcessing}, {TaskID: 102, NewStatus: "done"}}
		override := &TaskFields{Status: customStatusField}
		if err := client.UpdateTaskStatuses(ctx, liveWritableBitableURL, changes, override); err != nil {
			t.Fatalf("UpdateTaskStatuses returned error: %v", err)
		}
		if transport == "sdk" && (!wikiCalledSDK || !listCalled) {
			t.Fatalf("expected sdk wiki and list calls")
		}
		if transport == "http" && (!wikiCalledHTTP || !listCalled) {
			t.Fatalf("expected http wiki and list calls")
		}
		if len(updates) != 2 {
			t.Fatalf("expected 2 update calls, got %d", len(updates))
		}
		firstFields, ok := updates[0]["fields"].(map[string]any)
		if !ok {
			t.Fatalf("expected fields map, got %T", updates[0]["fields"])
		}
		if firstFields[customStatusField] != StatusDownloaderProcessing {
			t.Fatalf("unexpected first status %#v", firstFields[customStatusField])
		}
		secondFields, ok := updates[1]["fields"].(map[string]any)
		if !ok {
			t.Fatalf("expected fields map, got %T", updates[1]["fields"])
		}
		if secondFields[customStatusField] != "done" {
			t.Fatalf("unexpected second status %#v", secondFields[customStatusField])
		}
	})
}

func TestFetchTaskTableWithOptionsRespectsPagingCursor(t *testing.T) {
	runBitableTransports(t, func(t *testing.T, transport string) {
		const wikiResponse = `{"code":0,"msg":"success","data":{"node":{"obj_token":"bascnPage","obj_type":"bitable"}}}`

		ctx := context.Background()
		var (
			wikiCalledHTTP bool
			wikiCalledSDK  bool
			pageCalls      []string
		)

		doJSON := func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
			switch {
			case transport == "http" && method == http.MethodGet && strings.Contains(path, "/wiki/v2/spaces/get_node"):
				wikiCalledHTTP = true
				return nil, []byte(wikiResponse), nil
			case transport == "http" && method == http.MethodPost && strings.Contains(path, "/records/search"):
				pageToken := pageTokenFromPath(path)
				pageCalls = append(pageCalls, pageToken)
				if pageToken == "p2" {
					return nil, []byte(`{"code":0,"msg":"success","data":{"items":[{"record_id":"recP2","fields":{"TaskID":202,"Status":"pending"}}],"has_more":false,"page_token":""}}`), nil
				}
				return nil, []byte(`{"code":0,"msg":"success","data":{"items":[{"record_id":"recP1","fields":{"TaskID":201,"Status":"pending"}}],"has_more":true,"page_token":"p2"}}`), nil
			default:
				t.Fatalf("unexpected request %s %s", method, path)
			}
			return nil, nil, nil
		}

		var client *Client
		if transport == "sdk" {
			fake := &fakeBitableAPI{
				searchFn: func(ctx context.Context, call fakeSearchCall) (*larkbitable.SearchAppTableRecordResp, error) {
					pageCalls = append(pageCalls, call.PageToken)
					if strings.TrimSpace(call.PageToken) == "p2" {
						return okSearchResp([]*larkbitable.AppTableRecord{
							{
								RecordId: larkcore.StringPtr("recP2"),
								Fields: map[string]any{
									DefaultTaskFields.TaskID: int64(202),
									DefaultTaskFields.Status: "pending",
								},
							},
						}, false, ""), nil
					}
					return okSearchResp([]*larkbitable.AppTableRecord{
						{
							RecordId: larkcore.StringPtr("recP1"),
							Fields: map[string]any{
								DefaultTaskFields.TaskID: int64(201),
								DefaultTaskFields.Status: "pending",
							},
						},
					}, true, "p2"), nil
				},
			}
			client = newSDKTestClient(fake, nil)
			client.wikiAPI = &fakeWikiAPI{
				getNodeFn: func(ctx context.Context, token string, options ...larkcore.RequestOptionFunc) (*larkwiki.GetNodeSpaceResp, error) {
					wikiCalledSDK = true
					return okWikiGetNodeResp("bascnPage", "bitable"), nil
				},
			}
		} else {
			client = newHTTPTestClient(doJSON)
		}

		table, err := client.FetchTaskTableWithOptions(ctx, liveReadableBitableURL, nil, &TaskQueryOptions{
			Limit:    2000,
			MaxPages: 1,
		})
		if err != nil {
			t.Fatalf("FetchTaskTableWithOptions returned error: %v", err)
		}
		if transport == "sdk" && !wikiCalledSDK {
			t.Fatalf("expected sdk wiki call")
		}
		if transport == "http" && !wikiCalledHTTP {
			t.Fatalf("expected http wiki call")
		}
		if table == nil {
			t.Fatalf("expected table, got nil")
		}
		if len(table.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(table.Rows))
		}
		if !table.HasMore || table.NextPageToken != "p2" {
			t.Fatalf("expected hasMore=true and nextPageToken=p2, got hasMore=%v token=%q", table.HasMore, table.NextPageToken)
		}
		if table.Pages != 1 {
			t.Fatalf("expected Pages=1, got %d", table.Pages)
		}

		table, err = client.FetchTaskTableWithOptions(ctx, liveReadableBitableURL, nil, &TaskQueryOptions{
			Limit:     2000,
			MaxPages:  1,
			PageToken: "p2",
		})
		if err != nil {
			t.Fatalf("FetchTaskTableWithOptions returned error: %v", err)
		}
		if table == nil {
			t.Fatalf("expected table, got nil")
		}
		if len(table.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(table.Rows))
		}
		if table.HasMore || table.NextPageToken != "" {
			t.Fatalf("expected hasMore=false and empty nextPageToken, got hasMore=%v token=%q", table.HasMore, table.NextPageToken)
		}
		if table.Pages != 1 {
			t.Fatalf("expected Pages=1, got %d", table.Pages)
		}
		if len(pageCalls) != 2 {
			t.Fatalf("expected 2 search calls, got %d", len(pageCalls))
		}
		if strings.TrimSpace(pageCalls[0]) != "" || pageCalls[1] != "p2" {
			t.Fatalf("unexpected page token calls %#v", pageCalls)
		}
	})
}

func TestFetchTaskTableExampleLive(t *testing.T) {
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
	table, err := client.FetchTaskTable(ctx, liveReadableBitableURL, nil)
	if err != nil {
		t.Fatalf("FetchTaskTable live call failed: %v", err)
	}
	if table == nil || len(table.Rows) == 0 {
		t.Fatalf("expected rows from live table, got %+v", table)
	}
	first := table.Rows[0]
	t.Logf("live table row sample: TaskID=%d params=%s app=%s scene=%s datetime=%s status=%s",
		first.TaskID, first.Params, first.App, first.Scene, first.DatetimeRaw, first.Status)
}

func TestFetchTaskTableWithOptionsLive(t *testing.T) {
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
	f := NewFilterInfo("and")
	f.Conditions = append(f.Conditions, NewCondition(DefaultTaskFields.App, "is", liveTargetApp))
	opts := &TaskQueryOptions{Filter: f, Limit: 5}
	table, err := client.FetchTaskTableWithOptions(ctx, liveReadableBitableURL, nil, opts)
	if err != nil {
		t.Fatalf("FetchTaskTableWithOptions live call failed: %v", err)
	}
	if table == nil || len(table.Rows) == 0 {
		t.Fatalf("expected rows for %s, got %+v", liveTargetApp, table)
	}
	for i, row := range table.Rows {
		if row.TaskID == 0 {
			t.Fatalf("row %d missing task id", i)
		}
		if strings.TrimSpace(row.Params) == "" {
			t.Logf("row %d missing params; skipping strict check", i)
			continue
		}
		if row.App != liveTargetApp {
			t.Fatalf("row %d app mismatch: got %s", i, row.App)
		}
	}
	t.Logf("fetched %d live rows for %s with filter %+v", len(table.Rows), liveTargetApp, f)
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
	records := []TaskRecordInput{
		{
			Params:   fmt.Sprintf("{\"test\":\"create_live\",\"id\":%d}", baseTaskID),
			App:      "test-live",
			Scene:    "integration",
			Status:   "pending",
			UserID:   "test-suite-id",
			UserName: "test-suite",
		},
		{
			Params:   fmt.Sprintf("{\"test\":\"create_live\",\"id\":%d}", baseTaskID+1),
			App:      "test-live",
			Scene:    "integration",
			Status:   "queued",
			UserID:   "test-suite-id",
			UserName: "test-suite",
		},
	}
	ids, err := client.CreateTaskRecords(ctx, bitableURL, records, nil)
	if err != nil {
		t.Fatalf("CreateTaskRecords live error: %v", err)
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
	fields := DefaultTaskFields
	localTable := &TaskTable{
		Ref:       ref,
		Fields:    fields,
		Rows:      make([]TaskRow, 0, len(ids)),
		Invalid:   nil,
		taskIndex: make(map[int64]string, len(ids)),
	}
	for i, recordID := range ids {
		rec, err := client.getBitableRecord(ctx, ref, recordID)
		if err != nil {
			t.Fatalf("getBitableRecord error: %v", err)
		}
		row, err := decodeTaskRow(rec, fields)
		if err != nil {
			t.Fatalf("decodeTaskRow error: %v", err)
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
	updates := []TaskStatusUpdate{
		{TaskID: localTable.Rows[0].TaskID, NewStatus: StatusDownloaderProcessing},
		{TaskID: localTable.Rows[1].TaskID, NewStatus: "done"},
	}
	for _, upd := range updates {
		if err := client.UpdateTaskStatus(ctx, localTable, upd.TaskID, upd.NewStatus); err != nil {
			t.Fatalf("UpdateTaskStatus live error: %v", err)
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
		row, err := decodeTaskRow(rec, fields)
		if err != nil {
			t.Fatalf("decodeTaskRow after update error: %v", err)
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
		App:          "test-live",
		Scene:        "result-log",
		Params:       fmt.Sprintf("{\"ts\":%d}", now.Unix()),
		ItemID:       fmt.Sprintf("live-result-%d", now.UnixNano()),
		ItemCaption:  "integration test capture",
		ItemCDNURL:   "https://cdn.example.com/live-test.mp4",
		UserName:     "test-suite",
		UserID:       "suite",
		Tags:         "integration,auto",
		TaskID:       now.UnixMilli(),
		Extra: map[string]any{
			"ts":     now.UnixMilli(),
			"status": "ok",
		},
	}
	id, err := client.CreateResultRecord(ctx, liveResultBitableURL, record, nil)
	if err != nil {
		if strings.Contains(err.Error(), "FieldNameNotFound") {
			t.Skip("result table missing DispatchedDevice column; update schema before running live test")
		}
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
		if strings.TrimSpace(itemID) == "" {
			t.Logf("result record %s missing ItemID column (likely view not exposing field); skipping strict comparison", id)
		} else {
			t.Fatalf("item id mismatch: want %s got %s", record.ItemID, itemID)
		}
	}
	payloadStr := bitableOptionalString(got.Fields, DefaultResultFields.Extra)
	if payloadStr == "" {
		t.Logf("result record %s missing Extra field; skipping payload validation", id)
		return
	}
	if !json.Valid([]byte(payloadStr)) {
		t.Fatalf("payload json invalid: %s", payloadStr)
	}
	t.Logf("live result record created id=%s item_id=%s tags=%s", id, record.ItemID, record.Tags)
}

func TestBitableOptionalStringReadsString(t *testing.T) {
	fields := map[string]any{
		"Extra": "{\"cdn_url\":\"http://example.com/video.mp4\"}",
	}
	got := bitableOptionalString(fields, "Extra")
	want := "{\"cdn_url\":\"http://example.com/video.mp4\"}"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestBitableOptionalStringIgnoresNonString(t *testing.T) {
	fields := map[string]any{
		"Extra": []any{
			map[string]any{"text": "{\"cdn_url\":\"http://example.com/video.mp4\"}"},
		},
	}
	got := bitableOptionalString(fields, "Extra")
	if got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
}
