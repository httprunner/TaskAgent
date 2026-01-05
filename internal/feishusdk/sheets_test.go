package feishusdk

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larksheets "github.com/larksuite/oapi-sdk-go/v3/service/sheets/v3"
)

func TestSheetsV2FetchValues_Transports(t *testing.T) {
	runBitableTransports(t, func(t *testing.T, transport string) {
		ctx := context.Background()

		ref := SpreadsheetRef{SpreadsheetToken: "sht123"}
		col := 2
		row := 2
		sheet := &larksheets.Sheet{
			SheetId: larkcore.StringPtr("sheet1"),
			GridProperties: &larksheets.GridProperties{
				ColumnCount: &col,
				RowCount:    &row,
			},
		}
		rangeStr := "sheet1!A1:B2"

		const valuesResponse = `{"code":0,"msg":"success","data":{"valueRange":{"range":"sheet1!A1:B2","values":[["A","B"],["1","2"]]}}}`

		var client *Client
		if transport == "http" {
			client = newHTTPTestClient(func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
				if method != http.MethodGet {
					t.Fatalf("unexpected method %s", method)
				}
				wantPath := "/open-apis/sheets/v2/spreadsheets/sht123/values/" + url.PathEscape(rangeStr)
				if path != wantPath {
					t.Fatalf("unexpected path %q, want %q", path, wantPath)
				}
				return nil, []byte(valuesResponse), nil
			})
		} else {
			client = newSDKTestClient(&fakeBitableAPI{}, nil)
			client.doSDKRequestFunc = func(ctx context.Context, req *larkcore.ApiReq, cfg *larkcore.Config, options ...larkcore.RequestOptionFunc) (*larkcore.ApiResp, error) {
				if req.HttpMethod != http.MethodGet {
					t.Fatalf("unexpected method %s", req.HttpMethod)
				}
				if req.ApiPath != "/open-apis/sheets/v2/spreadsheets/:spreadsheet_token/values/:range" {
					t.Fatalf("unexpected api path %q", req.ApiPath)
				}
				if req.PathParams.Get("spreadsheet_token") != "sht123" {
					t.Fatalf("unexpected spreadsheet token %q", req.PathParams.Get("spreadsheet_token"))
				}
				if req.PathParams.Get("range") != rangeStr {
					t.Fatalf("unexpected range %q", req.PathParams.Get("range"))
				}
				opt := &larkcore.RequestOption{}
				for _, fn := range options {
					fn(opt)
				}
				if strings.TrimSpace(opt.TenantAccessToken) == "" {
					t.Fatalf("expected tenant access token")
				}
				return &larkcore.ApiResp{StatusCode: http.StatusOK, RawBody: []byte(valuesResponse)}, nil
			}
		}

		values, err := client.fetchSheetValues(ctx, ref, sheet)
		if err != nil {
			t.Fatalf("fetchSheetValues returned error: %v", err)
		}
		if len(values) != 2 || len(values[0]) != 2 || values[1][1] != "2" {
			t.Fatalf("unexpected values %#v", values)
		}
	})
}

func TestSheetsV2CreateSheet_Transports(t *testing.T) {
	runBitableTransports(t, func(t *testing.T, transport string) {
		ctx := context.Background()
		ref := SpreadsheetRef{SpreadsheetToken: "sht123"}
		const createResponse = `{"code":0,"msg":"success","data":{"replies":[{"addSheet":{"properties":{"sheetId":"sheet-new"}}}]}}`

		var client *Client
		if transport == "http" {
			client = newHTTPTestClient(func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
				if method != http.MethodPost {
					t.Fatalf("unexpected method %s", method)
				}
				if path != "/open-apis/sheets/v2/spreadsheets/sht123/sheets_batch_update" {
					t.Fatalf("unexpected path %q", path)
				}
				return nil, []byte(createResponse), nil
			})
		} else {
			client = newSDKTestClient(&fakeBitableAPI{}, nil)
			client.doSDKRequestFunc = func(ctx context.Context, req *larkcore.ApiReq, cfg *larkcore.Config, options ...larkcore.RequestOptionFunc) (*larkcore.ApiResp, error) {
				if req.HttpMethod != http.MethodPost {
					t.Fatalf("unexpected method %s", req.HttpMethod)
				}
				if req.ApiPath != "/open-apis/sheets/v2/spreadsheets/:spreadsheet_token/sheets_batch_update" {
					t.Fatalf("unexpected api path %q", req.ApiPath)
				}
				if req.PathParams.Get("spreadsheet_token") != "sht123" {
					t.Fatalf("unexpected spreadsheet token %q", req.PathParams.Get("spreadsheet_token"))
				}
				return &larkcore.ApiResp{StatusCode: http.StatusOK, RawBody: []byte(createResponse)}, nil
			}
		}

		sheetID, err := client.createSheet(ctx, ref, "Hello")
		if err != nil {
			t.Fatalf("createSheet returned error: %v", err)
		}
		if sheetID != "sheet-new" {
			t.Fatalf("unexpected sheet id %q", sheetID)
		}
	})
}

func TestSheetsV2WriteValues_Transports(t *testing.T) {
	runBitableTransports(t, func(t *testing.T, transport string) {
		ctx := context.Background()
		ref := SpreadsheetRef{SpreadsheetToken: "sht123"}
		rows := [][]string{{"A"}}

		const updateResponse = `{"code":0,"msg":"success"}`

		var client *Client
		if transport == "http" {
			client = newHTTPTestClient(func(ctx context.Context, method, path string, payload any) (*http.Response, []byte, error) {
				if method != http.MethodPost {
					t.Fatalf("unexpected method %s", method)
				}
				if path != "/open-apis/sheets/v2/spreadsheets/sht123/values_batch_update" {
					t.Fatalf("unexpected path %q", path)
				}
				return nil, []byte(updateResponse), nil
			})
		} else {
			client = newSDKTestClient(&fakeBitableAPI{}, nil)
			client.doSDKRequestFunc = func(ctx context.Context, req *larkcore.ApiReq, cfg *larkcore.Config, options ...larkcore.RequestOptionFunc) (*larkcore.ApiResp, error) {
				if req.HttpMethod != http.MethodPost {
					t.Fatalf("unexpected method %s", req.HttpMethod)
				}
				if req.ApiPath != "/open-apis/sheets/v2/spreadsheets/:spreadsheet_token/values_batch_update" {
					t.Fatalf("unexpected api path %q", req.ApiPath)
				}
				if req.PathParams.Get("spreadsheet_token") != "sht123" {
					t.Fatalf("unexpected spreadsheet token %q", req.PathParams.Get("spreadsheet_token"))
				}
				return &larkcore.ApiResp{StatusCode: http.StatusOK, RawBody: []byte(updateResponse)}, nil
			}
		}

		if err := client.writeSheetValues(ctx, ref, "sheet-new", "Title", rows); err != nil {
			t.Fatalf("writeSheetValues returned error: %v", err)
		}
	})
}

func TestSheetTitleLimitIsRuneAware(t *testing.T) {
	long := strings.Repeat("测", 101)
	if got := limitSheetTitle(long); len([]rune(got)) != 100 {
		t.Fatalf("expected 100 runes, got %d", len([]rune(got)))
	}
	if got := limitSheetTitle("ok"); got != "ok" {
		t.Fatalf("unexpected title %q", got)
	}
}
