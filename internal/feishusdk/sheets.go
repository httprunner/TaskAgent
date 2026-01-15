package feishusdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larksheets "github.com/larksuite/oapi-sdk-go/v3/service/sheets/v3"
)

// SpreadsheetRef captures identifiers parsed from a Feishu spreadsheet URL.
type SpreadsheetRef struct {
	RawURL           string
	SpreadsheetToken string
	SheetID          string
}

// SheetData contains spreadsheet rows read from Feishu.
type SheetData struct {
	Header           []string
	Rows             [][]string
	SheetTitle       string
	SheetID          string
	SpreadsheetToken string
	BaseName         string
}

// SheetMeta contains spreadsheet metadata and header row.
type SheetMeta struct {
	Ref         SpreadsheetRef
	Header      []string
	SheetTitle  string
	SheetID     string
	ColumnCount int
	RowCount    int
	RangeRef    string
}

// SheetCellUpdate represents a single-cell update in a spreadsheet.
// Row and Col are 1-based indices.
type SheetCellUpdate struct {
	Row   int
	Col   int
	Value string
}

const maxSheetValueRangesPerBatch = 500

var (
	hostAllowList = []string{"feishu.cn", "feishuapp.com", "larksuite.com", "larkoffice.com"}
	invalidNameRe = regexp.MustCompile(`[^\p{L}\p{N}._\-]+`)
)

func isAllowedFeishuHost(host string) bool {
	if host == "" {
		return false
	}
	lower := strings.ToLower(host)
	for _, allowed := range hostAllowList {
		if strings.HasSuffix(lower, allowed) {
			return true
		}
	}
	return false
}

// IsSpreadsheetURL returns true if the input looks like a Feishu spreadsheet link.
func IsSpreadsheetURL(raw string) bool {
	_, err := ParseSpreadsheetURL(raw)
	return err == nil
}

// ParseSpreadsheetURL extracts spreadsheet token and sheet ID from a Feishu link.
func ParseSpreadsheetURL(raw string) (SpreadsheetRef, error) {
	ref := SpreadsheetRef{RawURL: strings.TrimSpace(raw)}
	if ref.RawURL == "" {
		return ref, errors.New("empty url")
	}

	u, err := url.Parse(ref.RawURL)
	if err != nil {
		return ref, fmt.Errorf("invalid url: %w", err)
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return ref, fmt.Errorf("unsupported url scheme %q", u.Scheme)
	}

	if !isAllowedFeishuHost(u.Host) {
		return ref, fmt.Errorf("host %q is not recognized as Feishu", u.Host)
	}

	segments := strings.FieldsFunc(strings.Trim(u.Path, "/"), func(r rune) bool { return r == '/' })
	if len(segments) == 0 {
		return ref, errors.New("missing spreadsheet token in url path")
	}

	token := segments[len(segments)-1]
	if token == "" {
		return ref, errors.New("empty spreadsheet token")
	}

	ref.SpreadsheetToken = token
	ref.SheetID = u.Query().Get("sheet")
	if ref.SheetID == "" {
		ref.SheetID = u.Query().Get("sheet_id")
	}

	return ref, nil
}

// FetchSheet downloads table data for the given spreadsheet URL using Sheets APIs only.
func (c *Client) FetchSheet(ctx context.Context, rawURL string) (*SheetData, error) {
	ref, err := ParseSpreadsheetURL(rawURL)
	if err != nil {
		return nil, err
	}

	sheet, err := c.selectSheet(ctx, &ref)
	if err != nil {
		return nil, err
	}

	values, err := c.fetchSheetValues(ctx, ref, sheet)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, errors.New("feishu: sheet is empty")
	}

	header := make([]string, len(values[0]))
	copy(header, values[0])
	for i := range header {
		header[i] = stripBOM(strings.TrimSpace(header[i]))
	}

	rows := [][]string{}
	if len(values) > 1 {
		rows = values[1:]
	}

	base := sanitizeName(sheetTitle(sheet))

	return &SheetData{
		Header:           header,
		Rows:             rows,
		SheetTitle:       sheetTitle(sheet),
		SheetID:          sheetID(sheet),
		SpreadsheetToken: ref.SpreadsheetToken,
		BaseName:         base,
	}, nil
}

// FetchSheetMeta returns sheet metadata and header row for large-sheet processing.
func (c *Client) FetchSheetMeta(ctx context.Context, rawURL string) (*SheetMeta, error) {
	ref, err := ParseSpreadsheetURL(rawURL)
	if err != nil {
		return nil, err
	}
	sheet, err := c.selectSheet(ctx, &ref)
	if err != nil {
		return nil, err
	}
	colCount := 26
	rowCount := 0
	if gp := sheet.GridProperties; gp != nil {
		if gp.ColumnCount != nil && *gp.ColumnCount > 0 {
			colCount = *gp.ColumnCount
		}
		if gp.RowCount != nil && *gp.RowCount > 0 {
			rowCount = *gp.RowCount
		}
	}
	rangeRef := sheetRangeReference(sheet)
	headerValues, err := c.fetchSheetValuesByRange(ctx, ref, fmt.Sprintf("%s!A1:%s1", rangeRef, columnLabel(colCount)))
	if err != nil {
		return nil, err
	}
	header := []string{}
	if len(headerValues) > 0 {
		header = make([]string, len(headerValues[0]))
		copy(header, headerValues[0])
		for i := range header {
			header[i] = stripBOM(strings.TrimSpace(header[i]))
		}
	}
	return &SheetMeta{
		Ref:         ref,
		Header:      header,
		SheetTitle:  sheetTitle(sheet),
		SheetID:     sheetID(sheet),
		ColumnCount: colCount,
		RowCount:    rowCount,
		RangeRef:    rangeRef,
	}, nil
}

// FetchSheetRowsByRange returns rows in [startRow, endRow].
func (c *Client) FetchSheetRowsByRange(ctx context.Context, meta *SheetMeta, startRow, endRow int) ([][]string, error) {
	if meta == nil {
		return nil, errors.New("feishu: sheet meta is nil")
	}
	if startRow <= 0 || endRow <= 0 || endRow < startRow {
		return nil, fmt.Errorf("feishu: invalid sheet row range %d-%d", startRow, endRow)
	}
	rangeRef := meta.RangeRef
	if strings.TrimSpace(rangeRef) == "" {
		rangeRef = sheetRangeReferenceFromIDTitle(meta.SheetID, meta.SheetTitle)
	}
	rangeStr := fmt.Sprintf("%s!A%d:%s%d", rangeRef, startRow, columnLabel(meta.ColumnCount), endRow)
	return c.fetchSheetValuesByRange(ctx, meta.Ref, rangeStr)
}

// WriteSheet creates a new sheet and writes rows into it.
func (c *Client) WriteSheet(ctx context.Context, rawURL, desiredTitle string, rows [][]string) (string, error) {
	if len(rows) == 0 {
		return "", errors.New("feishu: no rows provided for upload")
	}

	ref, err := ParseSpreadsheetURL(rawURL)
	if err != nil {
		return "", err
	}

	sheetTitle := strings.TrimSpace(desiredTitle)
	if sheetTitle == "" {
		sheetTitle = fmt.Sprintf("AnyGrab_%s", time.Now().Format("20060102_150405"))
	}
	sheetTitle = limitSheetTitle(sheetTitle)

	sheetID, err := c.createSheet(ctx, ref, sheetTitle)
	if err != nil {
		return "", err
	}

	if err := c.writeSheetValues(ctx, ref, sheetID, sheetTitle, rows); err != nil {
		return "", err
	}

	return sheetTitle, nil
}

// UpdateSheetCells batch updates specific cells in a spreadsheet using values_batch_update.
func (c *Client) UpdateSheetCells(ctx context.Context, rawURL string, updates []SheetCellUpdate) error {
	if c == nil {
		return errors.New("feishu: client is nil")
	}
	if len(updates) == 0 {
		return errors.New("feishu: no sheet updates provided")
	}
	ref, err := ParseSpreadsheetURL(rawURL)
	if err != nil {
		return err
	}
	sheet, err := c.selectSheet(ctx, &ref)
	if err != nil {
		return err
	}
	rangeRef := sheetRangeReferenceFromIDTitle(sheetID(sheet), sheetTitle(sheet))
	valueRanges, err := buildSheetValueRanges(rangeRef, updates)
	if err != nil {
		return err
	}
	for start := 0; start < len(valueRanges); start += maxSheetValueRangesPerBatch {
		end := start + maxSheetValueRangesPerBatch
		if end > len(valueRanges) {
			end = len(valueRanges)
		}
		if err := c.updateSheetValueRanges(ctx, ref, valueRanges[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) selectSheet(ctx context.Context, ref *SpreadsheetRef) (*larksheets.Sheet, error) {
	req := larksheets.NewQuerySpreadsheetSheetReqBuilder().
		SpreadsheetToken(ref.SpreadsheetToken).
		Build()

	resp, err := c.larkClient.Sheets.V3.SpreadsheetSheet.Query(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("feishu: query sheets: %w", err)
	}
	if resp == nil || !resp.Success() {
		return nil, fmt.Errorf("feishu: query sheets failed code=%d msg=%s", resp.Code, resp.Msg)
	}
	if resp.Data == nil || len(resp.Data.Sheets) == 0 {
		return nil, errors.New("feishu: spreadsheet contains no sheets")
	}

	sheets := resp.Data.Sheets
	if ref.SheetID != "" {
		for _, sh := range sheets {
			if sh.SheetId != nil && *sh.SheetId == ref.SheetID {
				return sh, nil
			}
		}
		return nil, fmt.Errorf("feishu: sheet id %q not found", ref.SheetID)
	}

	sort.SliceStable(sheets, func(i, j int) bool {
		ii := math.MaxInt32
		jj := math.MaxInt32
		if sheets[i].Index != nil {
			ii = *sheets[i].Index
		}
		if sheets[j].Index != nil {
			jj = *sheets[j].Index
		}
		return ii < jj
	})

	selected := sheets[0]
	if selected.SheetId != nil {
		ref.SheetID = *selected.SheetId
	}
	return selected, nil
}

func (c *Client) fetchSheetValues(ctx context.Context, ref SpreadsheetRef, sheet *larksheets.Sheet) ([][]string, error) {
	colCount := 26
	rowCount := 1000
	if gp := sheet.GridProperties; gp != nil {
		if gp.ColumnCount != nil && *gp.ColumnCount > 0 {
			colCount = *gp.ColumnCount
		}
		if gp.RowCount != nil && *gp.RowCount > 0 {
			rowCount = *gp.RowCount
		}
	}

	rangeStr := fmt.Sprintf("%s!A1:%s%d", sheetRangeReference(sheet), columnLabel(colCount), rowCount)

	var raw []byte
	if c.useHTTP() {
		path := fmt.Sprintf("/open-apis/sheets/v2/spreadsheets/%s/values/%s", ref.SpreadsheetToken, url.PathEscape(rangeStr))
		_, body, err := c.doJSONRequest(ctx, http.MethodGet, path, nil)
		if err != nil {
			return nil, err
		}
		raw = body
	} else {
		token, err := c.getTenantAccessToken(ctx)
		if err != nil {
			return nil, err
		}
		req := &larkcore.ApiReq{
			HttpMethod: http.MethodGet,
			ApiPath:    "/open-apis/sheets/v2/spreadsheets/:spreadsheet_token/values/:range",
			PathParams: larkcore.PathParams{
				"spreadsheet_token": ref.SpreadsheetToken,
				"range":             rangeStr,
			},
			QueryParams:               larkcore.QueryParams{},
			SupportedAccessTokenTypes: []larkcore.AccessTokenType{larkcore.AccessTokenTypeTenant, larkcore.AccessTokenTypeUser},
		}
		resp, err := c.doSDKOpenAPIRequest(ctx, req, c.tenantRequestOptions(token)...)
		if err != nil {
			return nil, err
		}
		if resp == nil {
			return nil, errors.New("feishu: empty response when getting sheet values")
		}
		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("feishu: http %d response: %s", resp.StatusCode, strings.TrimSpace(string(resp.RawBody)))
		}
		raw = resp.RawBody
	}

	var resp struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			ValueRange struct {
				Range  string  `json:"range"`
				Values [][]any `json:"values"`
			} `json:"valueRange"`
			ValueRanges []struct {
				Range  string  `json:"range"`
				Values [][]any `json:"values"`
			} `json:"valueRanges"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("feishu: decode sheet values: %w", err)
	}
	if resp.Code != 0 {
		return nil, fmt.Errorf("feishu: get sheet values failed code=%d msg=%s", resp.Code, resp.Msg)
	}

	rawValues := resp.Data.ValueRange.Values
	if len(rawValues) == 0 && len(resp.Data.ValueRanges) > 0 {
		rawValues = resp.Data.ValueRanges[0].Values
	}
	if len(rawValues) == 0 {
		return [][]string{}, nil
	}

	values := normalizeSheetValues(rawValues)
	if len(values) == 0 {
		return [][]string{}, nil
	}

	maxCols := len(values[0])
	for _, row := range values {
		if len(row) > maxCols {
			maxCols = len(row)
		}
	}
	for i, row := range values {
		if len(row) < maxCols {
			padded := make([]string, maxCols)
			copy(padded, row)
			values[i] = padded
		}
	}

	return values, nil
}

func (c *Client) fetchSheetValuesByRange(ctx context.Context, ref SpreadsheetRef, rangeStr string) ([][]string, error) {
	var raw []byte
	if c.useHTTP() {
		path := fmt.Sprintf("/open-apis/sheets/v2/spreadsheets/%s/values/%s", ref.SpreadsheetToken, url.PathEscape(rangeStr))
		_, body, err := c.doJSONRequest(ctx, http.MethodGet, path, nil)
		if err != nil {
			return nil, err
		}
		raw = body
	} else {
		token, err := c.getTenantAccessToken(ctx)
		if err != nil {
			return nil, err
		}
		req := &larkcore.ApiReq{
			HttpMethod: http.MethodGet,
			ApiPath:    "/open-apis/sheets/v2/spreadsheets/:spreadsheet_token/values/:range",
			PathParams: larkcore.PathParams{
				"spreadsheet_token": ref.SpreadsheetToken,
				"range":             rangeStr,
			},
			QueryParams:               larkcore.QueryParams{},
			SupportedAccessTokenTypes: []larkcore.AccessTokenType{larkcore.AccessTokenTypeTenant, larkcore.AccessTokenTypeUser},
		}
		resp, err := c.doSDKOpenAPIRequest(ctx, req, c.tenantRequestOptions(token)...)
		if err != nil {
			return nil, err
		}
		if resp == nil {
			return nil, errors.New("feishu: empty response when getting sheet values")
		}
		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("feishu: http %d response: %s", resp.StatusCode, strings.TrimSpace(string(resp.RawBody)))
		}
		raw = resp.RawBody
	}

	var resp struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			ValueRange struct {
				Range  string  `json:"range"`
				Values [][]any `json:"values"`
			} `json:"valueRange"`
			ValueRanges []struct {
				Range  string  `json:"range"`
				Values [][]any `json:"values"`
			} `json:"valueRanges"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("feishu: decode sheet values: %w", err)
	}
	if resp.Code != 0 {
		return nil, fmt.Errorf("feishu: get sheet values failed code=%d msg=%s", resp.Code, resp.Msg)
	}

	rawValues := resp.Data.ValueRange.Values
	if len(rawValues) == 0 && len(resp.Data.ValueRanges) > 0 {
		rawValues = resp.Data.ValueRanges[0].Values
	}
	if len(rawValues) == 0 {
		return [][]string{}, nil
	}

	values := normalizeSheetValues(rawValues)
	if len(values) == 0 {
		return [][]string{}, nil
	}
	return values, nil
}

func (c *Client) createSheet(ctx context.Context, ref SpreadsheetRef, title string) (string, error) {
	payload := map[string]any{
		"requests": []any{
			map[string]any{
				"addSheet": map[string]any{
					"properties": map[string]any{
						"title": title,
					},
				},
			},
		},
	}

	var raw []byte
	if c.useHTTP() {
		_, body, err := c.doJSONRequest(ctx, http.MethodPost, fmt.Sprintf("/open-apis/sheets/v2/spreadsheets/%s/sheets_batch_update", ref.SpreadsheetToken), payload)
		if err != nil {
			return "", err
		}
		raw = body
	} else {
		token, err := c.getTenantAccessToken(ctx)
		if err != nil {
			return "", err
		}
		req := &larkcore.ApiReq{
			HttpMethod: http.MethodPost,
			ApiPath:    "/open-apis/sheets/v2/spreadsheets/:spreadsheet_token/sheets_batch_update",
			Body:       payload,
			PathParams: larkcore.PathParams{
				"spreadsheet_token": ref.SpreadsheetToken,
			},
			QueryParams:               larkcore.QueryParams{},
			SupportedAccessTokenTypes: []larkcore.AccessTokenType{larkcore.AccessTokenTypeTenant, larkcore.AccessTokenTypeUser},
		}
		resp, err := c.doSDKOpenAPIRequest(ctx, req, c.tenantRequestOptions(token)...)
		if err != nil {
			return "", err
		}
		if resp == nil {
			return "", errors.New("feishu: empty response when adding sheet")
		}
		if resp.StatusCode >= 400 {
			return "", fmt.Errorf("feishu: http %d response: %s", resp.StatusCode, strings.TrimSpace(string(resp.RawBody)))
		}
		raw = resp.RawBody
	}

	var resp struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			Replies []struct {
				AddSheet struct {
					Properties struct {
						SheetID string `json:"sheetId"`
					} `json:"properties"`
				} `json:"addSheet"`
			} `json:"replies"`
		} `json:"data"`
	}

	if err := json.Unmarshal(raw, &resp); err != nil {
		return "", fmt.Errorf("feishu: decode add sheet response: %w", err)
	}
	if resp.Code != 0 {
		return "", fmt.Errorf("feishu: add sheet failed code=%d msg=%s", resp.Code, resp.Msg)
	}
	if len(resp.Data.Replies) == 0 || resp.Data.Replies[0].AddSheet.Properties.SheetID == "" {
		return "", errors.New("feishu: add sheet response missing sheet id")
	}

	return resp.Data.Replies[0].AddSheet.Properties.SheetID, nil
}

func (c *Client) writeSheetValues(ctx context.Context, ref SpreadsheetRef, sheetID, sheetTitle string, rows [][]string) error {
	if len(rows) == 0 {
		return nil
	}

	maxCols := len(rows[0])
	for _, row := range rows {
		if len(row) > maxCols {
			maxCols = len(row)
		}
	}
	column := columnLabel(maxCols)
	rng := fmt.Sprintf("%s!A1:%s%d", sheetRangeReferenceFromIDTitle(sheetID, sheetTitle), column, len(rows))
	payload := map[string]any{
		"valueRanges": []any{
			map[string]any{
				"range":  rng,
				"values": rows,
			},
		},
		"valueInputOption": "RAW",
	}

	var raw []byte
	if c.useHTTP() {
		_, body, err := c.doJSONRequest(ctx, http.MethodPost, fmt.Sprintf("/open-apis/sheets/v2/spreadsheets/%s/values_batch_update", ref.SpreadsheetToken), payload)
		if err != nil {
			return err
		}
		raw = body
	} else {
		token, err := c.getTenantAccessToken(ctx)
		if err != nil {
			return err
		}
		req := &larkcore.ApiReq{
			HttpMethod: http.MethodPost,
			ApiPath:    "/open-apis/sheets/v2/spreadsheets/:spreadsheet_token/values_batch_update",
			Body:       payload,
			PathParams: larkcore.PathParams{
				"spreadsheet_token": ref.SpreadsheetToken,
			},
			QueryParams:               larkcore.QueryParams{},
			SupportedAccessTokenTypes: []larkcore.AccessTokenType{larkcore.AccessTokenTypeTenant, larkcore.AccessTokenTypeUser},
		}
		resp, err := c.doSDKOpenAPIRequest(ctx, req, c.tenantRequestOptions(token)...)
		if err != nil {
			return err
		}
		if resp == nil {
			return errors.New("feishu: empty response when updating sheet values")
		}
		if resp.StatusCode >= 400 {
			return fmt.Errorf("feishu: http %d response: %s", resp.StatusCode, strings.TrimSpace(string(resp.RawBody)))
		}
		raw = resp.RawBody
	}

	var resp struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return fmt.Errorf("feishu: decode values response: %w", err)
	}
	if resp.Code != 0 {
		return fmt.Errorf("feishu: update values failed code=%d msg=%s", resp.Code, resp.Msg)
	}

	return nil
}

type sheetValueRange struct {
	Range  string     `json:"range"`
	Values [][]string `json:"values"`
}

func buildSheetValueRanges(rangeRef string, updates []SheetCellUpdate) ([]sheetValueRange, error) {
	if strings.TrimSpace(rangeRef) == "" {
		return nil, errors.New("feishu: sheet range reference is empty")
	}
	if len(updates) == 0 {
		return nil, errors.New("feishu: no sheet updates provided")
	}
	cols := make(map[int][]SheetCellUpdate)
	for _, upd := range updates {
		if upd.Row <= 0 || upd.Col <= 0 {
			return nil, fmt.Errorf("feishu: invalid sheet update position row=%d col=%d", upd.Row, upd.Col)
		}
		cols[upd.Col] = append(cols[upd.Col], upd)
	}

	valueRanges := make([]sheetValueRange, 0, len(updates))
	for col, items := range cols {
		sort.Slice(items, func(i, j int) bool { return items[i].Row < items[j].Row })
		column := columnLabel(col)
		startRow := items[0].Row
		prevRow := items[0].Row
		values := [][]string{{items[0].Value}}
		for i := 1; i < len(items); i++ {
			row := items[i].Row
			if row == prevRow+1 {
				values = append(values, []string{items[i].Value})
			} else {
				valueRanges = append(valueRanges, sheetValueRange{
					Range:  fmt.Sprintf("%s!%s%d:%s%d", rangeRef, column, startRow, column, prevRow),
					Values: values,
				})
				startRow = row
				values = [][]string{{items[i].Value}}
			}
			prevRow = row
		}
		valueRanges = append(valueRanges, sheetValueRange{
			Range:  fmt.Sprintf("%s!%s%d:%s%d", rangeRef, column, startRow, column, prevRow),
			Values: values,
		})
	}
	return valueRanges, nil
}

func (c *Client) updateSheetValueRanges(ctx context.Context, ref SpreadsheetRef, ranges []sheetValueRange) error {
	if len(ranges) == 0 {
		return nil
	}
	payload := map[string]any{
		"valueRanges":      ranges,
		"valueInputOption": "RAW",
	}

	var raw []byte
	if c.useHTTP() {
		_, body, err := c.doJSONRequest(ctx, http.MethodPost, fmt.Sprintf("/open-apis/sheets/v2/spreadsheets/%s/values_batch_update", ref.SpreadsheetToken), payload)
		if err != nil {
			return err
		}
		raw = body
	} else {
		token, err := c.getTenantAccessToken(ctx)
		if err != nil {
			return err
		}
		req := &larkcore.ApiReq{
			HttpMethod: http.MethodPost,
			ApiPath:    "/open-apis/sheets/v2/spreadsheets/:spreadsheet_token/values_batch_update",
			Body:       payload,
			PathParams: larkcore.PathParams{
				"spreadsheet_token": ref.SpreadsheetToken,
			},
			QueryParams:               larkcore.QueryParams{},
			SupportedAccessTokenTypes: []larkcore.AccessTokenType{larkcore.AccessTokenTypeTenant, larkcore.AccessTokenTypeUser},
		}
		resp, err := c.doSDKOpenAPIRequest(ctx, req, c.tenantRequestOptions(token)...)
		if err != nil {
			return err
		}
		if resp == nil {
			return errors.New("feishu: empty response when updating sheet values")
		}
		if resp.StatusCode >= 400 {
			return fmt.Errorf("feishu: http %d response: %s", resp.StatusCode, strings.TrimSpace(string(resp.RawBody)))
		}
		raw = resp.RawBody
	}

	var resp struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return fmt.Errorf("feishu: decode values response: %w", err)
	}
	if resp.Code != 0 {
		return fmt.Errorf("feishu: update values failed code=%d msg=%s", resp.Code, resp.Msg)
	}
	return nil
}

func normalizeSheetValues(values [][]any) [][]string {
	if len(values) == 0 {
		return [][]string{}
	}
	out := make([][]string, 0, len(values))
	for _, row := range values {
		if len(row) == 0 {
			out = append(out, []string{})
			continue
		}
		cells := make([]string, len(row))
		for i, cell := range row {
			cells[i] = strings.TrimSpace(toString(cell))
		}
		out = append(out, cells)
	}
	return out
}

func stripBOM(s string) string {
	if strings.HasPrefix(s, "\uFEFF") {
		return strings.TrimPrefix(s, "\uFEFF")
	}
	if len(s) >= 3 {
		b := []byte(s)
		if b[0] == 0xEF && b[1] == 0xBB && b[2] == 0xBF {
			return string(b[3:])
		}
	}
	return s
}

func sanitizeName(input string) string {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return "feishu_sheet"
	}
	sanitized := invalidNameRe.ReplaceAllString(trimmed, "_")
	sanitized = strings.Trim(sanitized, "_")
	if sanitized == "" {
		sanitized = "feishu_sheet"
	}
	return sanitized
}

func limitSheetTitle(title string) string {
	const maxLen = 100
	if len([]rune(title)) <= maxLen {
		return title
	}
	runes := []rune(title)
	return string(runes[:maxLen])
}

func columnLabel(n int) string {
	if n <= 0 {
		return "A"
	}
	label := ""
	num := n
	for num > 0 {
		num--
		label = string(rune('A'+(num%26))) + label
		num /= 26
	}
	return label
}

func sheetTitle(sheet *larksheets.Sheet) string {
	if sheet != nil && sheet.Title != nil && *sheet.Title != "" {
		return *sheet.Title
	}
	return "Sheet1"
}

func sheetID(sheet *larksheets.Sheet) string {
	if sheet != nil && sheet.SheetId != nil {
		return *sheet.SheetId
	}
	return ""
}

func sheetRangeReference(sheet *larksheets.Sheet) string {
	if id := sheetID(sheet); id != "" {
		return id
	}
	return quoteSheetTitle(sheetTitle(sheet))
}

func sheetRangeReferenceFromIDTitle(sheetID, title string) string {
	if sheetID != "" {
		return sheetID
	}
	return quoteSheetTitle(title)
}

func quoteSheetTitle(title string) string {
	escaped := strings.ReplaceAll(title, "'", "''")
	return "'" + escaped + "'"
}
