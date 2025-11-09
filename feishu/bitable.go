package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	defaultBitablePageSize = 200
	maxBitablePageSize     = 500
)

// BitableRef captures identifiers parsed from a Feishu Bitable link.
type BitableRef struct {
	RawURL    string
	AppToken  string
	TableID   string
	ViewID    string
	WikiToken string
}

// TargetFields lists the expected column names inside the target table.
type TargetFields struct {
	TaskID       string
	Params       string
	App          string
	Scene        string
	Datetime     string
	Status       string
	User         string
	DeviceSerial string
}

// DefaultTargetFields matches the schema provided in the requirements.
var DefaultTargetFields = TargetFields{
	TaskID:       "TaskID",
	Params:       "Params",
	App:          "App",
	Scene:        "Scene",
	Datetime:     "Datetime",
	Status:       "Status",
	User:         "User",
	DeviceSerial: "DeviceSerial",
}

// TargetRow represents a single task row stored inside the target table.
type TargetRow struct {
	RecordID     string
	TaskID       int64
	Params       string
	App          string
	Scene        string
	User         string
	Datetime     *time.Time
	DatetimeRaw  string
	Status       string
	DeviceSerial string
}

// TargetRecordInput describes the payload needed to create a new record.
// If TaskID is zero, the field is omitted so Feishu can auto-increment it.
// DatetimeRaw takes precedence if both it and Datetime are set.
type TargetRecordInput struct {
	TaskID       int64
	Params       string
	App          string
	Scene        string
	Datetime     *time.Time
	DatetimeRaw  string
	Status       string
	User         string
	DeviceSerial string
}

// ResultFields defines the schema for the capture result table.
type ResultFields struct {
	Datetime     string
	DeviceSerial string
	App          string
	Scene        string
	Params       string
	ItemID       string
	ItemCaption  string
	ItemURL      string
	ItemDuration string
	UserName     string
	UserID       string
	Tags         string
	SubTaskID    string
	PayloadJSON  string
}

// DefaultResultFields matches the schema required by the capture result table.
var DefaultResultFields = ResultFields{
	Datetime:     "Datetime",
	DeviceSerial: "DeviceSerial",
	App:          "App",
	Scene:        "Scene",
	Params:       "Params",
	ItemID:       "ItemID",
	ItemCaption:  "ItemCaption",
	ItemURL:      "ItemURL",
	ItemDuration: "ItemDuration",
	UserName:     "UserName",
	UserID:       "UserID",
	Tags:         "Tags",
	SubTaskID:    "SubTaskID",
	PayloadJSON:  "PayloadJSON",
}

// ResultRecordInput contains the capture metadata uploaded to the result table.
// DatetimeRaw takes precedence when both it and Datetime are provided.
// PayloadJSON accepts either a JSON-serializable Go value, a json.RawMessage,
// []byte, or a string that contains valid JSON.
type ResultRecordInput struct {
	Datetime            *time.Time
	DatetimeRaw         string
	DeviceSerial        string
	App                 string
	Scene               string
	Params              string
	ItemID              string
	ItemCaption         string
	ItemURL             string
	ItemDurationSeconds *float64
	UserName            string
	UserID              string
	Tags                string
	SubTaskID           string
	PayloadJSON         any
}

// TargetStatusUpdate links a TaskID to the status value it should adopt.
type TargetStatusUpdate struct {
	TaskID    int64
	NewStatus string
}

// TargetRowError captures rows that failed to decode during FetchTargetTable.
type TargetRowError struct {
	RecordID string
	Err      error
}

// TargetTable contains decoded rows alongside a quick lookup index.
type TargetTable struct {
	Ref     BitableRef
	Fields  TargetFields
	Rows    []TargetRow
	Invalid []TargetRowError

	taskIndex map[int64]string
}

// TargetQueryOptions allows configuring additional filters when fetching target tables.
type TargetQueryOptions struct {
	ViewID string
	Filter string
	Limit  int
}

// RecordIDByTaskID returns the record id for a given TaskID if present.
func (t *TargetTable) RecordIDByTaskID(taskID int64) (string, bool) {
	if t == nil {
		return "", false
	}
	if t.taskIndex == nil {
		return "", false
	}
	id, ok := t.taskIndex[taskID]
	return id, ok
}

func (t *TargetTable) updateLocalStatus(taskID int64, status string) {
	if t == nil {
		return
	}
	for i := range t.Rows {
		if t.Rows[i].TaskID == taskID {
			t.Rows[i].Status = status
			break
		}
	}
}

func (t *TargetTable) updateLocalDeviceSerial(taskID int64, serial string) {
	if t == nil {
		return
	}
	for i := range t.Rows {
		if t.Rows[i].TaskID == taskID {
			t.Rows[i].DeviceSerial = serial
			break
		}
	}
}

// IsBitableURL returns true if the url matches a supported Feishu Bitable link.
func IsBitableURL(raw string) bool {
	_, err := ParseBitableURL(raw)
	return err == nil
}

// ParseBitableURL extracts app token, table id and view id from Feishu Bitable links.
func ParseBitableURL(raw string) (BitableRef, error) {
	ref := BitableRef{RawURL: strings.TrimSpace(raw)}
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
		return ref, errors.New("missing path segments in url")
	}

	for i := 0; i < len(segments)-1; i++ {
		switch segments[i] {
		case "base":
			ref.AppToken = segments[i+1]
		case "wiki":
			ref.WikiToken = segments[i+1]
		}
		if ref.AppToken != "" {
			break
		}
	}
	if ref.AppToken == "" && ref.WikiToken == "" {
		if len(segments) >= 2 && segments[0] == "wiki" {
			ref.WikiToken = segments[len(segments)-1]
		} else {
			ref.AppToken = segments[len(segments)-1]
		}
	}
	if ref.AppToken == "" && ref.WikiToken == "" {
		return ref, errors.New("missing app token or wiki token in url")
	}

	q := u.Query()
	for _, key := range []string{"table", "tableId", "table_id"} {
		if v := strings.TrimSpace(q.Get(key)); v != "" {
			ref.TableID = v
			break
		}
	}
	if ref.TableID == "" {
		return ref, errors.New("missing table id in url query")
	}

	for _, key := range []string{"view", "viewId", "view_id"} {
		if v := strings.TrimSpace(q.Get(key)); v != "" {
			ref.ViewID = v
			break
		}
	}

	return ref, nil
}

// FetchTargetTable downloads and decodes rows from the configured target table.
func (c *Client) FetchTargetTable(ctx context.Context, rawURL string, override *TargetFields) (*TargetTable, error) {
	return c.FetchTargetTableWithOptions(ctx, rawURL, override, nil)
}

// FetchTargetTableWithOptions downloads rows using the provided query options.
func (c *Client) FetchTargetTableWithOptions(ctx context.Context, rawURL string, override *TargetFields, opts *TargetQueryOptions) (*TargetTable, error) {
	if c == nil {
		return nil, errors.New("feishu: client is nil")
	}
	ref, err := ParseBitableURL(rawURL)
	if err != nil {
		return nil, err
	}

	if err := c.ensureBitableAppToken(ctx, &ref); err != nil {
		return nil, err
	}

	fields := DefaultTargetFields
	if override != nil {
		fields = fields.merge(*override)
	}

	pageSize := defaultBitablePageSize
	if opts != nil && opts.Limit > 0 && opts.Limit < pageSize {
		pageSize = opts.Limit
	}

	records, err := c.listBitableRecords(ctx, ref, pageSize, opts)
	if err != nil {
		return nil, err
	}

	table := &TargetTable{
		Ref:       ref,
		Fields:    fields,
		Rows:      make([]TargetRow, 0, len(records)),
		Invalid:   make([]TargetRowError, 0),
		taskIndex: make(map[int64]string, len(records)),
	}

	for _, rec := range records {
		row, err := decodeTargetRow(rec, fields)
		if err != nil {
			table.Invalid = append(table.Invalid, TargetRowError{RecordID: rec.RecordID, Err: err})
			continue
		}
		table.Rows = append(table.Rows, row)
		if _, exists := table.taskIndex[row.TaskID]; !exists {
			table.taskIndex[row.TaskID] = row.RecordID
		}
	}

	return table, nil
}

func (c *Client) ensureBitableAppToken(ctx context.Context, ref *BitableRef) error {
	if ref == nil {
		return errors.New("feishu: bitable reference is nil")
	}
	if strings.TrimSpace(ref.AppToken) != "" {
		return nil
	}
	if strings.TrimSpace(ref.WikiToken) == "" {
		return errors.New("feishu: bitable app token not found in url")
	}
	node, err := c.fetchWikiNode(ctx, ref.WikiToken)
	if err != nil {
		return err
	}
	if node.ObjToken == "" {
		return errors.New("feishu: wiki node response missing obj_token")
	}
	if node.ObjType != "bitable" {
		return fmt.Errorf("feishu: wiki node type %q is not bitable", node.ObjType)
	}
	ref.AppToken = node.ObjToken
	return nil
}

// UpdateTargetStatus updates the status field for a given TaskID using a previously fetched table.
func (c *Client) UpdateTargetStatus(ctx context.Context, table *TargetTable, taskID int64, newStatus string) error {
	if strings.TrimSpace(newStatus) == "" {
		return errors.New("feishu: new status cannot be empty")
	}
	return c.UpdateTargetFields(ctx, table, taskID, map[string]any{
		table.Fields.Status: newStatus,
	})
}

// UpdateTargetFields updates arbitrary fields for a given TaskID using a previously fetched table.
func (c *Client) UpdateTargetFields(ctx context.Context, table *TargetTable, taskID int64, fields map[string]any) error {
	if c == nil {
		return errors.New("feishu: client is nil")
	}
	if table == nil {
		return errors.New("feishu: target table is nil")
	}
	if len(fields) == 0 {
		return errors.New("feishu: no fields provided for update")
	}
	recordID, ok := table.RecordIDByTaskID(taskID)
	if !ok {
		return fmt.Errorf("feishu: task id %d not found in table", taskID)
	}
	if err := c.updateBitableRecord(ctx, table.Ref, recordID, fields); err != nil {
		return err
	}
	if statusField := strings.TrimSpace(table.Fields.Status); statusField != "" {
		if val, ok := fields[statusField]; ok {
			table.updateLocalStatus(taskID, toString(val))
		}
	}
	if deviceField := strings.TrimSpace(table.Fields.DeviceSerial); deviceField != "" {
		if val, ok := fields[deviceField]; ok {
			table.updateLocalDeviceSerial(taskID, toString(val))
		}
	}
	return nil
}

// UpdateTargetStatusByTaskID is a convenience helper that fetches the table before applying the update.
func (c *Client) UpdateTargetStatusByTaskID(ctx context.Context, rawURL string, taskID int64, newStatus string, override *TargetFields) error {
	table, err := c.FetchTargetTable(ctx, rawURL, override)
	if err != nil {
		return err
	}
	return c.UpdateTargetStatus(ctx, table, taskID, newStatus)
}

// CreateTargetRecord creates a single record inside the target table.
func (c *Client) CreateTargetRecord(ctx context.Context, rawURL string, record TargetRecordInput, override *TargetFields) (string, error) {
	ids, err := c.CreateTargetRecords(ctx, rawURL, []TargetRecordInput{record}, override)
	if err != nil {
		return "", err
	}
	if len(ids) == 0 {
		return "", errors.New("feishu: record creation returned no ids")
	}
	return ids[0], nil
}

// CreateTargetRecords creates one or more records that match the TargetFields schema.
func (c *Client) CreateTargetRecords(ctx context.Context, rawURL string, records []TargetRecordInput, override *TargetFields) ([]string, error) {
	if c == nil {
		return nil, errors.New("feishu: client is nil")
	}
	if len(records) == 0 {
		return nil, errors.New("feishu: no records provided for creation")
	}
	ref, err := ParseBitableURL(rawURL)
	if err != nil {
		return nil, err
	}
	if err := c.ensureBitableAppToken(ctx, &ref); err != nil {
		return nil, err
	}
	fields := DefaultTargetFields
	if override != nil {
		fields = fields.merge(*override)
	}
	payloads, err := buildTargetRecordPayloads(records, fields)
	if err != nil {
		return nil, err
	}
	if len(payloads) == 1 {
		id, err := c.createBitableRecord(ctx, ref, payloads[0])
		if err != nil {
			return nil, err
		}
		return []string{id}, nil
	}
	return c.batchCreateBitableRecords(ctx, ref, payloads)
}

// CreateResultRecord creates a single capture result entry in the configured table.
func (c *Client) CreateResultRecord(ctx context.Context, rawURL string, record ResultRecordInput, override *ResultFields) (string, error) {
	ids, err := c.CreateResultRecords(ctx, rawURL, []ResultRecordInput{record}, override)
	if err != nil {
		return "", err
	}
	if len(ids) == 0 {
		return "", errors.New("feishu: result record creation returned no ids")
	}
	return ids[0], nil
}

// CreateResultRecords creates one or more capture result rows.
func (c *Client) CreateResultRecords(ctx context.Context, rawURL string, records []ResultRecordInput, override *ResultFields) ([]string, error) {
	if c == nil {
		return nil, errors.New("feishu: client is nil")
	}
	if len(records) == 0 {
		return nil, errors.New("feishu: no result records provided for creation")
	}
	ref, err := ParseBitableURL(rawURL)
	if err != nil {
		return nil, err
	}
	if err := c.ensureBitableAppToken(ctx, &ref); err != nil {
		return nil, err
	}
	fields := DefaultResultFields
	if override != nil {
		fields = fields.merge(*override)
	}
	payloads, err := buildResultRecordPayloads(records, fields)
	if err != nil {
		return nil, err
	}
	if len(payloads) == 1 {
		id, err := c.createBitableRecord(ctx, ref, payloads[0])
		if err != nil {
			return nil, err
		}
		return []string{id}, nil
	}
	return c.batchCreateBitableRecords(ctx, ref, payloads)
}

// UpdateTargetStatuses updates the status field for multiple TaskIDs in one fetch cycle.
func (c *Client) UpdateTargetStatuses(ctx context.Context, rawURL string, updates []TargetStatusUpdate, override *TargetFields) error {
	if c == nil {
		return errors.New("feishu: client is nil")
	}
	if len(updates) == 0 {
		return errors.New("feishu: no updates provided")
	}
	table, err := c.FetchTargetTable(ctx, rawURL, override)
	if err != nil {
		return err
	}
	if strings.TrimSpace(table.Fields.Status) == "" {
		return errors.New("feishu: status field is not configured")
	}
	for _, upd := range updates {
		if upd.TaskID == 0 {
			return errors.New("feishu: task id cannot be zero")
		}
		if strings.TrimSpace(upd.NewStatus) == "" {
			return fmt.Errorf("feishu: task %d new status is empty", upd.TaskID)
		}
		recordID, ok := table.RecordIDByTaskID(upd.TaskID)
		if !ok {
			return fmt.Errorf("feishu: task id %d not found in table", upd.TaskID)
		}
		if err := c.updateBitableRecord(ctx, table.Ref, recordID, map[string]any{table.Fields.Status: upd.NewStatus}); err != nil {
			return err
		}
		table.updateLocalStatus(upd.TaskID, upd.NewStatus)
	}
	return nil
}

func (fields TargetFields) merge(override TargetFields) TargetFields {
	result := fields
	if strings.TrimSpace(override.TaskID) != "" {
		result.TaskID = override.TaskID
	}
	if strings.TrimSpace(override.Params) != "" {
		result.Params = override.Params
	}
	if strings.TrimSpace(override.App) != "" {
		result.App = override.App
	}
	if strings.TrimSpace(override.Scene) != "" {
		result.Scene = override.Scene
	}
	if strings.TrimSpace(override.Datetime) != "" {
		result.Datetime = override.Datetime
	}
	if strings.TrimSpace(override.Status) != "" {
		result.Status = override.Status
	}
	if strings.TrimSpace(override.User) != "" {
		result.User = override.User
	}
	if strings.TrimSpace(override.DeviceSerial) != "" {
		result.DeviceSerial = override.DeviceSerial
	}
	return result
}

func (fields ResultFields) merge(override ResultFields) ResultFields {
	result := fields
	if strings.TrimSpace(override.Datetime) != "" {
		result.Datetime = override.Datetime
	}
	if strings.TrimSpace(override.DeviceSerial) != "" {
		result.DeviceSerial = override.DeviceSerial
	}
	if strings.TrimSpace(override.App) != "" {
		result.App = override.App
	}
	if strings.TrimSpace(override.Scene) != "" {
		result.Scene = override.Scene
	}
	if strings.TrimSpace(override.Params) != "" {
		result.Params = override.Params
	}
	if strings.TrimSpace(override.ItemID) != "" {
		result.ItemID = override.ItemID
	}
	if strings.TrimSpace(override.ItemCaption) != "" {
		result.ItemCaption = override.ItemCaption
	}
	if strings.TrimSpace(override.ItemURL) != "" {
		result.ItemURL = override.ItemURL
	}
	if strings.TrimSpace(override.ItemDuration) != "" {
		result.ItemDuration = override.ItemDuration
	}
	if strings.TrimSpace(override.UserName) != "" {
		result.UserName = override.UserName
	}
	if strings.TrimSpace(override.UserID) != "" {
		result.UserID = override.UserID
	}
	if strings.TrimSpace(override.Tags) != "" {
		result.Tags = override.Tags
	}
	if strings.TrimSpace(override.SubTaskID) != "" {
		result.SubTaskID = override.SubTaskID
	}
	if strings.TrimSpace(override.PayloadJSON) != "" {
		result.PayloadJSON = override.PayloadJSON
	}
	return result
}

func buildTargetRecordPayloads(records []TargetRecordInput, fields TargetFields) ([]map[string]any, error) {
	if strings.TrimSpace(fields.TaskID) == "" {
		return nil, errors.New("feishu: TaskID column name is empty")
	}
	result := make([]map[string]any, 0, len(records))
	for idx, rec := range records {
		row := make(map[string]any)
		if rec.TaskID != 0 {
			row[fields.TaskID] = rec.TaskID
		}
		addOptionalField(row, fields.Params, rec.Params)
		addOptionalField(row, fields.App, rec.App)
		addOptionalField(row, fields.Scene, rec.Scene)
		if dt := formatRecordDatetimeString(rec.Datetime, rec.DatetimeRaw); dt != "" && strings.TrimSpace(fields.Datetime) != "" {
			row[fields.Datetime] = dt
		}
		if strings.TrimSpace(fields.Status) != "" && rec.Status != "" {
			row[fields.Status] = rec.Status
		}
		addOptionalField(row, fields.User, rec.User)
		addOptionalField(row, fields.DeviceSerial, rec.DeviceSerial)
		if len(row) == 0 {
			return nil, fmt.Errorf("feishu: record %d has no fields to set", idx)
		}
		result = append(result, row)
	}
	return result, nil
}

func buildResultRecordPayloads(records []ResultRecordInput, fields ResultFields) ([]map[string]any, error) {
	result := make([]map[string]any, 0, len(records))
	for idx, rec := range records {
		row := make(map[string]any)
		if strings.TrimSpace(fields.Datetime) != "" {
			if ts, ok, err := formatResultRecordTimestamp(rec.Datetime, rec.DatetimeRaw); err != nil {
				return nil, fmt.Errorf("feishu: result record %d datetime invalid: %w", idx, err)
			} else if ok {
				row[fields.Datetime] = ts
			}
		}
		addOptionalField(row, fields.DeviceSerial, rec.DeviceSerial)
		addOptionalField(row, fields.App, rec.App)
		addOptionalField(row, fields.Scene, rec.Scene)
		addOptionalField(row, fields.Params, rec.Params)
		addOptionalField(row, fields.ItemID, rec.ItemID)
		addOptionalField(row, fields.ItemCaption, rec.ItemCaption)
		addOptionalField(row, fields.ItemURL, rec.ItemURL)
		addOptionalNumber(row, fields.ItemDuration, rec.ItemDurationSeconds)
		addOptionalField(row, fields.UserName, rec.UserName)
		addOptionalField(row, fields.UserID, rec.UserID)
		addOptionalField(row, fields.Tags, rec.Tags)
		addOptionalField(row, fields.SubTaskID, rec.SubTaskID)
		if payload, err := encodePayloadJSON(rec.PayloadJSON); err != nil {
			return nil, fmt.Errorf("feishu: result record %d invalid payload json: %w", idx, err)
		} else if payload != "" && strings.TrimSpace(fields.PayloadJSON) != "" {
			row[fields.PayloadJSON] = payload
		}
		if len(row) == 0 {
			return nil, fmt.Errorf("feishu: result record %d has no fields to set", idx)
		}
		result = append(result, row)
	}
	return result, nil
}

func addOptionalField(dst map[string]any, column, value string) {
	if strings.TrimSpace(column) == "" || strings.TrimSpace(value) == "" {
		return
	}
	dst[column] = value
}

func addOptionalNumber(dst map[string]any, column string, value *float64) {
	if strings.TrimSpace(column) == "" || value == nil {
		return
	}
	dst[column] = *value
}

func formatRecordDatetimeString(dt *time.Time, raw string) string {
	if strings.TrimSpace(raw) != "" {
		return raw
	}
	if dt != nil {
		return dt.Format(time.RFC3339)
	}
	return ""
}

func formatResultRecordTimestamp(dt *time.Time, raw string) (int64, bool, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed != "" {
		if ts, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			if len(trimmed) == 10 {
				return ts * 1000, true, nil
			}
			return ts, true, nil
		}
		if parsed, err := parseBitableTime(trimmed); err == nil {
			return parsed.UTC().UnixMilli(), true, nil
		}
		return 0, false, fmt.Errorf("unable to parse datetime %q", raw)
	}
	if dt != nil {
		return dt.UTC().UnixMilli(), true, nil
	}
	return 0, false, nil
}

func encodePayloadJSON(value any) (string, error) {
	switch v := value.(type) {
	case nil:
		return "", nil
	case string:
		if strings.TrimSpace(v) == "" {
			return "", nil
		}
		if !json.Valid([]byte(v)) {
			return "", fmt.Errorf("payload json string is not valid JSON")
		}
		return v, nil
	case []byte:
		if len(v) == 0 {
			return "", nil
		}
		if strings.TrimSpace(string(v)) == "" {
			return "", nil
		}
		if !json.Valid(v) {
			return "", fmt.Errorf("payload json bytes are not valid JSON")
		}
		return string(v), nil
	case json.RawMessage:
		return encodePayloadJSON([]byte(v))
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("marshal payload json: %w", err)
		}
		return string(raw), nil
	}
}

type bitableRecord struct {
	RecordID string         `json:"record_id"`
	Fields   map[string]any `json:"fields"`
}

func (c *Client) listBitableRecords(ctx context.Context, ref BitableRef, pageSize int, opts *TargetQueryOptions) ([]bitableRecord, error) {
	if strings.TrimSpace(ref.AppToken) == "" {
		return nil, errors.New("feishu: bitable app token is empty")
	}
	if pageSize <= 0 {
		pageSize = defaultBitablePageSize
	}
	if pageSize > maxBitablePageSize {
		pageSize = maxBitablePageSize
	}

	basePath := fmt.Sprintf("/open-apis/bitable/v1/apps/%s/tables/%s/records", url.PathEscape(ref.AppToken), url.PathEscape(ref.TableID))
	values := url.Values{}
	values.Set("page_size", strconv.Itoa(pageSize))
	viewID := ref.ViewID
	if opts != nil && strings.TrimSpace(opts.ViewID) != "" {
		viewID = strings.TrimSpace(opts.ViewID)
	}
	if viewID != "" {
		values.Set("view_id", viewID)
	}
	if opts != nil && strings.TrimSpace(opts.Filter) != "" {
		values.Set("filter", strings.TrimSpace(opts.Filter))
	}

	all := make([]bitableRecord, 0, pageSize)
	pageToken := ""
	limit := 0
	if opts != nil && opts.Limit > 0 {
		limit = opts.Limit
	}

	for {
		values.Del("page_token")
		if pageToken != "" {
			values.Set("page_token", pageToken)
		}
		path := basePath
		if enc := values.Encode(); enc != "" {
			path = path + "?" + enc
		}

		_, raw, err := c.doJSONRequest(ctx, http.MethodGet, path, nil)
		if err != nil {
			return nil, err
		}

		var resp struct {
			Code int    `json:"code"`
			Msg  string `json:"msg"`
			Data struct {
				Items     []bitableRecord `json:"items"`
				HasMore   bool            `json:"has_more"`
				PageToken string          `json:"page_token"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &resp); err != nil {
			return nil, fmt.Errorf("feishu: decode bitable records: %w", err)
		}
		if resp.Code != 0 {
			return nil, fmt.Errorf("feishu: list bitable records failed code=%d msg=%s", resp.Code, resp.Msg)
		}
		all = append(all, resp.Data.Items...)
		if limit > 0 && len(all) >= limit {
			return all[:limit], nil
		}
		if !resp.Data.HasMore || strings.TrimSpace(resp.Data.PageToken) == "" {
			break
		}
		pageToken = resp.Data.PageToken
	}

	if limit > 0 && len(all) > limit {
		return all[:limit], nil
	}
	return all, nil
}

func decodeTargetRow(rec bitableRecord, fields TargetFields) (TargetRow, error) {
	if rec.Fields == nil {
		return TargetRow{}, fmt.Errorf("record %s has no fields", rec.RecordID)
	}
	taskID, err := bitableIntField(rec.Fields, fields.TaskID)
	if err != nil {
		return TargetRow{}, fmt.Errorf("record %s: %w", rec.RecordID, err)
	}
	status, err := bitableStringField(rec.Fields, fields.Status, true)
	if err != nil {
		return TargetRow{}, fmt.Errorf("record %s: %w", rec.RecordID, err)
	}

	row := TargetRow{
		RecordID:     rec.RecordID,
		TaskID:       taskID,
		Params:       bitableOptionalString(rec.Fields, fields.Params),
		App:          bitableOptionalString(rec.Fields, fields.App),
		Scene:        bitableOptionalString(rec.Fields, fields.Scene),
		Status:       status,
		User:         bitableOptionalString(rec.Fields, fields.User),
		DeviceSerial: bitableOptionalString(rec.Fields, fields.DeviceSerial),
	}

	if dt := bitableOptionalString(rec.Fields, fields.Datetime); dt != "" {
		row.DatetimeRaw = dt
		if parsed, err := parseBitableTime(dt); err == nil {
			row.Datetime = &parsed
		}
	}

	return row, nil
}

func bitableIntField(fields map[string]any, name string) (int64, error) {
	val, ok := fields[name]
	if !ok {
		return 0, fmt.Errorf("missing field %q", name)
	}
	return toInt64(val)
}

func bitableStringField(fields map[string]any, name string, allowEmpty bool) (string, error) {
	val, ok := fields[name]
	if !ok {
		return "", fmt.Errorf("missing field %q", name)
	}
	str := toString(val)
	if str == "" && !allowEmpty {
		return "", fmt.Errorf("field %q is empty", name)
	}
	return str, nil
}

func bitableOptionalString(fields map[string]any, name string) string {
	if name == "" {
		return ""
	}
	val, ok := fields[name]
	if !ok {
		return ""
	}
	return toString(val)
}

func parseBitableTime(raw string) (time.Time, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, fmt.Errorf("unable to parse datetime %q", raw)
	}
	if num, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		switch {
		case len(trimmed) >= 13 || num > 1e12:
			return time.UnixMilli(num).In(time.Local), nil
		default:
			return time.Unix(num, 0).In(time.Local), nil
		}
	}
	layouts := []string{
		time.RFC3339,
		"2006-01-02T15:04:05.000",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, raw, time.Local); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unable to parse datetime %q", raw)
}

func toInt64(value any) (int64, error) {
	switch v := value.(type) {
	case float64:
		if math.Mod(v, 1) != 0 {
			return 0, fmt.Errorf("value %v is not an integer", v)
		}
		return int64(v), nil
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case json.Number:
		return v.Int64()
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return 0, errors.New("empty numeric string")
		}
		return strconv.ParseInt(trimmed, 10, 64)
	case []any:
		if len(v) == 0 {
			return 0, fmt.Errorf("cannot parse empty list as integer")
		}
		return toInt64(v[0])
	default:
		return 0, fmt.Errorf("unsupported numeric type %T", value)
	}
}

func toString(value any) string {
	switch v := value.(type) {
	case json.Number:
		return v.String()
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	case float64:
		if math.Mod(v, 1) == 0 {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case []any:
		for _, item := range v {
			if str := toString(item); str != "" {
				return str
			}
		}
		return ""
	default:
		return ""
	}
}

func (c *Client) updateBitableRecord(ctx context.Context, ref BitableRef, recordID string, fields map[string]any) error {
	if recordID == "" {
		return errors.New("feishu: record id is empty")
	}
	if len(fields) == 0 {
		return errors.New("feishu: no fields provided for update")
	}
	if strings.TrimSpace(ref.AppToken) == "" {
		return errors.New("feishu: bitable app token is empty")
	}
	payload := map[string]any{"fields": fields}
	path := fmt.Sprintf("/open-apis/bitable/v1/apps/%s/tables/%s/records/%s", url.PathEscape(ref.AppToken), url.PathEscape(ref.TableID), url.PathEscape(recordID))

	_, raw, err := c.doJSONRequest(ctx, http.MethodPut, path, payload)
	if err != nil {
		return err
	}

	var resp struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return fmt.Errorf("feishu: decode update response: %w", err)
	}
	if resp.Code != 0 {
		return fmt.Errorf("feishu: update record failed code=%d msg=%s", resp.Code, resp.Msg)
	}
	return nil
}

func (c *Client) createBitableRecord(ctx context.Context, ref BitableRef, fields map[string]any) (string, error) {
	if len(fields) == 0 {
		return "", errors.New("feishu: no fields provided for creation")
	}
	if strings.TrimSpace(ref.AppToken) == "" {
		return "", errors.New("feishu: bitable app token is empty")
	}
	payload := map[string]any{"fields": fields}
	path := fmt.Sprintf("/open-apis/bitable/v1/apps/%s/tables/%s/records", url.PathEscape(ref.AppToken), url.PathEscape(ref.TableID))

	_, raw, err := c.doJSONRequest(ctx, http.MethodPost, path, payload)
	if err != nil {
		return "", err
	}

	var resp struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			Record bitableRecord `json:"record"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return "", fmt.Errorf("feishu: decode create response: %w", err)
	}
	if resp.Code != 0 {
		return "", fmt.Errorf("feishu: create record failed code=%d msg=%s", resp.Code, resp.Msg)
	}
	if resp.Data.Record.RecordID == "" {
		return "", errors.New("feishu: create record response missing record id")
	}
	return resp.Data.Record.RecordID, nil
}

func (c *Client) batchCreateBitableRecords(ctx context.Context, ref BitableRef, records []map[string]any) ([]string, error) {
	if len(records) == 0 {
		return nil, errors.New("feishu: no records provided for batch create")
	}
	if strings.TrimSpace(ref.AppToken) == "" {
		return nil, errors.New("feishu: bitable app token is empty")
	}
	items := make([]map[string]any, 0, len(records))
	for _, fields := range records {
		if len(fields) == 0 {
			return nil, errors.New("feishu: record payload is empty")
		}
		items = append(items, map[string]any{"fields": fields})
	}
	payload := map[string]any{"records": items}
	path := fmt.Sprintf("/open-apis/bitable/v1/apps/%s/tables/%s/records/batch_create", url.PathEscape(ref.AppToken), url.PathEscape(ref.TableID))

	_, raw, err := c.doJSONRequest(ctx, http.MethodPost, path, payload)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			Records []bitableRecord `json:"records"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("feishu: decode batch create response: %w", err)
	}
	if resp.Code != 0 {
		return nil, fmt.Errorf("feishu: batch create records failed code=%d msg=%s", resp.Code, resp.Msg)
	}
	ids := make([]string, 0, len(resp.Data.Records))
	for _, rec := range resp.Data.Records {
		if rec.RecordID == "" {
			return nil, errors.New("feishu: batch create response missing record id")
		}
		ids = append(ids, rec.RecordID)
	}
	return ids, nil
}

func (c *Client) getBitableRecord(ctx context.Context, ref BitableRef, recordID string) (bitableRecord, error) {
	if strings.TrimSpace(recordID) == "" {
		return bitableRecord{}, errors.New("feishu: record id is empty")
	}
	if strings.TrimSpace(ref.AppToken) == "" {
		return bitableRecord{}, errors.New("feishu: bitable app token is empty")
	}
	path := fmt.Sprintf("/open-apis/bitable/v1/apps/%s/tables/%s/records/%s", url.PathEscape(ref.AppToken), url.PathEscape(ref.TableID), url.PathEscape(recordID))
	_, raw, err := c.doJSONRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return bitableRecord{}, err
	}
	var resp struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			Record bitableRecord `json:"record"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return bitableRecord{}, fmt.Errorf("feishu: decode get record response: %w", err)
	}
	if resp.Code != 0 {
		return bitableRecord{}, fmt.Errorf("feishu: get record failed code=%d msg=%s", resp.Code, resp.Msg)
	}
	return resp.Data.Record, nil
}

type wikiNodeInfo struct {
	ObjToken string `json:"obj_token"`
	ObjType  string `json:"obj_type"`
}

func (c *Client) fetchWikiNode(ctx context.Context, wikiToken string) (wikiNodeInfo, error) {
	var empty wikiNodeInfo
	if strings.TrimSpace(wikiToken) == "" {
		return empty, errors.New("feishu: wiki token is empty")
	}
	path := fmt.Sprintf("/open-apis/wiki/v2/spaces/get_node?token=%s", url.QueryEscape(wikiToken))
	_, raw, err := c.doJSONRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return empty, err
	}
	var resp struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			Node wikiNodeInfo `json:"node"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return empty, fmt.Errorf("feishu: decode wiki node response: %w", err)
	}
	if resp.Code != 0 {
		return empty, fmt.Errorf("feishu: wiki get_node failed code=%d msg=%s", resp.Code, resp.Msg)
	}
	return resp.Data.Node, nil
}
