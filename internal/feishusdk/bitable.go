package feishusdk

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkbitable "github.com/larksuite/oapi-sdk-go/v3/service/bitable/v1"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
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

// TaskFields lists the expected column names inside the task status table.
type TaskFields struct {
	TaskID           string
	BizTaskID        string
	ParentTaskID     string
	App              string
	Scene            string
	Params           string
	ItemID           string
	BookID           string
	URL              string
	UserID           string
	UserName         string
	Date             string
	Status           string
	Webhook          string
	Extra            string
	Logs             string
	LastScreenShot   string
	GroupID          string
	DeviceSerial     string
	DispatchedDevice string
	DispatchedAt     string
	StartAt          string
	EndAt            string
	ElapsedSeconds   string
	ItemsCollected   string
	RetryCount       string
}

// CookieFields lists the expected columns for the cookies table.
type CookieFields struct {
	Cookies  string
	Platform string
	Status   string
}

// TaskRow represents a single task row stored inside the task status table.
type TaskRow struct {
	RecordID         string
	TaskID           int64
	BizTaskID        string
	ParentTaskID     int64
	Params           string
	ItemID           string
	BookID           string
	URL              string
	App              string
	Scene            string
	StartAt          *time.Time
	StartAtRaw       string
	EndAt            *time.Time
	EndAtRaw         string
	UserID           string
	UserName         string
	Extra            string
	Logs             string
	GroupID          string
	Datetime         *time.Time
	DatetimeRaw      string
	Status           string
	Webhook          string
	DeviceSerial     string
	DispatchedDevice string
	DispatchedAt     *time.Time
	DispatchedAtRaw  string
	ElapsedSeconds   int64
	ItemsCollected   int64
	RetryCount       int64
}

// CookieRow represents a single row stored inside the cookies table.
type CookieRow struct {
	RecordID string
	Cookies  string
	Platform string
	Status   string
}

// TaskRecordInput describes the payload needed to create a new record.
// If TaskID is zero, the field is omitted so Feishu can auto-increment it.
// DatetimeRaw takes precedence if both it and Datetime are set.
type TaskRecordInput struct {
	TaskID           int64
	BizTaskID        string
	ParentTaskID     int64
	Params           string
	ItemID           string
	BookID           string
	URL              string
	App              string
	Scene            string
	StartAt          *time.Time
	StartAtRaw       string
	EndAt            *time.Time
	EndAtRaw         string
	Datetime         *time.Time
	DatetimeRaw      string
	Status           string
	UserID           string
	UserName         string
	Extra            string
	Logs             string
	GroupID          string
	DeviceSerial     string
	DispatchedDevice string
	DispatchedAt     *time.Time
	DispatchedAtRaw  string
	ElapsedSeconds   *int64
	ItemsCollected   *int64
}

// ResultFields defines the schema for the capture result table.
type ResultFields struct {
	Datetime       string
	DeviceSerial   string
	App            string
	Scene          string
	Params         string
	ItemID         string
	ItemCaption    string
	ItemCDNURL     string
	ItemURL        string
	ItemDuration   string
	UserName       string
	UserID         string
	UserAlias      string
	UserAuthEntity string
	Tags           string
	TaskID         string
	Extra          string
	LikeCount      string
	ViewCount      string
	AnchorPoint    string
	CommentCount   string
	CollectCount   string
	ForwardCount   string
	ShareCount     string
	PayMode        string
	Collection     string
	Episode        string
	PublishTime    string
}

// ResultRecordInput contains the capture metadata uploaded to the result table.
// DatetimeRaw takes precedence when both it and Datetime are provided.
// Extra accepts either a JSON-serializable Go value, a json.RawMessage,
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
	ItemCDNURL          string
	ItemURL             string
	ItemDurationSeconds *float64
	UserName            string
	UserID              string
	UserAlias           string
	UserAuthEntity      string
	Tags                string
	TaskID              int64
	Extra               any
	LikeCount           int64
	ViewCount           int
	AnchorPoint         string
	CommentCount        int64
	CollectCount        int64
	ForwardCount        int64
	ShareCount          int64
	PayMode             string
	Collection          string
	Episode             string
	PublishTime         string
}

// SourceFields defines the schema shared by the source tables (drama catalog
// and account registry). Each field stores the column name as it appears in the
// bitable.
type SourceFields struct {
	TaskID                   string
	DramaID                  string
	DramaName                string
	TotalDuration            string
	EpisodeCount             string
	Priority                 string
	RightsProtectionScenario string
	SearchKeywords           string
	CaptureDate              string
	BizTaskID                string
	AccountID                string
	AccountName              string
	Platform                 string
}

// TaskStatusUpdate links a TaskID to the status value it should adopt.
type TaskStatusUpdate struct {
	TaskID    int64
	NewStatus string
}

// TaskRowError captures rows that failed to decode during FetchTaskTable.
type TaskRowError struct {
	RecordID string
	Err      error
}

// FetchCookieRows loads cookie rows filtered by platform/status.
func (c *Client) FetchCookieRows(ctx context.Context, rawURL string, override *CookieFields, platform string) ([]CookieRow, error) {
	if c == nil {
		return nil, errors.New("feishu: client is nil")
	}
	if strings.TrimSpace(rawURL) == "" {
		return nil, errors.New("feishu: cookie table url is empty")
	}
	fields := DefaultCookieFields
	if override != nil {
		fields = *override
	}
	ref, err := ParseBitableURL(rawURL)
	if err != nil {
		return nil, err
	}
	if err := c.ensureBitableAppToken(ctx, &ref); err != nil {
		return nil, err
	}
	f := NewFilterInfo("and")
	if trimmed := strings.TrimSpace(platform); trimmed != "" {
		if cond := NewCondition(fields.Platform, "is", trimmed); cond != nil {
			f.Conditions = append(f.Conditions, cond)
		}
	}
	opts := &QueryOptions{Filter: f}
	records, _, err := c.listBitableRecords(ctx, ref, defaultBitablePageSize, opts)
	if err != nil {
		return nil, err
	}
	rows := make([]CookieRow, 0, len(records))
	for _, rec := range records {
		if rec == nil || rec.Fields == nil {
			continue
		}
		value := strings.TrimSpace(bitableOptionalString(rec.Fields, fields.Cookies))
		if value == "" {
			continue
		}
		status := strings.TrimSpace(bitableOptionalString(rec.Fields, fields.Status))
		if isCookieStatusInvalid(status) {
			continue
		}
		row := CookieRow{
			RecordID: strings.TrimSpace(larkcore.StringValue(rec.RecordId)),
			Cookies:  value,
			Platform: strings.TrimSpace(bitableOptionalString(rec.Fields, fields.Platform)),
			Status:   status,
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// UpdateCookieStatus updates the Status column for a cookie record.
func (c *Client) UpdateCookieStatus(ctx context.Context, rawURL, recordID, status string, override *CookieFields) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "feishu: update cookie status failed")
		}
	}()
	if c == nil {
		return errors.New("feishu: client is nil")
	}
	recordID = strings.TrimSpace(recordID)
	if recordID == "" {
		return errors.New("feishu: cookie record id is empty")
	}
	status = strings.TrimSpace(status)
	if status == "" {
		return errors.New("feishu: cookie status is empty")
	}
	ref, err := ParseBitableURL(rawURL)
	if err != nil {
		return err
	}
	if err := c.ensureBitableAppToken(ctx, &ref); err != nil {
		return err
	}
	fields := DefaultCookieFields
	if override != nil {
		fields = *override
	}
	statusField := strings.TrimSpace(fields.Status)
	if statusField == "" {
		return errors.New("feishu: cookie status field is empty")
	}
	payload := []map[string]any{
		{
			"record_id": recordID,
			"fields": map[string]any{
				statusField: status,
			},
		},
	}
	_, err = c.BatchUpdateBitableRecords(ctx, ref, payload)
	return err
}

func isCookieStatusInvalid(status string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(status))
	if trimmed == "" {
		return false
	}
	if trimmed == strings.ToLower(CookieStatusInvalid) {
		return true
	}
	return trimmed != strings.ToLower(CookieStatusValid)
}

// TaskTable contains decoded rows alongside a quick lookup index.
type TaskTable struct {
	Ref     BitableRef
	Fields  TaskFields
	Rows    []TaskRow
	Invalid []TaskRowError

	taskIndex map[int64]string

	// HasMore and NextPageToken describe whether the underlying record search has
	// more pages. They are populated when FetchTaskTableWithOptions is called with
	// QueryOptions.MaxPages > 0 so callers can continue scanning on the next
	// tick without refetching the first page.
	HasMore       bool
	NextPageToken string
	Pages         int
}

// QueryOptions allows configuring additional filters when fetching task tables.
type QueryOptions struct {
	ViewID     string
	Filter     *FilterInfo
	Limit      int
	IgnoreView bool
	// PageToken controls where the record search begins. When empty, the first page is fetched.
	PageToken string
	// MaxPages caps how many pages are fetched in a single call (0 means no cap).
	MaxPages int
}

// RecordIDByTaskID returns the record id for a given TaskID if present.
func (t *TaskTable) RecordIDByTaskID(taskID int64) (string, bool) {
	if t == nil {
		return "", false
	}
	if t.taskIndex == nil {
		return "", false
	}
	id, ok := t.taskIndex[taskID]
	return id, ok
}

func (t *TaskTable) updateLocalStatus(taskID int64, status string) {
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

func (t *TaskTable) updateLocalWebhook(taskID int64, webhook string) {
	if t == nil {
		return
	}
	for i := range t.Rows {
		if t.Rows[i].TaskID == taskID {
			t.Rows[i].Webhook = webhook
			break
		}
	}
}

func (t *TaskTable) updateLocalDispatchedDevice(taskID int64, serial string) {
	if t == nil {
		return
	}
	for i := range t.Rows {
		if t.Rows[i].TaskID == taskID {
			t.Rows[i].DispatchedDevice = serial
			break
		}
	}
}

func (t *TaskTable) updateLocalDispatchedAt(taskID int64, raw string, dispatchedAt *time.Time) {
	if t == nil {
		return
	}
	for i := range t.Rows {
		if t.Rows[i].TaskID != taskID {
			continue
		}
		t.Rows[i].DispatchedAtRaw = raw
		if dispatchedAt != nil {
			ts := *dispatchedAt
			t.Rows[i].DispatchedAt = &ts
		} else {
			t.Rows[i].DispatchedAt = nil
		}
		break
	}
}

func (t *TaskTable) updateLocalElapsedSeconds(taskID int64, secs int64) {
	if t == nil {
		return
	}
	for i := range t.Rows {
		if t.Rows[i].TaskID == taskID {
			t.Rows[i].ElapsedSeconds = secs
			break
		}
	}
}

func (t *TaskTable) updateLocalTargetDevice(taskID int64, serial string) {
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

func (t *TaskTable) updateLocalStartAt(taskID int64, raw string, ts *time.Time) {
	if t == nil {
		return
	}
	for i := range t.Rows {
		if t.Rows[i].TaskID != taskID {
			continue
		}
		t.Rows[i].StartAtRaw = raw
		if ts != nil {
			val := *ts
			t.Rows[i].StartAt = &val
		} else {
			t.Rows[i].StartAt = nil
		}
		break
	}
}

func (t *TaskTable) updateLocalEndAt(taskID int64, raw string, ts *time.Time) {
	if t == nil {
		return
	}
	for i := range t.Rows {
		if t.Rows[i].TaskID != taskID {
			continue
		}
		t.Rows[i].EndAtRaw = raw
		if ts != nil {
			val := *ts
			t.Rows[i].EndAt = &val
		} else {
			t.Rows[i].EndAt = nil
		}
		break
	}
}

// IsBitableURL returns true if the url matches a supported Feishu Bitable link.
func IsBitableURL(raw string) bool {
	_, err := ParseBitableURL(raw)
	return err == nil
}

// ParseBitableURL extracts app token, table id and view id from Feishu Bitable links.
func ParseBitableURL(raw string) (ref BitableRef, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "parse bitable url failed")
		}
	}()

	ref = BitableRef{RawURL: strings.TrimSpace(raw)}
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

// FetchTaskTable downloads and decodes rows from the configured task table.
func (c *Client) FetchTaskTable(ctx context.Context, rawURL string, override *TaskFields) (*TaskTable, error) {
	return c.FetchTaskTableWithOptions(ctx, rawURL, override, nil)
}

// FetchTaskTableWithOptions downloads rows using the provided query options.
func (c *Client) FetchTaskTableWithOptions(ctx context.Context, rawURL string, override *TaskFields, opts *QueryOptions) (*TaskTable, error) {
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

	fields := DefaultTaskFields
	if override != nil {
		fields = fields.merge(*override)
	}

	pageSize := defaultBitablePageSize
	if opts != nil && opts.Limit > 0 && opts.Limit < pageSize {
		pageSize = opts.Limit
	}

	records, pageInfo, err := c.listBitableRecords(ctx, ref, pageSize, opts)
	if err != nil {
		return nil, err
	}

	table := &TaskTable{
		Ref:       ref,
		Fields:    fields,
		Rows:      make([]TaskRow, 0, len(records)),
		Invalid:   make([]TaskRowError, 0),
		taskIndex: make(map[int64]string, len(records)),
		HasMore:   pageInfo.HasMore,
		Pages:     pageInfo.Pages,
	}
	table.NextPageToken = strings.TrimSpace(pageInfo.NextPageToken)

	for _, rec := range records {
		if rec == nil {
			continue
		}
		row, err := decodeTaskRow(rec, fields)
		if err != nil {
			if isMissingFieldError(err, fields.Status) {
				log.Debug().
					Str("record_id", larkcore.StringValue(rec.RecordId)).
					Any("fields", rec.Fields).
					Msg("decode task row skipped (missing status)")
				continue
			}
			log.Error().Err(err).
				Str("record_id", larkcore.StringValue(rec.RecordId)).
				Any("fields", rec.Fields).
				Msg("decode task row failed")
			table.Invalid = append(table.Invalid, TaskRowError{RecordID: strings.TrimSpace(larkcore.StringValue(rec.RecordId)), Err: err})
			continue
		}
		if strings.TrimSpace(row.Status) == "" {
			log.Warn().Str("record_id", larkcore.StringValue(rec.RecordId)).Msg("decode task row skipped (empty status)")
			table.Invalid = append(table.Invalid, TaskRowError{RecordID: strings.TrimSpace(larkcore.StringValue(rec.RecordId)), Err: errors.New("empty status after decode")})
			continue
		}
		table.Rows = append(table.Rows, row)
		if _, exists := table.taskIndex[row.TaskID]; !exists {
			table.taskIndex[row.TaskID] = row.RecordID
		}
	}

	return table, nil
}

func (c *Client) ensureBitableAppToken(ctx context.Context, ref *BitableRef) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "ensure bitable app token failed")
		}
	}()

	if c == nil {
		return errors.New("feishu: client is nil")
	}
	if ref == nil {
		return errors.New("feishu: bitable reference is nil")
	}
	if strings.TrimSpace(ref.AppToken) != "" {
		return nil
	}
	wikiToken := strings.TrimSpace(ref.WikiToken)
	if wikiToken == "" {
		return errors.New("feishu: bitable app token not found in url")
	}

	if cached, ok := c.loadCachedAppToken(wikiToken); ok {
		ref.AppToken = cached
		return nil
	}

	val, err, _ := c.appTokenGroup.Do(wikiToken, func() (interface{}, error) {
		node, err := c.fetchWikiNode(ctx, wikiToken)
		if err != nil {
			return "", err
		}
		if node.ObjToken == "" {
			return "", errors.New("feishu: wiki node response missing obj_token")
		}
		if node.ObjType != "bitable" {
			return "", fmt.Errorf("feishu: wiki node type %q is not bitable", node.ObjType)
		}
		return node.ObjToken, nil
	})
	if err != nil {
		return err
	}

	appToken, _ := val.(string)
	if strings.TrimSpace(appToken) == "" {
		return errors.New("feishu: wiki node response missing obj_token")
	}

	c.storeAppTokenCache(wikiToken, appToken)
	ref.AppToken = appToken
	return nil
}

func (c *Client) loadCachedAppToken(wikiToken string) (string, bool) {
	if c == nil || strings.TrimSpace(wikiToken) == "" {
		return "", false
	}
	c.appTokenMu.RLock()
	defer c.appTokenMu.RUnlock()
	token, ok := c.appTokenCache[wikiToken]
	return token, ok
}

func (c *Client) storeAppTokenCache(wikiToken, appToken string) {
	if c == nil || strings.TrimSpace(wikiToken) == "" || strings.TrimSpace(appToken) == "" {
		return
	}
	c.appTokenMu.Lock()
	defer c.appTokenMu.Unlock()
	if c.appTokenCache == nil {
		c.appTokenCache = make(map[string]string)
	}
	c.appTokenCache[wikiToken] = appToken
}

// FetchBitableRows downloads raw records from a Feishu bitable so callers can read any column.
func (c *Client) FetchBitableRows(ctx context.Context, rawURL string, opts *QueryOptions) (rows []BitableRow, err error) {
	optionsPayload := ""
	if opts != nil {
		if payload, err := json.Marshal(opts); err == nil {
			optionsPayload = string(payload)
		} else {
			optionsPayload = fmt.Sprintf("marshal options failed: %v", err)
		}
	}

	startTime := time.Now()
	defer func() {
		elapsedSeconds := time.Since(startTime).Seconds()
		if err != nil {
			log.Error().Err(err).Str("url", rawURL).Str("options", optionsPayload).
				Float64("elapsed_seconds", elapsedSeconds).
				Int("rows_fetched", len(rows)).Msg("fetched bitable rows failed")
		} else if len(rows) == 0 {
			log.Debug().Str("options", optionsPayload).
				Float64("elapsed_seconds", elapsedSeconds).
				Int("rows_fetched", len(rows)).Msg("fetched bitable rows no matches")
		} else {
			log.Info().Str("url", rawURL).Str("options", optionsPayload).
				Float64("elapsed_seconds", elapsedSeconds).
				Int("rows_fetched", len(rows)).Msg("fetched bitable rows succeeded")
		}
	}()

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
	pageSize := defaultBitablePageSize
	if opts != nil && opts.Limit > 0 && opts.Limit < pageSize {
		pageSize = opts.Limit
	}
	records, _, err := c.listBitableRecords(ctx, ref, pageSize, opts)
	if err != nil {
		return nil, err
	}
	rows = make([]BitableRow, 0, len(records))
	for _, rec := range records {
		if rec == nil {
			continue
		}
		rows = append(rows, BitableRow{
			RecordID: strings.TrimSpace(larkcore.StringValue(rec.RecordId)),
			Fields:   rec.Fields,
		})
	}
	return rows, nil
}

// CreateBitableRecord creates a single record in an arbitrary bitable table.
// It is intended for advanced integrations such as dedicated webhook result tables.
func (c *Client) CreateBitableRecord(ctx context.Context, rawURL string, fields map[string]any) (string, error) {
	ref, err := ParseBitableURL(rawURL)
	if err != nil {
		return "", err
	}
	if err := c.ensureBitableAppToken(ctx, &ref); err != nil {
		return "", err
	}
	return c.createBitableRecord(ctx, ref, fields)
}

// UpdateBitableRecord updates a single record in an arbitrary bitable table.
func (c *Client) UpdateBitableRecord(ctx context.Context, rawURL, recordID string, fields map[string]any) error {
	ref, err := ParseBitableURL(rawURL)
	if err != nil {
		return err
	}
	if err := c.ensureBitableAppToken(ctx, &ref); err != nil {
		return err
	}
	return c.updateBitableRecord(ctx, ref, recordID, fields)
}

// UpdateTaskStatus updates the status field for a given TaskID using a previously fetched table.
func (c *Client) UpdateTaskStatus(ctx context.Context, table *TaskTable, taskID int64, newStatus string) error {
	if strings.TrimSpace(newStatus) == "" {
		return errors.New("feishu: new status cannot be empty")
	}
	return c.UpdateTaskFields(ctx, table, taskID, map[string]any{
		table.Fields.Status: newStatus,
	})
}

// TaskFieldUpdate describes a per-task field update for batch operations.
type TaskFieldUpdate struct {
	TaskID int64
	Fields map[string]any
}

// UpdateTaskFields updates arbitrary fields for a given TaskID using a previously fetched table.
func (c *Client) UpdateTaskFields(ctx context.Context, table *TaskTable, taskID int64, fields map[string]any) error {
	if c == nil {
		return errors.New("feishu: client is nil")
	}
	if table == nil {
		return errors.New("feishu: task table is nil")
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
	if startField := strings.TrimSpace(table.Fields.StartAt); startField != "" {
		if val, ok := fields[startField]; ok {
			raw := toString(val)
			var parsed *time.Time
			if raw != "" {
				if ts, err := parseBitableTime(raw); err == nil {
					parsed = &ts
				}
			}
			table.updateLocalStartAt(taskID, raw, parsed)
		}
	}
	if endField := strings.TrimSpace(table.Fields.EndAt); endField != "" {
		if val, ok := fields[endField]; ok {
			raw := toString(val)
			var parsed *time.Time
			if raw != "" {
				if ts, err := parseBitableTime(raw); err == nil {
					parsed = &ts
				}
			}
			table.updateLocalEndAt(taskID, raw, parsed)
		}
	}
	if webhookField := strings.TrimSpace(table.Fields.Webhook); webhookField != "" {
		if val, ok := fields[webhookField]; ok {
			table.updateLocalWebhook(taskID, toString(val))
		}
	}
	if dispatchedField := strings.TrimSpace(table.Fields.DispatchedDevice); dispatchedField != "" {
		if val, ok := fields[dispatchedField]; ok {
			table.updateLocalDispatchedDevice(taskID, toString(val))
		}
	}
	if targetField := strings.TrimSpace(table.Fields.DeviceSerial); targetField != "" {
		if val, ok := fields[targetField]; ok {
			table.updateLocalTargetDevice(taskID, toString(val))
		}
	}
	if dispatchedAtField := strings.TrimSpace(table.Fields.DispatchedAt); dispatchedAtField != "" {
		if val, ok := fields[dispatchedAtField]; ok {
			raw := toString(val)
			var parsed *time.Time
			if raw != "" {
				if ts, err := parseBitableTime(raw); err == nil {
					parsed = &ts
				}
			}
			table.updateLocalDispatchedAt(taskID, raw, parsed)
		}
	}
	if elapsedField := strings.TrimSpace(table.Fields.ElapsedSeconds); elapsedField != "" {
		if val, ok := fields[elapsedField]; ok {
			if secs, err := toInt64(val); err == nil {
				table.updateLocalElapsedSeconds(taskID, secs)
			}
		}
	}
	return nil
}

// BatchUpdateTaskFields updates multiple task rows with a single batch_update call.
// It requires the provided table to include record ids for all TaskIDs.
//
// Max 500 updates per request.
func (c *Client) BatchUpdateTaskFields(ctx context.Context, table *TaskTable, updates []TaskFieldUpdate) error {
	if c == nil {
		return errors.New("feishu: client is nil")
	}
	if table == nil {
		return errors.New("feishu: task table is nil")
	}
	if len(updates) == 0 {
		return errors.New("feishu: no updates provided for batch update")
	}
	const maxBatch = 500

	records := make([]map[string]any, 0, len(updates))
	localUpdates := make([]TaskFieldUpdate, 0, len(updates))
	for _, upd := range updates {
		if upd.TaskID == 0 {
			return errors.New("feishu: task id is zero in batch update")
		}
		if len(upd.Fields) == 0 {
			continue
		}
		recordID, ok := table.RecordIDByTaskID(upd.TaskID)
		if !ok {
			return fmt.Errorf("feishu: task id %d not found in table", upd.TaskID)
		}
		records = append(records, map[string]any{
			"record_id": recordID,
			"fields":    upd.Fields,
		})
		localUpdates = append(localUpdates, upd)
	}
	if len(records) == 0 {
		return nil
	}

	for start := 0; start < len(records); start += maxBatch {
		end := start + maxBatch
		if end > len(records) {
			end = len(records)
		}
		if _, err := c.BatchUpdateBitableRecords(ctx, table.Ref, records[start:end]); err != nil {
			return err
		}
	}

	// Best-effort local cache update so callers relying on the fetched table
	// can observe changes without refetching.
	for _, upd := range localUpdates {
		taskID := upd.TaskID
		fields := upd.Fields
		if fields == nil {
			continue
		}
		if statusField := strings.TrimSpace(table.Fields.Status); statusField != "" {
			if val, ok := fields[statusField]; ok {
				table.updateLocalStatus(taskID, toString(val))
			}
		}
		if startField := strings.TrimSpace(table.Fields.StartAt); startField != "" {
			if val, ok := fields[startField]; ok {
				raw := toString(val)
				var parsed *time.Time
				if raw != "" {
					if ts, err := parseBitableTime(raw); err == nil {
						parsed = &ts
					}
				}
				table.updateLocalStartAt(taskID, raw, parsed)
			}
		}
		if endField := strings.TrimSpace(table.Fields.EndAt); endField != "" {
			if val, ok := fields[endField]; ok {
				raw := toString(val)
				var parsed *time.Time
				if raw != "" {
					if ts, err := parseBitableTime(raw); err == nil {
						parsed = &ts
					}
				}
				table.updateLocalEndAt(taskID, raw, parsed)
			}
		}
		if webhookField := strings.TrimSpace(table.Fields.Webhook); webhookField != "" {
			if val, ok := fields[webhookField]; ok {
				table.updateLocalWebhook(taskID, toString(val))
			}
		}
		if dispatchedField := strings.TrimSpace(table.Fields.DispatchedDevice); dispatchedField != "" {
			if val, ok := fields[dispatchedField]; ok {
				table.updateLocalDispatchedDevice(taskID, toString(val))
			}
		}
		if targetField := strings.TrimSpace(table.Fields.DeviceSerial); targetField != "" {
			if val, ok := fields[targetField]; ok {
				table.updateLocalTargetDevice(taskID, toString(val))
			}
		}
		if dispatchedAtField := strings.TrimSpace(table.Fields.DispatchedAt); dispatchedAtField != "" {
			if val, ok := fields[dispatchedAtField]; ok {
				raw := toString(val)
				var parsed *time.Time
				if raw != "" {
					if ts, err := parseBitableTime(raw); err == nil {
						parsed = &ts
					}
				}
				table.updateLocalDispatchedAt(taskID, raw, parsed)
			}
		}
		if elapsedField := strings.TrimSpace(table.Fields.ElapsedSeconds); elapsedField != "" {
			if val, ok := fields[elapsedField]; ok {
				if secs, err := toInt64(val); err == nil {
					table.updateLocalElapsedSeconds(taskID, secs)
				}
			}
		}
	}

	return nil
}

// UpdateTaskStatusByTaskID is a convenience helper that fetches the table before applying the update.
func (c *Client) UpdateTaskStatusByTaskID(ctx context.Context, rawURL string, taskID int64, newStatus string, override *TaskFields) error {
	fields := DefaultTaskFields
	if override != nil {
		fields = fields.merge(*override)
	}
	table, err := c.FetchTaskTableWithOptions(ctx, rawURL, override, buildTaskIDQueryOptions(fields, []int64{taskID}))
	if err != nil {
		return err
	}
	return c.UpdateTaskStatus(ctx, table, taskID, newStatus)
}

// CreateTaskRecord creates a single record inside the task status table.
func (c *Client) CreateTaskRecord(ctx context.Context, rawURL string, record TaskRecordInput, override *TaskFields) (string, error) {
	ids, err := c.CreateTaskRecords(ctx, rawURL, []TaskRecordInput{record}, override)
	if err != nil {
		return "", err
	}
	if len(ids) == 0 {
		return "", errors.New("feishu: record creation returned no ids")
	}
	return ids[0], nil
}

// CreateTaskRecords creates one or more records that match the TaskFields schema.
func (c *Client) CreateTaskRecords(ctx context.Context, rawURL string, records []TaskRecordInput, override *TaskFields) ([]string, error) {
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
	fields := DefaultTaskFields
	if override != nil {
		fields = fields.merge(*override)
	}
	payloads, err := buildTaskRecordPayloads(records, fields)
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

// UpdateTaskStatuses updates the status field for multiple TaskIDs in one fetch cycle.
func (c *Client) UpdateTaskStatuses(ctx context.Context, rawURL string, updates []TaskStatusUpdate, override *TaskFields) error {
	if c == nil {
		return errors.New("feishu: client is nil")
	}
	if len(updates) == 0 {
		return errors.New("feishu: no updates provided")
	}
	fields := DefaultTaskFields
	if override != nil {
		fields = fields.merge(*override)
	}
	taskIDs := make([]int64, 0, len(updates))
	for _, upd := range updates {
		if upd.TaskID > 0 {
			taskIDs = append(taskIDs, upd.TaskID)
		}
	}
	table, err := c.FetchTaskTableWithOptions(ctx, rawURL, override, buildTaskIDQueryOptions(fields, taskIDs))
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

func (fields TaskFields) merge(override TaskFields) TaskFields {
	result := fields
	if strings.TrimSpace(override.TaskID) != "" {
		log.Warn().Str("new", override.TaskID).Msg("overriding field TaskID")
		result.TaskID = override.TaskID
	}
	if strings.TrimSpace(override.BizTaskID) != "" {
		log.Warn().Str("new", override.BizTaskID).Msg("overriding field BizTaskID")
		result.BizTaskID = override.BizTaskID
	}
	if strings.TrimSpace(override.Params) != "" {
		log.Warn().Str("new", override.Params).Msg("overriding field Params")
		result.Params = override.Params
	}
	if strings.TrimSpace(override.App) != "" {
		log.Warn().Str("new", override.App).Msg("overriding field App")
		result.App = override.App
	}
	if strings.TrimSpace(override.Scene) != "" {
		log.Warn().Str("new", override.Scene).Msg("overriding field Scene")
		result.Scene = override.Scene
	}
	if strings.TrimSpace(override.StartAt) != "" {
		log.Warn().Str("new", override.StartAt).Msg("overriding field StartAt")
		result.StartAt = override.StartAt
	}
	if strings.TrimSpace(override.EndAt) != "" {
		log.Warn().Str("new", override.EndAt).Msg("overriding field EndAt")
		result.EndAt = override.EndAt
	}
	if strings.TrimSpace(override.Date) != "" {
		log.Warn().Str("new", override.Date).Msg("overriding field Date")
		result.Date = override.Date
	}
	if strings.TrimSpace(override.DispatchedAt) != "" {
		log.Warn().Str("new", override.DispatchedAt).Msg("overriding field DispatchedAt")
		result.DispatchedAt = override.DispatchedAt
	}
	if strings.TrimSpace(override.Status) != "" {
		log.Warn().Str("new", override.Status).Msg("overriding field Status")
		result.Status = override.Status
	}
	if strings.TrimSpace(override.Webhook) != "" {
		log.Warn().Str("new", override.Webhook).Msg("overriding field Webhook")
		result.Webhook = override.Webhook
	}
	if strings.TrimSpace(override.UserID) != "" {
		log.Warn().Str("new", override.UserID).Msg("overriding field UserID")
		result.UserID = override.UserID
	}
	if strings.TrimSpace(override.UserName) != "" {
		log.Warn().Str("new", override.UserName).Msg("overriding field UserName")
		result.UserName = override.UserName
	}
	if strings.TrimSpace(override.Extra) != "" {
		log.Warn().Str("new", override.Extra).Msg("overriding field Extra")
		result.Extra = override.Extra
	}
	if strings.TrimSpace(override.GroupID) != "" {
		log.Warn().Str("new", override.GroupID).Msg("overriding field GroupID")
		result.GroupID = override.GroupID
	}
	if strings.TrimSpace(override.DeviceSerial) != "" {
		log.Warn().Str("new", override.DeviceSerial).Msg("overriding field DeviceSerial")
		result.DeviceSerial = override.DeviceSerial
	}
	if strings.TrimSpace(override.DispatchedDevice) != "" {
		log.Warn().Str("new", override.DispatchedDevice).Msg("overriding field DispatchedDevice")
		result.DispatchedDevice = override.DispatchedDevice
	}
	if strings.TrimSpace(override.ElapsedSeconds) != "" {
		log.Warn().Str("new", override.ElapsedSeconds).Msg("overriding field ElapsedSeconds")
		result.ElapsedSeconds = override.ElapsedSeconds
	}
	if strings.TrimSpace(override.DispatchedAt) != "" {
		log.Warn().Str("new", override.DispatchedAt).Msg("overriding field DispatchedAt")
		result.DispatchedAt = override.DispatchedAt
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
	if strings.TrimSpace(override.ItemCDNURL) != "" {
		result.ItemCDNURL = override.ItemCDNURL
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
	if strings.TrimSpace(override.UserAlias) != "" {
		result.UserAlias = override.UserAlias
	}
	if strings.TrimSpace(override.UserAuthEntity) != "" {
		result.UserAuthEntity = override.UserAuthEntity
	}
	if strings.TrimSpace(override.Tags) != "" {
		result.Tags = override.Tags
	}
	if strings.TrimSpace(override.TaskID) != "" {
		result.TaskID = override.TaskID
	}
	if strings.TrimSpace(override.Extra) != "" {
		result.Extra = override.Extra
	}
	if strings.TrimSpace(override.CommentCount) != "" {
		result.CommentCount = override.CommentCount
	}
	if strings.TrimSpace(override.CollectCount) != "" {
		result.CollectCount = override.CollectCount
	}
	if strings.TrimSpace(override.ForwardCount) != "" {
		result.ForwardCount = override.ForwardCount
	}
	if strings.TrimSpace(override.ShareCount) != "" {
		result.ShareCount = override.ShareCount
	}
	if strings.TrimSpace(override.PayMode) != "" {
		result.PayMode = override.PayMode
	}
	if strings.TrimSpace(override.Collection) != "" {
		result.Collection = override.Collection
	}
	if strings.TrimSpace(override.Episode) != "" {
		result.Episode = override.Episode
	}
	if strings.TrimSpace(override.PublishTime) != "" {
		result.PublishTime = override.PublishTime
	}
	return result
}

func buildTaskRecordPayloads(records []TaskRecordInput, fields TaskFields) ([]map[string]any, error) {
	if strings.TrimSpace(fields.TaskID) == "" {
		return nil, errors.New("feishu: TaskID column name is empty")
	}
	result := make([]map[string]any, 0, len(records))
	for idx, rec := range records {
		row := make(map[string]any)
		if rec.TaskID != 0 {
			row[fields.TaskID] = rec.TaskID
		}
		if strings.TrimSpace(rec.BizTaskID) != "" && strings.TrimSpace(fields.BizTaskID) != "" {
			row[fields.BizTaskID] = strings.TrimSpace(rec.BizTaskID)
		}
		if rec.ParentTaskID > 0 && strings.TrimSpace(fields.ParentTaskID) != "" {
			row[fields.ParentTaskID] = rec.ParentTaskID
		}
		addOptionalField(row, fields.ItemID, rec.ItemID)
		addOptionalField(row, fields.BookID, rec.BookID)
		addOptionalField(row, fields.URL, rec.URL)
		addOptionalField(row, fields.Params, rec.Params)
		addOptionalField(row, fields.App, rec.App)
		addOptionalField(row, fields.Scene, rec.Scene)
		if start := formatRecordDatetimeString(rec.StartAt, rec.StartAtRaw); start != "" && strings.TrimSpace(fields.StartAt) != "" {
			row[fields.StartAt] = normalizeDatetimePayload(start)
		}
		if end := formatRecordDatetimeString(rec.EndAt, rec.EndAtRaw); end != "" && strings.TrimSpace(fields.EndAt) != "" {
			row[fields.EndAt] = normalizeDatetimePayload(end)
		}
		if dt := formatRecordDatetimeString(rec.Datetime, rec.DatetimeRaw); dt != "" && strings.TrimSpace(fields.Date) != "" {
			row[fields.Date] = normalizeDatetimePayload(dt)
		}
		if dispatched := formatRecordDatetimeString(rec.DispatchedAt, rec.DispatchedAtRaw); dispatched != "" && strings.TrimSpace(fields.DispatchedAt) != "" {
			row[fields.DispatchedAt] = normalizeDatetimePayload(dispatched)
		}
		if strings.TrimSpace(fields.Status) != "" && rec.Status != "" {
			row[fields.Status] = rec.Status
		}
		addOptionalField(row, fields.Logs, rec.Logs)
		addOptionalField(row, fields.UserID, rec.UserID)
		addOptionalField(row, fields.UserName, rec.UserName)
		addOptionalField(row, fields.Extra, rec.Extra)
		addOptionalField(row, fields.GroupID, rec.GroupID)
		addOptionalField(row, fields.DeviceSerial, rec.DeviceSerial)
		addOptionalField(row, fields.DispatchedDevice, rec.DispatchedDevice)
		if strings.TrimSpace(fields.ElapsedSeconds) != "" && rec.ElapsedSeconds != nil {
			row[fields.ElapsedSeconds] = *rec.ElapsedSeconds
		}
		if strings.TrimSpace(fields.ItemsCollected) != "" && rec.ItemsCollected != nil {
			row[fields.ItemsCollected] = *rec.ItemsCollected
		}
		if len(row) == 0 {
			return nil, fmt.Errorf("feishu: record %d has no fields to set", idx)
		}
		result = append(result, row)
	}
	return result, nil
}

func buildResultRecordPayloads(records []ResultRecordInput, fields ResultFields) (result []map[string]any, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "build result record payloads failed")
		}
	}()

	result = make([]map[string]any, 0, len(records))
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
		addOptionalField(row, fields.ItemCDNURL, rec.ItemCDNURL)
		addOptionalField(row, fields.ItemURL, rec.ItemURL)
		addOptionalNumber(row, fields.ItemDuration, rec.ItemDurationSeconds)
		addOptionalField(row, fields.UserName, rec.UserName)
		addOptionalField(row, fields.UserID, rec.UserID)
		addOptionalField(row, fields.UserAlias, rec.UserAlias)
		addOptionalField(row, fields.UserAuthEntity, rec.UserAuthEntity)
		addOptionalField(row, fields.Tags, rec.Tags)
		if strings.TrimSpace(fields.TaskID) != "" && rec.TaskID != 0 {
			row[fields.TaskID] = rec.TaskID
		}
		if extra, err := encodeExtraPayload(rec.Extra); err != nil {
			return nil, fmt.Errorf("feishu: result record %d invalid extra payload: %w", idx, err)
		} else if extra != "" && strings.TrimSpace(fields.Extra) != "" {
			row[fields.Extra] = extra
		}
		addOptionalInt64(row, fields.LikeCount, rec.LikeCount)
		addOptionalInt64(row, fields.ViewCount, int64(rec.ViewCount))
		addOptionalInt64(row, fields.CommentCount, rec.CommentCount)
		addOptionalInt64(row, fields.CollectCount, rec.CollectCount)
		addOptionalInt64(row, fields.ForwardCount, rec.ForwardCount)
		addOptionalInt64(row, fields.ShareCount, rec.ShareCount)
		addOptionalField(row, fields.AnchorPoint, rec.AnchorPoint)
		addOptionalField(row, fields.PayMode, rec.PayMode)
		addOptionalField(row, fields.Collection, rec.Collection)
		addOptionalField(row, fields.Episode, rec.Episode)
		addOptionalField(row, fields.PublishTime, rec.PublishTime)
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

func addOptionalInt64(dst map[string]any, column string, value int64) {
	if strings.TrimSpace(column) == "" || value == 0 {
		return
	}
	dst[column] = value
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

func normalizeDatetimePayload(value string) any {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return trimmed
	}
	if num, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return num
	}
	return trimmed
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

func encodeExtraPayload(value any) (string, error) {
	switch v := value.(type) {
	case nil:
		return "", nil
	case string:
		if strings.TrimSpace(v) == "" {
			return "", nil
		}
		if !json.Valid([]byte(v)) {
			return "", fmt.Errorf("extra field string is not valid JSON")
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
			return "", fmt.Errorf("extra field bytes are not valid JSON")
		}
		return string(v), nil
	case json.RawMessage:
		return encodeExtraPayload([]byte(v))
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("marshal extra field: %w", err)
		}
		return string(raw), nil
	}
}

type bitablePageInfo struct {
	HasMore       bool
	NextPageToken string
	Pages         int
}

func clampBitablePageSize(pageSize int) int {
	if pageSize <= 0 {
		return defaultBitablePageSize
	}
	if pageSize > maxBitablePageSize {
		return maxBitablePageSize
	}
	return pageSize
}

func requireBitableAppToken(ref BitableRef) error {
	if strings.TrimSpace(ref.AppToken) == "" {
		return errors.New("feishu: bitable app token is empty")
	}
	return nil
}

func requireBitableTableID(ref BitableRef) error {
	if strings.TrimSpace(ref.TableID) == "" {
		return errors.New("feishu: bitable table id is empty")
	}
	return nil
}

func requireBitableAppTable(ref BitableRef) error {
	if err := requireBitableAppToken(ref); err != nil {
		return err
	}
	if err := requireBitableTableID(ref); err != nil {
		return err
	}
	return nil
}

func (c *Client) bitableSDK(ctx context.Context) (bitableAppTableRecordAPI, larkcore.RequestOptionFunc, error) {
	if c == nil {
		return nil, nil, errors.New("feishu: client is nil")
	}
	api := c.bitableAppTableRecord()
	if api == nil {
		return nil, nil, errors.New("feishu: bitable sdk client is nil")
	}
	token, err := c.getTenantAccessToken(ctx)
	if err != nil {
		return nil, nil, err
	}
	return api, larkcore.WithTenantAccessToken(token), nil
}

func ensureSDKSuccess(action string, ok bool, code int, msg, logID string) error {
	if ok {
		return nil
	}
	if strings.TrimSpace(logID) == "" {
		return fmt.Errorf("feishu: %s failed code=%d msg=%s", action, code, msg)
	}
	return fmt.Errorf("feishu: %s failed code=%d msg=%s log_id=%s", action, code, msg, logID)
}

// BitableRow wraps a raw bitable record so external packages can read arbitrary columns.
type BitableRow struct {
	RecordID string
	Fields   map[string]any
}

// BatchGetBitableRows wraps BatchGetBitableRecords and exposes BitableRow so
// external packages can inspect arbitrary columns for a given record id set.
func (c *Client) BatchGetBitableRows(ctx context.Context, ref BitableRef, recordIDs []string, userIDType string) ([]BitableRow, error) {
	records, err := c.BatchGetBitableRecords(ctx, ref, recordIDs, userIDType)
	if err != nil {
		return nil, err
	}
	rows := make([]BitableRow, 0, len(records))
	for _, rec := range records {
		if rec == nil {
			continue
		}
		rows = append(rows, BitableRow{
			RecordID: strings.TrimSpace(larkcore.StringValue(rec.RecordId)),
			Fields:   rec.Fields,
		})
	}
	return rows, nil
}

func (c *Client) listBitableRecords(ctx context.Context, ref BitableRef, pageSize int, opts *QueryOptions) ([]*larkbitable.AppTableRecord, bitablePageInfo, error) {
	if c.useHTTP() {
		return c.listBitableRecordsHTTP(ctx, ref, pageSize, opts)
	}
	return c.listBitableRecordsSDK(ctx, ref, pageSize, opts)
}

func (c *Client) listBitableRecordsHTTP(ctx context.Context, ref BitableRef, pageSize int, opts *QueryOptions) ([]*larkbitable.AppTableRecord, bitablePageInfo, error) {
	if err := requireBitableAppTable(ref); err != nil {
		return nil, bitablePageInfo{}, err
	}
	pageSize = clampBitablePageSize(pageSize)

	basePath := fmt.Sprintf("/open-apis/bitable/v1/apps/%s/tables/%s/records/search", url.PathEscape(ref.AppToken), url.PathEscape(ref.TableID))
	values := url.Values{}
	values.Set("page_size", strconv.Itoa(pageSize))

	limit := 0
	if opts != nil && opts.Limit > 0 {
		limit = opts.Limit
		if limit > 0 && limit < pageSize {
			values.Set("page_size", strconv.Itoa(limit))
		}
	}
	maxPages := 0
	if opts != nil && opts.MaxPages > 0 {
		maxPages = opts.MaxPages
	}

	var filterInfo *FilterInfo
	if opts != nil && opts.Filter != nil {
		filterInfo = CloneFilter(opts.Filter)
	}

	filterQuery := (*QueryOptions)(nil)
	if filterInfo != nil && filterInfo.QueryOptions != nil {
		filterQuery = filterInfo.QueryOptions
	}
	if maxPages <= 0 && filterQuery != nil && filterQuery.MaxPages > 0 {
		maxPages = filterQuery.MaxPages
	}

	useView := true
	if opts != nil && opts.IgnoreView {
		useView = false
	} else if filterQuery != nil && filterQuery.IgnoreView {
		useView = false
	}
	viewID := ""
	if useView {
		viewID = strings.TrimSpace(ref.ViewID)
		if opts != nil && strings.TrimSpace(opts.ViewID) != "" {
			viewID = strings.TrimSpace(opts.ViewID)
		} else if filterQuery != nil && strings.TrimSpace(filterQuery.ViewID) != "" {
			viewID = strings.TrimSpace(filterQuery.ViewID)
		}
	}

	var body map[string]any
	if viewID != "" || filterInfo != nil {
		body = make(map[string]any)
		if viewID != "" {
			body["view_id"] = viewID
		}
		if filterInfo != nil {
			body["filter"] = filterInfo
		}
	}

	all := make([]*larkbitable.AppTableRecord, 0, pageSize)
	pageToken := ""
	if opts != nil && strings.TrimSpace(opts.PageToken) != "" {
		pageToken = strings.TrimSpace(opts.PageToken)
	} else if filterQuery != nil && strings.TrimSpace(filterQuery.PageToken) != "" {
		pageToken = strings.TrimSpace(filterQuery.PageToken)
	}
	pageInfo := bitablePageInfo{}

	start := time.Now()
	page := 0

	for {
		if pageToken != "" {
			values.Set("page_token", pageToken)
		} else {
			values.Del("page_token")
		}
		path := basePath
		if enc := values.Encode(); enc != "" {
			path = path + "?" + enc
		}
		_, raw, err := c.doJSONRequest(ctx, http.MethodPost, path, body)
		if err != nil {
			pageInfo.Pages = page
			return nil, pageInfo, err
		}

		var resp struct {
			Code int    `json:"code"`
			Msg  string `json:"msg"`
			Data struct {
				Items     []*larkbitable.AppTableRecord `json:"items"`
				HasMore   bool                          `json:"has_more"`
				PageToken string                        `json:"page_token"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &resp); err != nil {
			pageInfo.Pages = page
			return nil, pageInfo, fmt.Errorf("feishu: decode bitable search response: %w", err)
		}
		if resp.Code != 0 {
			filterJSON := ""
			if filterInfo != nil {
				if b, err := json.Marshal(filterInfo); err == nil {
					filterJSON = string(b)
					const maxLen = 2048
					if len(filterJSON) > maxLen {
						filterJSON = filterJSON[:maxLen] + "...(truncated)"
					}
				}
			}
			pageInfo.Pages = page
			return nil, pageInfo, fmt.Errorf(
				"feishu: search bitable records failed code=%d msg=%s table_id=%s view_id=%s filter=%s",
				resp.Code, resp.Msg, ref.TableID, viewID, filterJSON,
			)
		}

		page++
		all = append(all, resp.Data.Items...)
		pageInfo.HasMore = resp.Data.HasMore
		pageInfo.NextPageToken = strings.TrimSpace(resp.Data.PageToken)
		pageInfo.Pages = page
		if limit > 0 && len(all) >= limit {
			break
		}
		if maxPages > 0 && page >= maxPages {
			break
		}
		if !resp.Data.HasMore || pageInfo.NextPageToken == "" {
			break
		}
		pageToken = pageInfo.NextPageToken
	}

	truncated := false
	if limit > 0 && len(all) > limit {
		all = all[:limit]
		truncated = true
	}

	log.Debug().
		Str("table_id", ref.TableID).
		Str("view_id", viewID).
		Str("filter", filterInfo.JSONString()).
		Int("pages", page).
		Int("count", len(all)).
		Bool("truncated", truncated).
		Int("page_size", pageSize).
		Int("limit", limit).
		Dur("elapsed", time.Since(start)).
		Msg("bitable records fetched (http)")

	return all, pageInfo, nil
}

func (c *Client) listBitableRecordsSDK(ctx context.Context, ref BitableRef, pageSize int, opts *QueryOptions) ([]*larkbitable.AppTableRecord, bitablePageInfo, error) {
	if err := requireBitableAppTable(ref); err != nil {
		return nil, bitablePageInfo{}, err
	}
	api, opt, err := c.bitableSDK(ctx)
	if err != nil {
		return nil, bitablePageInfo{}, err
	}
	pageSize = clampBitablePageSize(pageSize)

	limit := 0
	if opts != nil && opts.Limit > 0 {
		limit = opts.Limit
		if limit > 0 && limit < pageSize {
			pageSize = limit
		}
	}
	maxPages := 0
	if opts != nil && opts.MaxPages > 0 {
		maxPages = opts.MaxPages
	}

	var filterInfo *FilterInfo
	if opts != nil && opts.Filter != nil {
		filterInfo = CloneFilter(opts.Filter)
	}

	filterQuery := (*QueryOptions)(nil)
	if filterInfo != nil && filterInfo.QueryOptions != nil {
		filterQuery = filterInfo.QueryOptions
	}
	if maxPages <= 0 && filterQuery != nil && filterQuery.MaxPages > 0 {
		maxPages = filterQuery.MaxPages
	}

	useView := true
	if opts != nil && opts.IgnoreView {
		useView = false
	} else if filterQuery != nil && filterQuery.IgnoreView {
		useView = false
	}
	viewID := ""
	if useView {
		viewID = strings.TrimSpace(ref.ViewID)
		if opts != nil && strings.TrimSpace(opts.ViewID) != "" {
			viewID = strings.TrimSpace(opts.ViewID)
		} else if filterQuery != nil && strings.TrimSpace(filterQuery.ViewID) != "" {
			viewID = strings.TrimSpace(filterQuery.ViewID)
		}
	}

	var body *larkbitable.SearchAppTableRecordReqBody
	if viewID != "" || filterInfo != nil {
		body = &larkbitable.SearchAppTableRecordReqBody{}
		if viewID != "" {
			body.ViewId = larkcore.StringPtr(viewID)
		}
		if filterInfo != nil {
			body.Filter = &larkbitable.FilterInfo{
				Conjunction: filterInfo.Conjunction,
				Conditions:  filterInfo.Conditions,
				Children:    filterInfo.Children,
			}
		}
	}

	all := make([]*larkbitable.AppTableRecord, 0, pageSize)
	pageToken := ""
	if opts != nil && strings.TrimSpace(opts.PageToken) != "" {
		pageToken = strings.TrimSpace(opts.PageToken)
	} else if filterQuery != nil && strings.TrimSpace(filterQuery.PageToken) != "" {
		pageToken = strings.TrimSpace(filterQuery.PageToken)
	}
	pageInfo := bitablePageInfo{}

	start := time.Now()
	page := 0
	filterJSON := filterInfo.JSONString()

	for {
		resp, err := api.Search(ctx, ref.AppToken, ref.TableID, pageSize, pageToken, body, opt)
		if err != nil {
			pageInfo.Pages = page
			return nil, pageInfo, fmt.Errorf("feishu: search bitable records request failed: %w", err)
		}
		if resp == nil || resp.ApiResp == nil {
			pageInfo.Pages = page
			return nil, pageInfo, errors.New("feishu: empty response when searching bitable records")
		}
		if !resp.Success() {
			pageInfo.Pages = page
			return nil, pageInfo, fmt.Errorf(
				"feishu: search bitable records failed code=%d msg=%s table_id=%s view_id=%s filter=%s log_id=%s",
				resp.Code, resp.Msg, ref.TableID, viewID, filterJSON, resp.RequestId(),
			)
		}

		data := resp.Data
		items := []*larkbitable.AppTableRecord(nil)
		hasMore := false
		nextToken := ""
		if data != nil {
			items = data.Items
			hasMore = larkcore.BoolValue(data.HasMore)
			nextToken = strings.TrimSpace(larkcore.StringValue(data.PageToken))
		}

		page++
		for _, item := range items {
			if item == nil {
				continue
			}
			all = append(all, item)
		}

		pageInfo.HasMore = hasMore
		pageInfo.NextPageToken = nextToken
		pageInfo.Pages = page
		if limit > 0 && len(all) >= limit {
			break
		}
		if maxPages > 0 && page >= maxPages {
			break
		}
		if !hasMore || nextToken == "" {
			break
		}
		pageToken = nextToken
	}

	truncated := false
	if limit > 0 && len(all) > limit {
		all = all[:limit]
		truncated = true
	}

	log.Debug().
		Str("table_id", ref.TableID).
		Str("view_id", viewID).
		Str("filter", filterJSON).
		Int("pages", page).
		Int("count", len(all)).
		Bool("truncated", truncated).
		Int("page_size", pageSize).
		Int("limit", limit).
		Dur("elapsed", time.Since(start)).
		Msg("bitable records fetched (sdk)")

	return all, pageInfo, nil
}

func decodeTaskRow(rec *larkbitable.AppTableRecord, fields TaskFields) (TaskRow, error) {
	if rec == nil {
		return TaskRow{}, errors.New("record is nil")
	}
	if rec.Fields == nil {
		return TaskRow{}, fmt.Errorf("record %s has no fields", larkcore.StringValue(rec.RecordId))
	}
	taskID, err := bitableIntField(rec.Fields, fields.TaskID)
	if err != nil {
		return TaskRow{}, fmt.Errorf("record %s: %w", larkcore.StringValue(rec.RecordId), err)
	}
	status, err := bitableStringField(rec.Fields, fields.Status, true)
	if err != nil {
		return TaskRow{}, fmt.Errorf("record %s: %w", larkcore.StringValue(rec.RecordId), err)
	}

	var parentTaskID int64
	if field := strings.TrimSpace(fields.ParentTaskID); field != "" {
		if val, ok := rec.Fields[field]; ok {
			if n, err := toInt64(val); err == nil {
				parentTaskID = n
			}
		}
	}

	var bizTaskID string
	if field := strings.TrimSpace(fields.BizTaskID); field != "" {
		bizTaskID = bitableOptionalString(rec.Fields, field)
	}

	targetDevice := strings.TrimSpace(bitableOptionalString(rec.Fields, fields.DeviceSerial))
	dispatchedDevice := strings.TrimSpace(bitableOptionalString(rec.Fields, fields.DispatchedDevice))
	row := TaskRow{
		RecordID:         strings.TrimSpace(larkcore.StringValue(rec.RecordId)),
		TaskID:           taskID,
		BizTaskID:        bizTaskID,
		ParentTaskID:     parentTaskID,
		Params:           bitableOptionalString(rec.Fields, fields.Params),
		ItemID:           bitableOptionalString(rec.Fields, fields.ItemID),
		BookID:           bitableOptionalString(rec.Fields, fields.BookID),
		URL:              bitableOptionalString(rec.Fields, fields.URL),
		App:              bitableOptionalString(rec.Fields, fields.App),
		Scene:            bitableOptionalString(rec.Fields, fields.Scene),
		Status:           status,
		Webhook:          bitableOptionalString(rec.Fields, fields.Webhook),
		UserID:           bitableOptionalString(rec.Fields, fields.UserID),
		UserName:         bitableOptionalString(rec.Fields, fields.UserName),
		Extra:            bitableOptionalExtra(rec.Fields, fields.Extra, taskID),
		Logs:             bitableOptionalString(rec.Fields, fields.Logs),
		GroupID:          bitableOptionalString(rec.Fields, fields.GroupID),
		DeviceSerial:     targetDevice,
		DispatchedDevice: dispatchedDevice,
	}

	if raw, parsed := bitableOptionalTimestamp(rec.Fields, fields.StartAt); raw != "" {
		row.StartAtRaw = raw
		row.StartAt = parsed
	}
	if raw, parsed := bitableOptionalTimestamp(rec.Fields, fields.EndAt); raw != "" {
		row.EndAtRaw = raw
		row.EndAt = parsed
	}
	if raw, parsed := bitableOptionalTimestamp(rec.Fields, fields.Date); raw != "" {
		row.DatetimeRaw = raw
		row.Datetime = parsed
	}
	if raw, parsed := bitableOptionalTimestamp(rec.Fields, fields.DispatchedAt); raw != "" {
		row.DispatchedAtRaw = raw
		row.DispatchedAt = parsed
	}

	if field := strings.TrimSpace(fields.ElapsedSeconds); field != "" {
		if val, ok := rec.Fields[field]; ok {
			if secs, err := toInt64(val); err == nil {
				row.ElapsedSeconds = secs
			}
		}
	}
	if field := strings.TrimSpace(fields.ItemsCollected); field != "" {
		if val, ok := rec.Fields[field]; ok {
			if count, err := toInt64(val); err == nil {
				row.ItemsCollected = count
			}
		}
	}
	if field := strings.TrimSpace(fields.RetryCount); field != "" {
		if val, ok := rec.Fields[field]; ok {
			if count, err := toInt64(val); err == nil {
				row.RetryCount = count
			}
		}
	}

	return row, nil
}

func isMissingFieldError(err error, field string) bool {
	if err == nil || strings.TrimSpace(field) == "" {
		return false
	}
	needle := fmt.Sprintf("missing field %q", field)
	return strings.Contains(err.Error(), needle)
}

func bitableIntField(fields map[string]any, name string) (int64, error) {
	val, ok := fields[name]
	if !ok {
		return 0, fmt.Errorf("missing field %q", name)
	}
	return toInt64(val)
}

func bitableStringField(fields map[string]any, name string, allowEmpty bool) (string, error) {
	if fields == nil {
		return "", fmt.Errorf("missing field %q", name)
	}
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
	if name == "" || fields == nil {
		return ""
	}
	return toString(fields[name])
}

func bitableOptionalExtra(fields map[string]any, name string, taskID int64) string {
	if name == "" || fields == nil {
		return ""
	}
	val, ok := fields[name]
	if !ok {
		return ""
	}
	log.Trace().Any("extra", val).Int64("taskID", taskID).Msg("decoding bitable extra field")
	switch typed := val.(type) {
	case []any:
		var builder strings.Builder
		for _, item := range typed {
			segment := bitableExtraSegmentString(item)
			if segment == "" {
				continue
			}
			builder.WriteString(segment)
		}
		return builder.String()
	default:
		return bitableExtraSegmentString(typed)
	}
}

func bitableExtraSegmentString(value any) string {
	switch typed := value.(type) {
	case map[string]any:
		if raw, ok := typed["text"]; ok {
			if str := toString(raw); str != "" {
				return str
			}
		}
		if raw, ok := typed["link"]; ok {
			if str := toString(raw); str != "" {
				return str
			}
		}
		if raw, ok := typed["value"]; ok {
			if str := toString(raw); str != "" {
				return str
			}
		}
		return ""
	default:
		return toString(typed)
	}
}

func buildTaskIDQueryOptions(fields TaskFields, taskIDs []int64) *QueryOptions {
	filter := buildTaskIDFilter(fields, taskIDs)
	if filter == nil {
		return nil
	}
	limit := len(taskIDs)
	if limit < 0 {
		limit = 0
	}
	return &QueryOptions{
		Filter:     filter,
		Limit:      limit,
		IgnoreView: true,
	}
}

func buildTaskIDFilter(fields TaskFields, taskIDs []int64) *FilterInfo {
	field := strings.TrimSpace(fields.TaskID)
	if field == "" {
		return nil
	}
	seen := make(map[int64]struct{}, len(taskIDs))
	conds := make([]*Condition, 0, len(taskIDs))
	for _, id := range taskIDs {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		conds = append(conds, NewCondition(field, "is", fmt.Sprintf("%d", id)))
	}
	if len(conds) == 0 {
		return nil
	}
	filter := NewFilterInfo("or")
	filter.Conditions = append(filter.Conditions, conds...)
	return filter
}

func bitableOptionalTimestamp(fields map[string]any, name string) (string, *time.Time) {
	if name == "" || fields == nil {
		return "", nil
	}
	val, ok := fields[name]
	if !ok || val == nil {
		return "", nil
	}
	ts, ok := bitableTimestampValue(val)
	if !ok {
		return "", nil
	}
	parsed := time.UnixMilli(ts).In(time.Local)
	return strconv.FormatInt(ts, 10), &parsed
}

func bitableTimestampValue(value any) (int64, bool) {
	switch v := value.(type) {
	case float64:
		if math.Mod(v, 1) != 0 {
			return 0, false
		}
		return int64(v), true
	case int:
		return int64(v), true
	case int64:
		return v, true
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return 0, false
		}
		return n, true
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return 0, false
		}
		n, err := strconv.ParseInt(trimmed, 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	default:
		return 0, false
	}
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
	return BitableValueToInt64Strict(value)
}

func toString(value any) string {
	return BitableValueToString(value)
}

func (c *Client) updateBitableRecord(ctx context.Context, ref BitableRef, recordID string, fields map[string]any) error {
	if c.useHTTP() {
		return c.updateBitableRecordHTTP(ctx, ref, recordID, fields)
	}
	return c.updateBitableRecordSDK(ctx, ref, recordID, fields)
}

func (c *Client) updateBitableRecordHTTP(ctx context.Context, ref BitableRef, recordID string, fields map[string]any) error {
	if recordID == "" {
		return errors.New("feishu: record id is empty")
	}
	if len(fields) == 0 {
		return errors.New("feishu: no fields provided for update")
	}
	if err := requireBitableAppTable(ref); err != nil {
		return err
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

func (c *Client) updateBitableRecordSDK(ctx context.Context, ref BitableRef, recordID string, fields map[string]any) error {
	if strings.TrimSpace(recordID) == "" {
		return errors.New("feishu: record id is empty")
	}
	if len(fields) == 0 {
		return errors.New("feishu: no fields provided for update")
	}
	if err := requireBitableAppTable(ref); err != nil {
		return err
	}
	api, opt, err := c.bitableSDK(ctx)
	if err != nil {
		return err
	}

	record := larkbitable.NewAppTableRecordBuilder().
		Fields(fields).
		Build()
	resp, err := api.Update(ctx, ref.AppToken, ref.TableID, recordID, record, opt)
	if err != nil {
		return fmt.Errorf("feishu: update record request failed: %w", err)
	}
	if resp == nil || resp.ApiResp == nil {
		return errors.New("feishu: empty response when updating record")
	}
	return ensureSDKSuccess("update record", resp.Success(), resp.Code, resp.Msg, resp.RequestId())
}

func (c *Client) createBitableRecord(ctx context.Context, ref BitableRef, fields map[string]any) (recordID string, err error) {
	if c.useHTTP() {
		return c.createBitableRecordHTTP(ctx, ref, fields)
	}
	return c.createBitableRecordSDK(ctx, ref, fields)
}

func (c *Client) createBitableRecordHTTP(ctx context.Context, ref BitableRef, fields map[string]any) (recordID string, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "create bitable record failed")
		}
	}()

	if len(fields) == 0 {
		return "", errors.New("feishu: no fields provided for creation")
	}
	if err := requireBitableAppTable(ref); err != nil {
		return "", err
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
			Record *larkbitable.AppTableRecord `json:"record"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return "", fmt.Errorf("feishu: decode create response: %w", err)
	}
	if resp.Code != 0 {
		return "", fmt.Errorf("feishu: create record failed code=%d msg=%s", resp.Code, resp.Msg)
	}
	if resp.Data.Record == nil {
		return "", errors.New("feishu: create record response missing record")
	}
	id := strings.TrimSpace(larkcore.StringValue(resp.Data.Record.RecordId))
	if id == "" {
		return "", errors.New("feishu: create record response missing record id")
	}
	return id, nil
}

func (c *Client) createBitableRecordSDK(ctx context.Context, ref BitableRef, fields map[string]any) (recordID string, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "create bitable record failed")
		}
	}()

	if len(fields) == 0 {
		return "", errors.New("feishu: no fields provided for creation")
	}
	if err := requireBitableAppTable(ref); err != nil {
		return "", err
	}
	api, opt, err := c.bitableSDK(ctx)
	if err != nil {
		return "", err
	}

	record := larkbitable.NewAppTableRecordBuilder().
		Fields(fields).
		Build()

	resp, err := api.Create(ctx, ref.AppToken, ref.TableID, record, opt)
	if err != nil {
		return "", fmt.Errorf("feishu: create record request failed: %w", err)
	}
	if resp == nil || resp.ApiResp == nil {
		return "", errors.New("feishu: empty response when creating record")
	}
	if err := ensureSDKSuccess("create record", resp.Success(), resp.Code, resp.Msg, resp.RequestId()); err != nil {
		return "", err
	}
	if resp.Data == nil || resp.Data.Record == nil {
		return "", errors.New("feishu: create record response missing record")
	}
	id := strings.TrimSpace(larkcore.StringValue(resp.Data.Record.RecordId))
	if id == "" {
		return "", errors.New("feishu: create record response missing record id")
	}
	return id, nil
}

func (c *Client) batchCreateBitableRecords(ctx context.Context, ref BitableRef, records []map[string]any) (recordIDs []string, err error) {
	if c.useHTTP() {
		return c.batchCreateBitableRecordsHTTP(ctx, ref, records)
	}
	return c.batchCreateBitableRecordsSDK(ctx, ref, records)
}

func (c *Client) batchCreateBitableRecordsHTTP(ctx context.Context, ref BitableRef, records []map[string]any) (recordIDs []string, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "batch create bitable records failed")
		}
	}()

	if len(records) == 0 {
		return nil, errors.New("feishu: no records provided for batch create")
	}
	if err := requireBitableAppTable(ref); err != nil {
		return nil, err
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
			Records []*larkbitable.AppTableRecord `json:"records"`
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
		if rec == nil {
			continue
		}
		id := strings.TrimSpace(larkcore.StringValue(rec.RecordId))
		if id == "" {
			return nil, errors.New("feishu: batch create response missing record id")
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (c *Client) batchCreateBitableRecordsSDK(ctx context.Context, ref BitableRef, records []map[string]any) (recordIDs []string, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "batch create bitable records failed")
		}
	}()

	if len(records) == 0 {
		return nil, errors.New("feishu: no records provided for batch create")
	}
	if err := requireBitableAppTable(ref); err != nil {
		return nil, err
	}
	api, opt, err := c.bitableSDK(ctx)
	if err != nil {
		return nil, err
	}

	items := make([]*larkbitable.AppTableRecord, 0, len(records))
	for _, fields := range records {
		if len(fields) == 0 {
			return nil, errors.New("feishu: record payload is empty")
		}
		items = append(items, larkbitable.NewAppTableRecordBuilder().
			Fields(fields).
			Build(),
		)
	}

	body := larkbitable.NewBatchCreateAppTableRecordReqBodyBuilder().
		Records(items).
		Build()

	resp, err := api.BatchCreate(ctx, ref.AppToken, ref.TableID, body, opt)
	if err != nil {
		return nil, fmt.Errorf("feishu: batch create request failed: %w", err)
	}
	if resp == nil || resp.ApiResp == nil {
		return nil, errors.New("feishu: empty response when batch creating records")
	}
	if err := ensureSDKSuccess("batch create records", resp.Success(), resp.Code, resp.Msg, resp.RequestId()); err != nil {
		return nil, err
	}
	if resp.Data == nil {
		return nil, errors.New("feishu: batch create response missing data")
	}

	ids := make([]string, 0, len(resp.Data.Records))
	for _, rec := range resp.Data.Records {
		if rec == nil {
			continue
		}
		id := strings.TrimSpace(larkcore.StringValue(rec.RecordId))
		if id == "" {
			return nil, errors.New("feishu: batch create response missing record id")
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (c *Client) getBitableRecord(ctx context.Context, ref BitableRef, recordID string) (*larkbitable.AppTableRecord, error) {
	if strings.TrimSpace(recordID) == "" {
		return nil, errors.New("feishu: record id is empty")
	}
	records, err := c.BatchGetBitableRecords(ctx, ref, []string{recordID}, "")
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("feishu: record %s not found", recordID)
	}
	return records[0], nil
}

// BatchUpdateBitableRecords updates multiple records in a single request.
// Max 500 records per request.
func (c *Client) BatchUpdateBitableRecords(ctx context.Context, ref BitableRef, records []map[string]any) (updated []*larkbitable.AppTableRecord, err error) {
	if c.useHTTP() {
		return c.batchUpdateBitableRecordsHTTP(ctx, ref, records)
	}
	return c.batchUpdateBitableRecordsSDK(ctx, ref, records)
}

func (c *Client) batchUpdateBitableRecordsHTTP(ctx context.Context, ref BitableRef, records []map[string]any) (updated []*larkbitable.AppTableRecord, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "batch update bitable records failed")
		}
	}()

	if len(records) == 0 {
		return nil, errors.New("feishu: no records provided for batch update")
	}
	if err := requireBitableAppTable(ref); err != nil {
		return nil, err
	}

	items := make([]map[string]any, 0, len(records))
	for _, fields := range records {
		if len(fields) == 0 {
			continue
		}
		// check if record_id exists in fields, if so, move it to top level
		recordID, ok := fields["record_id"].(string)
		if !ok || recordID == "" {
			// try to find it in "fields" map if structure is flattened?
			// The input `records` is expected to be a list of objects, each containing `record_id` and `fields`.
			// However, looking at `batchCreateBitableRecords`, it takes `[]map[string]any` where each map is the fields.
			// But for update, we need record_id.
			// Let's assume the input `records` here is a list of objects that ALREADY has "record_id" and "fields" keys,
			// OR it is a map of fields that INCLUDES "record_id".
			// Let's stick to the official API structure:
			// { "records": [ { "record_id": "...", "fields": { ... } } ] }
			// So the input `records` should be `[]map[string]any` where each map has "record_id" and "fields".
			return nil, errors.New("feishu: record_id is missing in update payload")
		}
		items = append(items, fields)
	}

	payload := map[string]any{"records": items}
	path := fmt.Sprintf("/open-apis/bitable/v1/apps/%s/tables/%s/records/batch_update", url.PathEscape(ref.AppToken), url.PathEscape(ref.TableID))

	_, raw, err := c.doJSONRequest(ctx, http.MethodPost, path, payload)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			Records []*larkbitable.AppTableRecord `json:"records"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("feishu: decode batch update response: %w", err)
	}
	if resp.Code != 0 {
		return nil, fmt.Errorf("feishu: batch update records failed code=%d msg=%s", resp.Code, resp.Msg)
	}

	return resp.Data.Records, nil
}

func (c *Client) batchUpdateBitableRecordsSDK(ctx context.Context, ref BitableRef, records []map[string]any) (updated []*larkbitable.AppTableRecord, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "batch update bitable records failed")
		}
	}()

	if len(records) == 0 {
		return nil, errors.New("feishu: no records provided for batch update")
	}
	if err := requireBitableAppTable(ref); err != nil {
		return nil, err
	}
	api, opt, err := c.bitableSDK(ctx)
	if err != nil {
		return nil, err
	}

	items := make([]*larkbitable.AppTableRecord, 0, len(records))
	for _, record := range records {
		if len(record) == 0 {
			continue
		}
		recordID := toString(record["record_id"])
		if recordID == "" {
			return nil, errors.New("feishu: record_id is missing in update payload")
		}

		fields, ok := record["fields"].(map[string]any)
		if !ok {
			fields = make(map[string]any, len(record))
			for key, val := range record {
				if key == "record_id" {
					continue
				}
				fields[key] = val
			}
		}

		items = append(items, larkbitable.NewAppTableRecordBuilder().
			RecordId(recordID).
			Fields(fields).
			Build(),
		)
	}
	if len(items) == 0 {
		return nil, errors.New("feishu: no records provided for batch update")
	}

	body := larkbitable.NewBatchUpdateAppTableRecordReqBodyBuilder().
		Records(items).
		Build()

	resp, err := api.BatchUpdate(ctx, ref.AppToken, ref.TableID, body, opt)
	if err != nil {
		return nil, fmt.Errorf("feishu: batch update request failed: %w", err)
	}
	if resp == nil || resp.ApiResp == nil {
		return nil, errors.New("feishu: empty response when batch updating records")
	}
	if err := ensureSDKSuccess("batch update records", resp.Success(), resp.Code, resp.Msg, resp.RequestId()); err != nil {
		return nil, err
	}
	if resp.Data == nil {
		return nil, errors.New("feishu: batch update response missing data")
	}

	out := make([]*larkbitable.AppTableRecord, 0, len(resp.Data.Records))
	for _, rec := range resp.Data.Records {
		if rec == nil {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

// BatchGetBitableRecords retrieves multiple records by their IDs.
// Max 100 records per request.
func (c *Client) BatchGetBitableRecords(ctx context.Context, ref BitableRef, recordIDs []string, userIDType string) (records []*larkbitable.AppTableRecord, err error) {
	if c.useHTTP() {
		return c.batchGetBitableRecordsHTTP(ctx, ref, recordIDs, userIDType)
	}
	return c.batchGetBitableRecordsSDK(ctx, ref, recordIDs, userIDType)
}

func (c *Client) batchGetBitableRecordsHTTP(ctx context.Context, ref BitableRef, recordIDs []string, userIDType string) (records []*larkbitable.AppTableRecord, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "batch get bitable records failed")
		}
	}()

	if len(recordIDs) == 0 {
		return nil, errors.New("feishu: no record ids provided")
	}
	if err := requireBitableAppTable(ref); err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/open-apis/bitable/v1/apps/%s/tables/%s/records/batch_get", url.PathEscape(ref.AppToken), url.PathEscape(ref.TableID))
	if userIDType != "" {
		path += "?user_id_type=" + url.QueryEscape(userIDType)
	}

	payload := map[string]any{
		"record_ids": recordIDs,
	}

	_, raw, err := c.doJSONRequest(ctx, http.MethodPost, path, payload)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			Records   []*larkbitable.AppTableRecord `json:"records"`
			Absent    []string                      `json:"absent_record_ids"`
			Forbidden []string                      `json:"forbidden_record_ids"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("feishu: decode batch get response: %w", err)
	}
	if resp.Code != 0 {
		return nil, fmt.Errorf("feishu: batch get records failed code=%d msg=%s", resp.Code, resp.Msg)
	}

	if len(resp.Data.Absent) > 0 {
		log.Warn().Strs("absent_ids", resp.Data.Absent).Msg("feishu: some records were absent")
	}
	if len(resp.Data.Forbidden) > 0 {
		log.Warn().Strs("forbidden_ids", resp.Data.Forbidden).Msg("feishu: some records were forbidden")
	}

	return resp.Data.Records, nil
}

func (c *Client) batchGetBitableRecordsSDK(ctx context.Context, ref BitableRef, recordIDs []string, userIDType string) (records []*larkbitable.AppTableRecord, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "batch get bitable records failed")
		}
	}()

	if len(recordIDs) == 0 {
		return nil, errors.New("feishu: no record ids provided")
	}
	if err := requireBitableAppTable(ref); err != nil {
		return nil, err
	}
	api, opt, err := c.bitableSDK(ctx)
	if err != nil {
		return nil, err
	}

	body := larkbitable.NewBatchGetAppTableRecordReqBodyBuilder().
		RecordIds(recordIDs).
		Build()
	if strings.TrimSpace(userIDType) != "" {
		body.UserIdType = larkcore.StringPtr(strings.TrimSpace(userIDType))
	}

	resp, err := api.BatchGet(ctx, ref.AppToken, ref.TableID, body, opt)
	if err != nil {
		return nil, fmt.Errorf("feishu: batch get request failed: %w", err)
	}
	if resp == nil || resp.ApiResp == nil {
		return nil, errors.New("feishu: empty response when batch getting records")
	}
	if err := ensureSDKSuccess("batch get records", resp.Success(), resp.Code, resp.Msg, resp.RequestId()); err != nil {
		return nil, err
	}
	if resp.Data == nil {
		return nil, errors.New("feishu: batch get response missing data")
	}

	if len(resp.Data.AbsentRecordIds) > 0 {
		log.Warn().Strs("absent_ids", resp.Data.AbsentRecordIds).Msg("feishu: some records were absent")
	}
	if len(resp.Data.ForbiddenRecordIds) > 0 {
		log.Warn().Strs("forbidden_ids", resp.Data.ForbiddenRecordIds).Msg("feishu: some records were forbidden")
	}

	out := make([]*larkbitable.AppTableRecord, 0, len(resp.Data.Records))
	for _, rec := range resp.Data.Records {
		if rec == nil {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
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
	if c.useHTTP() {
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

	api, opts, err := c.wikiSDK(ctx)
	if err != nil {
		return empty, err
	}
	resp, err := api.GetNode(ctx, wikiToken, opts...)
	if err != nil {
		return empty, fmt.Errorf("feishu: wiki get_node request failed: %w", err)
	}
	if resp == nil || resp.ApiResp == nil {
		return empty, errors.New("feishu: empty response when getting wiki node")
	}
	if err := ensureSDKSuccess("wiki get_node", resp.Success(), resp.Code, resp.Msg, resp.RequestId()); err != nil {
		return empty, err
	}
	if resp.Data == nil || resp.Data.Node == nil {
		return empty, errors.New("feishu: wiki node response missing node")
	}

	return wikiNodeInfo{
		ObjToken: larkcore.StringValue(resp.Data.Node.ObjToken),
		ObjType:  larkcore.StringValue(resp.Data.Node.ObjType),
	}, nil
}
