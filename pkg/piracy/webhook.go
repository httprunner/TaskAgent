package piracy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/httprunner/TaskAgent/pkg/feishu"
	"github.com/rs/zerolog/log"
)

// WebhookSource enumerates the supported data sources for summary lookups.
type WebhookSource string

const (
	// WebhookSourceFeishu queries Feishu Bitables for both drama metadata and capture records.
	WebhookSourceFeishu WebhookSource = "feishu"
	// WebhookSourceSQLite queries the local tracking SQLite database.
	WebhookSourceSQLite WebhookSource = "sqlite"

	defaultRecordLimit = 200
)

// WebhookOptions configures how summary data should be queried and delivered.
type WebhookOptions struct {
	App        string
	Params     string
	UserID     string
	UserName   string
	WebhookURL string

	// Source controls where drama and record data are retrieved from.
	Source WebhookSource

	// RecordLimit caps the number of capture records returned in the payload.
	RecordLimit int

	// ResultFilter is an optional Feishu filter expression appended to the auto-generated filters.
	ResultFilter string

	// Optional overrides per source.
	DramaTableURL  string
	ResultTableURL string
	SQLitePath     string

	// Custom HTTP client; nil falls back to a short-lived default client.
	HTTPClient *http.Client
}

// CaptureRecordPayload wraps a record ID and its field map so callers can access raw capture data.
type CaptureRecordPayload struct {
	RecordID string
	Fields   map[string]any
}

// SendSummaryWebhook aggregates drama metadata plus capture records and posts the payload to the provided webhook.
// The returned map mirrors all columns defined by feishu.DramaFields plus an extra `records` field that contains
// a list of capture records shaped by feishu.ResultFields.
func SendSummaryWebhook(ctx context.Context, opts WebhookOptions) (map[string]any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	webhookURL := strings.TrimSpace(opts.WebhookURL)
	if webhookURL == "" {
		return nil, errors.New("webhook url is required")
	}
	params := strings.TrimSpace(opts.Params)
	if params == "" {
		return nil, errors.New("params is required")
	}

	fields := loadSummaryFieldConfig()
	source := opts.Source
	if source == "" {
		source = WebhookSourceFeishu
	}

	limit := opts.RecordLimit
	if limit <= 0 {
		limit = defaultRecordLimit
	}

	ds, err := newSummaryDataSource(source, fields, opts)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := ds.Close(); cerr != nil {
			log.Warn().Err(cerr).Msg("summary datasource close failed")
		}
	}()

	drama, err := ds.FetchDrama(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("fetch drama info failed: %w", err)
	}

	records, err := ds.FetchRecords(ctx, recordQuery{
		App:         strings.TrimSpace(opts.App),
		Params:      params,
		UserID:      strings.TrimSpace(opts.UserID),
		UserName:    strings.TrimSpace(opts.UserName),
		Limit:       limit,
		ExtraFilter: strings.TrimSpace(opts.ResultFilter),
	})
	if err != nil {
		return nil, fmt.Errorf("fetch capture records failed: %w", err)
	}
	if records == nil {
		records = make([]CaptureRecordPayload, 0)
	}

	payload := buildWebhookPayload(drama, records, fields)
	if err := postWebhook(ctx, webhookURL, payload, opts.HTTPClient); err != nil {
		return nil, err
	}
	return payload, nil
}

func postWebhook(ctx context.Context, url string, payload map[string]any, client *http.Client) error {
	if payload == nil {
		return errors.New("payload is nil")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal webhook payload failed: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build webhook request failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	httpClient := client
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("post webhook failed: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook responded with status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	if len(bodyBytes) > 0 {
		log.Debug().Str("webhook_url", url).RawJSON("response_body", bodyBytes).Msg("webhook response")
	}
	return nil
}

func buildWebhookPayload(drama *dramaInfo, records []CaptureRecordPayload, fields summaryFieldConfig) map[string]any {
	var raw map[string]any
	var dramaName string
	if drama != nil {
		raw = drama.RawFields
		dramaName = drama.Name
	}
	payload := flattenDramaFields(raw, fields.Drama)
	// Ensure drama name is always populated even if the raw field is empty.
	if val, ok := payload["DramaName"]; !ok || strings.TrimSpace(fmt.Sprint(val)) == "" {
		payload["DramaName"] = dramaName
	}
	payload["records"] = flattenRecordFields(records, fields.Result)
	return payload
}

func flattenDramaFields(raw map[string]any, schema feishu.DramaFields) map[string]any {
	fieldMap := structFieldMap(schema)
	payload := make(map[string]any, len(fieldMap))
	for engName, rawKey := range fieldMap {
		if raw != nil {
			payload[engName] = raw[rawKey]
		} else {
			payload[engName] = nil
		}
	}
	return payload
}

func flattenRecordFields(records []CaptureRecordPayload, schema feishu.ResultFields) []map[string]any {
	fieldMap := structFieldMap(schema)
	result := make([]map[string]any, 0, len(records))
	for _, rec := range records {
		entry := make(map[string]any, len(fieldMap)+1)
		entry["_record_id"] = rec.RecordID
		for engName, rawKey := range fieldMap {
			if val, ok := rec.Fields[rawKey]; ok {
				entry[engName] = val
			} else {
				entry[engName] = nil
			}
		}
		result = append(result, entry)
	}
	return result
}

// structFieldMap returns a mapping of struct field names (English keys) to their raw column names.
func structFieldMap(schema any) map[string]string {
	val := reflect.ValueOf(schema)
	if val.Kind() == reflect.Pointer {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil
	}
	typ := val.Type()
	names := make(map[string]string, val.NumField())
	for i := 0; i < val.NumField(); i++ {
		fieldVal := val.Field(i)
		if fieldVal.Kind() != reflect.String {
			continue
		}
		rawName := strings.TrimSpace(fieldVal.String())
		if rawName == "" {
			continue
		}
		engName := typ.Field(i).Name
		names[engName] = rawName
	}
	return names
}

func loadSummaryFieldConfig() summaryFieldConfig {
	cfg := Config{}
	cfg.ApplyDefaults()

	dramaFields := feishu.DefaultDramaFields
	if v := pickFieldEnv("DRAMA_FIELD_ID", dramaFields.DramaID); v != "" {
		dramaFields.DramaID = v
	}
	dramaFields.DramaName = pickFieldEnv("DRAMA_FIELD_NAME", dramaFields.DramaName)
	if strings.TrimSpace(cfg.DramaNameField) != "" {
		dramaFields.DramaName = cfg.DramaNameField
	}
	if v := pickFieldEnv("DRAMA_FIELD_PRIORITY", dramaFields.Priority); v != "" {
		dramaFields.Priority = v
	}
	if v := pickFieldEnv("DRAMA_FIELD_RIGHTS_SCENARIO", dramaFields.RightsProtectionScenario); v != "" {
		dramaFields.RightsProtectionScenario = v
	}
	if strings.TrimSpace(cfg.DramaDurationField) != "" {
		dramaFields.TotalDuration = cfg.DramaDurationField
	}

	resultFields := feishu.DefaultResultFields
	if strings.TrimSpace(cfg.ParamsField) != "" {
		resultFields.Params = cfg.ParamsField
	}
	if strings.TrimSpace(cfg.UserIDField) != "" {
		resultFields.UserID = cfg.UserIDField
	}
	if strings.TrimSpace(cfg.DurationField) != "" {
		resultFields.ItemDuration = cfg.DurationField
	}
	if v := pickFieldEnv("RESULT_FIELD_APP", resultFields.App); v != "" {
		resultFields.App = v
	}
	if v := pickFieldEnv("RESULT_FIELD_USERNAME", resultFields.UserName); v != "" {
		resultFields.UserName = v
	}

	return summaryFieldConfig{
		Drama:  dramaFields,
		Result: resultFields,
	}
}

func pickFieldEnv(key, fallback string) string {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		return val
	}
	return fallback
}

// summaryFieldConfig centralizes the column/field names needed by both data sources.
type summaryFieldConfig struct {
	Drama  feishu.DramaFields
	Result feishu.ResultFields
}

// dramaInfo holds normalized metadata for a single drama row.
type dramaInfo struct {
	ID             string
	Name           string
	Priority       string
	RightsScenario string
	RawFields      map[string]any
}

// recordQuery represents the generic filters used across data sources.
type recordQuery struct {
	App         string
	Params      string
	UserID      string
	UserName    string
	Limit       int
	ExtraFilter string
}

// summaryDataSource fetches drama metadata and capture records from a backend.
type summaryDataSource interface {
	FetchDrama(ctx context.Context, params string) (*dramaInfo, error)
	FetchRecords(ctx context.Context, query recordQuery) ([]CaptureRecordPayload, error)
	Close() error
}

func newSummaryDataSource(source WebhookSource, fields summaryFieldConfig, opts WebhookOptions) (summaryDataSource, error) {
	switch source {
	case WebhookSourceSQLite:
		return newSQLiteSummarySource(fields, opts)
	case WebhookSourceFeishu:
		fallthrough
	default:
		return newFeishuSummarySource(fields, opts)
	}
}
