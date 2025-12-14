package webhook

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/httprunner/TaskAgent/internal/feishusdk"
)

func TestSendSummaryWebhookSQLite(t *testing.T) {
	t.Cleanup(feishusdk.RefreshFieldMappings)
	t.Setenv("DRAMA_SQLITE_TABLE", "drama_catalog")
	t.Setenv("RESULT_SQLITE_TABLE", "capture_results")
	t.Setenv("DRAMA_FIELD_NAME", "Params")
	t.Setenv("DRAMA_FIELD_ID", "DramaID")
	t.Setenv("DRAMA_FIELD_PRIORITY", "Priority")
	t.Setenv("DRAMA_FIELD_RIGHTS_SCENARIO", "RightsProtectionScenario")
	t.Setenv("RESULT_FIELD_PARAMS", "Params")
	t.Setenv("RESULT_FIELD_APP", "App")
	t.Setenv("RESULT_FIELD_USERID", "UserID")
	t.Setenv("RESULT_FIELD_USERNAME", "UserName")
	t.Setenv("RESULT_FIELD_ITEMID", "ItemID")
	feishusdk.RefreshFieldMappings()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "records.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	createSQL := []string{
		`CREATE TABLE drama_catalog (
	            DramaID TEXT,
	            Params TEXT,
	            Priority TEXT,
	            RightsProtectionScenario TEXT
	        );`,
		`CREATE TABLE capture_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Params TEXT,
            DeviceSerial TEXT,
            App TEXT,
            ItemID TEXT,
            ItemCaption TEXT,
            ItemDuration REAL,
            UserName TEXT,
            UserID TEXT,
            Tags TEXT,
            TaskID INTEGER,
            Datetime INTEGER,
            Extra TEXT
        );`,
	}
	for _, stmt := range createSQL {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create table failed: %v", err)
		}
	}

	if _, err := db.Exec(`INSERT INTO drama_catalog (DramaID, Params, Priority, RightsProtectionScenario)
	        VALUES (?, ?, ?, ?)`,
		"7547613592992894014", "真千金她是学霸", "S300", "付费&免费都登记"); err != nil {
		t.Fatalf("insert drama row failed: %v", err)
	}

	extra := map[string]any{"quality": "hd"}
	extraJSON, _ := json.Marshal(extra)
	now := time.Date(2025, 11, 14, 12, 0, 0, 0, time.UTC)
	if _, err := db.Exec(`INSERT INTO capture_results
        (Params, DeviceSerial, App, ItemID, ItemCaption, ItemDuration, UserName, UserID, Tags, TaskID, Datetime, Extra)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"真千金她是学霸", "device-01", "kuaishou", "vid123", "示例标题", 123.0,
		"幽幽爱看", "3618381723", "rights", 42, now.UnixMilli(), string(extraJSON)); err != nil {
		t.Fatalf("insert capture row failed: %v", err)
	}

	payloadCh := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body failed: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("decode payload failed: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		payloadCh <- payload
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	opts := Options{
		Params:      "真千金她是学霸",
		App:         "kuaishou",
		UserID:      "3618381723",
		UserName:    "",
		Source:      SourceSQLite,
		SQLitePath:  dbPath,
		WebhookURL:  server.URL,
		RecordLimit: 5,
	}

	payload, err := SendSummaryWebhook(context.Background(), opts)
	if err != nil {
		t.Fatalf("SendSummaryWebhook failed: %v", err)
	}

	var posted map[string]any
	select {
	case posted = <-payloadCh:
		if len(posted) == 0 {
			t.Fatalf("webhook payload empty")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("webhook server did not receive payload")
	}

	if got := payload["DramaID"]; got != "7547613592992894014" {
		t.Fatalf("unexpected drama id %v", got)
	}
	if got := payload["Priority"]; got != "S300" {
		t.Fatalf("unexpected priority %v", got)
	}
	if got := payload["RightsProtectionScenario"]; got != "付费&免费都登记" {
		t.Fatalf("unexpected rights scenario %v", got)
	}
	records, ok := payload["records"].([]map[string]any)
	if !ok {
		t.Fatalf("records type mismatch: %#v", payload["records"])
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	record := records[0]
	if record["_record_id"].(string) == "" {
		t.Fatalf("missing record id")
	}
	if record["ItemID"] != "vid123" {
		t.Fatalf("unexpected ItemID %#v", record["ItemID"])
	}
	switch extraVal := record["Extra"].(type) {
	case map[string]any:
		if extraVal["quality"] != "hd" {
			t.Fatalf("extra field mismatch: %#v", record["Extra"])
		}
	case string:
		if extraVal != `{"quality":"hd"}` {
			t.Fatalf("extra field mismatch: %#v", record["Extra"])
		}
	default:
		t.Fatalf("extra field mismatch: %#v", record["Extra"])
	}
	switch ts := record["Datetime"].(type) {
	case int64:
		if ts != now.UnixMilli() {
			t.Fatalf("datetime field mismatch: %#v", record["Datetime"])
		}
	case string:
		if ts != strconv.FormatInt(now.UnixMilli(), 10) {
			t.Fatalf("datetime field mismatch: %#v", record["Datetime"])
		}
	default:
		t.Fatalf("datetime field type mismatch: %T %#v", record["Datetime"], record["Datetime"])
	}
	if posted["RightsProtectionScenario"] != "付费&免费都登记" {
		t.Fatalf("flattened payload missing drama field: %#v", posted)
	}
	if _, exists := posted["drama_extra"]; exists {
		t.Fatalf("unexpected drama_extra field present")
	}
}

func TestSendSummaryWebhookSkipDramaLookup(t *testing.T) {
	t.Cleanup(feishusdk.RefreshFieldMappings)
	t.Setenv("DRAMA_SQLITE_TABLE", "drama_catalog")
	t.Setenv("RESULT_SQLITE_TABLE", "capture_results")
	t.Setenv("DRAMA_FIELD_NAME", "Params")
	t.Setenv("DRAMA_FIELD_ID", "DramaID")
	t.Setenv("RESULT_FIELD_PARAMS", "Params")
	t.Setenv("RESULT_FIELD_APP", "App")
	t.Setenv("RESULT_FIELD_USERID", "UserID")
	t.Setenv("RESULT_FIELD_USERNAME", "UserName")
	t.Setenv("RESULT_FIELD_ITEMID", "ItemID")
	feishusdk.RefreshFieldMappings()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "records.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	createSQL := []string{
		`CREATE TABLE drama_catalog (
	            DramaID TEXT,
	            Params TEXT
	        );`,
		`CREATE TABLE capture_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Params TEXT,
            DeviceSerial TEXT,
            App TEXT,
            ItemID TEXT,
            ItemCaption TEXT,
            ItemDuration REAL,
            UserName TEXT,
            UserID TEXT,
            Tags TEXT,
            TaskID INTEGER,
            Datetime INTEGER,
            Extra TEXT
        );`,
	}
	for _, stmt := range createSQL {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create table failed: %v", err)
		}
	}

	params := `{"aid":"111234","eid":"3x59ziqfkfu3rya","type":"auto_additional_crawl"}`
	if _, err := db.Exec(`INSERT INTO capture_results
        (Params, DeviceSerial, App, ItemID, ItemCaption, ItemDuration, UserName, UserID, Tags, TaskID, Datetime, Extra)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"", "device-02", "com.smile.gifmaker", "3x59ziqfkfu3rya", "短剧截屏", 60.0,
		"捕捉者", "user-002", "", 99, time.Now().UnixMilli(), "{}"); err != nil {
		t.Fatalf("insert capture row failed: %v", err)
	}

	payloadCh := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		payloadCh <- payload
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Ensure the call would fail without SkipDramaLookup for missing drama rows.
	noSkipOpts := Options{
		Params:     params,
		App:        "com.smile.gifmaker",
		UserID:     "user-002",
		UserName:   "捕捉者",
		Source:     SourceSQLite,
		SQLitePath: dbPath,
		WebhookURL: server.URL,
	}
	if _, err := SendSummaryWebhook(context.Background(), noSkipOpts); err == nil {
		t.Fatalf("expected error without SkipDramaLookup")
	}

	opts := Options{
		Params:          params,
		App:             "com.smile.gifmaker",
		UserID:          "user-002",
		UserName:        "捕捉者",
		Source:          SourceSQLite,
		SQLitePath:      dbPath,
		WebhookURL:      server.URL,
		SkipDramaLookup: true,
		ItemID:          "3x59ziqfkfu3rya",
	}

	payload, err := SendSummaryWebhook(context.Background(), opts)
	if err != nil {
		t.Fatalf("SendSummaryWebhook with SkipDramaLookup failed: %v", err)
	}

	select {
	case <-payloadCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("webhook server did not receive payload")
	}

	if payload["DramaName"] != params {
		t.Fatalf("expected DramaName to fallback to params, got %#v", payload["DramaName"])
	}
	records, ok := payload["records"].([]map[string]any)
	if !ok || len(records) != 1 {
		t.Fatalf("unexpected records payload: %#v", payload["records"])
	}
	if records[0]["ItemID"] != "3x59ziqfkfu3rya" {
		t.Fatalf("unexpected ItemID %#v", records[0]["ItemID"])
	}

	opts.ItemID = ""
	if _, err := SendSummaryWebhook(context.Background(), opts); err == nil || !strings.Contains(err.Error(), "item id") {
		t.Fatalf("expected error when SkipDramaLookup without ItemID, got %v", err)
	}
}

func TestSendSummaryWebhookNoRecords(t *testing.T) {
	t.Cleanup(feishusdk.RefreshFieldMappings)
	t.Setenv("DRAMA_SQLITE_TABLE", "drama_catalog")
	t.Setenv("RESULT_SQLITE_TABLE", "capture_results")
	t.Setenv("DRAMA_FIELD_NAME", "Params")
	t.Setenv("DRAMA_FIELD_ID", "DramaID")
	t.Setenv("RESULT_FIELD_ITEMID", "ItemID")
	feishusdk.RefreshFieldMappings()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "records.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	createSQL := []string{
		`CREATE TABLE drama_catalog (
	            DramaID TEXT,
	            Params TEXT,
	            Priority TEXT,
	            RightsProtectionScenario TEXT
	        );`,
		`CREATE TABLE capture_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Params TEXT,
            DeviceSerial TEXT,
            App TEXT,
            ItemID TEXT,
            ItemCaption TEXT,
            ItemDuration REAL,
            UserName TEXT,
            UserID TEXT,
            Tags TEXT,
            TaskID INTEGER,
            Datetime INTEGER,
            Extra TEXT
        );`,
	}
	for _, stmt := range createSQL {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create table failed: %v", err)
		}
	}

	if _, err := db.Exec(`INSERT INTO drama_catalog (DramaID, Params) VALUES (?, ?)`,
		"7547613592992894014", "真千金她是学霸"); err != nil {
		t.Fatalf("insert drama row failed: %v", err)
	}

	opts := Options{
		Params:     "真千金她是学霸",
		App:        "kuaishou",
		UserID:     "3618381723",
		UserName:   "幽幽爱看",
		Source:     SourceSQLite,
		SQLitePath: dbPath,
		WebhookURL: "http://127.0.0.1",
	}

	if _, err := SendSummaryWebhook(context.Background(), opts); !errors.Is(err, ErrNoCaptureRecords) {
		t.Fatalf("expected ErrNoCaptureRecords, got %v", err)
	}
}

func TestSendSummaryWebhookFeishuLive(t *testing.T) {
	if os.Getenv("FEISHU_LIVE_TEST") != "1" {
		t.Skip("set FEISHU_LIVE_TEST=1 to run live Feishu webhook test")
	}
	required := []string{"FEISHU_APP_ID", "FEISHU_APP_SECRET", "DRAMA_BITABLE_URL", "RESULT_BITABLE_URL"}
	for _, key := range required {
		if strings.TrimSpace(os.Getenv(key)) == "" {
			t.Skipf("%s not configured", key)
		}
	}

	payloadCh := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read webhook body failed: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("decode webhook body failed: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		payloadCh <- payload
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts := Options{
		App:         "com.smile.gifmaker",
		Params:      "风风火火的她",
		UserID:      "859398427",
		WebhookURL:  server.URL,
		Source:      SourceFeishu,
		RecordLimit: 20,
	}

	payload, err := SendSummaryWebhook(ctx, opts)
	if err != nil {
		t.Fatalf("SendSummaryWebhook failed: %v", err)
	}

	select {
	case <-payloadCh:
		// ok
	case <-time.After(5 * time.Second):
		t.Fatalf("webhook server did not receive payload")
	}

	recordsAny, ok := payload["records"]
	if !ok {
		t.Fatalf("payload missing records field: %#v", payload)
	}
	records, ok := recordsAny.([]map[string]any)
	if !ok {
		t.Fatalf("records field type mismatch: %T", recordsAny)
	}
	if len(records) == 0 {
		t.Fatalf("expected at least one capture record for drama, got 0")
	}
}

func TestPostWebhookLogidExtraction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set X-Tt-Logid header in response
		w.Header().Set("X-Tt-Logid", "202511272111311B9477B53752848BC2AD")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"code":0,"data":0,"msg":"success"}`))
	}))
	defer server.Close()

	payload := map[string]any{
		"test": "data",
	}

	err := PostWebhook(context.Background(), server.URL, payload, nil)
	if err != nil {
		t.Fatalf("PostWebhook failed: %v", err)
	}
}

func TestPostWebhookLogidExtractionError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set X-Tt-Logid header in response even for error cases
		w.Header().Set("X-Tt-Logid", "202511272111311B9477B53752848BC2AD")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer server.Close()

	payload := map[string]any{
		"test": "data",
	}

	err := PostWebhook(context.Background(), server.URL, payload, nil)
	if err == nil {
		t.Fatal("expected PostWebhook to fail")
	}
}

func TestPickLatestRecords(t *testing.T) {
	records := []CaptureRecordPayload{
		{RecordID: "1", Fields: map[string]any{"Datetime": "1700000000000"}},
		{RecordID: "2", Fields: map[string]any{"Datetime": "1700000005000"}},
		{RecordID: "3", Fields: map[string]any{"Datetime": "1699999999000"}},
	}
	latest := PickLatestRecords(records)
	if len(latest) != 1 {
		to := len(latest)
		t.Fatalf("expected 1 record, got %d", to)
	}
	if latest[0].RecordID != "3" {
		t.Fatalf("expected last record, got %s", latest[0].RecordID)
	}
}

func TestPickLatestRecordsFallback(t *testing.T) {
	records := []CaptureRecordPayload{
		{RecordID: "a", Fields: map[string]any{"Datetime": ""}},
		{RecordID: "b", Fields: map[string]any{"Datetime": ""}},
	}
	latest := PickLatestRecords(records)
	if len(latest) != 1 {
		t.Fatalf("expected 1 record, got %d", len(latest))
	}
	if latest[0].RecordID != "b" {
		t.Fatalf("expected last record fallback, got %s", latest[0].RecordID)
	}
}
