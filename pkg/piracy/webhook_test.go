package piracy

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSendSummaryWebhookSQLite(t *testing.T) {
	t.Setenv("DRAMA_SQLITE_TABLE", "drama_catalog")
	t.Setenv("RESULT_SQLITE_TABLE", "capture_results")
	t.Setenv("DRAMA_NAME_FIELD", "Params")
	t.Setenv("DRAMA_ID_FIELD", "DramaID")
	t.Setenv("DRAMA_PRIORITY_FIELD", "Priority")
	t.Setenv("DRAMA_RIGHTS_SCENARIO_FIELD", "RightsProtectionScenario")
	t.Setenv("RESULT_PARAMS_FIELD", "Params")
	t.Setenv("RESULT_APP_FIELD", "App")
	t.Setenv("RESULT_USERID_FIELD", "UserID")
	t.Setenv("RESULT_USERNAME_FIELD", "UserName")

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

	opts := WebhookOptions{
		Params:      "真千金她是学霸",
		App:         "kuaishou",
		UserID:      "3618381723",
		UserName:    "",
		Source:      WebhookSourceSQLite,
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
	if extraMap, ok := record["Extra"].(map[string]any); !ok || extraMap["quality"] != "hd" {
		t.Fatalf("extra field mismatch: %#v", record["Extra"])
	}
	if ts, ok := record["Datetime"].(int64); !ok || ts != now.UnixMilli() {
		t.Fatalf("datetime field mismatch: %#v", record["Datetime"])
	}
	if posted["RightsProtectionScenario"] != "付费&免费都登记" {
		t.Fatalf("flattened payload missing drama field: %#v", posted)
	}
	if _, exists := posted["drama_extra"]; exists {
		t.Fatalf("unexpected drama_extra field present")
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

	opts := WebhookOptions{
		App:         "com.smile.gifmaker",
		Params:      "重生八三：从受气包到人生赢家",
		UserID:      "3807656067",
		WebhookURL:  server.URL,
		Source:      WebhookSourceFeishu,
		RecordLimit: 20,
	}

	payload, err := SendSummaryWebhook(ctx, opts)
	if err != nil {
		t.Fatalf("SendSummaryWebhook failed: %v", err)
	}
	// bytes, _ := json.MarshalIndent(payload, "", "  ")
	// fmt.Println(string(bytes))

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
