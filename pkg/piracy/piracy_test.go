package piracy

import (
	"os"
	"testing"

	"github.com/httprunner/TaskAgent/pkg/feishu"
)

func TestAnalyzeRows(t *testing.T) {
	// Setup test data
	params := "drama1"
	user := "user1"

	// Create result rows with item durations summing to 35 seconds
	resultRows := []Row{
		{Fields: map[string]any{"Params": params, "UserID": user, "ItemDuration": 10.0}},
		{Fields: map[string]any{"Params": params, "UserID": user, "ItemDuration": 20.0}},
		{Fields: map[string]any{"Params": params, "UserID": user, "ItemDuration": 5.0}},
	}

	// Create drama row with total duration of 60 seconds
	dramaRows := []Row{
		{Fields: map[string]any{"Params": params, "TotalDuration": 60.0}},
	}

	// Create config with threshold of 0.5 (50%)
	cfg := Config{
		ParamsField:        "Params",
		UserIDField:        "UserID",
		DurationField:      "ItemDuration",
		DramaNameField:     "Params",
		DramaDurationField: "TotalDuration",
		Threshold:          0.5,
	}

	// Run analysis
	report := analyzeRows(resultRows, dramaRows, cfg)

	// Verify results
	if len(report.Matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(report.Matches))
	}

	match := report.Matches[0]
	if match.Params != params {
		t.Errorf("expected params %s, got %s", params, match.Params)
	}
	if match.UserID != user {
		t.Errorf("expected user %s, got %s", user, match.UserID)
	}
	if match.SumDuration != 35 {
		t.Errorf("expected collected duration 35, got %v", match.SumDuration)
	}
	if match.TotalDuration != 60 {
		t.Errorf("expected total duration 60, got %v", match.TotalDuration)
	}
	if match.Ratio <= 0.5 || match.Ratio > 0.6 {
		t.Errorf("expected ratio around 0.583, got %v", match.Ratio)
	}
	if match.RecordCount != 3 {
		t.Errorf("expected record count 3, got %d", match.RecordCount)
	}

	// Verify row counts
	if report.ResultRows != 3 {
		t.Errorf("expected result rows 3, got %d", report.ResultRows)
	}
	if report.TaskRows != 1 {
		t.Errorf("expected target rows 1, got %d", report.TaskRows)
	}
}

func TestAnalyzeRowsBelowThreshold(t *testing.T) {
	// Setup test data
	params := "drama1"
	user := "user1"

	// Create result rows with item durations summing to 20 seconds (below 50% threshold)
	resultRows := []Row{
		{Fields: map[string]any{"Params": params, "UserID": user, "ItemDuration": 10.0}},
		{Fields: map[string]any{"Params": params, "UserID": user, "ItemDuration": 10.0}},
	}

	// Create drama row with total duration of 60 seconds
	dramaRows := []Row{
		{Fields: map[string]any{"Params": params, "TotalDuration": 60.0}},
	}

	cfg := Config{
		ParamsField:        "Params",
		UserIDField:        "UserID",
		DurationField:      "ItemDuration",
		DramaNameField:     "Params",
		DramaDurationField: "TotalDuration",
		Threshold:          0.5,
	}

	report := analyzeRows(resultRows, dramaRows, cfg)

	// Should have no matches since ratio (0.333) is below threshold (0.5)
	if len(report.Matches) != 0 {
		t.Fatalf("expected 0 matches, got %d", len(report.Matches))
	}
}

func TestAnalyzeRowsMissingTarget(t *testing.T) {
	// Setup test data
	params := "drama1"
	user := "user1"

	// Create result rows
	resultRows := []Row{
		{Fields: map[string]any{"Params": params, "UserID": user, "ItemDuration": 10.0}},
	}

	// Create drama rows without the expected params
	dramaRows := []Row{
		{Fields: map[string]any{"Params": "drama2", "TotalDuration": 60.0}},
	}

	cfg := Config{
		ParamsField:        "Params",
		UserIDField:        "UserID",
		DurationField:      "ItemDuration",
		DramaNameField:     "Params",
		DramaDurationField: "TotalDuration",
		Threshold:          0.5,
	}

	report := analyzeRows(resultRows, dramaRows, cfg)

	// Should have no matches since target is missing
	if len(report.Matches) != 0 {
		t.Fatalf("expected 0 matches, got %d", len(report.Matches))
	}

	// Should have missing params
	found := false
	for _, p := range report.MissingParams {
		if p == params {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected missing params to contain %s", params)
	}
}

func TestAnalyzeRowsMultipleMatches(t *testing.T) {
	// Setup test data for multiple users
	resultRows := []Row{
		// User 1 - 35 seconds for drama1
		{Fields: map[string]any{"Params": "drama1", "UserID": "user1", "ItemDuration": 10.0}},
		{Fields: map[string]any{"Params": "drama1", "UserID": "user1", "ItemDuration": 25.0}},

		// User 2 - 40 seconds for drama1
		{Fields: map[string]any{"Params": "drama1", "UserID": "user2", "ItemDuration": 20.0}},
		{Fields: map[string]any{"Params": "drama1", "UserID": "user2", "ItemDuration": 20.0}},
	}

	dramaRows := []Row{
		{Fields: map[string]any{"Params": "drama1", "TotalDuration": 60.0}},
	}

	cfg := Config{
		ParamsField:        "Params",
		UserIDField:        "UserID",
		DurationField:      "ItemDuration",
		DramaNameField:     "Params",
		DramaDurationField: "TotalDuration",
		Threshold:          0.5,
	}

	report := analyzeRows(resultRows, dramaRows, cfg)

	// Should have 2 matches (one for each user)
	if len(report.Matches) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(report.Matches))
	}

	// Verify both matches have params=drama1
	for _, match := range report.Matches {
		if match.Params != "drama1" {
			t.Errorf("expected params drama1, got %s", match.Params)
		}
	}
}

func TestAnalyzeRowsDedupByItemID(t *testing.T) {
	resultRows := []Row{
		{Fields: map[string]any{"Params": "drama1", "UserID": "user1", "ItemDuration": 30.0, "ItemID": "dup"}},
		{Fields: map[string]any{"Params": "drama1", "UserID": "user1", "ItemDuration": 30.0, "ItemID": "dup"}},
		{Fields: map[string]any{"Params": "drama1", "UserID": "user1", "ItemDuration": 40.0, "ItemID": "unique"}},
	}

	dramaRows := []Row{
		{Fields: map[string]any{"Params": "drama1", "TotalDuration": 60.0}},
	}

	cfg := Config{
		ParamsField:        "Params",
		UserIDField:        "UserID",
		DurationField:      "ItemDuration",
		ItemIDField:        "ItemID",
		DramaNameField:     "Params",
		DramaDurationField: "TotalDuration",
		Threshold:          0.5,
	}

	report := analyzeRows(resultRows, dramaRows, cfg)
	if len(report.Matches) != 1 {
		t.Fatalf("expected 1 match, got %d", len(report.Matches))
	}
	match := report.Matches[0]
	if match.SumDuration != 70 {
		t.Fatalf("expected deduped duration 70, got %v", match.SumDuration)
	}
	if match.RecordCount != 2 {
		t.Fatalf("expected record count 2 (unique ItemIDs), got %d", match.RecordCount)
	}
}

func TestGetString(t *testing.T) {
	tests := []struct {
		name   string
		fields map[string]any
		key    string
		want   string
	}{
		{"string value", map[string]any{"key": "value"}, "key", "value"},
		{"missing key", map[string]any{}, "key", ""},
		{"nil fields", nil, "key", ""},
		{"trim spaces", map[string]any{"key": "  value  "}, "key", "value"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getString(tt.fields, tt.key)
			if got != tt.want {
				t.Errorf("getString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetFloat(t *testing.T) {
	tests := []struct {
		name   string
		fields map[string]any
		key    string
		want   float64
		ok     bool
	}{
		{"float value", map[string]any{"key": 10.5}, "key", 10.5, true},
		{"int value", map[string]any{"key": 10}, "key", 10.0, true},
		{"string number", map[string]any{"key": "10.5"}, "key", 10.5, true},
		{"invalid string", map[string]any{"key": "invalid"}, "key", 0, false},
		{"missing key", map[string]any{}, "key", 0, false},
		{"nil fields", nil, "key", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := getFloat(tt.fields, tt.key)
			if got != tt.want || ok != tt.ok {
				t.Errorf("getFloat() = %v, %v, want %v, %v", got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestConfigApplyDefaultsWithEnv(t *testing.T) {
	// Save original env vars to restore later
	origParamsField := os.Getenv("RESULT_FIELD_PARAMS")
	origUserIDField := os.Getenv("RESULT_FIELD_USERID")
	origItemIDField := os.Getenv("RESULT_FIELD_ITEMID")
	origThreshold := os.Getenv("THRESHOLD")
	defer func() {
		os.Setenv("RESULT_FIELD_PARAMS", origParamsField)
		os.Setenv("RESULT_FIELD_USERID", origUserIDField)
		os.Setenv("RESULT_FIELD_ITEMID", origItemIDField)
		os.Setenv("THRESHOLD", origThreshold)
	}()

	// Test empty config with env vars set
	os.Setenv("RESULT_FIELD_PARAMS", "MyParams")
	os.Setenv("RESULT_FIELD_USERID", "MyUserID")
	os.Setenv("RESULT_FIELD_ITEMID", "VideoID")
	os.Setenv("THRESHOLD", "0.75")

	c := Config{}
	c.ApplyDefaults()

	if c.ParamsField != "MyParams" {
		t.Errorf("expected ParamsField to be 'MyParams', got %s", c.ParamsField)
	}
	if c.UserIDField != "MyUserID" {
		t.Errorf("expected UserIDField to be 'MyUserID', got %s", c.UserIDField)
	}
	if c.DurationField != "ItemDuration" { // should use default since not set
		t.Errorf("expected DurationField to be 'ItemDuration', got %s", c.DurationField)
	}
	if c.ItemIDField != "VideoID" {
		t.Errorf("expected ItemIDField to be 'VideoID', got %s", c.ItemIDField)
	}
	if c.Threshold != 0.75 {
		t.Errorf("expected Threshold to be 0.75, got %v", c.Threshold)
	}
}

func TestConfigApplyDefaults(t *testing.T) {
	tests := []struct {
		name   string
		config Config
		check  func(*Config)
	}{
		{
			"empty config",
			Config{},
			func(c *Config) {
				if c.ParamsField != "Params" {
					t.Errorf("expected ParamsField to be 'Params', got %s", c.ParamsField)
				}
				if c.UserIDField != "UserID" {
					t.Errorf("expected UserIDField to be 'UserID', got %s", c.UserIDField)
				}
				if c.DurationField != "ItemDuration" {
					t.Errorf("expected DurationField to be 'ItemDuration', got %s", c.DurationField)
				}
				if c.ItemIDField != "ItemID" {
					t.Errorf("expected ItemIDField to be 'ItemID', got %s", c.ItemIDField)
				}
				// Check DramaDurationField - may be overridden by environment variable
				expectedDramaDurationField := "TotalDuration"
				if os.Getenv("DRAMA_FIELD_DURATION") != "" {
					expectedDramaDurationField = os.Getenv("DRAMA_FIELD_DURATION")
				}
				if c.DramaDurationField != expectedDramaDurationField {
					t.Errorf("expected DramaDurationField to be '%s', got %s", expectedDramaDurationField, c.DramaDurationField)
				}
				if c.Threshold != 0.5 {
					t.Errorf("expected Threshold to be 0.5, got %v", c.Threshold)
				}
			},
		},
		{
			"partial config",
			Config{
				ParamsField: "CustomParams",
				Threshold:   0.75,
			},
			func(c *Config) {
				if c.ParamsField != "CustomParams" {
					t.Errorf("expected ParamsField to be 'CustomParams', got %s", c.ParamsField)
				}
				if c.UserIDField != "UserID" {
					t.Errorf("expected UserIDField to be 'UserID', got %s", c.UserIDField)
				}
				if c.Threshold != 0.75 {
					t.Errorf("expected Threshold to be 0.75, got %v", c.Threshold)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.config
			c.ApplyDefaults()
			tt.check(&c)
		})
	}
}

func TestMapAppFieldValue(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Kuaishou Android",
			input:    "com.smile.gifmaker",
			expected: "快手-Android",
		},
		{
			name:     "Kuaishou iOS",
			input:    "com.jiangjia.gif",
			expected: "快手-iOS",
		},
		{
			name:     "Unknown app - no mapping",
			input:    "com.example.app",
			expected: "com.example.app",
		},
		{
			name:     "Empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapAppFieldValue(tt.input)
			if result != tt.expected {
				t.Errorf("mapAppFieldValue(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestFlattenRecordsWithAppMapping(t *testing.T) {
	schema := feishu.ResultFields{
		App:      "App",
		ItemID:   "ItemID",
		UserID:   "UserID",
		UserName: "UserName",
		Params:   "Params",
	}

	records := []CaptureRecordPayload{
		{
			RecordID: "rec1",
			Fields: map[string]any{
				"App":      "com.smile.gifmaker",
				"ItemID":   "item123",
				"UserID":   "user1",
				"UserName": "测试用户",
				"Params":   "测试剧集",
			},
		},
		{
			RecordID: "rec2",
			Fields: map[string]any{
				"App":      "com.jiangjia.gif",
				"ItemID":   "item456",
				"UserID":   "user2",
				"UserName": "测试用户2",
				"Params":   "测试剧集2",
			},
		},
		{
			RecordID: "rec3",
			Fields: map[string]any{
				"App":      "com.other.app",
				"ItemID":   "item789",
				"UserID":   "user3",
				"UserName": "测试用户3",
				"Params":   "测试剧集3",
			},
		},
	}

	flattened, itemIDs := flattenRecordsAndCollectItemIDs(records, schema)

	if len(flattened) != 3 {
		t.Fatalf("expected 3 flattened records, got %d", len(flattened))
	}

	if len(itemIDs) != 3 {
		t.Fatalf("expected 3 item IDs, got %d", len(itemIDs))
	}

	// Check first record - should map to 快手-Android
	if flattened[0]["App"] != "快手-Android" {
		t.Errorf("expected App to be '快手-Android', got %q", flattened[0]["App"])
	}

	// Check second record - should map to 快手-iOS
	if flattened[1]["App"] != "快手-iOS" {
		t.Errorf("expected App to be '快手-iOS', got %q", flattened[1]["App"])
	}

	// Check third record - should remain unchanged
	if flattened[2]["App"] != "com.other.app" {
		t.Errorf("expected App to be 'com.other.app', got %q", flattened[2]["App"])
	}
}
