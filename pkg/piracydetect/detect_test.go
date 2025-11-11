package piracydetect

import (
	"context"
	"os"
	"testing"
)

// mockFetcher implements a simple row fetcher for testing
type mockFetcher struct {
	rows []Row
}

func (m *mockFetcher) Fetch(ctx context.Context, url string, opts interface{}) ([]Row, error) {
	return m.rows, nil
}

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
		DramaParamsField:   "Params",
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
	if report.TargetRows != 1 {
		t.Errorf("expected target rows 1, got %d", report.TargetRows)
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
		DramaParamsField:   "Params",
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
		DramaParamsField:   "Params",
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
		DramaParamsField:   "Params",
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
	origParamsField := os.Getenv("PARAMS_FIELD")
	origUserIDField := os.Getenv("USERID_FIELD")
	origThreshold := os.Getenv("THRESHOLD")
	defer func() {
		os.Setenv("PARAMS_FIELD", origParamsField)
		os.Setenv("USERID_FIELD", origUserIDField)
		os.Setenv("THRESHOLD", origThreshold)
	}()

	// Test empty config with env vars set
	os.Setenv("PARAMS_FIELD", "MyParams")
	os.Setenv("USERID_FIELD", "MyUserID")
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
				// Check DramaDurationField - may be overridden by environment variable
				expectedDramaDurationField := "TotalDuration"
				if os.Getenv("DRAMA_DURATION_FIELD") != "" {
					expectedDramaDurationField = os.Getenv("DRAMA_DURATION_FIELD")
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
