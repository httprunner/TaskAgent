package piracy

import "testing"

func TestExtractDramas(t *testing.T) {
	rows := []Row{
		{Fields: map[string]any{"Params": "DramaA", "TotalDuration": 60.0}},
		{Fields: map[string]any{"Params": "DramaB", "TotalDuration": 120.0}},
		{Fields: map[string]any{"Params": "DramaA", "TotalDuration": 80.0}},
		{Fields: map[string]any{"Params": " ", "TotalDuration": 10.0}},
		{Fields: map[string]any{"Params": "DramaC", "TotalDuration": "0"}},
	}

	cfg := Config{
		DramaParamsField:   "Params",
		DramaDurationField: "TotalDuration",
	}

	dramas := extractDramas(rows, cfg)
	if len(dramas) != 2 {
		t.Fatalf("expected 2 dramas, got %d", len(dramas))
	}

	if dramas[0].Name != "DramaA" || dramas[0].Duration != 80 {
		t.Fatalf("unexpected first drama %+v", dramas[0])
	}
	if dramas[1].Name != "DramaB" || dramas[1].Duration != 120 {
		t.Fatalf("unexpected second drama %+v", dramas[1])
	}
}
