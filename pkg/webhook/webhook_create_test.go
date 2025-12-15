package webhook

import (
	"encoding/json"
	"testing"

	"github.com/httprunner/TaskAgent/internal/feishusdk"
)

func TestNormalizeDramaInfoForStorage(t *testing.T) {
	t.Cleanup(feishusdk.RefreshFieldMappings)
	t.Setenv("DRAMA_FIELD_ID", "短剧 ID")
	t.Setenv("DRAMA_FIELD_NAME", "短剧名称")
	t.Setenv("DRAMA_FIELD_DURATION", "全剧时长（秒）")
	t.Setenv("DRAMA_FIELD_EPISODE_COUNT", "全剧集数")
	t.Setenv("DRAMA_FIELD_PRIORITY", "优先级")
	t.Setenv("DRAMA_FIELD_RIGHTS_SCENARIO", "维权场景")
	t.Setenv("DRAMA_FIELD_CAPTURE_DATE", "采集日期")
	feishusdk.RefreshFieldMappings()

	raw := map[string]any{
		"优先级":     []any{map[string]any{"text": "S-300部", "type": "text"}},
		"全剧时长（秒）": 9713.522,
		"全剧集数":    81,
		"短剧 ID":   []any{map[string]any{"text": "7536058886885739582", "type": "text"}},
		"短剧名称":    []any{map[string]any{"text": "掌生1：神算大小姐名满天下", "type": "text"}},
		"维权场景":    "付费&免费都登记",
		"采集日期":    1764259200000,
		"无关字段":    "should be ignored",
	}

	normalized := normalizeDramaInfoForStorage(raw)
	if normalized["Priority"] != "S-300部" {
		t.Fatalf("unexpected Priority: %#v", normalized["Priority"])
	}
	if normalized["TotalDuration"] != "9713.522" {
		t.Fatalf("unexpected TotalDuration: %#v", normalized["TotalDuration"])
	}
	if normalized["EpisodeCount"] != "81" {
		t.Fatalf("unexpected EpisodeCount: %#v", normalized["EpisodeCount"])
	}
	if normalized["DramaID"] != "7536058886885739582" {
		t.Fatalf("unexpected DramaID: %#v", normalized["DramaID"])
	}
	if normalized["DramaName"] != "掌生1：神算大小姐名满天下" {
		t.Fatalf("unexpected DramaName: %#v", normalized["DramaName"])
	}
	if normalized["RightsProtectionScenario"] != "付费&免费都登记" {
		t.Fatalf("unexpected RightsProtectionScenario: %#v", normalized["RightsProtectionScenario"])
	}
	if normalized["CaptureDate"] != "1764259200000" {
		t.Fatalf("unexpected CaptureDate: %#v", normalized["CaptureDate"])
	}
	if _, exists := normalized["无关字段"]; exists {
		t.Fatalf("unexpected raw field present: %#v", normalized)
	}
}

func TestDecodeDramaInfo_DoubleEncoded(t *testing.T) {
	raw := `"{\"DramaID\":\"123\",\"DramaName\":\"abc\"}"`
	decoded, err := decodeDramaInfo(raw)
	if err != nil {
		t.Fatalf("decodeDramaInfo failed: %v", err)
	}
	if decoded["DramaID"] != "123" {
		t.Fatalf("unexpected DramaID: %#v", decoded["DramaID"])
	}
	if decoded["DramaName"] != "abc" {
		t.Fatalf("unexpected DramaName: %#v", decoded["DramaName"])
	}
}

func TestDecodeDramaInfo_RichTextArray(t *testing.T) {
	rich := []any{
		map[string]any{
			"text": `{"DramaID":"123","DramaName":"abc"}`,
			"type": "text",
		},
	}
	b, err := json.Marshal(rich)
	if err != nil {
		t.Fatalf("marshal rich text failed: %v", err)
	}
	raw := string(b)
	decoded, err := decodeDramaInfo(raw)
	if err != nil {
		t.Fatalf("decodeDramaInfo failed: %v", err)
	}
	if decoded["DramaID"] != "123" {
		t.Fatalf("unexpected DramaID: %#v", decoded["DramaID"])
	}
	if decoded["DramaName"] != "abc" {
		t.Fatalf("unexpected DramaName: %#v", decoded["DramaName"])
	}
}
