package webhook

import (
	"encoding/json"
	"testing"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/internal/feishusdk"
)

func TestNormalizeDramaInfoForStorage(t *testing.T) {
	t.Cleanup(feishusdk.RefreshFieldMappings)
	t.Setenv("SOURCE_FIELD_DRAMA_ID", "短剧 ID")
	t.Setenv("SOURCE_FIELD_DRAMA_NAME", "短剧名称")
	t.Setenv("SOURCE_FIELD_TOTAL_DURATION", "全剧时长（秒）")
	t.Setenv("SOURCE_FIELD_EPISODE_COUNT", "全剧集数")
	t.Setenv("SOURCE_FIELD_PRIORITY", "优先级")
	t.Setenv("SOURCE_FIELD_RIGHTS_SCENARIO", "维权场景")
	t.Setenv("SOURCE_FIELD_CAPTURE_DATE", "采集日期")
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

func TestBuildWebhookPlanCandidates_GroupsByBizGroupDay(t *testing.T) {
	day := time.Date(2026, 1, 7, 10, 0, 0, 0, time.Local)
	tasks := []*taskagent.Task{
		{Payload: &taskagent.FeishuTask{
			TaskID:   1,
			Scene:    taskagent.SceneSingleURLCapture,
			GroupID:  "g1",
			BookID:   "b1",
			Datetime: &day,
		}},
		{Payload: &taskagent.FeishuTask{
			TaskID:   2,
			Scene:    taskagent.SceneSingleURLCapture,
			GroupID:  "g1",
			Datetime: &day,
		}},
		{Payload: &taskagent.FeishuTask{
			TaskID:   3,
			Scene:    taskagent.SceneVideoScreenCapture,
			GroupID:  "g2",
			Datetime: &day,
		}},
		{Payload: &taskagent.FeishuTask{
			TaskID:   4,
			Scene:    "unknown_scene",
			GroupID:  "g3",
			Datetime: &day,
		}},
	}

	cands := buildWebhookPlanCandidates(tasks)
	if len(cands) != 2 {
		t.Fatalf("unexpected candidate size: %d", len(cands))
	}

	su := cands["single_url_capture|g1|2026-01-07"]
	if su.BizType != WebhookBizTypeSingleURLCapture {
		t.Fatalf("unexpected su biz: %s", su.BizType)
	}
	if su.BookID != "b1" {
		t.Fatalf("unexpected su book: %s", su.BookID)
	}
	if len(uniqueInt64(su.TaskIDs)) != 2 {
		t.Fatalf("unexpected su task ids: %#v", su.TaskIDs)
	}

	vs := cands["video_screen_capture|g2|2026-01-07"]
	if vs.BizType != WebhookBizTypeVideoScreenCapture {
		t.Fatalf("unexpected vs biz: %s", vs.BizType)
	}
	if len(uniqueInt64(vs.TaskIDs)) != 1 || uniqueInt64(vs.TaskIDs)[0] != 3 {
		t.Fatalf("unexpected vs task ids: %#v", vs.TaskIDs)
	}
}

func TestWebhookPlanCandidate_ToCreateInputComputesDateMs(t *testing.T) {
	cand := webhookPlanCandidate{
		BizType: WebhookBizTypeSingleURLCapture,
		GroupID: "g1",
		Day:     "2026-01-07",
		TaskIDs: []int64{2, 1, 1},
	}
	input := cand.toCreateInput(nil, nil, "")
	if input.BizType != WebhookBizTypeSingleURLCapture {
		t.Fatalf("unexpected BizType: %s", input.BizType)
	}
	if input.GroupID != "g1" {
		t.Fatalf("unexpected GroupID: %s", input.GroupID)
	}
	if len(input.TaskIDs) != 2 {
		t.Fatalf("unexpected TaskIDs: %#v", input.TaskIDs)
	}
	dayTime, err := time.ParseInLocation("2006-01-02", "2026-01-07", time.Local)
	if err != nil {
		t.Fatalf("parse day failed: %v", err)
	}
	want := dayTime.UTC().UnixMilli()
	if input.DateMs != want {
		t.Fatalf("unexpected DateMs: got %d want %d", input.DateMs, want)
	}
	if input.DramaInfo != "{}" {
		t.Fatalf("unexpected DramaInfo: %s", input.DramaInfo)
	}
}
