package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/rs/zerolog/log"
)

// CaptureRecordPayload wraps a record ID and its field map so callers can access raw capture data.
type CaptureRecordPayload struct {
	RecordID string
	Fields   map[string]any
}

// ErrNoCaptureRecords indicates that the capture result query returned zero rows.
var ErrNoCaptureRecords = errors.New("no capture records found")

// PostWebhook sends the summary payload to the given URL and logs response metadata.
func PostWebhook(ctx context.Context, url string, payload map[string]any, client *http.Client) error {
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
		httpClient = &http.Client{Timeout: 60 * time.Second}
	}

	// Use signed payload authentication when VEDEM_DRAMA_AK/SK are present.
	if token := buildVedemAgwTokenSigned(body); token != "" {
		req.Header.Set("Agw-Auth", token)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("post webhook failed: %w", err)
	}
	bodyBytes, _ := io.ReadAll(resp.Body)
	defer resp.Body.Close()

	// Extract X-Tt-Logid from response headers for logging
	logid := resp.Header.Get("X-Tt-Logid")

	if resp.StatusCode >= 300 {
		log.Error().
			Str("webhook_url", url).
			Int("status_code", resp.StatusCode).
			Str("logid", logid).
			RawJSON("response_body", bodyBytes).
			Msg("webhook request failed")
		return fmt.Errorf("webhook responded with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	if len(bodyBytes) > 0 {
		log.Info().
			Str("webhook_url", url).
			Str("logid", logid).
			RawJSON("response_body", bodyBytes).
			Msg("webhook response received")
	} else {
		log.Info().
			Str("webhook_url", url).
			Str("logid", logid).
			Msg("webhook response received (empty body)")
	}
	return nil
}


func flattenDramaFields(raw map[string]any, schema taskagent.FeishuDramaFields) map[string]any {
	fieldMap := taskagent.StructFieldMap(schema)
	payload := make(map[string]any, len(fieldMap))
	for engName, rawKey := range fieldMap {
		// Feishu bitable fields often arrive as rich-text arrays or numeric
		// values. Downstream webhooks expect plain strings, so normalize the
		// value to a string to avoid validation errors.
		if raw != nil {
			// Support both raw Feishu field names (e.g. "短剧名称") and already-normalized
			// english keys (e.g. "DramaName") so stored DramaInfo can be schema-friendly.
			if _, ok := raw[engName]; ok {
				payload[engName] = getString(raw, engName)
			} else {
				payload[engName] = getString(raw, rawKey)
			}
			continue
		}
		payload[engName] = ""
	}
	return payload
}

// FlattenRecordsAndCollectItemIDs flattens record fields and collects unique ItemIDs.
func FlattenRecordsAndCollectItemIDs(records []CaptureRecordPayload, schema taskagent.FeishuResultFields) ([]map[string]any, []string) {
	fieldMap := taskagent.StructFieldMap(schema)
	result := make([]map[string]any, 0, len(records))
	rawItemKey := fieldMap["ItemID"]
	rawAppKey := fieldMap["App"]
	seenItem := make(map[string]struct{}, len(records))
	itemIDs := make([]string, 0, len(records))
	for _, rec := range records {
		if rawItemKey != "" {
			itemID := strings.TrimSpace(getString(rec.Fields, rawItemKey))
			if itemID != "" {
				if _, exists := seenItem[itemID]; exists {
					continue
				}
				seenItem[itemID] = struct{}{}
				itemIDs = append(itemIDs, itemID)
			}
		}
		entry := make(map[string]any, len(fieldMap)+1)
		entry["_record_id"] = rec.RecordID
		for engName, rawKey := range fieldMap {
			if _, ok := rec.Fields[rawKey]; !ok {
				entry[engName] = nil
				continue
			}
			// Downstream webhook schema expects strings for all capture fields, including Extra.
			fieldValue := getString(rec.Fields, rawKey)

			// Apply App field mapping if this is the App field
			if rawKey == rawAppKey && engName == "App" {
				fieldValue = taskagent.MapAppValue(fieldValue)
			}

			entry[engName] = fieldValue
		}
		result = append(result, entry)
	}
	return result, itemIDs
}


const vedemSignatureExpiration = 1800

// buildVedemAgwTokenSigned signs the actual request payload.
// When VEDEM_DRAMA_AK/SK are absent, it returns an empty string and auth is skipped.
func buildVedemAgwTokenSigned(payloadBytes []byte) string {
	ak := taskagent.EnvString("VEDEM_DRAMA_AK", "")
	sk := taskagent.EnvString("VEDEM_DRAMA_SK", "")
	if ak == "" || sk == "" {
		// When credentials are missing, explicitly skip signing to keep local/testing flows simple.
		return ""
	}

	// Sign function
	signKeyInfo := fmt.Sprintf("%s/%s/%d/%d", "auth-v2", ak, time.Now().Unix(), vedemSignatureExpiration)
	signKey := sha256HMAC([]byte(sk), []byte(signKeyInfo))
	signResult := sha256HMAC(signKey, payloadBytes)
	return fmt.Sprintf("%s/%s", signKeyInfo, string(signResult))
}

// sha256HMAC wraps the HMAC-SHA256 calculation used by the vedem signing scheme.
func sha256HMAC(key []byte, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return []byte(fmt.Sprintf("%x", mac.Sum(nil)))
}


// getString reads a field value from a Feishu bitable row fields map as string.
func getString(fields map[string]any, name string) string {
	if fields == nil || strings.TrimSpace(name) == "" {
		return ""
	}
	if val, ok := fields[name]; ok {
		if val == nil {
			return ""
		}
		switch v := val.(type) {
		case string:
			return strings.TrimSpace(v)
		case []byte:
			return strings.TrimSpace(string(v))
		case json.Number:
			return strings.TrimSpace(v.String())
		case []interface{}:
			if text := extractTextArray(v); text != "" {
				return text
			}
		default:
			rv := reflect.ValueOf(val)
			switch rv.Kind() {
			case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Interface, reflect.Func, reflect.Chan:
				if rv.IsNil() {
					return ""
				}
			}
			if b, err := json.Marshal(v); err == nil {
				out := strings.TrimSpace(string(b))
				// Ensure JSON null values never leak as the literal string "null".
				if strings.EqualFold(out, "null") {
					return ""
				}
				return out
			}
			return ""
		}
	}
	return ""
}

// extractTextArray flattens Feishu rich-text array values into a plain string.
func extractTextArray(arr []interface{}) string {
	if len(arr) == 0 {
		return ""
	}
	parts := make([]string, 0, len(arr))
	for _, item := range arr {
		if m, ok := item.(map[string]any); ok {
			if txt, ok := m["text"].(string); ok {
				trimmed := strings.TrimSpace(txt)
				if trimmed != "" {
					parts = append(parts, trimmed)
				}
			}
		}
	}
	return strings.Join(parts, " ")
}
