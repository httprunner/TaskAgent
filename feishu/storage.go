package feishu

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// VideoData carries the essential fields from apps.Video needed for Feishu reporting.
type VideoData struct {
	CacheKey  string
	VideoID   string
	Caption   string
	Title     string
	URL       string
	M3U8Url   string
	ShareLink string
	DataType  string
	VideoType string
	UserName  string
	UserID    string
	Tags      string
}

// ResultRecord summarizes a single video entry that will be uploaded to the result table.
type ResultRecord struct {
	Timestamp           int64
	DeviceSerial        string
	App                 string
	Query               string
	Video               VideoData
	ItemDurationSeconds *float64
	TaskID              int64
	PayloadJSON         any
}

// ResultStorage writes Feishu records into the configured result table.
type ResultStorage struct {
	client   *Client
	tableURL string
}

// NewResultStorage creates a new Feishu result storage pointing at the provided table.
func NewResultStorage(client *Client, tableURL string) *ResultStorage {
	if client == nil || strings.TrimSpace(tableURL) == "" {
		return nil
	}
	return &ResultStorage{client: client, tableURL: strings.TrimSpace(tableURL)}
}

// NewResultStorageFromEnv initializes a result storage using the FEISHU env vars.
func NewResultStorageFromEnv() (*ResultStorage, error) {
	rawURL := strings.TrimSpace(os.Getenv(EnvResultBitableURL))
	if rawURL == "" {
		return nil, nil
	}
	client, err := NewClientFromEnv()
	if err != nil {
		return nil, errors.Wrap(err, "feishu storage: init client failed")
	}
	log.Info().Str("tableURL", rawURL).Msg("feishu result reporting enabled")
	return NewResultStorage(client, rawURL), nil
}

// Write uploads a single record to the Feishu result table.
func (s *ResultStorage) Write(ctx context.Context, record ResultRecord) error {
	if s == nil || s.client == nil {
		return errors.New("feishu storage: storage is nil")
	}
	input := buildResultRecordInput(record)
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := s.client.CreateResultRecord(ctx, s.tableURL, input, nil)
	if err != nil {
		err = annotateFeishuFieldError(err, input)
		return errors.Wrap(err, "feishu storage: create result record failed")
	}
	return nil
}

// TableURL returns the configured bitable link.
func (s *ResultStorage) TableURL() string {
	if s == nil {
		return ""
	}
	return s.tableURL
}

func buildResultRecordInput(record ResultRecord) ResultRecordInput {
	itemID := pickFirstNonEmpty(record.Video.CacheKey, record.Video.VideoID)
	itemCaption := pickFirstNonEmpty(record.Video.Caption, record.Video.Title)
	return ResultRecordInput{
		DatetimeRaw:         formatRecordTimestamp(record.Timestamp),
		DeviceSerial:        strings.TrimSpace(record.DeviceSerial),
		App:                 strings.TrimSpace(record.App),
		Scene:               deriveScene(record.Video),
		Params:              strings.TrimSpace(record.Query),
		ItemID:              itemID,
		ItemCaption:         itemCaption,
		ItemCDNURL:          deriveItemCDNURL(record.Video),
		ItemURL:             deriveItemURL(record.App, itemID, record.Video),
		ItemDurationSeconds: record.ItemDurationSeconds,
		UserName:            strings.TrimSpace(record.Video.UserName),
		UserID:              strings.TrimSpace(record.Video.UserID),
		Tags:                strings.TrimSpace(record.Video.Tags),
		TaskID:              record.TaskID,
		PayloadJSON:         record.PayloadJSON,
	}
}

func formatRecordTimestamp(ts int64) string {
	if ts > 0 {
		return strconv.FormatInt(ts, 10)
	}
	return strconv.FormatInt(time.Now().UnixMilli(), 10)
}

func deriveScene(video VideoData) string {
	if scene := strings.TrimSpace(video.DataType); scene != "" {
		return scene
	}
	return strings.TrimSpace(video.VideoType)
}

func deriveItemCDNURL(video VideoData) string {
	candidates := []string{video.URL, video.M3U8Url, video.ShareLink}
	for _, candidate := range candidates {
		if trimmed := strings.TrimSpace(candidate); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func deriveItemURL(app, itemID string, video VideoData) string {
	if isKuaishouPackage(app) && strings.TrimSpace(itemID) != "" {
		return fmt.Sprintf("https://www.kuaishou.com/short-video/%s", strings.TrimSpace(itemID))
	}
	if trimmed := strings.TrimSpace(video.ShareLink); trimmed != "" {
		return trimmed
	}
	if trimmed := strings.TrimSpace(video.URL); trimmed != "" {
		return trimmed
	}
	return ""
}

func pickFirstNonEmpty(values ...string) string {
	for _, v := range values {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func isKuaishouPackage(pkg string) bool {
	switch strings.TrimSpace(pkg) {
	case "com.smile.gifmaker", "com.jiangjia.gif":
		return true
	default:
		return false
	}
}

func annotateFeishuFieldError(err error, record ResultRecordInput) error {
	if err == nil {
		return nil
	}
	if !strings.Contains(err.Error(), "FieldNameNotFound") {
		return err
	}
	fields := collectFilledResultFields(record, DefaultResultFields)
	if len(fields) == 0 {
		return errors.Wrap(err, "payload_fields=<none>")
	}
	return errors.Wrap(err, fmt.Sprintf("payload_fields=%s", strings.Join(fields, ",")))
}

func collectFilledResultFields(record ResultRecordInput, names ResultFields) []string {
	columns := make([]string, 0, 14)
	if strings.TrimSpace(record.DatetimeRaw) != "" || record.Datetime != nil {
		columns = append(columns, names.Datetime)
	}
	if strings.TrimSpace(record.DeviceSerial) != "" {
		columns = append(columns, names.DeviceSerial)
	}
	if strings.TrimSpace(record.App) != "" {
		columns = append(columns, names.App)
	}
	if strings.TrimSpace(record.Scene) != "" {
		columns = append(columns, names.Scene)
	}
	if strings.TrimSpace(record.Params) != "" {
		columns = append(columns, names.Params)
	}
	if strings.TrimSpace(record.ItemID) != "" {
		columns = append(columns, names.ItemID)
	}
	if strings.TrimSpace(record.ItemCaption) != "" {
		columns = append(columns, names.ItemCaption)
	}
	if strings.TrimSpace(record.ItemCDNURL) != "" {
		columns = append(columns, names.ItemCDNURL)
	}
	if strings.TrimSpace(record.ItemURL) != "" {
		columns = append(columns, names.ItemURL)
	}
	if record.ItemDurationSeconds != nil {
		columns = append(columns, names.ItemDuration)
	}
	if strings.TrimSpace(record.UserName) != "" {
		columns = append(columns, names.UserName)
	}
	if strings.TrimSpace(record.UserID) != "" {
		columns = append(columns, names.UserID)
	}
	if strings.TrimSpace(record.Tags) != "" {
		columns = append(columns, names.Tags)
	}
	if record.TaskID != 0 {
		columns = append(columns, names.TaskID)
	}
	if record.PayloadJSON != nil {
		columns = append(columns, names.PayloadJSON)
	}
	return columns
}
