package feishu

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

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
func (s *ResultStorage) Write(ctx context.Context, record ResultRecordInput) error {
	if s == nil || s.client == nil {
		return errors.New("feishu storage: storage is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := s.client.CreateResultRecord(ctx, s.tableURL, record, nil)
	if err != nil {
		err = annotateFeishuFieldError(err, record)
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

// Helper functions for building ResultRecordInput values

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
	if strings.TrimSpace(record.UserAuthEntity) != "" {
		columns = append(columns, names.UserAuthEntity)
	}
	if strings.TrimSpace(record.Tags) != "" {
		columns = append(columns, names.Tags)
	}
	if record.TaskID != 0 {
		columns = append(columns, names.TaskID)
	}
	if record.LikeCount != 0 {
		columns = append(columns, names.LikeCount)
	}
	if record.ViewCount != 0 {
		columns = append(columns, names.ViewCount)
	}
	if strings.TrimSpace(record.AnchorPoint) != "" {
		columns = append(columns, names.AnchorPoint)
	}
	if record.Extra != nil {
		columns = append(columns, names.Extra)
	}
	if strings.TrimSpace(record.PublishTime) != "" {
		columns = append(columns, names.PublishTime)
	}
	return columns
}
