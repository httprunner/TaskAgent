package feishusdk

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/httprunner/TaskAgent/internal/env"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// ResultStorage writes Feishu records into the configured result table.
type ResultStorage struct {
	client   *Client
	tableURL string
}

const maxResultRecordsPerRequest = 500

var (
	resultLimiterOnce sync.Once
	resultLimiter     *rateLimiter
)

// rateLimiter provides a simple fixed-interval limiter.
type rateLimiter struct {
	ticker *time.Ticker
}

func newRateLimiterFromEnv() *rateLimiter {
	val := env.String("FEISHU_REPORT_RPS", "")
	// Official rate limits: 50 RPS for writes, 20 RPS for reads.
	// We use 15 RPS as a safe default for both.
	rps := 15.0
	if val != "" {
		parsed, err := strconv.ParseFloat(val, 64)
		if err == nil && parsed > 0 && !math.IsInf(parsed, 0) && !math.IsNaN(parsed) {
			rps = parsed
		}
	}
	interval := time.Duration(float64(time.Second) / rps)
	if interval <= 0 {
		interval = time.Second
	}
	return &rateLimiter{ticker: time.NewTicker(interval)}
}

func (l *rateLimiter) wait(ctx context.Context) error {
	if l == nil || l.ticker == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.ticker.C:
		return nil
	}
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
	rawURL := env.String(EnvResultBitableURL, "")
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
	return s.writeRecords(ctx, []ResultRecordInput{record})
}

// WriteBatch uploads multiple records to the Feishu result table using batch_create API.
func (s *ResultStorage) WriteBatch(ctx context.Context, records []ResultRecordInput) error {
	if len(records) == 0 {
		return errors.New("feishu storage: no records provided for batch write")
	}
	return s.writeRecords(ctx, records)
}

func (s *ResultStorage) writeRecords(ctx context.Context, records []ResultRecordInput) error {
	if s == nil || s.client == nil {
		return errors.New("feishu storage: storage is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for start := 0; start < len(records); start += maxResultRecordsPerRequest {
		end := start + maxResultRecordsPerRequest
		if end > len(records) {
			end = len(records)
		}
		chunk := records[start:end]
		if err := s.writeChunk(ctx, chunk); err != nil {
			return err
		}
	}
	return nil
}

func (s *ResultStorage) writeChunk(ctx context.Context, chunk []ResultRecordInput) error {
	if len(chunk) == 0 {
		return nil
	}
	limiter := s.initLimiter()
	if err := limiter.wait(ctx); err != nil {
		return err
	}
	_, err := s.client.CreateResultRecords(ctx, s.tableURL, chunk, nil)
	if err != nil {
		if len(chunk) == 1 {
			err = annotateFeishuFieldError(err, chunk[0])
			return errors.Wrap(err, "feishu storage: create result record failed")
		}
		return errors.Wrapf(err, "feishu storage: create %d result records failed", len(chunk))
	}
	return nil
}

func (s *ResultStorage) initLimiter() *rateLimiter {
	resultLimiterOnce.Do(func() {
		resultLimiter = newRateLimiterFromEnv()
	})
	if resultLimiter == nil {
		resultLimiter = newRateLimiterFromEnv()
	}
	return resultLimiter
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
