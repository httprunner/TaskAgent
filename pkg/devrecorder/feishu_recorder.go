package devrecorder

import (
	"context"
	"os"
	"strings"
	"time"

	pool "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/pkg/feishu"
	"github.com/rs/zerolog/log"
)

// FeishuRecorder persists device snapshots to Feishu bitable tables.
type FeishuRecorder struct {
	client     *feishu.Client
	infoURL    string
	infoFields feishu.DeviceFields
	clock      func() time.Time
}

// NewFeishuRecorder returns nil when URL is empty, allowing graceful opt-out.
func NewFeishuRecorder(infoURL string) (*FeishuRecorder, error) {
	infoURL = strings.TrimSpace(infoURL)
	if infoURL == "" {
		return nil, nil
	}
	cli, err := feishu.NewClientFromEnv()
	if err != nil {
		return nil, err
	}
	return &FeishuRecorder{
		client:     cli,
		infoURL:    infoURL,
		infoFields: feishu.DeviceFieldsFromEnv(),
	}, nil
}

// NewFromEnv builds a recorder using environment variables; falls back to Noop when not configured.
func NewFromEnv() (DeviceRecorder, error) {
	infoURL := strings.TrimSpace(os.Getenv(feishu.EnvDeviceBitableURL))
	rec, err := NewFeishuRecorder(infoURL)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return NoopRecorder{}, nil
	}
	return rec, nil
}

func (r *FeishuRecorder) UpsertDevices(ctx context.Context, devices []pool.DeviceInfoUpdate) error {
	if r == nil || r.client == nil || r.infoURL == "" || len(devices) == 0 {
		return nil
	}
	now := r.now()
	for _, d := range devices {
		rec := feishu.DeviceRecordInput{
			DeviceSerial: d.DeviceSerial,
			OSType:       d.OSType,
			OSVersion:    d.OSVersion,
			IsRoot:       d.IsRoot,
			AgentVersion: d.AgentVersion,
			ProviderUUID: d.ProviderUUID,
			Status:       d.Status,
			LastError:    d.LastError,
			RunningTask:  d.RunningTask,
			PendingTasks: d.PendingTasks,
		}
		if !d.LastSeenAt.IsZero() {
			rec.LastSeenAt = &d.LastSeenAt
		} else {
			rec.LastSeenAt = &now
		}
		if err := r.client.UpsertDevice(ctx, r.infoURL, r.infoFields, rec); err != nil {
			log.Error().Err(err).Str("serial", d.DeviceSerial).Str("status", d.Status).Msg("feishu recorder: upsert device failed")
		}
	}
	return nil
}

func (r *FeishuRecorder) now() time.Time {
	if r.clock != nil {
		return r.clock()
	}
	return time.Now()
}
