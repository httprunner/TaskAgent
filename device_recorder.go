package taskagent

import (
	"context"
	"strings"
	"time"

	"github.com/httprunner/TaskAgent/internal/env"
	"github.com/httprunner/TaskAgent/internal/feishusdk"
	"github.com/rs/zerolog/log"
)

// feishuDeviceRecorder persists device snapshots to Feishu bitable tables.
// It implements DeviceRecorder and is configured via environment variables.
type feishuDeviceRecorder struct {
	client     *feishusdk.Client
	infoURL    string
	infoFields feishusdk.DeviceFields
	clock      func() time.Time
}

// NewDeviceRecorderFromEnv builds a DeviceRecorder using environment variables.
//
// Environment:
//   - DEVICE_BITABLE_URL: target table for device heartbeats; when empty,
//     a no-op recorder is returned.
func NewDeviceRecorderFromEnv() (DeviceRecorder, error) {
	infoURL := env.String(feishusdk.EnvDeviceBitableURL, "")
	infoURL = strings.TrimSpace(infoURL)
	if infoURL == "" {
		return noopRecorder{}, nil
	}

	cli, err := feishusdk.NewClientFromEnv()
	if err != nil {
		return nil, err
	}
	return &feishuDeviceRecorder{
		client:     cli,
		infoURL:    infoURL,
		infoFields: feishusdk.DeviceFieldsFromEnv(),
	}, nil
}

func (r *feishuDeviceRecorder) UpsertDevices(ctx context.Context, devices []DeviceInfoUpdate) error {
	if r == nil || r.client == nil || r.infoURL == "" || len(devices) == 0 {
		return nil
	}
	now := r.now()
	for _, d := range devices {
		serial := strings.TrimSpace(d.DeviceSerial)
		if serial == "" {
			log.Warn().Str("status", d.Status).Msg("taskagent recorder: skip device without serial")
			continue
		}
		rec := feishusdk.DeviceRecordInput{
			DeviceSerial: serial,
			OSType:       d.OSType,
			OSVersion:    d.OSVersion,
			IsRoot:       d.IsRoot,
			AgentVersion: d.AgentVersion,
			ProviderUUID: d.ProviderUUID,
			Status:       d.Status,
			LastError:    d.LastError,
			RunningTask:  strings.TrimSpace(d.RunningTask),
			PendingTasks: sanitizePendingTasks(d.PendingTasks),
		}
		if !d.LastSeenAt.IsZero() {
			rec.LastSeenAt = &d.LastSeenAt
		} else {
			rec.LastSeenAt = &now
		}

		if err := r.client.UpsertDevice(ctx, r.infoURL, r.infoFields, rec); err != nil {
			log.Error().
				Err(err).
				Str("serial", d.DeviceSerial).
				Str("status", d.Status).
				Str("running_field", r.infoFields.RunningTask).
				Str("running_task", rec.RunningTask).
				Str("pending_field", r.infoFields.PendingTasks).
				Strs("pending_tasks", rec.PendingTasks).
				Msg("taskagent recorder: upsert device failed")
		}
	}
	return nil
}

func (r *feishuDeviceRecorder) now() time.Time {
	if r.clock != nil {
		return r.clock()
	}
	return time.Now()
}

// sanitizePendingTasks removes blanks from the provided string slice.
func sanitizePendingTasks(values []string) []string {
	result := make([]string, 0, len(values))
	for _, v := range values {
		if s := strings.TrimSpace(v); s != "" {
			result = append(result, s)
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}
