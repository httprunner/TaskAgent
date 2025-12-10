package devrecorder

import (
	"context"
	"strings"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/internal/env"
	"github.com/httprunner/TaskAgent/internal/feishusdk"
	"github.com/rs/zerolog/log"
)

// DeviceRecorder captures device + job state to an external store (e.g., Feishu bitable).
// This package is internal; external callers should construct recorders via
// taskagent.NewDeviceRecorderFromEnv.
type DeviceRecorder interface {
	UpsertDevices(ctx context.Context, devices []taskagent.DeviceInfoUpdate) error
}

// NoopRecorder is the default implementation when recording is disabled.
type NoopRecorder struct{}

func (NoopRecorder) UpsertDevices(ctx context.Context, devices []taskagent.DeviceInfoUpdate) error {
	return nil
}

// DeviceInfoUpdate constructor helper for callers.
func NewDeviceInfo(serial, status, osType, osVersion, agentVersion, lastError string, lastSeen time.Time) taskagent.DeviceInfoUpdate {
	return taskagent.DeviceInfoUpdate{
		DeviceSerial: serial,
		Status:       status,
		OSType:       osType,
		OSVersion:    osVersion,
		AgentVersion: agentVersion,
		LastError:    lastError,
		LastSeenAt:   lastSeen,
	}
}

// FeishuRecorder persists device snapshots to Feishu bitable tables.
type FeishuRecorder struct {
	client     *feishusdk.Client
	infoURL    string
	infoFields feishusdk.DeviceFields
	clock      func() time.Time
}

// NewFeishuRecorder returns nil when URL is empty, allowing graceful opt-out.
func NewFeishuRecorder(infoURL string) (*FeishuRecorder, error) {
	infoURL = strings.TrimSpace(infoURL)
	if infoURL == "" {
		return nil, nil
	}
	cli, err := feishusdk.NewClientFromEnv()
	if err != nil {
		return nil, err
	}
	return &FeishuRecorder{
		client:     cli,
		infoURL:    infoURL,
		infoFields: feishusdk.DeviceFieldsFromEnv(),
	}, nil
}

// NewFromEnv builds a recorder using environment variables; falls back to Noop when not configured.
func NewFromEnv() (DeviceRecorder, error) {
	infoURL := env.String(feishusdk.EnvDeviceBitableURL, "")
	rec, err := NewFeishuRecorder(infoURL)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return NoopRecorder{}, nil
	}
	return rec, nil
}

func (r *FeishuRecorder) UpsertDevices(ctx context.Context, devices []taskagent.DeviceInfoUpdate) error {
	if r == nil || r.client == nil || r.infoURL == "" || len(devices) == 0 {
		return nil
	}
	now := r.now()
	for _, d := range devices {
		serial := strings.TrimSpace(d.DeviceSerial)
		if serial == "" {
			log.Warn().Str("status", d.Status).Msg("feishusdk recorder: skip device without serial")
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
			PendingTasks: sanitizeSlice(d.PendingTasks),
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
				Msg("feishusdk recorder: upsert device failed")
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

// sanitizeSlice removes blanks from the provided string slice.
func sanitizeSlice(values []string) []string {
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
