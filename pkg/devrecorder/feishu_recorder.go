package devrecorder

import (
	"context"
	"os"
	"strings"
	"time"

	pool "github.com/httprunner/TaskAgent"
	"github.com/httprunner/TaskAgent/pkg/feishu"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// FeishuRecorder persists device and job snapshots to Feishu bitable tables.
type FeishuRecorder struct {
	client     *feishu.Client
	infoURL    string
	taskURL    string
	infoFields feishu.DeviceInfoFields
	taskFields feishu.DeviceTaskFields
	clock      func() time.Time
}

// NewFeishuRecorder returns nil when both URLs are empty, allowing graceful opt-out.
func NewFeishuRecorder(infoURL, taskURL string) (*FeishuRecorder, error) {
	infoURL = strings.TrimSpace(infoURL)
	taskURL = strings.TrimSpace(taskURL)
	if infoURL == "" && taskURL == "" {
		return nil, nil
	}
	cli, err := feishu.NewClientFromEnv()
	if err != nil {
		return nil, err
	}
	return &FeishuRecorder{
		client:     cli,
		infoURL:    infoURL,
		taskURL:    taskURL,
		infoFields: feishu.DeviceInfoFieldsFromEnv(),
		taskFields: feishu.DeviceTaskFieldsFromEnv(),
	}, nil
}

// NewFromEnv builds a recorder using environment variables; falls back to Noop when not configured.
func NewFromEnv() (DeviceRecorder, error) {
	infoURL := strings.TrimSpace(os.Getenv(feishu.EnvDeviceInfoURL))
	taskURL := strings.TrimSpace(os.Getenv(feishu.EnvDeviceTaskURL))
	rec, err := NewFeishuRecorder(infoURL, taskURL)
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
		rec := feishu.DeviceInfoRecordInput{
			DeviceSerial: d.DeviceSerial,
			OSType:       d.OSType,
			OSVersion:    d.OSVersion,
			AgentVersion: d.AgentVersion,
			Status:       d.Status,
			LastError:    d.LastError,
		}
		if !d.LastSeenAt.IsZero() {
			rec.LastSeenAt = &d.LastSeenAt
		} else {
			rec.LastSeenAt = &now
		}
		if err := r.client.UpsertDeviceInfo(ctx, r.infoURL, r.infoFields, rec); err != nil {
			log.Error().Err(err).Str("serial", d.DeviceSerial).Str("status", d.Status).Msg("feishu recorder: upsert device failed")
		}
	}
	return nil
}

func (r *FeishuRecorder) CreateJob(ctx context.Context, rec *pool.DeviceJobRecord) error {
	if r == nil || r.client == nil || r.taskURL == "" || rec == nil {
		return nil
	}
	payload := feishu.DeviceTaskRecordInput{
		JobID:         rec.JobID,
		DeviceSerial:  rec.DeviceSerial,
		App:           rec.App,
		State:         rec.State,
		AssignedTasks: rec.AssignedTasks,
		RunningTask:   rec.RunningTask,
	}
	if !rec.StartAt.IsZero() {
		payload.StartAt = &rec.StartAt
	}
	if _, err := r.client.CreateDeviceTaskRecord(ctx, r.taskURL, payload, r.taskFields); err != nil {
		return errors.Wrap(err, "feishu recorder: create job record failed")
	}
	return nil
}

func (r *FeishuRecorder) UpdateJob(ctx context.Context, jobID string, upd *pool.DeviceJobUpdate) error {
	if r == nil || r.client == nil || r.taskURL == "" || upd == nil {
		return nil
	}
	payload := feishu.DeviceTaskUpdate{
		State:        upd.State,
		RunningTask:  upd.RunningTask,
		ErrorMessage: upd.ErrorMessage,
	}
	if upd.EndAt != nil {
		payload.EndAt = upd.EndAt
	}
	if upd.ElapsedSeconds != nil {
		payload.Elapsed = upd.ElapsedSeconds
	}
	return r.client.UpdateDeviceTaskByJob(ctx, r.taskURL, jobID, r.taskFields, payload)
}

func (r *FeishuRecorder) now() time.Time {
	if r.clock != nil {
		return r.clock()
	}
	return time.Now()
}
