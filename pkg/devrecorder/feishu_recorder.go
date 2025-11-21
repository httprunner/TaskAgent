package devrecorder

import (
	"context"
	"fmt"
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
			IsRoot:       d.IsRoot,
			AgentVersion: d.AgentVersion,
			ProviderUUID: d.ProviderUUID,
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

	// Before starting a new job on this device, close any previous rows that
	// are still marked as running. This prevents stale running states when the
	// agent restarts mid-job.
	if updated, err := r.failRunningJobsForDevice(ctx, rec.DeviceSerial); err != nil {
		log.Error().
			Err(err).
			Str("serial", rec.DeviceSerial).
			Msg("feishu recorder: fail stale running jobs")
	} else if updated > 0 {
		log.Info().
			Str("serial", rec.DeviceSerial).
			Int("closed_running_jobs", updated).
			Msg("feishu recorder: closed stale running jobs before creating new job")
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

// failRunningJobsForDevice marks all "running" rows for the specified device as
// failed with EndAt set to now. It best-effort logs errors instead of failing
// the caller to avoid blocking dispatch.
func (r *FeishuRecorder) failRunningJobsForDevice(ctx context.Context, deviceSerial string) (int, error) {
	if r == nil || r.client == nil || r.taskURL == "" {
		return 0, nil
	}
	serial := strings.TrimSpace(deviceSerial)
	if serial == "" {
		return 0, nil
	}

	escapedSerial := strings.ReplaceAll(serial, "\"", "\\\"")
	filter := fmt.Sprintf("AND(CurrentValue.[%s] = \"%s\", CurrentValue.[%s] = \"running\")",
		r.taskFields.DeviceSerial, escapedSerial, r.taskFields.State)
	opts := &feishu.TargetQueryOptions{
		Filter: filter,
	}

	table, err := r.client.FetchDeviceTaskTableWithOptions(ctx, r.taskURL, &r.taskFields, opts)
	if err != nil {
		return 0, err
	}

	updated := 0
	for _, row := range table.Rows {
		if strings.TrimSpace(row.DeviceSerial) != serial {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(row.State), "running") {
			continue
		}

		endAt := r.now()
		if err := r.client.UpdateDeviceTaskByJob(ctx, r.taskURL, row.JobID, r.taskFields, feishu.DeviceTaskUpdate{
			State:       feishu.StatusFailed,
			RunningTask: "",
			EndAt:       &endAt,
		}); err != nil {
			log.Error().
				Err(err).
				Str("job_id", row.JobID).
				Str("serial", serial).
				Msg("feishu recorder: mark stale running job failed")
			continue
		}
		updated++
	}

	return updated, nil
}
