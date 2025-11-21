package pool

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	envload "github.com/httprunner/TaskAgent/internal"
	"github.com/httprunner/TaskAgent/pkg/feishu"
	adbprovider "github.com/httprunner/TaskAgent/providers/adb"
	gadb "github.com/httprunner/httprunner/v5/pkg/gadb"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

func init() {
	envload.Ensure()
}

// Task represents a single unit of work to be executed on a device.
type Task struct {
	ID           string
	Payload      any
	DeviceSerial string
	ResultStatus string
}

// DeviceProvider returns the set of currently connected device serials.
type DeviceProvider interface {
	ListDevices(ctx context.Context) ([]string, error)
}

// TaskManager owns task lifecycle hooks (fetch/dispatch/complete).
type TaskManager interface {
	FetchAvailableTasks(ctx context.Context, app string, limit int) ([]*Task, error)
	OnTasksDispatched(ctx context.Context, deviceSerial string, tasks []*Task) error
	OnTasksCompleted(ctx context.Context, deviceSerial string, tasks []*Task, jobErr error) error
}

// JobRequest bundles the execution details for a device.
type JobRequest struct {
	DeviceSerial string
	Tasks        []*Task
}

// JobRunner executes tasks on a concrete device.
type JobRunner interface {
	RunJob(ctx context.Context, req JobRequest) error
}

// Config controls DevicePoolAgent behavior.
type Config struct {
	PollInterval    time.Duration
	MaxTasksPerJob  int
	MaxFetchPerPoll int
	MaxJobRetries   int
	JobRetryBackoff time.Duration
	OSType          string
	Provider        DeviceProvider
	TaskManager     TaskManager
	BitableURL      string
	AgentVersion    string
	DeviceRecorder  DeviceRecorder
}

// DevicePoolAgent coordinates plug-and-play devices with a task source.
type DevicePoolAgent struct {
	cfg             Config
	deviceProvider  DeviceProvider
	taskManager     TaskManager
	recorder        DeviceRecorder
	jobRunner       JobRunner
	deviceMu        sync.Mutex
	devices         map[string]*deviceState
	backgroundGroup sync.WaitGroup
	adbClient       gadb.Client
	adbReady        bool
	hostUUID        string
}

type deviceStatus string

const (
	statusIdle    deviceStatus = "idle"
	statusRunning deviceStatus = "running"
)

const offlineThreshold = 5 * time.Minute

type deviceState struct {
	serial         string
	status         deviceStatus
	lastSeen       time.Time
	removeAfterJob bool
	currentJob     *deviceJob
	// meta contains immutable device info (os version/root). Fetch once to avoid repeated adb calls.
	meta      deviceMeta
	metaReady bool
}

type deviceMeta struct {
	osType       string
	osVersion    string
	isRoot       string
	providerUUID string
}

type deviceJob struct {
	deviceSerial string
	jobID        string
	tasks        []*Task
	cancel       context.CancelFunc
	startAt      time.Time
}

type noopRecorder struct{}

func (noopRecorder) UpsertDevices(ctx context.Context, devices []DeviceInfoUpdate) error { return nil }
func (noopRecorder) CreateJob(ctx context.Context, rec *DeviceJobRecord) error           { return nil }
func (noopRecorder) UpdateJob(ctx context.Context, jobID string, upd *DeviceJobUpdate) error {
	return nil
}

type deviceAssignment struct {
	device   *deviceState
	tasks    []*Task
	capacity int
}

// NewDevicePoolAgent builds an agent with the provided configuration and job runner.
func NewDevicePoolAgent(cfg Config, runner JobRunner) (*DevicePoolAgent, error) {
	if runner == nil {
		return nil, errors.New("job runner cannot be nil")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Minute
	}
	if cfg.MaxTasksPerJob <= 0 {
		cfg.MaxTasksPerJob = 1
	}
	if cfg.MaxJobRetries <= 0 {
		cfg.MaxJobRetries = 2
	}
	if cfg.JobRetryBackoff <= 0 {
		cfg.JobRetryBackoff = 5 * time.Second
	}

	provider := cfg.Provider
	if provider == nil {
		var err error
		provider, err = defaultDeviceProvider(cfg)
		if err != nil {
			return nil, err
		}
	}

	manager := cfg.TaskManager
	if manager == nil {
		var err error
		manager, err = NewFeishuTaskClient(cfg.BitableURL)
		if err != nil {
			return nil, err
		}
	}
	if manager == nil {
		return nil, errors.New("task manager cannot be nil")
	}

	agent := &DevicePoolAgent{
		cfg:            cfg,
		deviceProvider: provider,
		taskManager:    manager,
		recorder:       cfg.DeviceRecorder,
		jobRunner:      runner,
		devices:        make(map[string]*deviceState),
	}
	if agent.recorder == nil {
		agent.recorder = noopRecorder{}
	}
	// cache host UUID for ProviderUUID field; ignore error silently
	if uuid, err := getHostUUID(); err == nil {
		agent.hostUUID = uuid
	}
	return agent, nil
}

func defaultDeviceProvider(cfg Config) (DeviceProvider, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.OSType)) {
	case "", "android":
		client, err := gadb.NewClient()
		if err != nil {
			return nil, errors.Wrap(err, "init adb client for pool")
		}
		return adbprovider.New(client), nil
	default:
		return nil, fmt.Errorf("no default device provider for os type %s", cfg.OSType)
	}
}

// Start begins the polling loop until the context is cancelled.
func (a *DevicePoolAgent) Start(ctx context.Context, app string) error {
	log.Info().Msg("start device pool agent")
	if ctx == nil {
		return errors.New("context cannot be nil")
	}
	if err := a.runCycle(ctx, app); err != nil {
		log.Error().Err(err).Msg("device pool initial cycle failed")
	}

	ticker := time.NewTicker(a.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			a.backgroundGroup.Wait()
			return nil
		case <-ticker.C:
			if err := a.runCycle(ctx, app); err != nil {
				log.Error().Err(err).Msg("device pool cycle failed")
			}
		}
	}
}

// RunOnce exposes a single refresh/dispatch iteration and waits for any
// dispatched jobs started during the cycle to finish.
func (a *DevicePoolAgent) RunOnce(ctx context.Context, app string) error {
	if err := a.runCycle(ctx, app); err != nil {
		return err
	}
	a.backgroundGroup.Wait()
	return nil
}

func (a *DevicePoolAgent) runCycle(ctx context.Context, app string) error {
	if err := a.refreshDevices(ctx); err != nil {
		return errors.Wrap(err, "refresh devices failed")
	}
	if err := a.dispatch(ctx, app); err != nil {
		return errors.Wrap(err, "dispatch failed")
	}
	return nil
}

func (a *DevicePoolAgent) refreshDevices(ctx context.Context) error {
	log.Info().Msg("refresh devices for device pool agent")
	serials, err := a.deviceProvider.ListDevices(ctx)
	if err != nil {
		return errors.Wrap(err, "list devices failed")
	}
	now := time.Now()
	seen := make(map[string]struct{}, len(serials))
	updates := make([]DeviceInfoUpdate, 0, len(serials))

	a.deviceMu.Lock()
	defer a.deviceMu.Unlock()

	for _, serial := range serials {
		serial = strings.TrimSpace(serial)
		if serial == "" {
			continue
		}
		seen[serial] = struct{}{}
		if dev, ok := a.devices[serial]; ok {
			dev.lastSeen = now
			if !dev.metaReady {
				dev.meta = a.fetchDeviceMeta(serial)
				dev.metaReady = true
			}
			updates = append(updates,
				DeviceInfoUpdate{
					DeviceSerial: serial,
					Status:       string(dev.status),
					OSType:       dev.meta.osType,
					OSVersion:    dev.meta.osVersion,
					IsRoot:       dev.meta.isRoot,
					ProviderUUID: dev.meta.providerUUID,
					AgentVersion: a.cfg.AgentVersion,
					LastSeenAt:   now,
				})
			continue
		}
		meta := a.fetchDeviceMeta(serial)
		a.devices[serial] = &deviceState{
			serial:    serial,
			status:    statusIdle,
			lastSeen:  now,
			meta:      meta,
			metaReady: true,
		}
		updates = append(updates, DeviceInfoUpdate{
			DeviceSerial: serial,
			Status:       string(statusIdle),
			OSType:       meta.osType,
			OSVersion:    meta.osVersion,
			IsRoot:       meta.isRoot,
			ProviderUUID: meta.providerUUID,
			AgentVersion: a.cfg.AgentVersion,
			LastSeenAt:   now,
		})
		log.Info().Str("serial", serial).Msg("device connected")
	}

	for serial, dev := range a.devices {
		if _, ok := seen[serial]; ok {
			continue
		}
		if dev.status == statusRunning {
			dev.removeAfterJob = true
			log.Warn().Str("serial", serial).Msg("device disconnected during job, will remove after completion")
			continue
		}
		if now.Sub(dev.lastSeen) < offlineThreshold {
			// wait until threshold before marking offline
			continue
		}
		if !dev.metaReady {
			dev.meta = a.fetchDeviceMeta(serial)
			dev.metaReady = true
		}
		delete(a.devices, serial)
		updates = append(updates,
			DeviceInfoUpdate{
				DeviceSerial: serial,
				Status:       "offline",
				OSType:       dev.meta.osType,
				OSVersion:    dev.meta.osVersion,
				IsRoot:       dev.meta.isRoot,
				ProviderUUID: dev.meta.providerUUID,
				AgentVersion: a.cfg.AgentVersion,
				LastSeenAt:   dev.lastSeen,
			})
		log.Info().Str("serial", serial).Msg("device disconnected")
	}

	if err := a.recorder.UpsertDevices(ctx, updates); err != nil {
		log.Error().Err(err).Msg("device recorder upsert failed")
	}
	return nil
}

func (a *DevicePoolAgent) dispatch(ctx context.Context, app string) error {
	idle := a.idleDevices()
	if len(idle) == 0 {
		log.Info().Msg("device pool dispatch skipped: no idle devices")
		return nil
	}
	maxTasks := len(idle) * a.cfg.MaxTasksPerJob
	fetchCap := a.cfg.MaxFetchPerPoll
	if fetchCap > 0 && maxTasks > fetchCap {
		maxTasks = fetchCap
	}
	log.Info().
		Int("idle_devices", len(idle)).
		Int("max_tasks_per_job", a.cfg.MaxTasksPerJob).
		Int("fetch_cap", fetchCap).
		Int("max_fetch", maxTasks).
		Msg("device pool dispatch requesting tasks")
	tasks, err := a.taskManager.FetchAvailableTasks(ctx, app, maxTasks)
	if err != nil {
		return errors.Wrap(err, "fetch available tasks failed")
	}
	if len(tasks) == 0 {
		log.Info().
			Int("idle_devices", len(idle)).
			Int("max_fetch", maxTasks).
			Msg("device pool dispatch found no available tasks")
		return nil
	}

	targeted := make(map[string][]*Task)
	general := make([]*Task, 0, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		target := strings.TrimSpace(task.DeviceSerial)
		if target == "" {
			general = append(general, task)
			continue
		}
		targeted[target] = append(targeted[target], task)
	}
	assignments := make([]*deviceAssignment, 0, len(idle))
	for _, dev := range idle {
		capacity := a.cfg.MaxTasksPerJob
		if capacity <= 0 {
			capacity = 1
		}
		assignment := &deviceAssignment{
			device:   dev,
			tasks:    make([]*Task, 0, capacity),
			capacity: capacity,
		}
		if list := targeted[dev.serial]; len(list) > 0 {
			take := assignment.capacity
			if take > len(list) {
				take = len(list)
			}
			assignment.tasks = append(assignment.tasks, list[:take]...)
			assignment.capacity -= take
		}
		assignments = append(assignments, assignment)
	}
	generalIdx := 0
	for generalIdx < len(general) {
		progress := false
		for _, assignment := range assignments {
			if assignment.capacity <= 0 {
				continue
			}
			assignment.tasks = append(assignment.tasks, general[generalIdx])
			assignment.capacity--
			generalIdx++
			progress = true
			if generalIdx >= len(general) {
				break
			}
		}
		if !progress {
			break
		}
	}
	for _, assignment := range assignments {
		if len(assignment.tasks) == 0 || assignment.device == nil {
			continue
		}
		dev := assignment.device
		if err := a.taskManager.OnTasksDispatched(ctx, dev.serial, assignment.tasks); err != nil {
			log.Error().Err(err).Str("serial", dev.serial).Msg("task dispatch hook failed")
			continue
		}
		now := time.Now()
		jobID := fmt.Sprintf("%s-%s", dev.serial, now.Format("0601021504"))
		if err := a.recorder.CreateJob(ctx, &DeviceJobRecord{
			JobID:         jobID,
			DeviceSerial:  dev.serial,
			App:           app,
			State:         "running",
			AssignedTasks: extractTaskIDs(assignment.tasks),
			RunningTask:   firstTaskID(assignment.tasks),
			StartAt:       now,
		}); err != nil {
			log.Error().Err(err).Str("serial", dev.serial).Msg("device recorder create job failed")
			jobID = "" // avoid later update attempts when creation failed
		}
		log.Info().
			Str("serial", dev.serial).
			Int("assigned_tasks", len(assignment.tasks)).
			Msg("device pool dispatched tasks to device")
		a.startDeviceJob(ctx, dev, assignment.tasks, jobID)
	}
	return nil
}

func (a *DevicePoolAgent) idleDevices() []*deviceState {
	a.deviceMu.Lock()
	defer a.deviceMu.Unlock()
	result := make([]*deviceState, 0, len(a.devices))
	for _, dev := range a.devices {
		if dev.status == statusIdle && !dev.removeAfterJob {
			result = append(result, dev)
		}
	}
	return result
}

func (a *DevicePoolAgent) startDeviceJob(ctx context.Context, dev *deviceState, tasks []*Task, jobID string) {
	jobCtx, cancel := context.WithCancel(ctx)
	job := &deviceJob{
		deviceSerial: dev.serial,
		jobID:        jobID,
		tasks:        tasks,
		cancel:       cancel,
		startAt:      time.Now(),
	}

	a.deviceMu.Lock()
	dev.status = statusRunning
	dev.currentJob = job
	a.deviceMu.Unlock()

	a.backgroundGroup.Add(1)
	go a.runDeviceJob(jobCtx, dev, job)
}

func (a *DevicePoolAgent) runDeviceJob(ctx context.Context, dev *deviceState, job *deviceJob) {
	defer a.backgroundGroup.Done()
	defer job.cancel()

	log.Info().
		Str("serial", job.deviceSerial).
		Int("task_count", len(job.tasks)).
		Msg("start device job")

	maxAttempts := a.cfg.MaxJobRetries
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	backoff := a.cfg.JobRetryBackoff
	if backoff <= 0 {
		backoff = 5 * time.Second
	}

	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = a.jobRunner.RunJob(ctx, JobRequest{
			DeviceSerial: job.deviceSerial,
			Tasks:        job.tasks,
		})
		if err == nil {
			if attempt > 1 {
				log.Info().
					Str("serial", job.deviceSerial).
					Int("attempts", attempt).
					Msg("device job recovered after retry")
			}
			break
		}

		if attempt == maxAttempts {
			break
		}

		log.Warn().
			Err(err).
			Str("serial", job.deviceSerial).
			Int("attempt", attempt).
			Int("max_attempts", maxAttempts).
			Dur("backoff", backoff).
			Msg("device job failed, scheduling retry")

		select {
		case <-ctx.Done():
			err = ctx.Err()
			attempt = maxAttempts
		case <-time.After(backoff):
		}
	}

	a.handleTaskResults(job, err)

	a.deviceMu.Lock()
	dev.status = statusIdle
	dev.currentJob = nil
	remove := dev.removeAfterJob
	a.deviceMu.Unlock()

	if job.jobID != "" {
		endAt := time.Now()
		elapsed := endAt.Sub(job.startAt)
		elapsedSecs := int64(elapsed.Seconds())
		state := "success"
		if err != nil {
			state = "failed"
		}
		if remove {
			state = "offline"
		}
		if recErr := a.recorder.UpdateJob(context.Background(), job.jobID, &DeviceJobUpdate{
			State:          state,
			RunningTask:    "",
			EndAt:          &endAt,
			ElapsedSeconds: &elapsedSecs,
			ErrorMessage:   errString(err),
		}); recErr != nil {
			log.Error().Err(recErr).Str("job_id", job.jobID).Msg("device recorder update job failed")
		}
	}

	if err != nil {
		log.Error().Err(err).Str("serial", job.deviceSerial).Msg("device job failed")
	} else {
		log.Info().Str("serial", job.deviceSerial).Msg("device job finished")
	}

	if remove {
		a.removeDevice(job.deviceSerial)
	}
}

func (a *DevicePoolAgent) handleTaskResults(job *deviceJob, jobErr error) {
	ctx := context.Background()
	successTasks, failedTasks, pendingTasks := splitTasksByResultStatus(job.tasks)

	report := func(tasks []*Task, err error) {
		if len(tasks) == 0 {
			return
		}
		if hookErr := a.taskManager.OnTasksCompleted(ctx, job.deviceSerial, tasks, err); hookErr != nil {
			log.Error().Err(hookErr).Str("serial", job.deviceSerial).Msg("task completion hook failed")
		}
	}

	report(successTasks, nil)
	if len(failedTasks) > 0 {
		report(failedTasks, errors.New("job runner reported task failure"))
	}
	if len(pendingTasks) > 0 {
		report(pendingTasks, jobErr)
	}
}

func splitTasksByResultStatus(tasks []*Task) (success []*Task, failed []*Task, pending []*Task) {
	for _, task := range tasks {
		if task == nil {
			continue
		}
		switch strings.TrimSpace(task.ResultStatus) {
		case feishu.StatusSuccess:
			success = append(success, task)
		case feishu.StatusFailed:
			failed = append(failed, task)
		default:
			pending = append(pending, task)
		}
	}
	return
}

func extractTaskIDs(tasks []*Task) []string {
	result := make([]string, 0, len(tasks))
	for _, t := range tasks {
		if t == nil {
			continue
		}
		if id := strings.TrimSpace(t.ID); id != "" {
			result = append(result, id)
		}
	}
	return result
}

func firstTaskID(tasks []*Task) string {
	for _, t := range tasks {
		if t == nil {
			continue
		}
		if id := strings.TrimSpace(t.ID); id != "" {
			return id
		}
	}
	return ""
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (a *DevicePoolAgent) removeDevice(serial string) {
	a.deviceMu.Lock()
	defer a.deviceMu.Unlock()
	delete(a.devices, serial)
	log.Info().Str("serial", serial).Msg("device removed from pool")
}
