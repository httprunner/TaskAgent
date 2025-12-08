package pool

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	envload "github.com/httprunner/TaskAgent/internal"
	agentdevice "github.com/httprunner/TaskAgent/internal/agent/device"
	agentlifecycle "github.com/httprunner/TaskAgent/internal/agent/lifecycle"
	agenttasks "github.com/httprunner/TaskAgent/internal/agent/tasks"
	"github.com/httprunner/TaskAgent/pkg/feishu"
	adbprovider "github.com/httprunner/TaskAgent/providers/adb"
	gadb "github.com/httprunner/httprunner/v5/pkg/gadb"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

func init() {
	envload.Ensure()
}

// Re-export核心类型，保持向后兼容。
type Task = agenttasks.Task
type TaskLifecycle = agentlifecycle.Callbacks
type DeviceProvider = agentdevice.Provider
type DeviceRecorder = agentdevice.Recorder
type DeviceInfoUpdate = agentdevice.InfoUpdate
type TaskManager = agenttasks.Source
type TaskStartNotifier = agenttasks.StartNotifier
type TaskResultNotifier = agenttasks.ResultNotifier

// JobRequest bundles the execution details for a device.
type JobRequest struct {
	DeviceSerial string
	Tasks        []*Task
	Lifecycle    *TaskLifecycle
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
	AllowedScenes   []string
}

// DevicePoolAgent coordinates plug-and-play devices with a task source.
type DevicePoolAgent struct {
	cfg             Config
	deviceProvider  DeviceProvider
	taskManager     TaskManager
	recorder        DeviceRecorder
	jobRunner       JobRunner
	deviceManager   *agentdevice.Manager
	jobsMu          sync.RWMutex
	jobs            map[string]*deviceJob
	backgroundGroup sync.WaitGroup
	adbClient       gadb.Client
	adbReady        bool
	hostUUID        string
}

const (
	statusIdle    = agentdevice.StatusIdle
	statusRunning = agentdevice.StatusRunning
)

type deviceJob struct {
	deviceSerial string
	tasks        []*Task
	cancel       context.CancelFunc
	startAt      time.Time
	lifecycle    *TaskLifecycle
}

type noopRecorder struct{}

func (noopRecorder) UpsertDevices(ctx context.Context, devices []DeviceInfoUpdate) error { return nil }

type deviceAssignment struct {
	serial   string
	meta     agentdevice.Meta
	tasks    []*Task
	capacity int
}

func defaultDeviceAllowedScenes() []string {
	return []string{
		SceneVideoScreenCapture,
		SceneGeneralSearch,
		SceneProfileSearch,
		SceneCollection,
		SceneAnchorCapture,
	}
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
		allowed := cfg.AllowedScenes
		if len(allowed) == 0 {
			allowed = defaultDeviceAllowedScenes()
		}
		var err error
		manager, err = NewFeishuTaskClientWithOptions(cfg.BitableURL, FeishuTaskClientOptions{AllowedScenes: allowed})
		if err != nil {
			return nil, err
		}
	}
	if manager == nil {
		return nil, errors.New("task manager cannot be nil")
	}

	hostUUID := ""
	if uuid, err := getHostUUID(); err == nil {
		hostUUID = uuid
	}

	agent := &DevicePoolAgent{
		cfg:            cfg,
		deviceProvider: provider,
		taskManager:    manager,
		recorder:       cfg.DeviceRecorder,
		jobRunner:      runner,
		jobs:           make(map[string]*deviceJob),
		hostUUID:       hostUUID,
	}
	if agent.recorder == nil {
		agent.recorder = noopRecorder{}
	}
	agent.deviceManager = agentdevice.NewManager(agent.deviceProvider, agent.recorder, agent.cfg.AgentVersion, agent.hostUUID)
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
	if a.deviceManager == nil {
		return errors.New("device manager is not initialized")
	}
	return a.deviceManager.Refresh(ctx, a.fetchDeviceMeta, a.snapshotJobTasks)
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
	for _, serial := range idle {
		capacity := a.cfg.MaxTasksPerJob
		if capacity <= 0 {
			capacity = 1
		}
		meta := agentdevice.Meta{}
		if a.deviceManager != nil {
			meta = a.deviceManager.Meta(serial)
		}
		assignment := &deviceAssignment{
			serial:   serial,
			meta:     meta,
			tasks:    make([]*Task, 0, capacity),
			capacity: capacity,
		}
		if list := targeted[serial]; len(list) > 0 {
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
		if len(assignment.tasks) == 0 || strings.TrimSpace(assignment.serial) == "" {
			continue
		}
		serial := assignment.serial
		if err := a.taskManager.OnTasksDispatched(ctx, serial, assignment.tasks); err != nil {
			log.Error().Err(err).Str("serial", serial).Msg("task dispatch hook failed")
			continue
		}
		now := time.Now()
		pending := extractTaskIDs(assignment.tasks)
		if err := a.recorder.UpsertDevices(ctx, []DeviceInfoUpdate{{
			DeviceSerial: serial,
			Status:       string(statusRunning),
			OSType:       assignment.meta.OSType,
			OSVersion:    assignment.meta.OSVersion,
			IsRoot:       assignment.meta.IsRoot,
			ProviderUUID: assignment.meta.ProviderUUID,
			AgentVersion: a.cfg.AgentVersion,
			LastSeenAt:   now,
			RunningTask:  "",
			PendingTasks: pending,
		}}); err != nil {
			log.Error().Err(err).Str("serial", serial).Msg("device recorder update running state failed")
		}
		log.Info().
			Str("serial", serial).
			Int("assigned_tasks", len(assignment.tasks)).
			Msg("device pool dispatched tasks to device")
		a.startDeviceJob(ctx, serial, assignment.tasks)
	}
	return nil
}

func (a *DevicePoolAgent) idleDevices() []string {
	if a.deviceManager == nil {
		return nil
	}
	return a.deviceManager.IdleDevices()
}

func (a *DevicePoolAgent) snapshotJobTasks(serial string) (string, []string) {
	a.jobsMu.RLock()
	job := a.jobs[serial]
	a.jobsMu.RUnlock()
	if job == nil {
		return "", nil
	}
	running := firstTaskID(job.tasks)
	pending := remainingTaskIDs(job.tasks)
	return running, pending
}

func (a *DevicePoolAgent) startDeviceJob(ctx context.Context, serial string, tasks []*Task) {
	jobCtx, cancel := context.WithCancel(ctx)
	job := &deviceJob{
		deviceSerial: serial,
		tasks:        tasks,
		cancel:       cancel,
		startAt:      time.Now(),
	}
	job.lifecycle = a.buildTaskLifecycle(jobCtx, serial, job)

	a.jobsMu.Lock()
	a.jobs[serial] = job
	a.jobsMu.Unlock()

	if a.deviceManager != nil {
		a.deviceManager.MarkRunning(serial)
	}

	a.backgroundGroup.Add(1)
	go a.runDeviceJob(jobCtx, serial, job)
}

func (a *DevicePoolAgent) buildTaskLifecycle(ctx context.Context, serial string, job *deviceJob) *TaskLifecycle {
	if ctx == nil {
		ctx = context.Background()
	}
	return &TaskLifecycle{
		OnTaskStarted: func(task *Task) {
			a.handleTaskStarted(ctx, serial, job, task)
		},
		OnTaskResult: func(task *Task, err error) {
			if task == nil {
				return
			}
			if err != nil {
				task.ResultStatus = feishu.StatusFailed
			} else {
				task.ResultStatus = feishu.StatusSuccess
			}
			a.handleTaskResultEvent(ctx, job, task, err)
		},
	}
}

func (a *DevicePoolAgent) runDeviceJob(ctx context.Context, serial string, job *deviceJob) {
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
			Lifecycle:    job.lifecycle,
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

	remove := false
	if a.deviceManager != nil {
		remove = a.deviceManager.MarkIdle(serial)
	}
	a.jobsMu.Lock()
	delete(a.jobs, serial)
	a.jobsMu.Unlock()

	if err != nil {
		log.Error().Err(err).Str("serial", job.deviceSerial).Msg("device job failed")
	} else {
		log.Info().Str("serial", job.deviceSerial).Msg("device job finished")
	}

	finalStatus := string(statusIdle)
	if remove {
		finalStatus = "offline"
	}
	meta := agentdevice.Meta{}
	if a.deviceManager != nil {
		meta = a.deviceManager.Meta(serial)
	}
	if recErr := a.recorder.UpsertDevices(context.Background(), []DeviceInfoUpdate{{
		DeviceSerial: job.deviceSerial,
		Status:       finalStatus,
		OSType:       meta.OSType,
		OSVersion:    meta.OSVersion,
		IsRoot:       meta.IsRoot,
		ProviderUUID: meta.ProviderUUID,
		AgentVersion: a.cfg.AgentVersion,
		LastSeenAt:   time.Now(),
		LastError:    errString(err),
		RunningTask:  "",
		PendingTasks: nil,
	}}); recErr != nil {
		log.Error().Err(recErr).Str("serial", job.deviceSerial).Msg("device recorder finalize state failed")
	}

	if remove {
		a.removeDevice(job.deviceSerial)
	}
}

func (a *DevicePoolAgent) handleTaskStarted(ctx context.Context, serial string, job *deviceJob, task *Task) {
	if ctx == nil {
		ctx = context.Background()
	}
	if job == nil || task == nil {
		return
	}
	now := time.Now()

	meta := agentdevice.Meta{}
	if a.deviceManager != nil {
		meta = a.deviceManager.Meta(serial)
	}
	running := firstTaskID(job.tasks)
	if running == "" && strings.TrimSpace(task.ID) != "" {
		running = strings.TrimSpace(task.ID)
	}
	pending := remainingTaskIDs(job.tasks)

	if a.recorder != nil {
		if err := a.recorder.UpsertDevices(ctx, []DeviceInfoUpdate{{
			DeviceSerial: serial,
			Status:       string(statusRunning),
			OSType:       meta.OSType,
			OSVersion:    meta.OSVersion,
			IsRoot:       meta.IsRoot,
			ProviderUUID: meta.ProviderUUID,
			AgentVersion: a.cfg.AgentVersion,
			LastSeenAt:   now,
			RunningTask:  running,
			PendingTasks: pending,
		}}); err != nil {
			log.Error().
				Err(err).
				Str("serial", serial).
				Str("running_task", running).
				Strs("pending_tasks", pending).
				Msg("device recorder update running task failed")
		}
	}
	if notifier, ok := a.taskManager.(TaskStartNotifier); ok && notifier != nil {
		if err := notifier.OnTaskStarted(ctx, serial, task); err != nil {
			log.Warn().
				Err(err).
				Str("serial", serial).
				Str("task_id", task.ID).
				Msg("task manager handle task started failed")
		}
	}
}

func (a *DevicePoolAgent) handleTaskResultEvent(ctx context.Context, job *deviceJob, task *Task, runErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if job == nil || task == nil {
		return
	}
	if notifier, ok := a.taskManager.(TaskResultNotifier); ok && notifier != nil {
		if err := notifier.OnTaskResult(ctx, job.deviceSerial, task, runErr); err != nil {
			log.Warn().
				Err(err).
				Str("serial", job.deviceSerial).
				Str("task_id", task.ID).
				Msg("task manager handle task result failed")
		}
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
		if t == nil || isTaskFinished(t) {
			continue
		}
		if id := strings.TrimSpace(t.ID); id != "" {
			return id
		}
	}
	return ""
}

func remainingTaskIDs(tasks []*Task) []string {
	if len(tasks) <= 1 {
		return nil
	}
	result := make([]string, 0, len(tasks)-1)
	skipFirst := true
	for _, t := range tasks {
		if t == nil || isTaskFinished(t) {
			continue
		}
		if id := strings.TrimSpace(t.ID); id != "" {
			if skipFirst {
				skipFirst = false
				continue
			}
			result = append(result, id)
		}
	}
	return result
}

// isTaskFinished reports whether the task already has a terminal result.
func isTaskFinished(t *Task) bool {
	if t == nil {
		return false
	}
	switch strings.TrimSpace(t.ResultStatus) {
	case feishu.StatusSuccess, feishu.StatusFailed:
		return true
	default:
		return false
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (a *DevicePoolAgent) removeDevice(serial string) {
	if a.deviceManager != nil {
		a.deviceManager.Remove(serial)
	}
	a.jobsMu.Lock()
	delete(a.jobs, serial)
	a.jobsMu.Unlock()
}
