package taskagent

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/httprunner/TaskAgent/internal/feishusdk"
	adbprovider "github.com/httprunner/TaskAgent/internal/providers/adb"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

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
	DispatchPlanner DispatchPlanner
}

// DevicePoolAgent coordinates plug-and-play devices with a task source.
type DevicePoolAgent struct {
	cfg             Config
	deviceProvider  DeviceProvider
	taskManager     TaskManager
	recorder        DeviceRecorder
	jobRunner       JobRunner
	deviceManager   *deviceManager
	dispatchPlanner DispatchPlanner
	jobsMu          sync.RWMutex
	jobs            map[string]*deviceJob
	backgroundGroup sync.WaitGroup
	hostUUID        string
}

const (
	statusIdle       deviceStatus = deviceStatusIdle
	statusDispatched deviceStatus = deviceStatusDispatched
	statusRunning    deviceStatus = deviceStatusRunning
)

type deviceJob struct {
	deviceSerial string
	tasks        []*Task
	cancel       context.CancelFunc
	startAt      time.Time
	notifier     TaskNotifier
	runningTask  string
	started      bool
}

type noopRecorder struct{}

func (noopRecorder) UpsertDevices(ctx context.Context, devices []DeviceInfoUpdate) error { return nil }

type deviceAssignment struct {
	serial   string
	meta     deviceMeta
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
		manager, err = NewFeishuTaskClientWithOptions(
			cfg.BitableURL,
			FeishuTaskClientOptions{
				AllowedScenes: allowed,
			},
		)
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
		cfg:             cfg,
		deviceProvider:  provider,
		taskManager:     manager,
		recorder:        cfg.DeviceRecorder,
		jobRunner:       runner,
		jobs:            make(map[string]*deviceJob),
		hostUUID:        hostUUID,
		dispatchPlanner: cfg.DispatchPlanner,
	}
	if agent.recorder == nil {
		agent.recorder = noopRecorder{}
	}
	agent.deviceManager = newDeviceManager(agent.deviceProvider, agent.recorder, agent.cfg.AgentVersion, agent.hostUUID)
	return agent, nil
}

func defaultDeviceProvider(cfg Config) (DeviceProvider, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.OSType)) {
	case "", "android":
		provider, err := adbprovider.NewDefault()
		if err != nil {
			return nil, errors.Wrap(err, "init default adb provider for pool")
		}
		return provider, nil
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

	// Fast-start: run one cycle immediately instead of waiting for the first tick.
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
	if a.deviceManager == nil {
		return errors.New("device manager is not initialized")
	}
	return a.deviceManager.Refresh(ctx, a.fetchDeviceMeta, a.snapshotJobTasks)
}

func (a *DevicePoolAgent) dispatch(ctx context.Context, app string) error {
	dispatchStart := time.Now()
	dispatchedInCycle := make(map[string]struct{})

	round := 0
	for {
		round++
		idle := a.idleDevices()
		if len(idle) == 0 {
			event := log.Info()
			if round > 1 {
				event = log.Debug()
			}
			event.Int("dispatch_round", round).
				Msg("device pool dispatch finished: no idle devices")
			return nil
		}

		maxTasks := len(idle) * a.cfg.MaxTasksPerJob
		fetchCap := a.cfg.MaxFetchPerPoll
		if fetchCap > 0 && maxTasks > fetchCap {
			maxTasks = fetchCap
		}

		event := log.Info()
		if round > 1 {
			event = log.Debug()
		}
		event.
			Int("dispatch_round", round).
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
			event.
				Int("dispatch_round", round).
				Int("idle_devices", len(idle)).
				Int("max_fetch", maxTasks).
				Msg("device pool dispatch found no available tasks")
			return nil
		}

		tasks = filterUndispatchedTasks(tasks, dispatchedInCycle)
		if len(tasks) == 0 {
			log.Debug().
				Int("dispatch_round", round).
				Msg("device pool dispatch drained after cycle-local de-dup; stopping")
			return nil
		}

		dispatchedDevices, err := a.dispatchTasks(ctx, idle, tasks, dispatchedInCycle)
		if err != nil {
			return err
		}

		if dispatchedDevices == 0 {
			log.Debug().
				Int("dispatch_round", round).
				Msg("device pool dispatch made no progress; stopping")
			return nil
		}

		// Respect per-poll caps and avoid spending too long looping within a single cycle.
		if a.cfg.MaxFetchPerPoll > 0 {
			return nil
		}
		if time.Since(dispatchStart) > 10*time.Second {
			log.Debug().
				Dur("elapsed", time.Since(dispatchStart)).
				Int("dispatch_rounds", round).
				Msg("device pool dispatch reached time budget; stopping")
			return nil
		}
	}
}

func (a *DevicePoolAgent) dispatchTasks(ctx context.Context, idle []string, tasks []*Task, dispatched map[string]struct{}) (int, error) {
	if a == nil || a.taskManager == nil {
		return 0, nil
	}
	if len(idle) == 0 || len(tasks) == 0 {
		return 0, nil
	}
	if a.dispatchPlanner != nil {
		assignments, err := a.dispatchPlanner.PlanDispatch(ctx, idle, tasks)
		if err != nil {
			return 0, errors.Wrap(err, "dispatch planner failed")
		}
		dispatchedDevices := 0
		for _, assignment := range assignments {
			serial := strings.TrimSpace(assignment.DeviceSerial)
			if serial == "" || len(assignment.Tasks) == 0 {
				continue
			}
			meta := deviceMeta{}
			if a.deviceManager != nil {
				meta = a.deviceManager.Meta(serial)
			}
			ok := a.dispatchToDevice(ctx, serial, assignment.Tasks, meta, "device pool dispatched tasks to device (planner)", dispatched)
			if ok {
				dispatchedDevices++
			}
		}
		return dispatchedDevices, nil
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
		meta := deviceMeta{}
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
	dispatchedDevices := 0
	for _, assignment := range assignments {
		serial := strings.TrimSpace(assignment.serial)
		if serial == "" || len(assignment.tasks) == 0 {
			continue
		}
		ok := a.dispatchToDevice(ctx, serial, assignment.tasks, assignment.meta, "device pool dispatched tasks to device", dispatched)
		if ok {
			dispatchedDevices++
		}
	}
	return dispatchedDevices, nil
}

func (a *DevicePoolAgent) dispatchToDevice(ctx context.Context, serial string, tasks []*Task, meta deviceMeta, msg string, dispatched map[string]struct{}) bool {
	if a == nil || a.taskManager == nil || len(tasks) == 0 {
		return false
	}
	if err := a.taskManager.OnTasksDispatched(ctx, serial, tasks); err != nil {
		log.Error().Err(err).Str("serial", serial).Msg("task dispatch hook failed")
		return false
	}
	taskIDs := extractTaskIDs(tasks)
	if dispatched != nil {
		for _, id := range taskIDs {
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}
			dispatched[id] = struct{}{}
		}
	}
	now := time.Now()
	if err := a.recorder.UpsertDevices(ctx, []DeviceInfoUpdate{{
		DeviceSerial: serial,
		Status:       string(statusDispatched),
		OSType:       meta.OSType,
		OSVersion:    meta.OSVersion,
		IsRoot:       meta.IsRoot,
		ProviderUUID: meta.ProviderUUID,
		AgentVersion: a.cfg.AgentVersion,
		LastSeenAt:   now,
		RunningTask:  "",
		PendingTasks: taskIDs,
	}}); err != nil {
		log.Error().Err(err).Str("serial", serial).Msg("device recorder update dispatch state failed")
	}
	log.Info().
		Str("serial", serial).
		Int("assigned_tasks", len(tasks)).
		Msg(msg)
	a.startDeviceJob(ctx, serial, tasks)
	return true
}

func filterUndispatchedTasks(tasks []*Task, dispatched map[string]struct{}) []*Task {
	if len(tasks) == 0 {
		return tasks
	}
	if len(dispatched) == 0 {
		return tasks
	}
	filtered := tasks[:0]
	for _, task := range tasks {
		if task == nil {
			continue
		}
		id := strings.TrimSpace(task.ID)
		if id == "" {
			filtered = append(filtered, task)
			continue
		}
		if _, ok := dispatched[id]; ok {
			continue
		}
		filtered = append(filtered, task)
	}
	return filtered
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
	if job.started {
		running := strings.TrimSpace(job.runningTask)
		if running == "" {
			running = firstTaskID(job.tasks)
		}
		pending := remainingTaskIDs(job.tasks)
		return running, pending
	}
	return "", extractTaskIDs(job.tasks)
}

func (a *DevicePoolAgent) startDeviceJob(ctx context.Context, serial string, tasks []*Task) {
	jobCtx, cancel := context.WithCancel(ctx)
	job := &deviceJob{
		deviceSerial: serial,
		tasks:        tasks,
		cancel:       cancel,
		startAt:      time.Now(),
	}
	job.notifier = a.buildTaskNotifier(jobCtx, serial, job)

	a.jobsMu.Lock()
	a.jobs[serial] = job
	a.jobsMu.Unlock()

	if a.deviceManager != nil {
		a.deviceManager.MarkDispatched(serial)
	}

	a.backgroundGroup.Add(1)
	go a.runDeviceJob(jobCtx, serial, job)
}

func (a *DevicePoolAgent) buildTaskNotifier(ctx context.Context, serial string, job *deviceJob) TaskNotifier {
	if ctx == nil {
		ctx = context.Background()
	}
	return taskNotifier{
		ctx:    ctx,
		agent:  a,
		job:    job,
		serial: serial,
	}
}

type taskNotifier struct {
	ctx    context.Context
	agent  *DevicePoolAgent
	job    *deviceJob
	serial string
}

func (n taskNotifier) OnTaskStarted(ctx context.Context, deviceSerial string, task *Task) error {
	if n.agent == nil {
		return nil
	}
	if ctx == nil {
		ctx = n.ctx
	}
	n.agent.handleTaskStarted(ctx, n.serial, n.job, task)
	return nil
}

func (n taskNotifier) OnTaskResult(ctx context.Context, deviceSerial string, task *Task, runErr error) error {
	if n.agent == nil {
		return nil
	}
	if task == nil {
		return nil
	}
	if ctx == nil {
		ctx = n.ctx
	}
	if runErr != nil {
		task.ResultStatus = feishusdk.StatusFailed
	} else {
		task.ResultStatus = feishusdk.StatusSuccess
	}
	n.agent.handleTaskResultEvent(ctx, n.job, task, runErr)
	return nil
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
			Notifier:     job.notifier,
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
	meta := deviceMeta{}
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

	meta := deviceMeta{}
	if a.deviceManager != nil {
		meta = a.deviceManager.Meta(serial)
	}
	running := firstTaskID(job.tasks)
	if running == "" {
		running = strings.TrimSpace(task.ID)
	}
	if running == "" {
		if ft, ok := task.Payload.(*FeishuTask); ok && ft != nil && ft.TaskID > 0 {
			running = strconv.FormatInt(ft.TaskID, 10)
		}
	}
	shouldUpdateDevice := strings.TrimSpace(running) != ""
	if !shouldUpdateDevice {
		log.Warn().Str("serial", serial).Msg("task started without valid task id; keep dispatched status")
	} else {
		job.started = true
		job.runningTask = running
		if a.deviceManager != nil {
			a.deviceManager.MarkRunning(serial)
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
	}
	if a.taskManager != nil {
		if err := a.taskManager.OnTaskStarted(ctx, serial, task); err != nil {
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
	if a.taskManager != nil {
		if err := a.taskManager.OnTaskResult(ctx, job.deviceSerial, task, runErr); err != nil {
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
		case feishusdk.StatusSuccess:
			success = append(success, task)
		case feishusdk.StatusFailed:
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
	case feishusdk.StatusSuccess, feishusdk.StatusFailed:
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

// fetchDeviceMeta collects OS version and root status via adb where possible.
func (a *DevicePoolAgent) fetchDeviceMeta(serial string) deviceMeta {
	type shellRunner interface {
		RunShell(serial string, args ...string) (string, error)
	}

	meta := deviceMeta{ProviderUUID: a.hostUUID}
	meta.OSType = strings.TrimSpace(a.cfg.OSType)
	if meta.OSType == "" {
		meta.OSType = "android"
	}

	provider, ok := a.deviceProvider.(shellRunner)
	if !ok || provider == nil {
		return meta
	}

	if output, err := provider.RunShell(serial, "getprop", "ro.build.version.release"); err == nil {
		meta.OSVersion = strings.TrimSpace(output)
	}

	if output, err := provider.RunShell(serial, "su", "-c", "id"); err == nil {
		if strings.Contains(output, "uid=0") {
			meta.IsRoot = "true"
			return meta
		}
	}
	if output, err := provider.RunShell(serial, "which", "su"); err == nil {
		if strings.TrimSpace(output) != "" {
			meta.IsRoot = "true"
			return meta
		}
	}
	meta.IsRoot = "false"
	return meta
}

// getHostUUID returns a best-effort hardware UUID for the host to populate ProviderUUID.
// On macOS it uses `system_profiler`; on Linux it prefers /etc/machine-id then falls back to /sys/class/dmi/id/product_uuid.
func getHostUUID() (string, error) {
	switch runtime.GOOS {
	case "darwin":
		cmd := exec.CommandContext(context.Background(), "bash", "-c", "system_profiler SPHardwareDataType | awk '/Hardware UUID/ {print $3}'")
		out, err := cmd.Output()
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(out)), nil
	case "linux":
		if id, err := readSystemFile("/etc/machine-id"); err == nil && id != "" {
			return id, nil
		}
		if id, err := readSystemFile("/sys/class/dmi/id/product_uuid"); err == nil && id != "" {
			return id, nil
		}
		return "", nil
	default:
		return "", nil
	}
}

func readSystemFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}
