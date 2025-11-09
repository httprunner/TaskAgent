package pool

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	adbprovider "github.com/httprunner/TaskAgent/providers/adb"
	gadb "github.com/httprunner/httprunner/v5/pkg/gadb"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// Task represents a single unit of work to be executed on a device.
type Task struct {
	ID      string
	Payload any
}

// DeviceProvider returns the set of currently connected device serials.
type DeviceProvider interface {
	ListDevices(ctx context.Context) ([]string, error)
}

// TaskManager owns task lifecycle hooks (fetch/dispatch/complete).
type TaskManager interface {
	FetchAvailableTasks(ctx context.Context, maxTasks int) ([]*Task, error)
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
	PollInterval   time.Duration
	MaxTasksPerJob int
	OSType         string
	Provider       DeviceProvider
	TaskManager    TaskManager
	FeishuConfig   *FeishuTaskConfig
}

// DevicePoolAgent coordinates plug-and-play devices with a task source.
type DevicePoolAgent struct {
	cfg             Config
	deviceProvider  DeviceProvider
	taskManager     TaskManager
	jobRunner       JobRunner
	deviceMu        sync.Mutex
	devices         map[string]*deviceState
	backgroundGroup sync.WaitGroup
}

type deviceStatus string

const (
	statusIdle       deviceStatus = "idle"
	statusCollecting deviceStatus = "collecting"
)

type deviceState struct {
	serial         string
	status         deviceStatus
	lastSeen       time.Time
	removeAfterJob bool
	currentJob     *deviceJob
}

type deviceJob struct {
	deviceSerial string
	tasks        []*Task
	cancel       context.CancelFunc
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
		manager, err = defaultTaskManager(cfg)
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
		jobRunner:      runner,
		devices:        make(map[string]*deviceState),
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

func defaultTaskManager(cfg Config) (TaskManager, error) {
	if cfg.FeishuConfig != nil {
		return newFeishuTaskManager(cfg.FeishuConfig)
	}
	return nil, nil
}

// Start begins the polling loop until the context is cancelled.
func (a *DevicePoolAgent) Start(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context cannot be nil")
	}
	if err := a.runCycle(ctx); err != nil {
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
			if err := a.runCycle(ctx); err != nil {
				log.Error().Err(err).Msg("device pool cycle failed")
			}
		}
	}
}

// RunOnce exposes a single refresh/dispatch iteration (primarily for tests).
func (a *DevicePoolAgent) RunOnce(ctx context.Context) error {
	return a.runCycle(ctx)
}

func (a *DevicePoolAgent) runCycle(ctx context.Context) error {
	if err := a.refreshDevices(ctx); err != nil {
		return err
	}
	return a.dispatch(ctx)
}

func (a *DevicePoolAgent) refreshDevices(ctx context.Context) error {
	serials, err := a.deviceProvider.ListDevices(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	seen := make(map[string]struct{}, len(serials))

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
			continue
		}
		a.devices[serial] = &deviceState{
			serial:   serial,
			status:   statusIdle,
			lastSeen: now,
		}
		log.Info().Str("serial", serial).Msg("device connected")
	}

	for serial, dev := range a.devices {
		if _, ok := seen[serial]; ok {
			continue
		}
		if dev.status == statusCollecting {
			dev.removeAfterJob = true
			log.Warn().Str("serial", serial).Msg("device disconnected during job, will remove after completion")
			continue
		}
		delete(a.devices, serial)
		log.Info().Str("serial", serial).Msg("device disconnected")
	}
	return nil
}

func (a *DevicePoolAgent) dispatch(ctx context.Context) error {
	idle := a.idleDevices()
	if len(idle) == 0 {
		return nil
	}
	maxTasks := len(idle) * a.cfg.MaxTasksPerJob
	tasks, err := a.taskManager.FetchAvailableTasks(ctx, maxTasks)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return nil
	}

	offset := 0
	for _, dev := range idle {
		if offset >= len(tasks) {
			break
		}
		assign := a.cfg.MaxTasksPerJob
		if remaining := len(tasks) - offset; remaining < assign {
			assign = remaining
		}
		selected := make([]*Task, assign)
		copy(selected, tasks[offset:offset+assign])
		offset += assign
		if len(selected) == 0 {
			continue
		}
		if err := a.taskManager.OnTasksDispatched(ctx, dev.serial, selected); err != nil {
			log.Error().Err(err).Str("serial", dev.serial).Msg("task dispatch hook failed")
			continue
		}
		a.startDeviceJob(ctx, dev, selected)
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

func (a *DevicePoolAgent) startDeviceJob(ctx context.Context, dev *deviceState, tasks []*Task) {
	jobCtx, cancel := context.WithCancel(ctx)
	job := &deviceJob{
		deviceSerial: dev.serial,
		tasks:        tasks,
		cancel:       cancel,
	}

	a.deviceMu.Lock()
	dev.status = statusCollecting
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

	err := a.jobRunner.RunJob(ctx, JobRequest{
		DeviceSerial: job.deviceSerial,
		Tasks:        job.tasks,
	})

	if hookErr := a.taskManager.OnTasksCompleted(context.Background(), job.deviceSerial, job.tasks, err); hookErr != nil {
		log.Error().Err(hookErr).Str("serial", job.deviceSerial).Msg("task completion hook failed")
	}

	a.deviceMu.Lock()
	dev.status = statusIdle
	dev.currentJob = nil
	remove := dev.removeAfterJob
	a.deviceMu.Unlock()

	if err != nil {
		log.Error().Err(err).Str("serial", job.deviceSerial).Msg("device job failed")
	} else {
		log.Info().Str("serial", job.deviceSerial).Msg("device job finished")
	}

	if remove {
		a.removeDevice(job.deviceSerial)
	}
}

func (a *DevicePoolAgent) removeDevice(serial string) {
	a.deviceMu.Lock()
	defer a.deviceMu.Unlock()
	delete(a.devices, serial)
	log.Info().Str("serial", serial).Msg("device removed from pool")
}
