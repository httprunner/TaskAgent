package taskagent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type stubDeviceProvider struct {
	devices []string
	err     error
}

func (s *stubDeviceProvider) ListDevices(ctx context.Context) ([]string, error) {
	if s.err != nil {
		return nil, s.err
	}
	out := make([]string, len(s.devices))
	copy(out, s.devices)
	return out, nil
}

type stubTaskManager struct {
	mu          sync.Mutex
	tasks       []*Task
	dispatched  int
	completions int
	doneCh      chan struct{}
	lastErr     error
}

func (s *stubTaskManager) FetchAvailableTasks(ctx context.Context, app string, max int) ([]*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.tasks) == 0 {
		return nil, nil
	}
	if len(s.tasks) > max {
		return s.tasks[:max], nil
	}
	return s.tasks, nil
}

func (s *stubTaskManager) OnTasksDispatched(ctx context.Context, device string, tasks []*Task) error {
	s.mu.Lock()
	s.dispatched += len(tasks)
	s.mu.Unlock()
	return nil
}

func (s *stubTaskManager) OnTaskStarted(ctx context.Context, device string, task *Task) error {
	return nil
}

func (s *stubTaskManager) OnTaskResult(ctx context.Context, device string, task *Task, runErr error) error {
	return nil
}

func (s *stubTaskManager) OnTasksCompleted(ctx context.Context, device string, tasks []*Task, jobErr error) error {
	s.mu.Lock()
	s.completions += len(tasks)
	s.lastErr = jobErr
	s.mu.Unlock()
	if s.doneCh != nil {
		close(s.doneCh)
	}
	return nil
}

type channelJobRunner struct {
	resultErr error
	ch        chan JobRequest
}

func (r *channelJobRunner) RunJob(ctx context.Context, req JobRequest) error {
	r.ch <- req
	return r.resultErr
}

type flakyJobRunner struct {
	failures       int
	attemptCounter int
	ch             chan JobRequest
}

func (r *flakyJobRunner) RunJob(ctx context.Context, req JobRequest) error {
	if r.ch != nil {
		r.ch <- req
	}
	r.attemptCounter++
	if r.attemptCounter <= r.failures {
		return errors.New("boom")
	}
	return nil
}

type stubDispatchPlanner struct {
	assignments []DispatchAssignment
}

func (s *stubDispatchPlanner) PlanDispatch(ctx context.Context, idleDevices []string, tasks []*Task) ([]DispatchAssignment, error) {
	return s.assignments, nil
}

func TestDevicePoolAgentDispatchesAcrossIdleDevices(t *testing.T) {
	ctx := context.Background()
	provider := &stubDeviceProvider{devices: []string{"device-A", "device-B", "device-C"}}
	manager := &stubTaskManager{
		tasks: make([]*Task, 15),
	}
	for i := range manager.tasks {
		manager.tasks[i] = &Task{ID: fmt.Sprintf("task-%d", i+1)}
	}
	jobCh := make(chan JobRequest, 3)
	runner := &channelJobRunner{ch: jobCh}

	agent, err := NewDevicePoolAgent(Config{
		PollInterval:   time.Millisecond,
		MaxTasksPerJob: 5,
		Provider:       provider,
		TaskManager:    manager,
	}, runner)
	if err != nil {
		t.Fatalf("NewDevicePoolAgent returned error: %v", err)
	}
	if err := agent.RunOnce(ctx, "com.smile.gifmaker"); err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	got := make(map[string]JobRequest)
	for i := 0; i < 3; i++ {
		select {
		case req := <-jobCh:
			got[req.DeviceSerial] = req
		case <-time.After(time.Second):
			t.Fatalf("expected job %d to be scheduled", i+1)
		}
	}

	if len(got) != 3 {
		t.Fatalf("expected jobs for 3 devices, got %d", len(got))
	}
	for serial, req := range got {
		if len(req.Tasks) != 5 {
			t.Fatalf("device %s expected 5 tasks, got %d", serial, len(req.Tasks))
		}
	}
}

func TestDevicePoolAgentAssignsTasks(t *testing.T) {
	ctx := context.Background()
	provider := &stubDeviceProvider{devices: []string{"device-1"}}
	manager := &stubTaskManager{
		tasks:  []*Task{{ID: "1", Payload: "payload"}},
		doneCh: make(chan struct{}),
	}
	jobCh := make(chan JobRequest, 1)
	runner := &channelJobRunner{ch: jobCh}

	agent, err := NewDevicePoolAgent(Config{
		PollInterval:   time.Millisecond,
		MaxTasksPerJob: 2,
		Provider:       provider,
		TaskManager:    manager,
	}, runner)
	if err != nil {
		t.Fatalf("NewDevicePoolAgent returned error: %v", err)
	}
	if err := agent.RunOnce(ctx, "com.smile.gifmaker"); err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	select {
	case req := <-jobCh:
		if req.DeviceSerial != "device-1" {
			t.Fatalf("expected device-1, got %s", req.DeviceSerial)
		}
		if len(req.Tasks) != 1 || req.Tasks[0].ID != "1" {
			t.Fatalf("unexpected tasks: %#v", req.Tasks)
		}
	case <-time.After(time.Second):
		t.Fatalf("job was not scheduled")
	}

	select {
	case <-manager.doneCh:
	case <-time.After(time.Second):
		t.Fatalf("completion hook not called")
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.dispatched != 1 || manager.completions != 1 {
		t.Fatalf("unexpected dispatcher stats: dispatched=%d completed=%d", manager.dispatched, manager.completions)
	}
	if manager.lastErr != nil {
		t.Fatalf("expected nil job error, got %v", manager.lastErr)
	}
}

func TestDevicePoolAgentRespectsTargetDeviceSerial(t *testing.T) {
	ctx := context.Background()
	provider := &stubDeviceProvider{devices: []string{"device-1", "device-2"}}
	manager := &stubTaskManager{
		tasks: []*Task{
			{ID: "only-device-1", DeviceSerial: "device-1"},
			{ID: "only-device-2", DeviceSerial: "device-2"},
			{ID: "offline-only", DeviceSerial: "device-3"},
			{ID: "general"},
		},
	}
	jobCh := make(chan JobRequest, len(provider.devices))
	runner := &channelJobRunner{ch: jobCh}

	agent, err := NewDevicePoolAgent(Config{
		PollInterval:   time.Millisecond,
		MaxTasksPerJob: 2,
		Provider:       provider,
		TaskManager:    manager,
	}, runner)
	if err != nil {
		t.Fatalf("NewDevicePoolAgent returned error: %v", err)
	}
	if err := agent.RunOnce(ctx, "com.smile.gifmaker"); err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	collected := make(map[string]JobRequest, len(provider.devices))
	for range provider.devices {
		select {
		case req := <-jobCh:
			collected[req.DeviceSerial] = req
		case <-time.After(time.Second):
			t.Fatalf("expected job for device")
		}
	}

	for serial, req := range collected {
		for _, task := range req.Tasks {
			if task == nil {
				continue
			}
			if target := strings.TrimSpace(task.DeviceSerial); target != "" && target != serial {
				t.Fatalf("task %s targeted device %s but dispatched to %s", task.ID, target, serial)
			}
			if task.ID == "offline-only" {
				t.Fatalf("offline-only task should not be dispatched, got %s", serial)
			}
		}
	}
}

func TestDevicePoolAgentUsesDispatchPlanner(t *testing.T) {
	ctx := context.Background()
	provider := &stubDeviceProvider{devices: []string{"device-1", "device-2"}}
	manager := &stubTaskManager{
		tasks: []*Task{
			{ID: "1"},
			{ID: "2"},
		},
	}
	jobCh := make(chan JobRequest, 2)
	runner := &channelJobRunner{ch: jobCh}

	planner := &stubDispatchPlanner{assignments: []DispatchAssignment{{
		DeviceSerial: "device-2",
		Tasks:        manager.tasks,
	}}}

	agent, err := NewDevicePoolAgent(Config{
		PollInterval:    time.Millisecond,
		MaxTasksPerJob:  10,
		Provider:        provider,
		TaskManager:     manager,
		DispatchPlanner: planner,
		MaxJobRetries:   1,
		JobRetryBackoff: time.Millisecond,
		MaxFetchPerPoll: 0,
	}, runner)
	if err != nil {
		t.Fatalf("NewDevicePoolAgent returned error: %v", err)
	}
	if err := agent.RunOnce(ctx, "com.smile.gifmaker"); err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	select {
	case req := <-jobCh:
		if req.DeviceSerial != "device-2" {
			t.Fatalf("expected planner job scheduled for device-2, got %s", req.DeviceSerial)
		}
		if len(req.Tasks) != 2 {
			t.Fatalf("expected 2 tasks, got %d", len(req.Tasks))
		}
	default:
		t.Fatalf("expected job scheduled")
	}

	select {
	case req := <-jobCh:
		t.Fatalf("expected only one job, got extra for %s", req.DeviceSerial)
	default:
	}
}

func TestDevicePoolAgentPropagatesJobError(t *testing.T) {
	ctx := context.Background()
	provider := &stubDeviceProvider{devices: []string{"device-2"}}
	manager := &stubTaskManager{
		tasks:  []*Task{{ID: "2"}},
		doneCh: make(chan struct{}),
	}
	jobCh := make(chan JobRequest, 1)
	runner := &channelJobRunner{ch: jobCh, resultErr: errors.New("boom")}

	agent, err := NewDevicePoolAgent(Config{
		PollInterval:    time.Millisecond,
		MaxTasksPerJob:  1,
		MaxJobRetries:   1,
		JobRetryBackoff: time.Millisecond,
		Provider:        provider,
		TaskManager:     manager,
	}, runner)
	if err != nil {
		t.Fatalf("NewDevicePoolAgent returned error: %v", err)
	}
	if err := agent.RunOnce(ctx, "com.smile.gifmaker"); err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}
	<-jobCh
	select {
	case <-manager.doneCh:
	case <-time.After(time.Second):
		t.Fatalf("completion hook not called for error case")
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.lastErr == nil {
		t.Fatalf("expected job error propagated")
	}
}

func TestDevicePoolAgentRetriesFailedJobs(t *testing.T) {
	ctx := context.Background()
	provider := &stubDeviceProvider{devices: []string{"device-retry"}}
	manager := &stubTaskManager{
		tasks:  []*Task{{ID: "retry-task"}},
		doneCh: make(chan struct{}),
	}
	jobCh := make(chan JobRequest, 2)
	runner := &flakyJobRunner{failures: 1, ch: jobCh}

	agent, err := NewDevicePoolAgent(Config{
		PollInterval:    time.Millisecond,
		MaxTasksPerJob:  1,
		MaxJobRetries:   2,
		JobRetryBackoff: time.Millisecond,
		Provider:        provider,
		TaskManager:     manager,
	}, runner)
	if err != nil {
		t.Fatalf("NewDevicePoolAgent returned error: %v", err)
	}
	if err := agent.RunOnce(ctx, "com.smile.gifmaker"); err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	select {
	case <-jobCh:
	case <-time.After(time.Second):
		t.Fatalf("expected job request to be sent")
	}

	select {
	case <-manager.doneCh:
	case <-time.After(time.Second):
		t.Fatalf("completion hook not called after retries")
	}

	if runner.attemptCounter != 2 {
		t.Fatalf("expected 2 attempts, got %d", runner.attemptCounter)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.lastErr != nil {
		t.Fatalf("expected final job success, got %v", manager.lastErr)
	}
}

func TestDevicePoolAgentStartRunsInitialCycleImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider := &stubDeviceProvider{devices: []string{"device-1"}}
	manager := &stubTaskManager{
		tasks: []*Task{{ID: "task-1"}},
	}
	jobCh := make(chan JobRequest, 1)
	runner := &channelJobRunner{ch: jobCh}

	agent, err := NewDevicePoolAgent(Config{
		PollInterval:    10 * time.Second,
		MaxTasksPerJob:  1,
		MaxJobRetries:   1,
		JobRetryBackoff: time.Millisecond,
		Provider:        provider,
		TaskManager:     manager,
	}, runner)
	if err != nil {
		t.Fatalf("NewDevicePoolAgent returned error: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- agent.Start(ctx, "com.smile.gifmaker")
	}()

	select {
	case <-jobCh:
		// Cancel after we observe the initial dispatch.
		cancel()
	case <-time.After(500 * time.Millisecond):
		cancel()
		t.Fatalf("expected Start() to dispatch before first poll tick")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected Start() to exit cleanly, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Start() did not exit after cancellation")
	}
}
