package pool

import (
	"context"
	"errors"
	"fmt"
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

func (s *stubTaskManager) FetchAvailableTasks(ctx context.Context, max int) ([]*Task, error) {
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
	if err := agent.RunOnce(ctx); err != nil {
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
	if err := agent.RunOnce(ctx); err != nil {
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
		PollInterval:   time.Millisecond,
		MaxTasksPerJob: 1,
		Provider:       provider,
		TaskManager:    manager,
	}, runner)
	if err != nil {
		t.Fatalf("NewDevicePoolAgent returned error: %v", err)
	}
	if err := agent.RunOnce(ctx); err != nil {
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
