package taskagent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type stubGroupBaseTM struct {
	tasks []*Task
}

func (s *stubGroupBaseTM) FetchAvailableTasks(_ context.Context, _ string, limit int) ([]*Task, error) {
	if limit <= 0 {
		return nil, nil
	}
	if len(s.tasks) <= limit {
		return append([]*Task(nil), s.tasks...), nil
	}
	return append([]*Task(nil), s.tasks[:limit]...), nil
}

func (s *stubGroupBaseTM) OnTasksDispatched(context.Context, string, []*Task) error { return nil }
func (s *stubGroupBaseTM) OnTaskStarted(context.Context, string, *Task) error       { return nil }
func (s *stubGroupBaseTM) OnTaskResult(context.Context, string, *Task, error) error { return nil }
func (s *stubGroupBaseTM) OnTasksCompleted(context.Context, string, []*Task, error) error {
	return nil
}

type stubGroupCounter struct {
	remainingByKey map[string]int
	errByKey       map[string]error
	calls          map[string]int
}

func (c *stubGroupCounter) CountRemaining(_ context.Context, _ string, key GroupKey) (int, error) {
	if c.calls == nil {
		c.calls = make(map[string]int)
	}
	k := key.String()
	c.calls[k]++
	if err := c.errByKey[k]; err != nil {
		return 0, err
	}
	return c.remainingByKey[k], nil
}

func makeFeishuTask(id string, scene, groupID, day string) *Task {
	dayTime, _ := time.ParseInLocation("2006-01-02", day, time.Local)
	ft := &FeishuTask{
		TaskID:   1,
		Scene:    scene,
		GroupID:  groupID,
		Datetime: &dayTime,
	}
	return &Task{ID: id, Payload: ft}
}

func TestGroupTaskPrioritizerOrdersByRemaining(t *testing.T) {
	base := &stubGroupBaseTM{
		tasks: []*Task{
			makeFeishuTask("A1", SceneGeneralSearch, "G-A", "2026-01-06"),
			makeFeishuTask("B1", SceneSingleURLCapture, "G-B", "2026-01-06"),
			makeFeishuTask("A2", SceneProfileSearch, "G-A", "2026-01-06"),
		},
	}
	counter := &stubGroupCounter{
		remainingByKey: map[string]int{
			(GroupKey{BizType: TaskBizTypePiracyGeneralSearch, GroupID: "G-A", Day: "2026-01-06"}).String(): 3,
			(GroupKey{BizType: TaskBizTypeSingleURLCapture, GroupID: "G-B", Day: "2026-01-06"}).String():    1,
		},
	}
	p := &GroupTaskPrioritizer{
		Base:    base,
		Counter: counter,
		Opts: GroupTaskPrioritizerOptions{
			Oversample:        4,
			CountTTL:          10 * time.Second,
			MaxGroupsPerFetch: 10,
		},
	}

	out, err := p.FetchAvailableTasks(context.Background(), "kwai", 2)
	if err != nil {
		t.Fatalf("FetchAvailableTasks error: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(out))
	}
	if strings.TrimSpace(out[0].ID) != "B1" {
		t.Fatalf("expected B1 first, got %q", out[0].ID)
	}
	if strings.TrimSpace(out[1].ID) != "A1" {
		t.Fatalf("expected A1 second, got %q", out[1].ID)
	}
}

func TestGroupTaskPrioritizerCacheTTL(t *testing.T) {
	now := time.Date(2026, 1, 6, 10, 0, 0, 0, time.Local)
	clock := func() time.Time { return now }

	base := &stubGroupBaseTM{
		tasks: []*Task{
			makeFeishuTask("A1", SceneGeneralSearch, "G-A", "2026-01-06"),
			makeFeishuTask("A2", SceneProfileSearch, "G-A", "2026-01-06"),
		},
	}
	key := (GroupKey{BizType: TaskBizTypePiracyGeneralSearch, GroupID: "G-A", Day: "2026-01-06"}).String()
	counter := &stubGroupCounter{
		remainingByKey: map[string]int{key: 2},
	}
	p := &GroupTaskPrioritizer{
		Base:    base,
		Counter: counter,
		Opts: GroupTaskPrioritizerOptions{
			Oversample:        2,
			CountTTL:          30 * time.Second,
			MaxGroupsPerFetch: 10,
			Clock:             clock,
		},
	}

	if _, err := p.FetchAvailableTasks(context.Background(), "kwai", 1); err != nil {
		t.Fatalf("fetch 1 error: %v", err)
	}
	if _, err := p.FetchAvailableTasks(context.Background(), "kwai", 1); err != nil {
		t.Fatalf("fetch 2 error: %v", err)
	}
	if counter.calls[key] != 1 {
		t.Fatalf("expected 1 counter call within TTL, got %d", counter.calls[key])
	}

	now = now.Add(31 * time.Second)
	if _, err := p.FetchAvailableTasks(context.Background(), "kwai", 1); err != nil {
		t.Fatalf("fetch 3 error: %v", err)
	}
	if counter.calls[key] != 2 {
		t.Fatalf("expected counter call after TTL, got %d", counter.calls[key])
	}
}

func TestGroupTaskPrioritizerOnTaskResultDecrementsCacheOnSuccess(t *testing.T) {
	base := &stubGroupBaseTM{
		tasks: []*Task{
			makeFeishuTask("A1", SceneGeneralSearch, "G-A", "2026-01-06"),
			makeFeishuTask("A2", SceneProfileSearch, "G-A", "2026-01-06"),
		},
	}
	key := (GroupKey{BizType: TaskBizTypePiracyGeneralSearch, GroupID: "G-A", Day: "2026-01-06"}).String()
	counter := &stubGroupCounter{
		remainingByKey: map[string]int{key: 2},
	}
	p := &GroupTaskPrioritizer{
		Base:    base,
		Counter: counter,
		Opts: GroupTaskPrioritizerOptions{
			Oversample:        2,
			CountTTL:          time.Minute,
			MaxGroupsPerFetch: 10,
		},
	}
	out, err := p.FetchAvailableTasks(context.Background(), "kwai", 1)
	if err != nil {
		t.Fatalf("fetch error: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 task, got %d", len(out))
	}
	if err := p.OnTaskResult(context.Background(), "D1", out[0], nil); err != nil {
		t.Fatalf("OnTaskResult error: %v", err)
	}

	p.mu.Lock()
	entry, ok := p.cache[key]
	p.mu.Unlock()
	if !ok {
		t.Fatalf("expected cache entry to remain after decrement")
	}
	if entry.remaining != 1 {
		t.Fatalf("expected remaining=1, got %d", entry.remaining)
	}

	if err := p.OnTaskResult(context.Background(), "D1", out[0], errors.New("failed")); err != nil {
		t.Fatalf("OnTaskResult error: %v", err)
	}
	p.mu.Lock()
	entry2 := p.cache[key]
	p.mu.Unlock()
	if entry2.remaining != 1 {
		t.Fatalf("expected remaining unchanged on failure, got %d", entry2.remaining)
	}
}
