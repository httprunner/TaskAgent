package taskagent

import (
	"strings"
	"sync"
	"time"
)

type taskRateLimiter struct {
	mu      sync.Mutex
	limit   int
	window  time.Duration
	records map[string][]time.Time
}

func newTaskRateLimiter(limit int, window time.Duration) *taskRateLimiter {
	return &taskRateLimiter{
		limit:   limit,
		window:  window,
		records: make(map[string][]time.Time),
	}
}

func (r *taskRateLimiter) remaining(deviceSerial string, now time.Time) int {
	if r == nil {
		return 0
	}
	deviceSerial = strings.TrimSpace(deviceSerial)
	if deviceSerial == "" {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	list := r.pruneLocked(deviceSerial, now)
	if r.limit <= 0 {
		return 0
	}
	remaining := r.limit - len(list)
	if remaining < 0 {
		return 0
	}
	return remaining
}

func (r *taskRateLimiter) recordStart(deviceSerial string, now time.Time) int {
	if r == nil {
		return 0
	}
	deviceSerial = strings.TrimSpace(deviceSerial)
	if deviceSerial == "" {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	list := r.pruneLocked(deviceSerial, now)
	list = append(list, now)
	r.records[deviceSerial] = list
	return len(list)
}

func (r *taskRateLimiter) pruneLocked(deviceSerial string, now time.Time) []time.Time {
	list := r.records[deviceSerial]
	if len(list) == 0 {
		return nil
	}
	cutoff := now.Add(-r.window)
	idx := 0
	for idx < len(list) && list[idx].Before(cutoff) {
		idx++
	}
	if idx == 0 {
		return list
	}
	list = list[idx:]
	r.records[deviceSerial] = list
	return list
}

func applyRateLimitAssignments(assignments []DispatchAssignment, caps map[string]int) []DispatchAssignment {
	if len(assignments) == 0 || len(caps) == 0 {
		return assignments
	}
	out := make([]DispatchAssignment, 0, len(assignments))
	for _, assignment := range assignments {
		serial := strings.TrimSpace(assignment.DeviceSerial)
		if serial == "" {
			continue
		}
		capacity := caps[serial]
		if capacity <= 0 || len(assignment.Tasks) == 0 {
			continue
		}
		if len(assignment.Tasks) > capacity {
			assignment.Tasks = assignment.Tasks[:capacity]
		}
		out = append(out, assignment)
	}
	return out
}
