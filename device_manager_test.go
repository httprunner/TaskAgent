package taskagent

import (
	"context"
	"sync"
	"testing"

	"github.com/httprunner/httprunner/v5/pkg/gadb"
)

type stubStateProvider struct {
	states map[string]string
}

func (p *stubStateProvider) ListDevices(ctx context.Context) ([]string, error) {
	result := make([]string, 0, len(p.states))
	for serial := range p.states {
		result = append(result, serial)
	}
	return result, nil
}

func (p *stubStateProvider) ListDevicesWithState(ctx context.Context) (map[string]string, error) {
	return p.states, nil
}

type stubRecorder struct {
	mu      sync.Mutex
	updates []DeviceInfoUpdate
}

func (r *stubRecorder) UpsertDevices(ctx context.Context, devices []DeviceInfoUpdate) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.updates = append(r.updates, devices...)
	return nil
}

func TestDeviceManagerRefreshSkipsOfflineDevices(t *testing.T) {
	provider := &stubStateProvider{
		states: map[string]string{
			"online-1":  string(gadb.StateOnline),
			"offline-1": string(gadb.StateOffline),
		},
	}
	recorder := &stubRecorder{}
	manager := newDeviceManager(provider, recorder, "v1", "host-1", nil)

	if err := manager.Refresh(context.Background(), nil, nil); err != nil {
		t.Fatalf("refresh failed: %v", err)
	}

	idle := manager.IdleDevices()
	if contains(idle, "offline-1") {
		t.Fatalf("offline device should not be idle: %v", idle)
	}
	if !contains(idle, "online-1") {
		t.Fatalf("online device should be idle: %v", idle)
	}

	foundOffline := false
	for _, update := range recorder.updates {
		if update.DeviceSerial == "offline-1" {
			foundOffline = true
			if update.Status != "offline" {
				t.Fatalf("offline device status mismatch: %s", update.Status)
			}
		}
	}
	if !foundOffline {
		t.Fatal("offline device update not recorded")
	}
}

func contains(values []string, target string) bool {
	for _, v := range values {
		if v == target {
			return true
		}
	}
	return false
}
