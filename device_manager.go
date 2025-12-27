package taskagent

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

const offlineThreshold = 5 * time.Minute

// deviceManager 负责维护设备状态并与 recorder 同步。
type deviceManager struct {
	provider     DeviceProvider
	recorder     DeviceRecorder
	agentVersion string
	hostUUID     string
	allowlist    map[string]struct{}

	mu      sync.Mutex
	devices map[string]*deviceState
}

type deviceState struct {
	serial         string
	status         deviceStatus
	lastSeen       time.Time
	removeAfterJob bool
	meta           deviceMeta
	metaReady      bool
}

func newDeviceManager(provider DeviceProvider, recorder DeviceRecorder, agentVersion, hostUUID string, allowlist map[string]struct{}) *deviceManager {
	return &deviceManager{
		provider:     provider,
		recorder:     recorder,
		agentVersion: agentVersion,
		hostUUID:     hostUUID,
		allowlist:    cloneStringSet(allowlist),
		devices:      make(map[string]*deviceState),
	}
}

func (m *deviceManager) isAllowed(serial string) bool {
	if m == nil || len(m.allowlist) == 0 {
		return true
	}
	_, ok := m.allowlist[strings.TrimSpace(serial)]
	return ok
}

// Refresh 刷新设备列表并同步 recorder。
func (m *deviceManager) Refresh(ctx context.Context, fetchMeta func(serial string) deviceMeta, inspect func(serial string) (string, []string)) error {
	if m == nil || m.provider == nil {
		return errors.New("device manager: provider is nil")
	}
	serials, err := m.provider.ListDevices(ctx)
	if err != nil {
		return errors.Wrap(err, "list devices failed")
	}
	now := time.Now()
	seen := make(map[string]struct{}, len(serials))
	updates := make([]DeviceInfoUpdate, 0, len(serials))

	m.mu.Lock()
	if len(m.allowlist) > 0 {
		for serial, dev := range m.devices {
			if m.isAllowed(serial) {
				continue
			}
			if dev != nil && dev.status == deviceStatusRunning {
				dev.removeAfterJob = true
				continue
			}
			delete(m.devices, serial)
		}
	}
	for _, serial := range serials {
		serial = strings.TrimSpace(serial)
		if serial == "" {
			continue
		}
		if !m.isAllowed(serial) {
			continue
		}
		seen[serial] = struct{}{}
		dev, exists := m.devices[serial]
		if exists {
			dev.lastSeen = now
			if !dev.metaReady && fetchMeta != nil {
				dev.meta = fetchMeta(serial)
				dev.metaReady = true
			}
		} else {
			meta := deviceMeta{ProviderUUID: m.hostUUID}
			if fetchMeta != nil {
				meta = fetchMeta(serial)
			}
			if strings.TrimSpace(meta.ProviderUUID) == "" {
				meta.ProviderUUID = m.hostUUID
			}
			dev = &deviceState{
				serial:    serial,
				status:    deviceStatusIdle,
				lastSeen:  now,
				meta:      meta,
				metaReady: true,
			}
			m.devices[serial] = dev
			log.Info().Str("serial", serial).Msg("device connected")
		}
		running, pending := "", ([]string)(nil)
		reportStatus := string(dev.status)
		if inspect != nil {
			running, pending = inspect(serial)
			if strings.TrimSpace(running) != "" {
				reportStatus = string(deviceStatusRunning)
			} else if len(pending) > 0 {
				reportStatus = string(deviceStatusDispatched)
			}
		}
		updates = append(updates, DeviceInfoUpdate{
			DeviceSerial: serial,
			Status:       reportStatus,
			OSType:       dev.meta.OSType,
			OSVersion:    dev.meta.OSVersion,
			IsRoot:       dev.meta.IsRoot,
			ProviderUUID: dev.meta.ProviderUUID,
			AgentVersion: m.agentVersion,
			LastSeenAt:   now,
			RunningTask:  running,
			PendingTasks: pending,
		})
	}

	for serial, dev := range m.devices {
		if _, ok := seen[serial]; ok {
			continue
		}
		if dev.status == deviceStatusRunning {
			dev.removeAfterJob = true
			log.Warn().Str("serial", serial).Msg("device disconnected during job, will remove after completion")
			continue
		}
		if now.Sub(dev.lastSeen) < offlineThreshold {
			continue
		}
		if !dev.metaReady && fetchMeta != nil {
			dev.meta = fetchMeta(serial)
			dev.metaReady = true
		}
		delete(m.devices, serial)
		updates = append(updates, DeviceInfoUpdate{
			DeviceSerial: serial,
			Status:       "offline",
			OSType:       dev.meta.OSType,
			OSVersion:    dev.meta.OSVersion,
			IsRoot:       dev.meta.IsRoot,
			ProviderUUID: dev.meta.ProviderUUID,
			AgentVersion: m.agentVersion,
			LastSeenAt:   dev.lastSeen,
		})
		log.Info().Str("serial", serial).Msg("device disconnected")
	}
	m.mu.Unlock()

	if m.recorder != nil && len(updates) > 0 {
		if err := m.recorder.UpsertDevices(ctx, updates); err != nil {
			log.Error().Err(err).Msg("device recorder upsert failed")
		}
	}
	return nil
}

// IdleDevices 返回当前可调度的设备序列号。
func (m *deviceManager) IdleDevices() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, 0, len(m.devices))
	for serial, dev := range m.devices {
		if dev.status == deviceStatusIdle && !dev.removeAfterJob {
			if !m.isAllowed(serial) {
				continue
			}
			result = append(result, serial)
		}
	}
	return result
}

// OnlineDeviceCount returns how many devices are currently tracked as online.
// It reflects the latest Refresh() result, respects allowlist, and excludes
// devices scheduled for removal after job completion.
func (m *deviceManager) OnlineDeviceCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for serial, dev := range m.devices {
		if dev == nil || dev.removeAfterJob {
			continue
		}
		if !m.isAllowed(serial) {
			continue
		}
		count++
	}
	return count
}

// MarkDispatched sets the device to dispatched state.
func (m *deviceManager) MarkDispatched(serial string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isAllowed(serial) {
		return
	}
	if dev, ok := m.devices[serial]; ok {
		dev.status = deviceStatusDispatched
	}
}

// MarkRunning 将设备置为运行状态。
func (m *deviceManager) MarkRunning(serial string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isAllowed(serial) {
		return
	}
	if dev, ok := m.devices[serial]; ok {
		dev.status = deviceStatusRunning
	}
}

// MarkIdle 将设备置为空闲，并返回是否需要在作业完成后移除。
func (m *deviceManager) MarkIdle(serial string) (removeAfterJob bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isAllowed(serial) {
		return false
	}
	if dev, ok := m.devices[serial]; ok {
		dev.status = deviceStatusIdle
		remove := dev.removeAfterJob
		dev.removeAfterJob = false
		return remove
	}
	return false
}

// Remove 从管理器中删除设备。
func (m *deviceManager) Remove(serial string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.devices, serial)
	log.Info().Str("serial", serial).Msg("device removed from pool")
}

// Meta 返回设备的元信息。
func (m *deviceManager) Meta(serial string) deviceMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	if dev, ok := m.devices[serial]; ok {
		return dev.meta
	}
	return deviceMeta{}
}

// SetMeta 手动覆盖设备元信息。
func (m *deviceManager) SetMeta(serial string, meta deviceMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if dev, ok := m.devices[serial]; ok {
		dev.meta = meta
		dev.metaReady = true
	}
}

var onlineDeviceCount int64

// SetOnlineDeviceCount stores the latest online device count for the current process.
// It is updated by DevicePoolAgent refresh cycles and can be consumed by other workers
// (for example, to size fetch limits) without introducing tight coupling.
func SetOnlineDeviceCount(count int) {
	if count < 0 {
		count = 0
	}
	atomic.StoreInt64(&onlineDeviceCount, int64(count))
}

// OnlineDeviceCount returns the latest online device count recorded in this process.
func OnlineDeviceCount() int {
	return int(atomic.LoadInt64(&onlineDeviceCount))
}
