package taskagent

import (
	"context"
	"strings"
	"sync"
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

func newDeviceManager(provider DeviceProvider, recorder DeviceRecorder, agentVersion, hostUUID string) *deviceManager {
	return &deviceManager{
		provider:     provider,
		recorder:     recorder,
		agentVersion: agentVersion,
		hostUUID:     hostUUID,
		devices:      make(map[string]*deviceState),
	}
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
	for _, serial := range serials {
		serial = strings.TrimSpace(serial)
		if serial == "" {
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
		if inspect != nil {
			running, pending = inspect(serial)
		}
		updates = append(updates, DeviceInfoUpdate{
			DeviceSerial: serial,
			Status:       string(dev.status),
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
			result = append(result, serial)
		}
	}
	return result
}

// MarkRunning 将设备置为运行状态。
func (m *deviceManager) MarkRunning(serial string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if dev, ok := m.devices[serial]; ok {
		dev.status = deviceStatusRunning
	}
}

// MarkIdle 将设备置为空闲，并返回是否需要在作业完成后移除。
func (m *deviceManager) MarkIdle(serial string) (removeAfterJob bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
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
