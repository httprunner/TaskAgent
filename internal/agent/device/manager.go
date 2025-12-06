package device

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

const offlineThreshold = 5 * time.Minute

// Status 描述设备在调度中的状态。
type Status string

const (
	StatusIdle    Status = "idle"
	StatusRunning Status = "running"
)

// Meta 保存设备的静态信息。
type Meta struct {
	OSType       string
	OSVersion    string
	IsRoot       string
	ProviderUUID string
}

// MetaFetcher 在需要时填充设备元信息。
type MetaFetcher func(serial string) Meta

// JobSnapshot 提供运行中任务的快照，便于 recorder 填充 Running/Pending 字段。
type JobSnapshot func(serial string) (runningTask string, pendingTasks []string)

// Manager 负责维护设备状态并与 recorder 同步。
type Manager struct {
	provider     Provider
	recorder     Recorder
	agentVersion string
	hostUUID     string

	mu      sync.Mutex
	devices map[string]*state
}

type state struct {
	serial         string
	status         Status
	lastSeen       time.Time
	removeAfterJob bool
	meta           Meta
	metaReady      bool
}

// NewManager 构建设备状态管理器。
func NewManager(provider Provider, recorder Recorder, agentVersion, hostUUID string) *Manager {
	return &Manager{
		provider:     provider,
		recorder:     recorder,
		agentVersion: agentVersion,
		hostUUID:     hostUUID,
		devices:      make(map[string]*state),
	}
}

// Refresh 刷新设备列表并同步 recorder。
func (m *Manager) Refresh(ctx context.Context, fetchMeta MetaFetcher, inspect JobSnapshot) error {
	if m == nil || m.provider == nil {
		return errors.New("device manager: provider is nil")
	}
	serials, err := m.provider.ListDevices(ctx)
	if err != nil {
		return errors.Wrap(err, "list devices failed")
	}
	now := time.Now()
	seen := make(map[string]struct{}, len(serials))
	updates := make([]InfoUpdate, 0, len(serials))

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
			meta := Meta{ProviderUUID: m.hostUUID}
			if fetchMeta != nil {
				meta = fetchMeta(serial)
			}
			if strings.TrimSpace(meta.ProviderUUID) == "" {
				meta.ProviderUUID = m.hostUUID
			}
			dev = &state{
				serial:    serial,
				status:    StatusIdle,
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
		updates = append(updates, InfoUpdate{
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
		if dev.status == StatusRunning {
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
		updates = append(updates, InfoUpdate{
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
func (m *Manager) IdleDevices() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, 0, len(m.devices))
	for serial, dev := range m.devices {
		if dev.status == StatusIdle && !dev.removeAfterJob {
			result = append(result, serial)
		}
	}
	return result
}

// MarkRunning 将设备置为运行状态。
func (m *Manager) MarkRunning(serial string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if dev, ok := m.devices[serial]; ok {
		dev.status = StatusRunning
	}
}

// MarkIdle 将设备置为空闲，并返回是否需要在作业完成后移除。
func (m *Manager) MarkIdle(serial string) (removeAfterJob bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if dev, ok := m.devices[serial]; ok {
		dev.status = StatusIdle
		remove := dev.removeAfterJob
		dev.removeAfterJob = false
		return remove
	}
	return false
}

// Remove 从管理器中删除设备。
func (m *Manager) Remove(serial string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.devices, serial)
	log.Info().Str("serial", serial).Msg("device removed from pool")
}

// Meta 返回设备的元信息。
func (m *Manager) Meta(serial string) Meta {
	m.mu.Lock()
	defer m.mu.Unlock()
	if dev, ok := m.devices[serial]; ok {
		return dev.meta
	}
	return Meta{}
}

// SetMeta 手动覆盖设备元信息。
func (m *Manager) SetMeta(serial string, meta Meta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if dev, ok := m.devices[serial]; ok {
		dev.meta = meta
		dev.metaReady = true
	}
}
