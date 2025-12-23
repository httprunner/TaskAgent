package webhook

import (
	"context"
	"strings"
	"time"

	taskagent "github.com/httprunner/TaskAgent"
	"github.com/pkg/errors"
)

// ShardedTaskManager wraps FeishuTaskClient and applies webhook prioritization.
type ShardedTaskManager struct {
	base     *taskagent.FeishuTaskClient
	priority *WebhookTaskPrioritizer
}

// ShardedTaskManagerOptions customizes the sharded task manager.
type ShardedTaskManagerOptions struct {
	FeishuClient *taskagent.FeishuTaskClient
	FeishuOpts   taskagent.FeishuTaskClientOptions

	NodeIndex int
	NodeTotal int

	WebhookTableURL    string
	PriorityBatchLimit int
	PriorityTTL        time.Duration
}

// NewShardedTaskManager creates a sharded task manager with defaults.
func NewShardedTaskManager(bitableURL string) (*ShardedTaskManager, error) {
	return NewShardedTaskManagerWithOptions(bitableURL, ShardedTaskManagerOptions{})
}

// NewShardedTaskManagerWithOptions creates a sharded task manager with custom options.
func NewShardedTaskManagerWithOptions(bitableURL string, opts ShardedTaskManagerOptions) (*ShardedTaskManager, error) {
	base := opts.FeishuClient
	feishuOpts := opts.FeishuOpts
	if base == nil {
		if feishuOpts.NodeTotal == 0 && feishuOpts.NodeIndex == 0 {
			feishuOpts.NodeIndex = opts.NodeIndex
			feishuOpts.NodeTotal = opts.NodeTotal
		}
		var err error
		base, err = taskagent.NewFeishuTaskClientWithOptions(bitableURL, feishuOpts)
		if err != nil {
			return nil, err
		}
	}

	nodeIndex := opts.NodeIndex
	nodeTotal := opts.NodeTotal
	if nodeIndex == 0 && nodeTotal == 0 {
		nodeIndex = feishuOpts.NodeIndex
		nodeTotal = feishuOpts.NodeTotal
	}
	if base != nil {
		if idx, total, enabled := base.ShardConfig(); enabled {
			nodeIndex = idx
			nodeTotal = total
		}
	}
	nodeIndex, nodeTotal = taskagent.NormalizeShardConfig(nodeIndex, nodeTotal)

	var priority *WebhookTaskPrioritizer
	if strings.TrimSpace(opts.WebhookTableURL) != "" {
		priority = NewWebhookTaskPrioritizerWithOptions(opts.WebhookTableURL, WebhookTaskPrioritizerOptions{
			BatchLimit: opts.PriorityBatchLimit,
			TTL:        opts.PriorityTTL,
			NodeIndex:  nodeIndex,
			NodeTotal:  nodeTotal,
			Fetcher:    base,
		})
	}

	return &ShardedTaskManager{
		base:     base,
		priority: priority,
	}, nil
}

func (m *ShardedTaskManager) FetchAvailableTasks(ctx context.Context, app string, limit int) ([]*taskagent.Task, error) {
	if m == nil || m.base == nil {
		return nil, errors.New("sharded task manager is not initialized")
	}
	app = strings.TrimSpace(app)
	if limit <= 0 {
		limit = 1
	}
	if m.priority != nil {
		return m.priority.FetchAvailableTasks(ctx, app, limit)
	}
	return m.base.FetchAvailableTasks(ctx, app, limit)
}

func (m *ShardedTaskManager) OnTasksDispatched(ctx context.Context, deviceSerial string, tasks []*taskagent.Task) error {
	if m == nil || m.base == nil {
		return errors.New("sharded task manager is not initialized")
	}
	return m.base.OnTasksDispatched(ctx, deviceSerial, tasks)
}

func (m *ShardedTaskManager) OnTasksCompleted(ctx context.Context, deviceSerial string, tasks []*taskagent.Task, jobErr error) error {
	if m == nil || m.base == nil {
		return errors.New("sharded task manager is not initialized")
	}
	return m.base.OnTasksCompleted(ctx, deviceSerial, tasks, jobErr)
}

// OnTaskStarted forwards lifecycle callbacks to the underlying FeishuTaskClient
// so that Logs and timestamps are updated as before.
func (m *ShardedTaskManager) OnTaskStarted(ctx context.Context, deviceSerial string, task *taskagent.Task) error {
	if m == nil || m.base == nil {
		return errors.New("sharded task manager is not initialized")
	}
	return m.base.OnTaskStarted(ctx, deviceSerial, task)
}

// OnTaskResult forwards lifecycle callbacks to the underlying FeishuTaskClient
// so that per-task status overrides behave consistently with non-sharded setups.
func (m *ShardedTaskManager) OnTaskResult(ctx context.Context, deviceSerial string, task *taskagent.Task, runErr error) error {
	if m == nil || m.base == nil {
		return errors.New("sharded task manager is not initialized")
	}
	return m.base.OnTaskResult(ctx, deviceSerial, task, runErr)
}
