package taskagent

import (
	"hash/fnv"
	"strings"

	"github.com/rs/zerolog/log"
)

// NormalizeShardConfig sanitizes shard inputs to safe defaults.
func NormalizeShardConfig(nodeIndex, nodeTotal int) (int, int) {
	if nodeTotal <= 0 {
		nodeTotal = 1
	}
	if nodeIndex < 0 || nodeIndex >= nodeTotal {
		nodeIndex = 0
	}
	return nodeIndex, nodeTotal
}

// FilterTasksByShard keeps tasks matching the shard and applies the limit.
// When nodeTotal <= 1, it simply applies the limit.
func FilterTasksByShard(tasks []*Task, nodeIndex, nodeTotal, limit int) []*Task {
	filtered := make([]*Task, 0, len(tasks))
	if nodeTotal <= 1 {
		if limit > 0 && len(tasks) > limit {
			return append(filtered, tasks[:limit]...)
		}
		return append(filtered, tasks...)
	}
	for _, task := range tasks {
		if task == nil {
			continue
		}
		ft, ok := task.Payload.(*FeishuTask)
		if !ok || ft == nil {
			log.Warn().
				Str("task_id", strings.TrimSpace(task.ID)).
				Msg("skip task with invalid feishusdk payload during sharding")
			continue
		}

		taskNumericID := resolveTaskShardID(task, ft, nodeTotal)
		if taskNumericID < 0 {
			filtered = append(filtered, task)
		} else {
			shard := int(taskNumericID % int64(nodeTotal))
			if shard == nodeIndex {
				filtered = append(filtered, task)
			}
		}

		if limit > 0 && len(filtered) >= limit {
			break
		}
	}

	return filtered
}

func resolveTaskShardID(task *Task, ft *FeishuTask, nodeTotal int) int64 {
	if nodeTotal > 1 && ft != nil {
		bookID := strings.TrimSpace(ft.BookID)
		app := strings.TrimSpace(ft.App)
		if bookID != "" && app != "" {
			key := bookID + "|" + app
			hasher := fnv.New32a()
			_, _ = hasher.Write([]byte(key))
			return int64(hasher.Sum32())
		}
	}
	return resolveTaskNumericID(task, ft)
}

func resolveTaskNumericID(task *Task, ft *FeishuTask) int64 {
	if ft != nil && ft.TaskID > 0 {
		return ft.TaskID
	}
	if task != nil {
		id := strings.TrimSpace(task.ID)
		if id != "" {
			h := fnv.New32a()
			_, _ = h.Write([]byte(id))
			return int64(h.Sum32())
		}
	}
	return -1
}
