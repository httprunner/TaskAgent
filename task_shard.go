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

		shardKey := ResolveShardIDForTaskRow(FeishuTaskRow{BookID: ft.BookID, App: ft.App})
		shard := int(shardKey % int64(nodeTotal))
		if shard == nodeIndex {
			filtered = append(filtered, task)
		}

		if limit > 0 && len(filtered) >= limit {
			break
		}
	}

	return filtered
}

// ResolveShardIDForTaskRow returns a stable shard key for webhook task rows.
// It uses BookID+App as the sole sharding key.
func ResolveShardIDForTaskRow(row FeishuTaskRow) int64 {
	bookID := strings.TrimSpace(row.BookID)
	app := strings.TrimSpace(row.App)
	key := bookID + "|" + app
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return int64(hasher.Sum32())
}
