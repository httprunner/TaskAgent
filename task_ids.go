package taskagent

import (
	"context"
	"strconv"
	"strings"

	"github.com/httprunner/TaskAgent/internal/feishusdk"
	"github.com/pkg/errors"
)

// FetchTasksByIDs returns tasks matched by TaskIDs, filtered by app/date policy and status.
// Status filtering is limited to pending/failed to align with dispatch requirements.
func (c *FeishuTaskClient) FetchTasksByIDs(ctx context.Context, app string, taskIDs []int64, limit int) ([]*Task, error) {
	if c == nil {
		return nil, errors.New("feishusdk: task client is nil")
	}
	feishuTasks, err := fetchFeishuTasksByIDsWithDatePolicy(ctx, c.client, c.bitableURL, app, taskIDs, limit, c.datePolicy)
	if err != nil {
		return nil, err
	}
	if err := c.syncTaskMirror(feishuTasks); err != nil {
		return nil, err
	}
	filtered := make([]*FeishuTask, 0, len(feishuTasks))
	for _, task := range feishuTasks {
		if task == nil {
			continue
		}
		if len(c.allowedScenes) > 0 {
			if _, ok := c.allowedScenes[strings.TrimSpace(task.Scene)]; !ok {
				continue
			}
		}
		status := strings.TrimSpace(task.Status)
		if status != feishusdk.StatusPending && status != feishusdk.StatusFailed {
			continue
		}
		filtered = append(filtered, task)
	}
	result := make([]*Task, 0, len(filtered))
	for _, t := range filtered {
		result = append(result, &Task{
			ID:           strconv.FormatInt(t.TaskID, 10),
			Payload:      t,
			DeviceSerial: strings.TrimSpace(t.DeviceSerial),
		})
	}
	return result, nil
}

func fetchFeishuTasksByIDsWithDatePolicy(ctx context.Context, client TargetTableClient, bitableURL, app string, taskIDs []int64, limit int, policy TaskDatePolicy) ([]*FeishuTask, error) {
	preset := strings.TrimSpace(policy.Primary)
	if preset == "" {
		preset = TaskDateToday
	}
	tasks, err := fetchFeishuTasksByIDsWithDatePreset(ctx, client, bitableURL, app, taskIDs, limit, preset)
	if err != nil {
		return nil, err
	}
	if len(tasks) == 0 && len(policy.Fallback) > 0 {
		for _, fallback := range policy.Fallback {
			if strings.TrimSpace(fallback) == "" {
				continue
			}
			tasks, err = fetchFeishuTasksByIDsWithDatePreset(ctx, client, bitableURL, app, taskIDs, limit, fallback)
			if err != nil {
				return nil, err
			}
			if len(tasks) > 0 {
				break
			}
		}
	}
	return tasks, nil
}

func fetchFeishuTasksByIDsWithDatePreset(ctx context.Context, client TargetTableClient, bitableURL, app string, taskIDs []int64, limit int, datePreset string) ([]*FeishuTask, error) {
	if client == nil {
		return nil, errors.New("feishusdk: client is nil")
	}
	if strings.TrimSpace(bitableURL) == "" {
		return nil, errors.New("feishusdk: bitable url is empty")
	}
	if len(taskIDs) == 0 {
		return nil, nil
	}
	fields := feishusdk.DefaultTaskFields
	taskIDField := strings.TrimSpace(fields.TaskID)
	if taskIDField == "" {
		return nil, errors.New("feishusdk: task table TaskID field mapping is empty")
	}

	uniqueIDs := uniqueTaskIDs(taskIDs)
	if len(uniqueIDs) == 0 {
		return nil, nil
	}
	result := make([]*FeishuTask, 0, len(uniqueIDs))
	seen := make(map[int64]struct{}, len(uniqueIDs))

	baseSpecs := buildFeishuBaseConditionSpecs(fields, app, "", datePreset)
	baseConds := appendConditionsFromSpecs(nil, baseSpecs)
	var baseFilter *feishusdk.FilterInfo
	if len(baseConds) > 0 {
		baseFilter = feishusdk.NewFilterInfo("and")
		baseFilter.Conditions = append(baseFilter.Conditions, baseConds...)
	}

	const maxConditions = 50
	for start := 0; start < len(uniqueIDs); start += maxConditions {
		end := start + maxConditions
		if end > len(uniqueIDs) {
			end = len(uniqueIDs)
		}
		chunk := uniqueIDs[start:end]
		idFilter := feishusdk.NewFilterInfo("or")
		for _, id := range chunk {
			idFilter.Conditions = append(idFilter.Conditions, feishusdk.NewCondition(taskIDField, "is", strconv.FormatInt(id, 10)))
		}
		filter := idFilter
		if baseFilter != nil {
			filter = feishusdk.MergeFiltersAND(baseFilter, idFilter)
		}

		remaining := limit
		if remaining > 0 {
			remaining = limit - len(result)
			if remaining <= 0 {
				break
			}
		}
		batch, err := FetchFeishuTasksWithFilter(ctx, client, bitableURL, filter, remaining)
		if err != nil {
			return nil, err
		}
		result = AppendUniqueFeishuTasks(result, batch, limit, seen)
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	if len(result) > limit && limit > 0 {
		result = result[:limit]
	}
	return result, nil
}

func uniqueTaskIDs(taskIDs []int64) []int64 {
	seen := make(map[int64]struct{}, len(taskIDs))
	unique := make([]int64, 0, len(taskIDs))
	for _, id := range taskIDs {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	return unique
}
