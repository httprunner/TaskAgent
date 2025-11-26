package piracy

import (
	"context"
	"fmt"
	"strings"

	"github.com/httprunner/TaskAgent/pkg/feishu"
)

// FetchParamsFromTaskTable reads Params values from the task status bitable
// filtered by app + scene + status. It returns a de-duplicated slice of
// trimmed params sorted in insertion order (encounter order from Feishu).
func FetchParamsFromTaskTable(ctx context.Context, client *feishu.Client, tableURL, app, scene, status string) ([]string, error) {
	if strings.TrimSpace(tableURL) == "" {
		return nil, fmt.Errorf("task table url is required")
	}
	fields := feishu.DefaultTaskFields

	conds := []*feishu.Condition{}
	if strings.TrimSpace(app) != "" {
		conds = append(conds, feishu.NewCondition(fields.App, "is", strings.TrimSpace(app)))
	}
	if strings.TrimSpace(scene) != "" {
		conds = append(conds, feishu.NewCondition(fields.Scene, "is", strings.TrimSpace(scene)))
	}
	if strings.TrimSpace(status) != "" {
		conds = append(conds, feishu.NewCondition(fields.Status, "is", strings.TrimSpace(status)))
	}

	var filter *feishu.FilterInfo
	if len(conds) > 0 {
		filter = feishu.NewFilterInfo("and")
		filter.Conditions = conds
	}

	table, err := client.FetchTaskTableWithOptions(ctx, tableURL, nil, &feishu.TaskQueryOptions{Filter: filter})
	if err != nil {
		return nil, fmt.Errorf("fetch task table failed: %w", err)
	}

	seen := make(map[string]struct{})
	params := make([]string, 0, len(table.Rows))
	for _, row := range table.Rows {
		p := strings.TrimSpace(row.Params)
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		params = append(params, p)
	}
	return params, nil
}
