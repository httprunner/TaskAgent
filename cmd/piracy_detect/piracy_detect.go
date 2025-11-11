package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/httprunner/TaskAgent/feishu"
)

// PiracyOptions configures the detection run.
type PiracyOptions struct {
	ResultTableURL      string
	TargetTableURL      string
	ResultViewID        string
	TargetViewID        string
	ResultFilter        string
	TargetFilter        string
	ResultLimit         int
	TargetLimit         int
	ParamsField         string
	UserIDField         string
	DurationField       string
	TargetDurationField string
	ThresholdRatio      float64
}

// PiracyMatch describes a suspicious Params+UserID combination.
type PiracyMatch struct {
	Params         string
	UserID         string
	ItemDuration   float64
	TargetDuration float64
	Ratio          float64
	RecordCount    int
}

// PiracyReport captures the aggregated results.
type PiracyReport struct {
	Matches              []PiracyMatch
	TargetRows           int
	ResultRows           int
	ThresholdRatio       float64
	MissingTargetParams  []string
	ResultRowsWithNoTask int
}

// RunPiracyDetection compares result rows against target durations.
func RunPiracyDetection(ctx context.Context, opts PiracyOptions) (PiracyReport, error) {
	if strings.TrimSpace(opts.ResultTableURL) == "" {
		return PiracyReport{}, fmt.Errorf("result table url is required")
	}
	if strings.TrimSpace(opts.TargetTableURL) == "" {
		return PiracyReport{}, fmt.Errorf("target table url is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.ThresholdRatio <= 0 {
		opts.ThresholdRatio = 0.5
	}
	client, err := feishu.NewClientFromEnv()
	if err != nil {
		return PiracyReport{}, fmt.Errorf("create feishu client failed: %w", err)
	}
	paramsField := normalizeField(opts.ParamsField, feishu.DefaultResultFields.Params)
	userIDField := normalizeField(opts.UserIDField, feishu.DefaultResultFields.UserID)
	durationField := normalizeField(opts.DurationField, feishu.DefaultResultFields.ItemDuration)
	targetDurationField := normalizeField(opts.TargetDurationField, "TotalDuration")

	resultOpts := &feishu.TargetQueryOptions{}
	if v := strings.TrimSpace(opts.ResultViewID); v != "" {
		resultOpts.ViewID = v
	}
	if f := strings.TrimSpace(opts.ResultFilter); f != "" {
		resultOpts.Filter = f
	}
	if opts.ResultLimit > 0 {
		resultOpts.Limit = opts.ResultLimit
	}

	targetOpts := &feishu.TargetQueryOptions{}
	if v := strings.TrimSpace(opts.TargetViewID); v != "" {
		targetOpts.ViewID = v
	}
	if f := strings.TrimSpace(opts.TargetFilter); f != "" {
		targetOpts.Filter = f
	}
	if opts.TargetLimit > 0 {
		targetOpts.Limit = opts.TargetLimit
	}

	resultRows, err := client.FetchBitableRows(ctx, opts.ResultTableURL, resultOpts)
	if err != nil {
		return PiracyReport{}, fmt.Errorf("fetch result table failed: %w", err)
	}
	targetRows, err := client.FetchBitableRows(ctx, opts.TargetTableURL, targetOpts)
	if err != nil {
		return PiracyReport{}, fmt.Errorf("fetch target table failed: %w", err)
	}

	targetDurations := make(map[string]float64)
	targetNoDuration := make(map[string]struct{})
	for _, row := range targetRows {
		params := strings.TrimSpace(getStringField(row.Fields, paramsField))
		if params == "" {
			continue
		}
		dur, ok := getFloatField(row.Fields, targetDurationField)
		if !ok || dur <= 0 {
			targetNoDuration[params] = struct{}{}
			continue
		}
		if existing, found := targetDurations[params]; !found || dur > existing {
			targetDurations[params] = dur
		}
	}

	type key struct {
		params string
		user   string
	}

	userDurations := make(map[key]float64)
	userCounts := make(map[key]int)
	missingTargets := make(map[string]struct{})
	for _, row := range resultRows {
		params := strings.TrimSpace(getStringField(row.Fields, paramsField))
		if params == "" {
			continue
		}
		userID := strings.TrimSpace(getStringField(row.Fields, userIDField))
		if userID == "" {
			continue
		}
		dur, ok := getFloatField(row.Fields, durationField)
		if !ok || dur <= 0 {
			continue
		}
		k := key{params: params, user: userID}
		userDurations[k] += dur
		userCounts[k]++
		if _, exists := targetDurations[params]; !exists {
			missingTargets[params] = struct{}{}
		}
	}

	var matches []PiracyMatch
	for k, total := range userDurations {
		targetTotal, ok := targetDurations[k.params]
		if !ok || targetTotal <= 0 {
			continue
		}
		ratio := total / targetTotal
		if ratio > opts.ThresholdRatio {
			matches = append(matches, PiracyMatch{
				Params:         k.params,
				UserID:         k.user,
				ItemDuration:   total,
				TargetDuration: targetTotal,
				Ratio:          ratio,
				RecordCount:    userCounts[k],
			})
		}
	}

	sort.Slice(matches, func(i, j int) bool {
		if matches[i].Ratio == matches[j].Ratio {
			return matches[i].ItemDuration > matches[j].ItemDuration
		}
		return matches[i].Ratio > matches[j].Ratio
	})

	missingParamsSet := make(map[string]struct{}, len(missingTargets)+len(targetNoDuration))
	for params := range missingTargets {
		missingParamsSet[params] = struct{}{}
	}
	for params := range targetNoDuration {
		missingParamsSet[params] = struct{}{}
	}

	missingSlice := make([]string, 0, len(missingParamsSet))
	for params := range missingParamsSet {
		missingSlice = append(missingSlice, params)
	}
	sort.Strings(missingSlice)

	report := PiracyReport{
		Matches:             matches,
		TargetRows:          len(targetRows),
		ResultRows:          len(resultRows),
		ThresholdRatio:      opts.ThresholdRatio,
		MissingTargetParams: missingSlice,
	}
	report.ResultRowsWithNoTask = len(missingTargets)
	return report, nil
}

func normalizeField(userInput, fallback string) string {
	if trimmed := strings.TrimSpace(userInput); trimmed != "" {
		return trimmed
	}
	return fallback
}

func getStringField(fields map[string]any, name string) string {
	if fields == nil || name == "" {
		return ""
	}
	if val, ok := fields[name]; ok {
		switch v := val.(type) {
		case string:
			return strings.TrimSpace(v)
		case fmt.Stringer:
			return strings.TrimSpace(v.String())
		case []byte:
			return strings.TrimSpace(string(v))
		default:
			return strings.TrimSpace(fmt.Sprintf("%v", v))
		}
	}
	return ""
}

func getFloatField(fields map[string]any, name string) (float64, bool) {
	if fields == nil || name == "" {
		return 0, false
	}
	val, ok := fields[name]
	if !ok {
		return 0, false
	}
	switch v := val.(type) {
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case json.Number:
		if f, err := v.Float64(); err == nil {
			return f, true
		}
	case string:
		if strings.TrimSpace(v) == "" {
			return 0, false
		}
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, true
		}
	case []byte:
		if len(v) == 0 {
			return 0, false
		}
		if f, err := strconv.ParseFloat(strings.TrimSpace(string(v)), 64); err == nil {
			return f, true
		}
	}
	return 0, false
}
