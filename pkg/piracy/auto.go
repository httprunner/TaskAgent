package piracy

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/httprunner/TaskAgent/feishu"
	"github.com/rs/zerolog/log"
)

// AutoOptions defines the configuration for the auto command pipeline.
type AutoOptions struct {
	App          string
	OutputCSV    string
	ResultFilter string
	DramaFilter  string
	Concurrency  int
}

// AutoSummary captures the aggregated outcome of an auto run.
type AutoSummary struct {
	CSVPath     string
	MatchCount  int
	DramaCount  int
	Matches     []Match
	Dramas      []Drama
	Threshold   float64
	Concurrency int
}

// RunAuto executes the end-to-end piracy detection and reporting workflow.
func RunAuto(ctx context.Context, opts AutoOptions) (*AutoSummary, error) {
	if strings.TrimSpace(opts.App) == "" {
		return nil, errors.New("app is required")
	}

	reporter := NewReporter()
	if !reporter.IsConfigured() {
		return nil, errors.New("reporter not configured. Please set RESULT_BITABLE_URL, DRAMA_BITABLE_URL, TARGET_BITABLE_URL")
	}

	cfg := reporter.Config()

	client, err := feishu.NewClientFromEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to create feishu client: %w", err)
	}

	dramaRows, err := fetchRows(ctx, client, TableConfig{
		URL:    reporter.DramaTableURL(),
		Filter: opts.DramaFilter,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch drama rows: %w", err)
	}

	dramas := extractDramas(dramaRows, cfg)
	if len(dramas) == 0 {
		log.Warn().Msg("no dramas found in drama table")
	}

	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = 10
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, concurrency)
	var (
		wg         sync.WaitGroup
		errOnce    sync.Once
		runErr     error
		matchesMu  sync.Mutex
		allMatches = make([]Match, 0)
	)

	for idx, drama := range dramas {
		name := strings.TrimSpace(drama.Name)
		if name == "" || drama.Duration <= 0 {
			continue
		}

		if ctx.Err() != nil {
			break
		}

		wg.Add(1)
		go func(idx int, drama Drama, name string) {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()

			paramFilter := buildParamsFilter([]string{name}, cfg.ParamsField)
			resultFilter := combineFilters(opts.ResultFilter, paramFilter)

			log.Info().
				Int("index", idx+1).
				Int("total", len(dramas)).
				Str("drama", name).
				Str("result_filter", resultFilter).
				Msg("Running auto detection for drama")

			resultRows, err := fetchRows(ctx, client, TableConfig{
				URL:    reporter.ResultTableURL(),
				Filter: resultFilter,
			})
			if err != nil {
				if isFeishuDataNotReady(err) {
					log.Warn().
						Str("drama", name).
						Msg("Result table data not ready, skipping")
					return
				}
				errOnce.Do(func() {
					runErr = fmt.Errorf("failed to fetch result rows for %s: %w", name, err)
					cancel()
				})
				return
			}

			dramaRow := Row{Fields: map[string]any{
				cfg.DramaParamsField:   name,
				cfg.DramaDurationField: drama.Duration,
			}}

			report := analyzeRows(resultRows, []Row{dramaRow}, cfg)
			if len(report.Matches) == 0 {
				log.Info().
					Str("drama", name).
					Int("records", len(resultRows)).
					Msg("No suspicious combos found for drama")
				return
			}

			if err := reporter.ReportMatches(ctx, opts.App, report.Matches); err != nil {
				errOnce.Do(func() {
					runErr = fmt.Errorf("failed to report matches for %s: %w", name, err)
					cancel()
				})
				return
			}

			matchesMu.Lock()
			allMatches = append(allMatches, report.Matches...)
			matchesMu.Unlock()
		}(idx, drama, name)
	}

	wg.Wait()

	if runErr != nil {
		return nil, runErr
	}

	csvPath := strings.TrimSpace(opts.OutputCSV)
	if csvPath == "" {
		csvPath = defaultAutoCSVPath()
	}

	if err := writeAutoCSV(csvPath, allMatches); err != nil {
		return nil, err
	}

	return &AutoSummary{
		CSVPath:     csvPath,
		MatchCount:  len(allMatches),
		DramaCount:  len(dramas),
		Matches:     allMatches,
		Dramas:      dramas,
		Threshold:   cfg.Threshold,
		Concurrency: concurrency,
	}, nil
}

func defaultAutoCSVPath() string {
	ts := time.Now().Format("20060102-150405")
	return fmt.Sprintf("piracy_auto_%s.csv", ts)
}

func writeAutoCSV(path string, matches []Match) error {
	if err := ensureDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("prepare csv directory failed: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create csv failed: %w", err)
	}
	defer f.Close()

	writer := csv.NewWriter(f)
	defer writer.Flush()

	header := []string{"DramaName", "UserID", "UserName", "SumDurationSec", "TotalDurationSec", "RatioPct", "RecordCount"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("write csv header failed: %w", err)
	}

	for _, match := range matches {
		row := []string{
			match.Params,
			match.UserID,
			match.UserName,
			fmt.Sprintf("%.1f", match.SumDuration),
			fmt.Sprintf("%.1f", match.TotalDuration),
			fmt.Sprintf("%.2f", match.Ratio*100),
			fmt.Sprintf("%d", match.RecordCount),
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("write csv row failed: %w", err)
		}
	}

	if err := writer.Error(); err != nil {
		return fmt.Errorf("flush csv failed: %w", err)
	}

	log.Info().Str("path", path).Int("rows", len(matches)).Msg("auto results written to csv")
	return nil
}

func ensureDir(dir string) error {
	if dir == "" || dir == "." {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

func extractDramas(rows []Row, cfg Config) []Drama {
	durations := make(map[string]float64)
	for _, row := range rows {
		name := strings.TrimSpace(getString(row.Fields, cfg.DramaParamsField))
		if name == "" {
			continue
		}
		duration, ok := getFloat(row.Fields, cfg.DramaDurationField)
		if !ok || duration <= 0 {
			continue
		}
		if current, exists := durations[name]; !exists || duration > current {
			durations[name] = duration
		}
	}

	names := make([]string, 0, len(durations))
	for name := range durations {
		names = append(names, name)
	}
	sort.Strings(names)

	dramas := make([]Drama, 0, len(names))
	for _, name := range names {
		dramas = append(dramas, Drama{Name: name, Duration: durations[name]})
	}
	return dramas
}
