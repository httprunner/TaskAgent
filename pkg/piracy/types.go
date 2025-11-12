package piracy

import "github.com/httprunner/TaskAgent/pkg/feishu"

// Config defines the configuration for piracy detection.
type Config struct {
	// Fields in result table (source A)
	ParamsField   string
	UserIDField   string
	DurationField string // item duration seconds

	// Fields in target table (source B)
	TargetParamsField   string // Params field in target table
	TargetDurationField string // total duration seconds

	// Fields in original drama table (source C)
	DramaIDField       string // 短剧 ID
	DramaParamsField   string // Params field in drama table
	DramaDurationField string // 全剧时长（秒）

	// Threshold ratio (0..1), e.g., 0.5 means 50%
	Threshold float64
}

// TableConfig defines configuration for a Feishu table.
type TableConfig struct {
	URL    string
	ViewID string
	Filter string
	Limit  int
}

// Options defines the input options for piracy detection.
type Options struct {
	ResultTable TableConfig
	TargetTable TableConfig
	DramaTable  TableConfig // New table for original drama information
	Config      Config
}

// Match represents a suspicious Params+UserID combination.
type Match struct {
	Params        string
	UserID        string
	UserName      string
	SumDuration   float64
	TotalDuration float64
	Ratio         float64
	RecordCount   int
}

// Drama captures a single drama entry from the drama table.
type Drama struct {
	Name     string
	Duration float64
}

// Report captures the aggregated detection results.
type Report struct {
	Matches       []Match
	ResultRows    int
	TargetRows    int
	MissingParams []string // Params in result without corresponding target
	Threshold     float64
}

// Row represents a raw Feishu bitable record.
type Row = feishu.BitableRow

// UserDramaInfo holds user information and their drama list from piracy detection results
type UserDramaInfo struct {
	UserID   string   // 用户ID
	UserName string   // 用户名称
	Dramas   []string // 短剧名称列表
}
