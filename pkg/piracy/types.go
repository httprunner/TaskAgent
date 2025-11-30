package piracy

import "github.com/httprunner/TaskAgent/pkg/feishu"

// Config defines the configuration for piracy detection.
type Config struct {
	// Fields in result table (source A)
	ParamsField   string
	UserIDField   string
	DurationField string // item duration seconds
	ItemIDField   string // video/item identifier column (用于去重)

	// Fields in task table (source B)
	TaskParamsField string // Params field in task table

	// Fields in original drama table (source C)
	DramaIDField       string // 短剧 ID
	DramaNameField     string // Drama name/title column
	DramaDurationField string // 全剧时长（秒）

	// Threshold ratio (0..1), e.g., 0.5 means 50%
	Threshold float64
}

// TableConfig defines configuration for a Feishu table.
type TableConfig struct {
	URL    string
	ViewID string
	Filter *feishu.FilterInfo
	Limit  int
}

// Options defines the input options for piracy detection.
type Options struct {
	ResultTable TableConfig
	TaskTable   TableConfig
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
	TaskRows      int
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

// VideoDetail contains detailed information about a captured video.
type VideoDetail struct {
	ItemID      string // 视频 ID
	Tags        string // 标签，枚举值：空值、合集、短剧
	AnchorPoint string // JSON 格式，可能包含 appLink 字段
}

// MatchDetail contains a piracy match along with its associated video details.
type MatchDetail struct {
	Match  Match         // 盗版线索信息
	Videos []VideoDetail // 该线索下的所有视频
}
