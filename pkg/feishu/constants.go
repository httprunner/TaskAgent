package feishu

// Shared constants for Feishu bitable integrations. Keep these in TaskAgent so
// downstream agents can reference a single source of truth when wiring Feishu task workflows.
const (
	// EnvTaskBitableURL indicates where to pull Feishu target tasks from
	// when running against a bitable-backed queue.
	EnvTaskBitableURL = "TASK_BITABLE_URL"
	// EnvResultBitableURL indicates where to push Feishu result rows.
	EnvResultBitableURL = "RESULT_BITABLE_URL"

	// StatusPending marks a task row as pending execution.
	StatusPending = "pending"
	// StatusSuccess marks a task row as completed successfully.
	StatusSuccess = "success"
	// StatusFailed marks a task row as failed.
	StatusFailed = "failed"
	// StatusDispatched marks a task row as dispatched.
	StatusDispatched = "dispatched"
	// WebhookPending marks a task that requires webhook synchronization.
	WebhookPending = "pending"
	// WebhookSuccess marks a task whose webhook synchronization succeeded.
	WebhookSuccess = "success"
	// WebhookFailed marks a task whose webhook synchronization failed.
	WebhookFailed = "failed"
)

// DefaultDramaFields matches the default schema used by the built-in drama
// catalog template.
var DefaultDramaFields = DramaFields{
	DramaID:                  "短剧 ID",
	DramaName:                "短剧名称",
	TotalDuration:            "全剧时长（秒）",
	EpisodeCount:             "全剧集数",
	Priority:                 "优先级",
	RightsProtectionScenario: "维权场景",
}

// DefaultTaskFields matches the schema provided in the requirements.
var DefaultTaskFields = TaskFields{
	TaskID:           "TaskID",           // 任务 ID
	App:              "App",              // 平台名称
	Scene:            "Scene",            // 场景名称
	Params:           "Params",           // 查询参数
	UserID:           "UserID",           // 用户 ID
	UserName:         "UserName",         // 用户名称
	Datetime:         "Datetime",         // 任务执行时间配置
	Status:           "Status",           // 任务状态
	Webhook:          "Webhook",          // Webhook 同步状态
	DeviceSerial:     "DeviceSerial",     // 目标执行设备
	DispatchedDevice: "DispatchedDevice", // 实际派发设备
	DispatchedAt:     "DispatchedAt",     // 任务派发时间
	StartAt:          "StartAt",          // 任务开始时间
	EndAt:            "EndAt",            // 任务结束时间
	ElapsedSeconds:   "ElapsedSeconds",   // 任务耗时（秒）
	Extra:            "Extra",            // 额外信息
}

// DefaultResultFields matches the schema required by the capture result table.
var DefaultResultFields = ResultFields{
	Datetime:       "Datetime",       // 抓取时间
	App:            "App",            // 平台名称
	Scene:          "Scene",          // 场景名称
	Params:         "Params",         // 查询参数
	ItemID:         "ItemID",         // 内容 ID
	ItemCaption:    "ItemCaption",    // 内容标题
	ItemCDNURL:     "ItemCDNURL",     // 内容CDN链接
	ItemURL:        "ItemURL",        // 内容分享链接
	ItemDuration:   "ItemDuration",   // 内容时长（秒），仅限视频
	UserName:       "UserName",       // 用户名称
	UserID:         "UserID",         // 用户 ID
	UserAuthEntity: "UserAuthEntity", // 用户认证实体
	Tags:           "Tags",           // 内容标签
	LikeCount:      "LikeCount",      // 点赞数
	ViewCount:      "ViewCount",      // 播放/观看数
	CommentCount:   "CommentCount",   // 评论数
	CollectCount:   "CollectCount",   // 收藏数
	ForwardCount:   "ForwardCount",   // 转发数
	ShareCount:     "ShareCount",     // 分享数
	AnchorPoint:    "AnchorPoint",    // 锚点信息
	PayMode:        "PayMode",        // 付费模式
	Collection:     "Collection",     // 所属合集
	Episode:        "Episode",        // 剧集信息（总集数/免费数/付费数）
	PublishTime:    "PublishTime",    // 发布时间
	TaskID:         "TaskID",         // 任务 ID（关联 TaskFields）
	DeviceSerial:   "DeviceSerial",   // 设备序列号
	Extra:          "Extra",          // 额外信息
}
