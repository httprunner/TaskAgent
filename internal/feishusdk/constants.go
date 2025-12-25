package feishusdk

import (
	"os"
	"strings"

	"github.com/httprunner/TaskAgent/internal/env"
)

// Shared constants for Feishu bitable integrations. Keep these in TaskAgent so
// downstream agents can reference a single source of truth when wiring Feishu task workflows.
const (
	// EnvTaskBitableURL indicates where to pull Feishu target tasks from
	// when running against a bitable-backed queue.
	EnvTaskBitableURL = "TASK_BITABLE_URL"
	// EnvResultBitableURL indicates where to push Feishu result rows.
	EnvResultBitableURL = "RESULT_BITABLE_URL"
	// EnvWebhookBitableURL points to the dedicated webhook result table that
	// aggregates group/task webhook delivery status.
	EnvWebhookBitableURL = "WEBHOOK_BITABLE_URL"
	// EnvDeviceBitableURL indicates where to pull/push Feishu device status rows.
	// StatusError marks a task row as permanently errored and skipped.
	StatusError         = "error"
	EnvDeviceBitableURL = "DEVICE_BITABLE_URL"
	// EnvCookieBitableURL points to the dedicated cookies table for SingleURLWorker.
	EnvCookieBitableURL = "COOKIE_BITABLE_URL"

	// StatusPending marks a task row as pending execution.
	StatusPending = "pending"
	// StatusSuccess marks a task row as completed successfully.
	StatusSuccess = "success"
	// StatusReady marks a task row ready for downstream processing.
	StatusReady = "ready"
	// StatusFailed marks a task row as failed.
	StatusFailed = "failed"
	// StatusDispatched marks a task row as dispatched.
	StatusDispatched = "dispatched"
	// StatusRunning marks a task row as currently executing.
	StatusRunning = "running"
	// CookieStatusValid marks a cookie row as usable.
	CookieStatusValid = "valid"
	// CookieStatusInvalid marks a cookie row as unusable.
	CookieStatusInvalid = "invalid"
	// WebhookPending marks a task that requires webhook synchronization.
	WebhookPending = "pending"
	// WebhookSuccess marks a task whose webhook synchronization succeeded.
	WebhookSuccess = "success"
	// WebhookFailed marks a task whose webhook synchronization failed.
	WebhookFailed = "failed"
	// WebhookError marks a task whose webhook synchronization was skipped due to missing data.
	WebhookError = "error"
)

// DefaultDramaFields matches the default schema used by the built-in drama
// catalog template.
var (
	baseDramaFields = DramaFields{
		DramaID:                  "短剧 ID",
		DramaName:                "短剧名称",
		TotalDuration:            "全剧时长（秒）",
		EpisodeCount:             "全剧集数",
		Priority:                 "优先级",
		RightsProtectionScenario: "维权场景",
		SearchAlias:              "搜索别名",
		CaptureDate:              "采集日期",
	}
	DefaultDramaFields = baseDramaFields
)

// DefaultTaskFields matches the schema provided in the requirements.
var (
	baseTaskFields = TaskFields{
		TaskID:           "TaskID",           // 任务 ID
		ParentTaskID:     "ParentTaskID",     // Parent task ID (general search TaskID)
		App:              "App",              // 平台名称
		Scene:            "Scene",            // 场景名称
		Params:           "Params",           // 查询参数
		ItemID:           "ItemID",           // 单资源标识
		BookID:           "BookID",           // 短剧 Book ID
		URL:              "URL",              // 视频 URL
		UserID:           "UserID",           // 用户 ID
		UserName:         "UserName",         // 用户名称
		Datetime:         "Datetime",         // 任务执行时间配置
		Status:           "Status",           // 任务状态
		Webhook:          "Webhook",          // Webhook 同步状态
		Logs:             "Logs",             // 日志目录
		LastScreenShot:   "LastScreenShot",   // Last task screenshot (attachment)
		GroupID:          "GroupID",          // 任务分组ID，用于关联同一业务线索的多场景任务
		DeviceSerial:     "DeviceSerial",     // 目标执行设备
		DispatchedDevice: "DispatchedDevice", // 实际派发设备
		DispatchedAt:     "DispatchedAt",     // 任务派发时间
		StartAt:          "StartAt",          // 任务开始时间
		EndAt:            "EndAt",            // 任务结束时间
		ElapsedSeconds:   "ElapsedSeconds",   // 任务耗时（秒）
		ItemsCollected:   "ItemsCollected",   // 本次任务采集到的内容条数
		Extra:            "Extra",            // 额外信息
		RetryCount:       "RetryCount",       // 重试次数
	}
	DefaultTaskFields = baseTaskFields
)

var (
	baseCookieFields = CookieFields{
		Cookies:  "Cookies",
		Platform: "Platform",
		Status:   "Status",
	}
	DefaultCookieFields = baseCookieFields
)

// DefaultResultFields matches the schema required by the capture result table.
var (
	baseResultFields = ResultFields{
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
		UserAlias:      "UserAlias",      // 用户别名/展示 ID
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
	DefaultResultFields = baseResultFields
)

func init() {
	_ = env.Ensure()
	RefreshFieldMappings()
}

// RefreshFieldMappings re-applies environment overrides to all exported field maps.
// Call this after loading .env files or mutating related environment variables at runtime.
func RefreshFieldMappings() {
	DefaultTaskFields = baseTaskFields
	DefaultResultFields = baseResultFields
	DefaultDramaFields = baseDramaFields
	DefaultCookieFields = baseCookieFields
	applyTaskFieldEnvOverrides(&DefaultTaskFields)
	applyResultFieldEnvOverrides(&DefaultResultFields)
	applyDramaFieldEnvOverrides(&DefaultDramaFields)
	applyCookieFieldEnvOverrides(&DefaultCookieFields)
}

func applyTaskFieldEnvOverrides(fields *TaskFields) {
	if fields == nil {
		return
	}
	overrideFieldFromEnv("TASK_FIELD_TASKID", &fields.TaskID)
	overrideFieldFromEnv("TASK_FIELD_PARENT_TASK_ID", &fields.ParentTaskID)
	overrideFieldFromEnv("TASK_FIELD_APP", &fields.App)
	overrideFieldFromEnv("TASK_FIELD_SCENE", &fields.Scene)
	overrideFieldFromEnv("TASK_FIELD_PARAMS", &fields.Params)
	overrideFieldFromEnv("TASK_FIELD_ITEMID", &fields.ItemID)
	overrideFieldFromEnv("TASK_FIELD_BOOKID", &fields.BookID)
	overrideFieldFromEnv("TASK_FIELD_URL", &fields.URL)
	overrideFieldFromEnv("TASK_FIELD_USERID", &fields.UserID)
	overrideFieldFromEnv("TASK_FIELD_USERNAME", &fields.UserName)
	overrideFieldFromEnv("TASK_FIELD_DATETIME", &fields.Datetime)
	overrideFieldFromEnv("TASK_FIELD_STATUS", &fields.Status)
	overrideFieldFromEnv("TASK_FIELD_WEBHOOK", &fields.Webhook)
	overrideFieldFromEnv("TASK_FIELD_LOGS", &fields.Logs)
	overrideFieldFromEnv("TASK_FIELD_LAST_SCREENSHOT", &fields.LastScreenShot)
	overrideFieldFromEnv("TASK_FIELD_EXTRA", &fields.Extra)
	overrideFieldFromEnv("TASK_FIELD_GROUPID", &fields.GroupID)
	overrideFieldFromEnv("TASK_FIELD_DEVICE_SERIAL", &fields.DeviceSerial)
	overrideFieldFromEnv("TASK_FIELD_DISPATCHED_DEVICE", &fields.DispatchedDevice)
	overrideFieldFromEnv("TASK_FIELD_DISPATCHED_AT", &fields.DispatchedAt)
	overrideFieldFromEnv("TASK_FIELD_START_AT", &fields.StartAt)
	overrideFieldFromEnv("TASK_FIELD_END_AT", &fields.EndAt)
	overrideFieldFromEnv("TASK_FIELD_ELAPSED_SECONDS", &fields.ElapsedSeconds)
	overrideFieldFromEnv("TASK_FIELD_ITEMS_COLLECTED", &fields.ItemsCollected)
	overrideFieldFromEnv("TASK_FIELD_RETRYCOUNT", &fields.RetryCount)
}

func applyResultFieldEnvOverrides(fields *ResultFields) {
	if fields == nil {
		return
	}
	overrideFieldFromEnv("RESULT_FIELD_DATETIME", &fields.Datetime)
	overrideFieldFromEnv("RESULT_FIELD_DEVICE_SERIAL", &fields.DeviceSerial)
	overrideFieldFromEnv("RESULT_FIELD_APP", &fields.App)
	overrideFieldFromEnv("RESULT_FIELD_SCENE", &fields.Scene)
	overrideFieldFromEnv("RESULT_FIELD_PARAMS", &fields.Params)
	overrideFieldFromEnv("RESULT_FIELD_ITEMID", &fields.ItemID)
	overrideFieldFromEnv("RESULT_FIELD_ITEMCAPTION", &fields.ItemCaption)
	overrideFieldFromEnv("RESULT_FIELD_ITEMCDNURL", &fields.ItemCDNURL)
	overrideFieldFromEnv("RESULT_FIELD_ITEMURL", &fields.ItemURL)
	overrideFieldFromEnv("RESULT_FIELD_DURATION", &fields.ItemDuration)
	overrideFieldFromEnv("RESULT_FIELD_USERNAME", &fields.UserName)
	overrideFieldFromEnv("RESULT_FIELD_USERID", &fields.UserID)
	overrideFieldFromEnv("RESULT_FIELD_USERALIAS", &fields.UserAlias)
	overrideFieldFromEnv("RESULT_FIELD_USERAUTHENTITY", &fields.UserAuthEntity)
	overrideFieldFromEnv("RESULT_FIELD_TAGS", &fields.Tags)
	overrideFieldFromEnv("RESULT_FIELD_TASKID", &fields.TaskID)
	overrideFieldFromEnv("RESULT_FIELD_EXTRA", &fields.Extra)
	overrideFieldFromEnv("RESULT_FIELD_LIKECOUNT", &fields.LikeCount)
	overrideFieldFromEnv("RESULT_FIELD_VIEWCOUNT", &fields.ViewCount)
	overrideFieldFromEnv("RESULT_FIELD_ANCHORPOINT", &fields.AnchorPoint)
	overrideFieldFromEnv("RESULT_FIELD_COMMENTCOUNT", &fields.CommentCount)
	overrideFieldFromEnv("RESULT_FIELD_COLLECTCOUNT", &fields.CollectCount)
	overrideFieldFromEnv("RESULT_FIELD_FORWARDCOUNT", &fields.ForwardCount)
	overrideFieldFromEnv("RESULT_FIELD_SHARECOUNT", &fields.ShareCount)
	overrideFieldFromEnv("RESULT_FIELD_PAYMODE", &fields.PayMode)
	overrideFieldFromEnv("RESULT_FIELD_COLLECTION", &fields.Collection)
	overrideFieldFromEnv("RESULT_FIELD_EPISODE", &fields.Episode)
	overrideFieldFromEnv("RESULT_FIELD_PUBLISHTIME", &fields.PublishTime)
}

func applyDramaFieldEnvOverrides(fields *DramaFields) {
	if fields == nil {
		return
	}
	overrideFieldFromEnv("DRAMA_FIELD_ID", &fields.DramaID)
	overrideFieldFromEnv("DRAMA_FIELD_NAME", &fields.DramaName)
	overrideFieldFromEnv("DRAMA_FIELD_DURATION", &fields.TotalDuration)
	overrideFieldFromEnv("DRAMA_FIELD_EPISODE_COUNT", &fields.EpisodeCount)
	overrideFieldFromEnv("DRAMA_FIELD_PRIORITY", &fields.Priority)
	overrideFieldFromEnv("DRAMA_FIELD_RIGHTS_SCENARIO", &fields.RightsProtectionScenario)
	overrideFieldFromEnv("DRAMA_FIELD_SEARCH_ALIAS", &fields.SearchAlias)
	overrideFieldFromEnv("DRAMA_FIELD_CAPTURE_DATE", &fields.CaptureDate)
}

func applyCookieFieldEnvOverrides(fields *CookieFields) {
	if fields == nil {
		return
	}
	overrideFieldFromEnv("COOKIE_FIELD_COOKIES", &fields.Cookies)
	overrideFieldFromEnv("COOKIE_FIELD_PLATFORM", &fields.Platform)
	overrideFieldFromEnv("COOKIE_FIELD_STATUS", &fields.Status)
}

func overrideFieldFromEnv(env string, target *string) {
	if target == nil {
		return
	}
	if val, ok := os.LookupEnv(env); ok {
		*target = strings.TrimSpace(val)
	}
}

// DefaultDeviceFields provides sensible defaults matching the design doc.
var DefaultDeviceFields = DeviceFields{
	DeviceSerial: "DeviceSerial",
	OSType:       "OSType",
	OSVersion:    "OSVersion",
	IPLocation:   "IPLocation",
	IsRoot:       "IsRoot",
	ProviderUUID: "ProviderUUID",
	AgentVersion: "AgentVersion",
	Status:       "Status",
	LastSeenAt:   "LastSeenAt",
	LastError:    "LastError",
	Tags:         "Tags",
	RunningTask:  "RunningTask",
	PendingTasks: "PendingTasks",
}
