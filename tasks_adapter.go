package pool

import feishusource "github.com/httprunner/TaskAgent/pkg/tasksource/feishu"

// 向后兼容：重新导出 Feishu 任务客户端及相关类型。
type FeishuTaskClient = feishusource.FeishuTaskClient
type FeishuTaskClientOptions = feishusource.FeishuTaskClientOptions
type FeishuTask = feishusource.FeishuTask
type TaskExtraUpdate = feishusource.TaskExtraUpdate

func NewFeishuTaskClient(bitableURL string) (*FeishuTaskClient, error) {
	return feishusource.NewFeishuTaskClient(bitableURL)
}

func NewFeishuTaskClientWithOptions(bitableURL string, opts FeishuTaskClientOptions) (*FeishuTaskClient, error) {
	return feishusource.NewFeishuTaskClientWithOptions(bitableURL, opts)
}

const (
	SceneGeneralSearch      = feishusource.SceneGeneralSearch
	SceneProfileSearch      = feishusource.SceneProfileSearch
	SceneCollection         = feishusource.SceneCollection
	SceneAnchorCapture      = feishusource.SceneAnchorCapture
	SceneVideoScreenCapture = feishusource.SceneVideoScreenCapture
	SceneSingleURLCapture   = feishusource.SceneSingleURLCapture
)
