// internal/provider/azureai/init.go
// 文件功能：在 init() 中把 "azureai" 注册到 provider 工厂表，使 provider.Create("azureai")
// 可用。本文件不实现连接逻辑：Realtime 长连接复用 openai 包的四协程实现，Azure 与 OpenAI
// 的差异由配置层生成 URL/Header。
package azureai

import (
	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/provider"
	openaiRealtime "TozoAI-Chat-Api/internal/provider/openai"
)

// init 注册 Azure OpenAI 模型 Provider。
// Realtime 长连接复用 openai 包的四协程实现；NewClientWithName 以 "azureai" 作为 Provider
// 名称，供日志与配置查找区分来源。
func init() {
	provider.Register("azureai", func(cfg *conf.ModelConfig) provider.Provider {
		return openaiRealtime.NewClientWithName(cfg, "azureai")
	})
}
