package azureai

import (
	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/provider"
	openaiRealtime "TozoAI-Chat-Api/internal/provider/openai"
)

// init 注册 Azure OpenAI Realtime Provider。
// Realtime 长连接复用 openai 包中的四协程实现；Azure 和 OpenAI 的差异由配置层生成 URL/Header。
func init() {
	provider.Register("azureai", func(cfg *conf.ModelConfig) provider.Provider {
		return openaiRealtime.NewClientWithName(cfg, "azureai")
	})
}
