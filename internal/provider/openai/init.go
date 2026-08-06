// internal/provider/openai/init.go
// OpenAI Provider 工厂注册：通过 init() 自动注册工厂函数，供 provider.Create("openai") 使用。
// 输入：conf.ModelConfig；输出：实现 provider.Provider 接口的 openai.Client。
// 明确不负责：模型解析、连接建立与请求发送，均由注册后创建的 Client 承担。
// main.go 通过空导入触发注册：import _ "TozoAI-Chat-Api/internal/provider/openai"。
package openai

import (
	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/provider"
)

func init() {
	// 注册工厂函数：provider.Create("openai") 时接收模型配置并返回实现了 provider.Provider 的 Client。
	provider.Register("openai", func(cfg *conf.ModelConfig) provider.Provider {
		return NewClient(cfg)
	})
}
