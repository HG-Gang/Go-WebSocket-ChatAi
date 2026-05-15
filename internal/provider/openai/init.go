// internal/provider/openai/init.go
// OpenAI Provider 工厂注册
//
// 工厂模式核心文件：通过 init() 自动注册 OpenAI 的 Provider 工厂函数。
// main.go 中需要通过空导入触发 init()：
//   import _ "TozoAI-Chat-Api/internal/provider/openai"
//
// 这样 provider.Create("openai") 就能正确创建 Client 实例。
package openai

import (
	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/provider"
)

func init() {
	// 注册 OpenAI Provider 工厂函数
	// 当调用 provider.Create("openai") 时，会执行此工厂函数：
	//   1. 接收 conf.ModelConfig（从配置文件加载的模型配置）
	//   2. 创建并返回 openai.Client 实例
	//   3. Client 实现了 provider.Provider 接口的所有方法
	provider.Register("openai", func(cfg *conf.ModelConfig) provider.Provider {
		return NewClient(cfg)
	})
}
