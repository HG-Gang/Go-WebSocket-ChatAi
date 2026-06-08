// internal/provider/provider.go
// Provider 工厂模式核心文件：
// 1. 定义 Provider 接口（所有模型必须实现）
// 2. 提供工厂注册表和工厂方法
// 3. 通过 init() + Register() 实现自动注册
//
// 使用方式：
//
//	各模型包（如 openai）在 init.go 中调用 Register() 注册工厂函数，
//	main.go 中通过 _ "TozoAI-Chat-Api/internal/provider/openai" 触发 init()，
//	业务代码调用 provider.Create("openai") 获取实例。
package provider

import (
	"context"

	"TozoAI-Chat-Api/conf"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// Provider 模型 Provider 接口（每个模型必须实现）
// 设计原则：
//   - Name(): 返回模型标识，用于日志和配置查找
//   - Connect(): 建立与模型 API 的连接（如 WS 连接）
//   - HandleWS(): 处理双向 WS 转发（核心方法，阻塞直到会话结束）
//   - HandleFallback(): HTTP 降级处理（非 WS 场景）
//   - Close(): 释放资源（连接、定时器等）
type Provider interface {
	Name() string                                                // 返回模型名称标识（如 "openai"）
	Connect(ctx context.Context) error                           // 连接模型 API 服务
	HandleWS(ctx context.Context, appConn *websocket.Conn) error // 处理 WS 双向转发（阻塞）
	HandleFallback(c *gin.Context)                               // HTTP 降级处理
	Close() error                                                // 关闭连接，释放资源
}

// LoggerProvider 可选接口：支持注入带 request_id 的 logger
// 实现此接口的 Provider 会在 session.Start 时自动注入带 request_id 的 logger，
// 使 Provider 内部所有日志都携带 request_id + user_id + session_id 字段。
type LoggerProvider interface {
	SetLogger(log *zap.Logger)
}

// SessionContextProvider 可选接口：Provider 需要把用量、指标与业务会话绑定时实现。
type SessionContextProvider interface {
	SetSessionContext(userID, sessionID string)
}

// ProviderFactory 工厂函数类型：接收模型配置，返回 Provider 实例
type ProviderFactory func(cfg *conf.ModelConfig) Provider

// factories 工厂注册表（key: 模型名称，value: 工厂函数）
// 通过各模型包的 init() 函数自动填充
var factories = make(map[string]ProviderFactory)

// Register 注册模型 Provider 工厂函数
// 由各模型包的 init() 调用，如：
//
//	provider.Register("openai", func(cfg *conf.ModelConfig) provider.Provider { ... })
//
// 参数：
//   - name: 模型名称（与配置文件中 models.{name} 对应）
//   - factory: 工厂函数
func Register(name string, factory ProviderFactory) {
	factories[name] = factory
}

// Create 工厂方法：根据模型名创建 Provider 实例
// 核心逻辑：
//  1. 从配置中获取模型配置
//  2. 检查模型是否启用
//  3. 查找已注册的工厂函数
//  4. 调用工厂函数创建实例
//
// 参数：name - 模型名称（如 "openai"）
// 返回：Provider 实例（nil 表示模型未启用或未注册）
func Create(name string) Provider {
	// 获取模型配置
	cfg := conf.GetModel(name)
	return CreateWithConfig(name, cfg)
}

// CreateWithConfig 使用调用方提供的模型配置创建 Provider。
// 适用于单次连接临时覆盖上游地址、API Key 或模型，不写回全局配置。
func CreateWithConfig(name string, cfg *conf.ModelConfig) Provider {
	if cfg == nil || !cfg.Enabled {
		return nil // 模型未配置或未启用
	}

	// 查找工厂函数
	factory, ok := factories[name]
	if !ok {
		return nil // 模型未注册工厂函数（可能忘记在 main.go 导入对应包）
	}

	// 调用工厂函数创建实例
	return factory(cfg)
}
