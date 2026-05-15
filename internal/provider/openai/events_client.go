// internal/provider/openai/events_client.go
// 客户端事件处理器：专注处理「App → OpenAI」的事件解析、校验、转发
//
// 设计说明：
//   当前实现采用「透传模式」—— App 发来的 JSON 事件直接转发给 OpenAI，
//   不做额外处理。如果将来需要对特定事件做拦截、修改或校验（如限制音频大小、
//   注入系统指令等），可以在此文件中添加对应的 handle 方法。
//
// 与 go-xiaozhi 的区别：
//   go-xiaozhi 需要做 xiaozhi ↔ openai 的协议转换，所以 DispatchClientEvent 很重。
//   本项目直接透传 OpenAI 协议，所以事件处理器较轻。
package openai

import (
	"fmt"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	protocol "TozoAI-Chat-Api/pkg/protocol/openai"
)

// ClientEventProcessor 客户端事件处理器
// 职责：解析 App 发来的客户端事件，转发到 OpenAI API
type ClientEventProcessor struct {
	apiConn *websocket.Conn // OpenAI API 的 WS 连接
	log     *zap.Logger     // 日志实例
}

// NewClientEventProcessor 创建客户端事件处理器实例
func NewClientEventProcessor(apiConn *websocket.Conn, log *zap.Logger) *ClientEventProcessor {
	return &ClientEventProcessor{
		apiConn: apiConn,
		log:     log,
	}
}

// Handle 处理客户端事件（入口方法）
// 核心逻辑：
//   1. 解析事件类型
//   2. 按类型分发到具体处理方法
//   3. 转发到 OpenAI API
func (p *ClientEventProcessor) Handle(msg []byte) error {
	// 解析事件基础类型
	event, err := protocol.UnmarshalClientEvent(msg)
	if err != nil {
		return fmt.Errorf("解析客户端事件失败: %w", err)
	}

	// 按事件类型分发处理
	switch event.ClientEventType() {
	case protocol.ClientEventTypeSessionUpdate:
		// 会话更新事件：可在此添加指令注入、参数校验等逻辑
		p.log.Debug("收到会话更新事件")
		return p.passThrough(msg)

	case protocol.ClientEventTypeInputAudioBufferAppend:
		// 音频追加事件：高频事件，仅透传，不打日志
		return p.passThrough(msg)

	case protocol.ClientEventTypeInputAudioBufferCommit:
		// 音频提交事件：标记音频输入完成
		p.log.Debug("收到音频提交事件")
		return p.passThrough(msg)

	case protocol.ClientEventTypeInputAudioBufferClear:
		// 音频清空事件
		p.log.Debug("收到音频清空事件")
		return p.passThrough(msg)

	case protocol.ClientEventTypeResponseCreate:
		// 创建响应事件：触发模型生成
		p.log.Debug("收到创建响应事件")
		return p.passThrough(msg)

	case protocol.ClientEventTypeResponseCancel:
		// 取消响应事件
		p.log.Debug("收到取消响应事件")
		return p.passThrough(msg)

	default:
		// 未知事件类型：尝试透传（兼容未来新事件）
		p.log.Debug("收到未知客户端事件，直接透传",
			zap.String("type", string(event.ClientEventType())))
		return p.passThrough(msg)
	}
}

// passThrough 透传消息到 OpenAI API
// 直接将原始 JSON 消息转发，不做修改
func (p *ClientEventProcessor) passThrough(msg []byte) error {
	if p.apiConn == nil {
		return fmt.Errorf("OpenAI 连接为空，无法透传")
	}
	return p.apiConn.WriteMessage(websocket.TextMessage, msg)
}
