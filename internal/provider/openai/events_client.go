// internal/provider/openai/events_client.go
// 文件功能：处理 App → OpenAI 方向的客户端事件。输入为 App 经 WebSocket 发来的
// 原始 JSON 消息，解析出事件类型后原样转发到 OpenAI API 连接，不做拦截或修改。
// 安全边界：不涉及鉴权与密钥；转发前只校验 JSON 可解析性，解析失败的非法消息
// 返回错误、不进入上游连接，避免畸形 JSON 破坏 OpenAI 侧协议状态。
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

// Handle 处理客户端事件：解析事件类型并分发，最终都通过 passThrough 转发给 OpenAI。
// 参数 msg 为 App 发来的原始 JSON；返回写入 WebSocket 的错误，调用方据此处理连接。
// 与服务端方向不同，这里解析失败直接返回错误（fail-closed），
// 未通过协议校验的消息不转发，防止破坏上游协议状态。
func (p *ClientEventProcessor) Handle(msg []byte) error {
	event, err := protocol.UnmarshalClientEvent(msg)
	if err != nil {
		return fmt.Errorf("解析客户端事件失败: %w", err)
	}

	switch event.ClientEventType() {
	case protocol.ClientEventTypeSessionUpdate:
		p.log.Debug("收到会话更新事件")
		return p.passThrough(msg)

	case protocol.ClientEventTypeInputAudioBufferAppend:
		// 音频追加事件频率高，不记日志直接透传，避免日志洪峰。
		return p.passThrough(msg)

	case protocol.ClientEventTypeInputAudioBufferCommit:
		p.log.Debug("收到音频提交事件")
		return p.passThrough(msg)

	case protocol.ClientEventTypeInputAudioBufferClear:
		p.log.Debug("收到音频清空事件")
		return p.passThrough(msg)

	case protocol.ClientEventTypeResponseCreate:
		p.log.Debug("收到创建响应事件")
		return p.passThrough(msg)

	case protocol.ClientEventTypeResponseCancel:
		p.log.Debug("收到取消响应事件")
		return p.passThrough(msg)

	default:
		// 未知事件类型仍透传，兼容 OpenAI 后续新增的客户端事件。
		p.log.Debug("收到未知客户端事件，直接透传",
			zap.String("type", string(event.ClientEventType())))
		return p.passThrough(msg)
	}
}

// passThrough 把 App 原始 JSON 消息写入 OpenAI 的 WebSocket 连接，不做任何修改；
// 连接为空时返回错误而不是伪造成功，由调用方决定是否断开会话。
func (p *ClientEventProcessor) passThrough(msg []byte) error {
	if p.apiConn == nil {
		return fmt.Errorf("OpenAI 连接为空，无法透传")
	}
	return p.apiConn.WriteMessage(websocket.TextMessage, msg)
}
