// internal/provider/openai/events_server.go
// 文件功能：处理 OpenAI → App 方向的服务端事件。输入为 OpenAI Realtime WebSocket
// 推送的原始 JSON 消息，解析出事件类型后按需增强日志（错误、Token 统计），
// 其余事件统一原样透传给 App 的 WS 连接。采用透传模式，不做音频格式转换等加工。
// 安全边界：不涉及鉴权与密钥；日志只记录事件类型、错误码与 Token 用量等字段，
// 不打印事件完整 payload。解析失败时仍原样透传，避免未知新事件被网关静默丢弃。
package openai

import (
	"fmt"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	protocol "TozoAI-Chat-Api/pkg/protocol/openai"
)

// ServerEventProcessor 服务端事件处理器
// 职责：解析 OpenAI 返回的服务端事件，转发到 App
type ServerEventProcessor struct {
	appConn *websocket.Conn // App 的 WS 连接
	log     *zap.Logger     // 日志实例
}

// NewServerEventProcessor 创建服务端事件处理器实例
func NewServerEventProcessor(appConn *websocket.Conn, log *zap.Logger) *ServerEventProcessor {
	return &ServerEventProcessor{
		appConn: appConn,
		log:     log,
	}
}

// Handle 处理服务端事件：解析事件类型并分发，最终都通过 passThrough 转发给 App。
// 参数 msg 为 OpenAI 推送的原始 JSON；返回写入 WebSocket 的错误，调用方据此处理连接。
// 解析失败的事件同样透传（fail-open）：上游可能新增网关未识别的事件类型，
// 丢弃会破坏 App 的协议状态，透传由 App 侧自行兼容。
func (p *ServerEventProcessor) Handle(msg []byte) error {
	event, err := protocol.UnmarshalServerEvent(msg)
	if err != nil {
		p.log.Warn("解析服务端事件失败，直接透传", zap.Error(err))
		return p.passThrough(msg)
	}

	switch event.ServerEventType() {
	case protocol.ServerEventTypeError:
		// 错误事件需要额外记录错误码与消息，便于定位上游问题。
		return p.handleErrorEvent(msg, event)

	case protocol.ServerEventTypeSessionCreated:
		p.log.Info("OpenAI 会话已创建")
		return p.passThrough(msg)

	case protocol.ServerEventTypeSessionUpdated:
		p.log.Info("OpenAI 会话已更新")
		return p.passThrough(msg)

	case protocol.ServerEventTypeResponseDone:
		// 响应完成事件携带 Token 用量，进入统计分支后仍透传。
		p.log.Debug("OpenAI 响应完成")
		return p.handleResponseDone(msg, event)

	case protocol.ServerEventTypeResponseAudioDelta, protocol.ServerEventTypeLegacyResponseAudioDelta:
		// 音频增量事件频率高，不记日志直接透传，避免日志洪峰。
		return p.passThrough(msg)

	default:
		// 未识别或无需处理的事件一律透传，保持上游事件流原样。
		return p.passThrough(msg)
	}
}

// handleErrorEvent 记录上游错误事件的关键字段（错误码、消息、类型），
// 并原样透传给 App，由 App 侧决定如何提示用户；错误不中断事件流。
func (p *ServerEventProcessor) handleErrorEvent(msg []byte, event protocol.ServerEvent) error {
	if errEvt, ok := event.(*protocol.ErrorEvent); ok {
		p.log.Error("OpenAI 服务端错误",
			zap.String("code", errEvt.Error.Code),
			zap.String("message", errEvt.Error.Message),
			zap.String("type", errEvt.Error.Type))
	}
	return p.passThrough(msg)
}

// handleResponseDone 在响应完成事件中记录 Token 用量统计，随后仍将事件透传给 App。
func (p *ServerEventProcessor) handleResponseDone(msg []byte, event protocol.ServerEvent) error {
	if doneEvt, ok := event.(*protocol.ResponseDoneEvent); ok {
		if doneEvt.Response.Usage != nil {
			p.log.Info("响应 Token 统计",
				zap.Int("input_tokens", doneEvt.Response.Usage.InputTokens),
				zap.Int("output_tokens", doneEvt.Response.Usage.OutputTokens),
				zap.Int("total_tokens", doneEvt.Response.Usage.TotalTokens))
		}
	}
	return p.passThrough(msg)
}

// passThrough 把上游原始 JSON 消息写入 App 的 WebSocket 连接；
// 连接为空时返回错误而不是伪造成功，由调用方决定是否断开会话。
func (p *ServerEventProcessor) passThrough(msg []byte) error {
	if p.appConn == nil {
		return fmt.Errorf("App 连接为空，无法透传")
	}
	return p.appConn.WriteMessage(websocket.TextMessage, msg)
}
