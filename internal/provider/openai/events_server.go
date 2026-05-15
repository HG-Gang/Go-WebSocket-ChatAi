// internal/provider/openai/events_server.go
// 服务端事件处理器：专注处理「OpenAI → App」的事件解析、格式化、转发
//
// 设计说明：
//
//	当前实现采用「透传模式」—— OpenAI 返回的 JSON 事件直接转发给 App。
//	如果将来需要对特定事件做处理（如音频格式转换、Token 统计、错误翻译等），
//	可以在此文件中添加对应的 handle 方法。
//
// 与 go-xiaozhi 的区别：
//
//	go-xiaozhi 需要将 OpenAI 事件转换为 xiaozhi 协议事件（如 audio.delta → opus 帧），
//	本项目直接透传 OpenAI 协议，所以处理较轻。
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

// Handle 处理服务端事件（入口方法）
// 核心逻辑：
//  1. 解析事件类型
//  2. 按类型分发处理（特殊事件增强处理，其他透传）
//  3. 转发到 App
func (p *ServerEventProcessor) Handle(msg []byte) error {
	// 解析事件基础类型
	event, err := protocol.UnmarshalServerEvent(msg)
	if err != nil {
		// 解析失败也尝试透传（兼容未定义的事件类型）
		p.log.Warn("解析服务端事件失败，直接透传", zap.Error(err))
		return p.passThrough(msg)
	}

	// 按事件类型分发处理
	switch event.ServerEventType() {
	case protocol.ServerEventTypeError:
		// 错误事件：增强日志记录
		return p.handleErrorEvent(msg, event)

	case protocol.ServerEventTypeSessionCreated:
		// 会话创建事件：记录会话ID
		p.log.Info("OpenAI 会话已创建")
		return p.passThrough(msg)

	case protocol.ServerEventTypeSessionUpdated:
		// 会话更新事件
		p.log.Info("OpenAI 会话已更新")
		return p.passThrough(msg)

	case protocol.ServerEventTypeResponseDone:
		// 响应完成事件：可在此统计 Token 消耗
		p.log.Debug("OpenAI 响应完成")
		return p.handleResponseDone(msg, event)

	case protocol.ServerEventTypeResponseAudioDelta, protocol.ServerEventTypeLegacyResponseAudioDelta:
		// 音频增量事件（高频，不打日志）
		return p.passThrough(msg)

	default:
		// 其他事件直接透传
		return p.passThrough(msg)
	}
}

// handleErrorEvent 处理错误事件（增强日志）
func (p *ServerEventProcessor) handleErrorEvent(msg []byte, event protocol.ServerEvent) error {
	if errEvt, ok := event.(*protocol.ErrorEvent); ok {
		p.log.Error("OpenAI 服务端错误",
			zap.String("code", errEvt.Error.Code),
			zap.String("message", errEvt.Error.Message),
			zap.String("type", errEvt.Error.Type))
	}
	// 错误事件也透传给 App，让前端处理
	return p.passThrough(msg)
}

// handleResponseDone 处理响应完成事件
// 可在此扩展 Token 统计逻辑
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

// passThrough 透传消息到 App
func (p *ServerEventProcessor) passThrough(msg []byte) error {
	if p.appConn == nil {
		return fmt.Errorf("App 连接为空，无法透传")
	}
	return p.appConn.WriteMessage(websocket.TextMessage, msg)
}
