// pkg/protocol/openai/server_events.go
// 文件功能：定义 OpenAI Realtime API 的服务端事件结构体及其 JSON 序列化/反序列化，
// 覆盖 OpenAI 官方文档列出的全部服务端事件类型（参考 go-xiaozhi 的完整实现）。
// 输入：OpenAI 侧下发的单条事件 JSON 字节；输出：按 type 字段分发后的具体事件
// 结构体，或序列化后的 JSON 字节。
// 不负责：事件的业务处理、透传与缓存（见 internal/provider/openai/events_server.go）。
// 兼容说明：旧版预览事件名（response.text.delta 等）与新版事件名解析到同一结构体，
// 保证升级过渡期两端事件语义一致，见下方常量与 UnmarshalServerEvent。
package openai

import (
	"encoding/json"
	"fmt"
)

// ======================== 服务端事件类型常量 ========================
// 完整对齐 OpenAI Realtime API 文档：
// https://platform.openai.com/docs/guides/realtime/server-events
const (
	// 错误
	ServerEventTypeError ServerEventType = "error"

	// 会话
	ServerEventTypeSessionCreated ServerEventType = "session.created" // 会话创建
	ServerEventTypeSessionUpdated ServerEventType = "session.updated" // 会话更新

	// 对话
	ServerEventTypeConversationCreated ServerEventType = "conversation.created" // 对话创建

	// 输入音频缓冲区
	ServerEventTypeInputAudioBufferCommitted     ServerEventType = "input_audio_buffer.committed"      // 音频已提交
	ServerEventTypeInputAudioBufferCleared       ServerEventType = "input_audio_buffer.cleared"        // 音频已清空
	ServerEventTypeInputAudioBufferSpeechStarted ServerEventType = "input_audio_buffer.speech_started" // 语音开始（VAD）
	ServerEventTypeInputAudioBufferSpeechStopped ServerEventType = "input_audio_buffer.speech_stopped" // 语音结束（VAD）

	// 对话条目
	ServerEventTypeConversationItemCreated                          ServerEventType = "conversation.item.created"                             // 条目创建
	ServerEventTypeConversationItemInputAudioTranscriptionCompleted ServerEventType = "conversation.item.input_audio_transcription.completed" // 音频转写完成
	ServerEventTypeConversationItemInputAudioTranscriptionFailed    ServerEventType = "conversation.item.input_audio_transcription.failed"    // 音频转写失败
	ServerEventTypeConversationItemTruncated                        ServerEventType = "conversation.item.truncated"                           // 条目截断
	ServerEventTypeConversationItemDeleted                          ServerEventType = "conversation.item.deleted"                             // 条目删除

	// 响应
	ServerEventTypeResponseCreated   ServerEventType = "response.created"   // 响应创建
	ServerEventTypeResponseCancelled ServerEventType = "response.cancelled" // 响应取消
	ServerEventTypeResponseDone      ServerEventType = "response.done"      // 响应完成

	// 响应输出条目
	ServerEventTypeResponseOutputItemAdded ServerEventType = "response.output_item.added" // 输出条目添加
	ServerEventTypeResponseOutputItemDone  ServerEventType = "response.output_item.done"  // 输出条目完成

	// 响应内容片段
	ServerEventTypeResponseContentPartAdded ServerEventType = "response.content_part.added" // 内容片段添加
	ServerEventTypeResponseContentPartDone  ServerEventType = "response.content_part.done"  // 内容片段完成

	// 文本增量
	ServerEventTypeResponseTextDelta       ServerEventType = "response.output_text.delta" // 文本增量
	ServerEventTypeResponseTextDone        ServerEventType = "response.output_text.done"  // 文本完成
	ServerEventTypeLegacyResponseTextDelta ServerEventType = "response.text.delta"        // 旧版预览事件名
	ServerEventTypeLegacyResponseTextDone  ServerEventType = "response.text.done"         // 旧版预览事件名

	// 音频转写增量
	ServerEventTypeResponseAudioTranscriptDelta       ServerEventType = "response.output_audio_transcript.delta" // 音频转写增量
	ServerEventTypeResponseAudioTranscriptDone        ServerEventType = "response.output_audio_transcript.done"  // 音频转写完成
	ServerEventTypeLegacyResponseAudioTranscriptDelta ServerEventType = "response.audio_transcript.delta"        // 旧版预览事件名
	ServerEventTypeLegacyResponseAudioTranscriptDone  ServerEventType = "response.audio_transcript.done"         // 旧版预览事件名

	// 音频增量
	ServerEventTypeResponseAudioDelta       ServerEventType = "response.output_audio.delta" // 音频增量数据
	ServerEventTypeResponseAudioDone        ServerEventType = "response.output_audio.done"  // 音频完成
	ServerEventTypeLegacyResponseAudioDelta ServerEventType = "response.audio.delta"        // 旧版预览事件名
	ServerEventTypeLegacyResponseAudioDone  ServerEventType = "response.audio.done"         // 旧版预览事件名

	// 函数调用
	ServerEventTypeResponseFunctionCallArgumentsDelta ServerEventType = "response.function_call_arguments.delta" // 函数参数增量
	ServerEventTypeResponseFunctionCallArgumentsDone  ServerEventType = "response.function_call_arguments.done"  // 函数参数完成

	// 速率限制
	ServerEventTypeRateLimitsUpdated ServerEventType = "rate_limits.updated" // 速率限制更新
)

// ======================== 服务端事件结构体 ========================

// ErrorEvent 错误事件
type ErrorEvent struct {
	ServerEventBase
	Error Error `json:"error"` // 错误详情
}

// SessionCreatedEvent 会话创建事件（连接建立时自动发送）
type SessionCreatedEvent struct {
	ServerEventBase
	Session ServerSession `json:"session"` // 会话配置
}

// SessionUpdatedEvent 会话更新事件（session.update 后返回）
type SessionUpdatedEvent struct {
	ServerEventBase
	Session ServerSession `json:"session"` // 更新后的会话配置
}

// ConversationCreatedEvent 对话创建事件
type ConversationCreatedEvent struct {
	ServerEventBase
	Conversation Conversation `json:"conversation"` // 对话信息
}

// InputAudioBufferCommittedEvent 音频缓冲区已提交事件
type InputAudioBufferCommittedEvent struct {
	ServerEventBase
	PreviousItemID string `json:"previous_item_id,omitempty"` // 前一条消息ID
	ItemID         string `json:"item_id"`                    // 创建的消息ID
}

// InputAudioBufferClearedEvent 音频缓冲区已清空事件
type InputAudioBufferClearedEvent struct {
	ServerEventBase
}

// InputAudioBufferSpeechStartedEvent 语音开始事件（VAD 检测到语音）
type InputAudioBufferSpeechStartedEvent struct {
	ServerEventBase
	AudioStartMs int64  `json:"audio_start_ms"` // 语音开始位置（毫秒）
	ItemID       string `json:"item_id"`        // 对应的消息ID
}

// InputAudioBufferSpeechStoppedEvent 语音结束事件（VAD 检测到停顿）
type InputAudioBufferSpeechStoppedEvent struct {
	ServerEventBase
	AudioEndMs int64  `json:"audio_end_ms"` // 语音结束位置（毫秒）
	ItemID     string `json:"item_id"`      // 对应的消息ID
}

// ConversationItemCreatedEvent 对话条目创建事件
type ConversationItemCreatedEvent struct {
	ServerEventBase
	PreviousItemID string              `json:"previous_item_id,omitempty"` // 前一条消息ID
	Item           ResponseMessageItem `json:"item"`                       // 创建的消息条目
}

// ConversationItemInputAudioTranscriptionCompletedEvent 音频转写完成事件
type ConversationItemInputAudioTranscriptionCompletedEvent struct {
	ServerEventBase
	ItemID       string `json:"item_id"`       // 消息ID
	ContentIndex int    `json:"content_index"` // 内容索引
	Transcript   string `json:"transcript"`    // 转写文本
}

// ConversationItemInputAudioTranscriptionFailedEvent 音频转写失败事件
type ConversationItemInputAudioTranscriptionFailedEvent struct {
	ServerEventBase
	ItemID       string `json:"item_id"`       // 消息ID
	ContentIndex int    `json:"content_index"` // 内容索引
	Error        Error  `json:"error"`         // 错误详情
}

// ConversationItemTruncatedEvent 对话条目截断事件
type ConversationItemTruncatedEvent struct {
	ServerEventBase
	ItemID       string `json:"item_id"`       // 被截断的消息ID
	ContentIndex int    `json:"content_index"` // 内容索引
	AudioEndMs   int    `json:"audio_end_ms"`  // 截断位置（毫秒）
}

// ConversationItemDeletedEvent 对话条目删除事件
type ConversationItemDeletedEvent struct {
	ServerEventBase
	ItemID string `json:"item_id"` // 被删除的消息ID
}

// ResponseCreatedEvent 响应创建事件（开始生成响应）
type ResponseCreatedEvent struct {
	ServerEventBase
	Response Response `json:"response"` // 响应信息
}

// ResponseCancelledEvent 响应取消事件
type ResponseCancelledEvent struct {
	ServerEventBase
}

// ResponseDoneEvent 响应完成事件（响应生成结束）
type ResponseDoneEvent struct {
	ServerEventBase
	Response Response `json:"response"` // 响应信息（含 usage 统计）
}

// ResponseOutputItemAddedEvent 输出条目添加事件
type ResponseOutputItemAddedEvent struct {
	ServerEventBase
	ResponseID  string              `json:"response_id"`  // 响应ID
	OutputIndex int                 `json:"output_index"` // 输出索引
	Item        ResponseMessageItem `json:"item"`         // 输出条目
}

// ResponseOutputItemDoneEvent 输出条目完成事件
type ResponseOutputItemDoneEvent struct {
	ServerEventBase
	ResponseID  string              `json:"response_id"`  // 响应ID
	OutputIndex int                 `json:"output_index"` // 输出索引
	Item        ResponseMessageItem `json:"item"`         // 完成的条目
}

// ResponseContentPartAddedEvent 内容片段添加事件
type ResponseContentPartAddedEvent struct {
	ServerEventBase
	ResponseID   string             `json:"response_id"`   // 响应ID
	ItemID       string             `json:"item_id"`       // 消息ID
	OutputIndex  int                `json:"output_index"`  // 输出索引
	ContentIndex int                `json:"content_index"` // 内容索引
	Part         MessageContentPart `json:"part"`          // 内容片段
}

// ResponseContentPartDoneEvent 内容片段完成事件
type ResponseContentPartDoneEvent struct {
	ServerEventBase
	ResponseID   string             `json:"response_id"`   // 响应ID
	ItemID       string             `json:"item_id"`       // 消息ID
	OutputIndex  int                `json:"output_index"`  // 输出索引
	ContentIndex int                `json:"content_index"` // 内容索引
	Part         MessageContentPart `json:"part"`          // 完成的内容片段
}

// ResponseTextDeltaEvent 文本增量事件
type ResponseTextDeltaEvent struct {
	ServerEventBase
	ResponseID   string `json:"response_id"`   // 响应ID
	ItemID       string `json:"item_id"`       // 消息ID
	OutputIndex  int    `json:"output_index"`  // 输出索引
	ContentIndex int    `json:"content_index"` // 内容索引
	Delta        string `json:"delta"`         // 文本增量
}

// ResponseTextDoneEvent 文本完成事件
type ResponseTextDoneEvent struct {
	ServerEventBase
	ResponseID   string `json:"response_id"`   // 响应ID
	ItemID       string `json:"item_id"`       // 消息ID
	OutputIndex  int    `json:"output_index"`  // 输出索引
	ContentIndex int    `json:"content_index"` // 内容索引
	Text         string `json:"text"`          // 完整文本
}

// ResponseAudioTranscriptDeltaEvent 音频转写增量事件
type ResponseAudioTranscriptDeltaEvent struct {
	ServerEventBase
	ResponseID   string `json:"response_id"`   // 响应ID
	ItemID       string `json:"item_id"`       // 消息ID
	OutputIndex  int    `json:"output_index"`  // 输出索引
	ContentIndex int    `json:"content_index"` // 内容索引
	Delta        string `json:"delta"`         // 转写增量
}

// ResponseAudioTranscriptDoneEvent 音频转写完成事件
type ResponseAudioTranscriptDoneEvent struct {
	ServerEventBase
	ResponseID   string `json:"response_id"`   // 响应ID
	ItemID       string `json:"item_id"`       // 消息ID
	OutputIndex  int    `json:"output_index"`  // 输出索引
	ContentIndex int    `json:"content_index"` // 内容索引
	Transcript   string `json:"transcript"`    // 完整转写文本
}

// ResponseAudioDeltaEvent 音频增量事件（Base64 编码的音频数据）
type ResponseAudioDeltaEvent struct {
	ServerEventBase
	ResponseID   string `json:"response_id"`   // 响应ID
	ItemID       string `json:"item_id"`       // 消息ID
	OutputIndex  int    `json:"output_index"`  // 输出索引
	ContentIndex int    `json:"content_index"` // 内容索引
	Delta        string `json:"delta"`         // Base64 编码的音频增量
}

// ResponseAudioDoneEvent 音频完成事件
type ResponseAudioDoneEvent struct {
	ServerEventBase
	ResponseID   string `json:"response_id"`   // 响应ID
	ItemID       string `json:"item_id"`       // 消息ID
	OutputIndex  int    `json:"output_index"`  // 输出索引
	ContentIndex int    `json:"content_index"` // 内容索引
}

// ResponseFunctionCallArgumentsDeltaEvent 函数调用参数增量事件
type ResponseFunctionCallArgumentsDeltaEvent struct {
	ServerEventBase
	ResponseID  string `json:"response_id"`  // 响应ID
	ItemID      string `json:"item_id"`      // 消息ID
	OutputIndex int    `json:"output_index"` // 输出索引
	CallID      string `json:"call_id"`      // 函数调用ID
	Delta       string `json:"delta"`        // 参数增量（JSON 字符串）
}

// ResponseFunctionCallArgumentsDoneEvent 函数调用参数完成事件
type ResponseFunctionCallArgumentsDoneEvent struct {
	ServerEventBase
	ResponseID  string `json:"response_id"`  // 响应ID
	ItemID      string `json:"item_id"`      // 消息ID
	OutputIndex int    `json:"output_index"` // 输出索引
	CallID      string `json:"call_id"`      // 函数调用ID
	Arguments   string `json:"arguments"`    // 完整参数（JSON 字符串）
	Name        string `json:"name"`         // 函数名称
}

// RateLimitsUpdatedEvent 速率限制更新事件（每次 response.done 后发送）
type RateLimitsUpdatedEvent struct {
	ServerEventBase
	RateLimits []RateLimit `json:"rate_limits"` // 速率限制列表
}

// ======================== 服务端事件泛型约束 ========================

// ServerEventInterface 所有服务端事件的泛型约束（用于泛型反序列化）
type ServerEventInterface interface {
	ErrorEvent |
		SessionCreatedEvent |
		SessionUpdatedEvent |
		ConversationCreatedEvent |
		InputAudioBufferCommittedEvent |
		InputAudioBufferClearedEvent |
		InputAudioBufferSpeechStartedEvent |
		InputAudioBufferSpeechStoppedEvent |
		ConversationItemCreatedEvent |
		ConversationItemInputAudioTranscriptionCompletedEvent |
		ConversationItemInputAudioTranscriptionFailedEvent |
		ConversationItemTruncatedEvent |
		ConversationItemDeletedEvent |
		ResponseCreatedEvent |
		ResponseCancelledEvent |
		ResponseDoneEvent |
		ResponseOutputItemAddedEvent |
		ResponseOutputItemDoneEvent |
		ResponseContentPartAddedEvent |
		ResponseContentPartDoneEvent |
		ResponseTextDeltaEvent |
		ResponseTextDoneEvent |
		ResponseAudioTranscriptDeltaEvent |
		ResponseAudioTranscriptDoneEvent |
		ResponseAudioDeltaEvent |
		ResponseAudioDoneEvent |
		ResponseFunctionCallArgumentsDeltaEvent |
		ResponseFunctionCallArgumentsDoneEvent |
		RateLimitsUpdatedEvent
}

// ======================== 序列化/反序列化 ========================

// unmarshalServerEvent 泛型反序列化服务端事件；JSON 不合法时返回错误，不返回部分解析结果。
func unmarshalServerEvent[T ServerEventInterface](data []byte) (*T, error) {
	var t T
	err := json.Unmarshal(data, &t)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// MarshalServerEvent 将服务端事件序列化为 JSON 字节；失败仅发生在结构体本身不可序列化时。
func MarshalServerEvent(event ServerEvent) ([]byte, error) {
	return json.Marshal(event)
}

// UnmarshalServerEvent 反序列化 JSON 为服务端事件
// 核心逻辑：先提取 type 字段，再根据类型分发到具体结构体；旧版预览事件名映射到新版结构体
func UnmarshalServerEvent(data []byte) (ServerEvent, error) {
	// 先只解析 type 字段：类型未知时不再解析整个 payload，避免把未识别数据误当作已知事件。
	var eventType struct {
		Type ServerEventType `json:"type"`
	}
	if err := json.Unmarshal(data, &eventType); err != nil {
		return nil, fmt.Errorf("解析服务端事件类型失败: %w", err)
	}

	// 旧版预览事件名与新版事件共用同一结构体解析，保证升级过渡期客户端事件透传不受影响。
	switch eventType.Type {
	case ServerEventTypeError:
		return unmarshalServerEvent[ErrorEvent](data)
	case ServerEventTypeSessionCreated:
		return unmarshalServerEvent[SessionCreatedEvent](data)
	case ServerEventTypeSessionUpdated:
		return unmarshalServerEvent[SessionUpdatedEvent](data)
	case ServerEventTypeConversationCreated:
		return unmarshalServerEvent[ConversationCreatedEvent](data)
	case ServerEventTypeInputAudioBufferCommitted:
		return unmarshalServerEvent[InputAudioBufferCommittedEvent](data)
	case ServerEventTypeInputAudioBufferCleared:
		return unmarshalServerEvent[InputAudioBufferClearedEvent](data)
	case ServerEventTypeInputAudioBufferSpeechStarted:
		return unmarshalServerEvent[InputAudioBufferSpeechStartedEvent](data)
	case ServerEventTypeInputAudioBufferSpeechStopped:
		return unmarshalServerEvent[InputAudioBufferSpeechStoppedEvent](data)
	case ServerEventTypeConversationItemCreated:
		return unmarshalServerEvent[ConversationItemCreatedEvent](data)
	case ServerEventTypeConversationItemInputAudioTranscriptionCompleted:
		return unmarshalServerEvent[ConversationItemInputAudioTranscriptionCompletedEvent](data)
	case ServerEventTypeConversationItemInputAudioTranscriptionFailed:
		return unmarshalServerEvent[ConversationItemInputAudioTranscriptionFailedEvent](data)
	case ServerEventTypeConversationItemTruncated:
		return unmarshalServerEvent[ConversationItemTruncatedEvent](data)
	case ServerEventTypeConversationItemDeleted:
		return unmarshalServerEvent[ConversationItemDeletedEvent](data)
	case ServerEventTypeResponseCreated:
		return unmarshalServerEvent[ResponseCreatedEvent](data)
	case ServerEventTypeResponseCancelled:
		return unmarshalServerEvent[ResponseCancelledEvent](data)
	case ServerEventTypeResponseDone:
		return unmarshalServerEvent[ResponseDoneEvent](data)
	case ServerEventTypeResponseOutputItemAdded:
		return unmarshalServerEvent[ResponseOutputItemAddedEvent](data)
	case ServerEventTypeResponseOutputItemDone:
		return unmarshalServerEvent[ResponseOutputItemDoneEvent](data)
	case ServerEventTypeResponseContentPartAdded:
		return unmarshalServerEvent[ResponseContentPartAddedEvent](data)
	case ServerEventTypeResponseContentPartDone:
		return unmarshalServerEvent[ResponseContentPartDoneEvent](data)
	case ServerEventTypeResponseTextDelta, ServerEventTypeLegacyResponseTextDelta:
		return unmarshalServerEvent[ResponseTextDeltaEvent](data)
	case ServerEventTypeResponseTextDone, ServerEventTypeLegacyResponseTextDone:
		return unmarshalServerEvent[ResponseTextDoneEvent](data)
	case ServerEventTypeResponseAudioTranscriptDelta, ServerEventTypeLegacyResponseAudioTranscriptDelta:
		return unmarshalServerEvent[ResponseAudioTranscriptDeltaEvent](data)
	case ServerEventTypeResponseAudioTranscriptDone, ServerEventTypeLegacyResponseAudioTranscriptDone:
		return unmarshalServerEvent[ResponseAudioTranscriptDoneEvent](data)
	case ServerEventTypeResponseAudioDelta, ServerEventTypeLegacyResponseAudioDelta:
		return unmarshalServerEvent[ResponseAudioDeltaEvent](data)
	case ServerEventTypeResponseAudioDone, ServerEventTypeLegacyResponseAudioDone:
		return unmarshalServerEvent[ResponseAudioDoneEvent](data)
	case ServerEventTypeResponseFunctionCallArgumentsDelta:
		return unmarshalServerEvent[ResponseFunctionCallArgumentsDeltaEvent](data)
	case ServerEventTypeResponseFunctionCallArgumentsDone:
		return unmarshalServerEvent[ResponseFunctionCallArgumentsDoneEvent](data)
	case ServerEventTypeRateLimitsUpdated:
		return unmarshalServerEvent[RateLimitsUpdatedEvent](data)
	default:
		// 未识别的事件类型返回错误而非静默忽略，便于上游新增事件时及时适配。
		return nil, fmt.Errorf("未知的服务端事件类型: %s", eventType.Type)
	}
}
