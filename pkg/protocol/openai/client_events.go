// pkg/protocol/openai/client_events.go
// 文件功能：定义 OpenAI Realtime API 的客户端事件结构体及其 JSON 序列化/反序列化，
// 覆盖 OpenAI 官方文档列出的全部客户端事件类型（参考 go-xiaozhi 的完整实现）。
// 输入：App 侧经 WebSocket 发来的单条事件 JSON 字节；输出：按 type 字段分发后的
// 具体客户端事件结构体，或序列化后的 JSON 字节。
// 不负责：事件的鉴权、业务校验与转发（见 internal/provider/openai/events_client.go）。
// 序列化边界：事件 type 值必须与 OpenAI 线协议完全一致，客户端结构与 OpenAI 侧
// 字段一一对应；UnmarshalClientEvent 对未知类型失败返回，不透传畸形事件。
package openai

import (
	"encoding/json"
	"fmt"
)

// ======================== 客户端事件类型常量 ========================
// 完整对齐 OpenAI Realtime API 文档：
// https://platform.openai.com/docs/guides/realtime/client-events
const (
	ClientEventTypeSessionUpdate            ClientEventType = "session.update"             // 更新会话配置
	ClientEventTypeInputAudioBufferAppend   ClientEventType = "input_audio_buffer.append"  // 追加音频到缓冲区
	ClientEventTypeInputAudioBufferCommit   ClientEventType = "input_audio_buffer.commit"  // 提交音频缓冲区
	ClientEventTypeInputAudioBufferClear    ClientEventType = "input_audio_buffer.clear"   // 清空音频缓冲区
	ClientEventTypeConversationItemCreate   ClientEventType = "conversation.item.create"   // 创建对话条目
	ClientEventTypeConversationItemTruncate ClientEventType = "conversation.item.truncate" // 截断对话条目
	ClientEventTypeConversationItemDelete   ClientEventType = "conversation.item.delete"   // 删除对话条目
	ClientEventTypeResponseCreate           ClientEventType = "response.create"            // 触发生成响应
	ClientEventTypeResponseCancel           ClientEventType = "response.cancel"            // 取消响应生成
)

// ======================== 客户端会话配置 ========================

// ClientSession 客户端会话配置（session.update 事件的核心字段）
// 用于更新会话的默认配置，如模态、音色、音频格式、断句检测等
type ClientSession struct {
	Modalities              []Modality               `json:"modalities,omitempty"`                 // 支持的模态（text/audio）
	Instructions            *string                  `json:"instructions,omitempty"`               // 系统指令
	Voice                   *Voice                   `json:"voice,omitempty"`                      // 语音音色
	InputAudioFormat        *AudioFormat             `json:"input_audio_format,omitempty"`         // 输入音频格式
	OutputAudioFormat       *AudioFormat             `json:"output_audio_format,omitempty"`        // 输出音频格式
	InputAudioTranscription *InputAudioTranscription `json:"input_audio_transcription,omitempty"`  // 输入音频转写配置
	TurnDetection           *TurnDetection           `json:"turn_detection"`                       // 断句检测配置（nil=手动模式）
	Tools                   []Tool                   `json:"tools,omitempty"`                      // 可用工具
	ToolChoice              interface{}              `json:"tool_choice,omitempty"`                // 工具选择策略
	Temperature             *float32                 `json:"temperature,omitempty"`                // 采样温度
	MaxOutputTokens         *IntOrInf                `json:"max_response_output_tokens,omitempty"` // 最大输出token数
}

// ======================== 客户端事件结构体 ========================

// SessionUpdateEvent 会话更新事件（客户端 → OpenAI）
// 用途：修改会话配置（如切换模态、更新系统指令、调整音频格式等）
type SessionUpdateEvent struct {
	ClientEventBase
	Session ClientSession `json:"session"` // 会话配置
}

func (m SessionUpdateEvent) ClientEventType() ClientEventType {
	return ClientEventTypeSessionUpdate
}

// InputAudioBufferAppendEvent 音频缓冲区追加事件（客户端 → OpenAI）
// 用途：将 Base64 编码的 PCM16 音频数据追加到服务端缓冲区
type InputAudioBufferAppendEvent struct {
	ClientEventBase
	Audio string `json:"audio"` // Base64 编码的音频数据
}

func (m InputAudioBufferAppendEvent) ClientEventType() ClientEventType {
	return ClientEventTypeInputAudioBufferAppend
}

// InputAudioBufferCommitEvent 音频缓冲区提交事件（客户端 → OpenAI）
// 用途：告知服务端音频输入完成，可以开始处理
type InputAudioBufferCommitEvent struct {
	ClientEventBase
}

func (m InputAudioBufferCommitEvent) ClientEventType() ClientEventType {
	return ClientEventTypeInputAudioBufferCommit
}

// InputAudioBufferClearEvent 音频缓冲区清空事件（客户端 → OpenAI）
// 用途：清空服务端音频缓冲区
type InputAudioBufferClearEvent struct {
	ClientEventBase
}

func (m InputAudioBufferClearEvent) ClientEventType() ClientEventType {
	return ClientEventTypeInputAudioBufferClear
}

// ConversationItemCreateEvent 创建对话条目事件（客户端 → OpenAI）
// 用途：手动向对话中添加消息条目
type ConversationItemCreateEvent struct {
	ClientEventBase
	PreviousItemID string      `json:"previous_item_id,omitempty"` // 前一条消息ID
	Item           MessageItem `json:"item"`                       // 消息条目
}

func (m ConversationItemCreateEvent) ClientEventType() ClientEventType {
	return ClientEventTypeConversationItemCreate
}

// ConversationItemTruncateEvent 截断对话条目事件（客户端 → OpenAI）
// 用途：截断助手消息的音频输出
type ConversationItemTruncateEvent struct {
	ClientEventBase
	ItemID       string `json:"item_id"`       // 要截断的消息ID
	ContentIndex int    `json:"content_index"` // 内容片段索引
	AudioEndMs   int    `json:"audio_end_ms"`  // 音频截断位置（毫秒）
}

func (m ConversationItemTruncateEvent) ClientEventType() ClientEventType {
	return ClientEventTypeConversationItemTruncate
}

// ConversationItemDeleteEvent 删除对话条目事件（客户端 → OpenAI）
// 用途：从对话中删除指定消息
type ConversationItemDeleteEvent struct {
	ClientEventBase
	ItemID string `json:"item_id"` // 要删除的消息ID
}

func (m ConversationItemDeleteEvent) ClientEventType() ClientEventType {
	return ClientEventTypeConversationItemDelete
}

// ResponseCreateEvent 创建响应事件（客户端 → OpenAI）
// 用途：触发模型生成响应
type ResponseCreateEvent struct {
	ClientEventBase
}

func (m ResponseCreateEvent) ClientEventType() ClientEventType {
	return ClientEventTypeResponseCreate
}

// ResponseCancelEvent 取消响应事件（客户端 → OpenAI）
// 用途：取消正在生成的响应
type ResponseCancelEvent struct {
	ClientEventBase
}

func (m ResponseCancelEvent) ClientEventType() ClientEventType {
	return ClientEventTypeResponseCancel
}

// ======================== 客户端事件泛型约束 ========================

// ClientEventInterface 所有客户端事件的泛型约束（用于泛型反序列化）
type ClientEventInterface interface {
	SessionUpdateEvent |
		InputAudioBufferAppendEvent |
		InputAudioBufferCommitEvent |
		InputAudioBufferClearEvent |
		ConversationItemCreateEvent |
		ConversationItemTruncateEvent |
		ConversationItemDeleteEvent |
		ResponseCreateEvent |
		ResponseCancelEvent
}

// ======================== 序列化/反序列化 ========================

// unmarshalClientEvent 泛型反序列化客户端事件；JSON 不合法时返回错误，不返回部分解析结果。
func unmarshalClientEvent[T ClientEventInterface](data []byte) (*T, error) {
	var t T
	err := json.Unmarshal(data, &t)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// MarshalClientEvent 将客户端事件序列化为 JSON 字节；失败仅发生在结构体本身不可序列化时。
func MarshalClientEvent(event ClientEvent) ([]byte, error) {
	return json.Marshal(event)
}

// UnmarshalClientEvent 反序列化 JSON 为客户端事件
// 核心逻辑：先提取 type 字段，再根据类型分发到具体结构体
func UnmarshalClientEvent(data []byte) (ClientEvent, error) {
	// 先只解析 type 字段：类型未知时不再解析剩余 payload，避免未识别数据被误当作已知事件。
	var eventType struct {
		Type ClientEventType `json:"type"`
	}
	if err := json.Unmarshal(data, &eventType); err != nil {
		return nil, fmt.Errorf("解析客户端事件类型失败: %w", err)
	}

	// 按 type 分发到对应结构体二次解析；未匹配的 case 落入 default 返回错误。
	switch eventType.Type {
	case ClientEventTypeSessionUpdate:
		return unmarshalClientEvent[SessionUpdateEvent](data)
	case ClientEventTypeInputAudioBufferAppend:
		return unmarshalClientEvent[InputAudioBufferAppendEvent](data)
	case ClientEventTypeInputAudioBufferCommit:
		return unmarshalClientEvent[InputAudioBufferCommitEvent](data)
	case ClientEventTypeInputAudioBufferClear:
		return unmarshalClientEvent[InputAudioBufferClearEvent](data)
	case ClientEventTypeConversationItemCreate:
		return unmarshalClientEvent[ConversationItemCreateEvent](data)
	case ClientEventTypeConversationItemTruncate:
		return unmarshalClientEvent[ConversationItemTruncateEvent](data)
	case ClientEventTypeConversationItemDelete:
		return unmarshalClientEvent[ConversationItemDeleteEvent](data)
	case ClientEventTypeResponseCreate:
		return unmarshalClientEvent[ResponseCreateEvent](data)
	case ClientEventTypeResponseCancel:
		return unmarshalClientEvent[ResponseCancelEvent](data)
	default:
		// 未识别的事件类型返回错误，调用方不得将其透传至 OpenAI 上游连接，
		// 防止畸形事件破坏上游协议状态。
		return nil, fmt.Errorf("未知的客户端事件类型: %s", eventType.Type)
	}
}
