// pkg/protocol/openai/types.go
// 文件功能：定义 OpenAI Realtime API 的共享基础类型——特殊数值、音频格式、模态、
// 断句检测、会话配置、消息结构与 Usage 等，并实现 IntOrInf 的 JSON 序列化边界。
// 参考 go-xiaozhi 的完整实现。输入：无（纯类型定义）；输出：供客户端与服务端
// 事件结构体（server_events.go、client_events.go）复用的基础类型。
// 不负责：具体事件结构的定义与事件的 JSON 序列化/反序列化分发。
package openai

import (
	"encoding/json"
	"math"
)

// ======================== 特殊数值类型 ========================

// Inf 表示无限大（用于 max_response_output_tokens = "inf"）
// 以 math.MaxInt 作为内部哨兵值，合法的 token 数不会达到该值，可与普通整数形式区分。
const Inf IntOrInf = IntOrInf(math.MaxInt)

// IntOrInf 可以是整数或 "inf" 的类型（OpenAI API 中 max_output_tokens 字段支持 "inf"）
type IntOrInf int

// IsInf 判断是否为无限大
func (m IntOrInf) IsInf() bool {
	return m == Inf
}

// MarshalJSON 自定义序列化：Inf → "inf"，其他 → 整数
func (m IntOrInf) MarshalJSON() ([]byte, error) {
	if m == Inf {
		return []byte("\"inf\""), nil
	}
	return json.Marshal(int(m))
}

// UnmarshalJSON 自定义反序列化："inf" → Inf，其他 → 整数
func (m *IntOrInf) UnmarshalJSON(data []byte) error {
	if string(data) == "\"inf\"" {
		*m = Inf
		return nil
	}
	// 空字节按零值（0）处理：上游缺省该字段时不会走到 json.Unmarshal，避免空串解析报错。
	if len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, (*int)(m))
}

// ======================== 客户端事件基础 ========================

// ClientEventType 客户端事件类型（字符串枚举）
type ClientEventType string

// ClientEvent 客户端事件接口（所有客户端事件必须实现）
type ClientEvent interface {
	ClientEventType() ClientEventType // 返回事件类型
	GetEventID() string               // 返回事件ID
}

// ClientEventBase 客户端事件基础结构体（所有客户端事件的嵌入父结构）
type ClientEventBase struct {
	EventID string          `json:"event_id,omitempty"` // 事件唯一ID（可选）
	Type    ClientEventType `json:"type"`               // 事件类型标识
}

// ClientEventType 返回事件类型
func (e ClientEventBase) ClientEventType() ClientEventType {
	return e.Type
}

// GetEventID 返回事件ID
func (e ClientEventBase) GetEventID() string {
	return e.EventID
}

// ======================== 服务端事件基础 ========================

// ServerEventType 服务端事件类型（字符串枚举）
type ServerEventType string

// ServerEvent 服务端事件接口（所有服务端事件必须实现）
type ServerEvent interface {
	ServerEventType() ServerEventType // 返回事件类型
	GetEventID() string               // 返回事件ID
}

// ServerEventBase 服务端事件基础结构体（所有服务端事件的嵌入父结构）
type ServerEventBase struct {
	EventID string          `json:"event_id,omitempty"` // 事件唯一ID
	Type    ServerEventType `json:"type"`               // 事件类型标识
}

// ServerEventType 返回事件类型
func (m *ServerEventBase) ServerEventType() ServerEventType {
	return m.Type
}

// GetEventID 返回事件ID
func (m *ServerEventBase) GetEventID() string {
	return m.EventID
}

// ======================== 音频/语音类型 ========================

// Voice 语音音色类型（OpenAI 支持的所有音色）
type Voice string

const (
	VoiceAlloy   Voice = "alloy"
	VoiceAsh     Voice = "ash"
	VoiceBallad  Voice = "ballad"
	VoiceCoral   Voice = "coral"
	VoiceEcho    Voice = "echo"
	VoiceSage    Voice = "sage"
	VoiceShimmer Voice = "shimmer"
	VoiceVerse   Voice = "verse"
)

// AudioFormat 音频格式类型
type AudioFormat string

const (
	AudioFormatPCM16    AudioFormat = "pcm16"     // 16位PCM格式（默认）
	AudioFormatG711ULaw AudioFormat = "g711_ulaw" // G.711 μ-law
	AudioFormatG711ALaw AudioFormat = "g711_alaw" // G.711 A-law
)

// Modality 模态类型（文本/音频）
type Modality string

const (
	ModalityText  Modality = "text"  // 文本模态
	ModalityAudio Modality = "audio" // 音频模态
)

// ======================== 断句检测 ========================

// ClientTurnDetectionType 客户端断句检测类型
type ClientTurnDetectionType string

const (
	ClientTurnDetectionTypeServerVad   ClientTurnDetectionType = "server_vad" // 服务端VAD检测
	ClientTurnDetectionTypeUnspecified ClientTurnDetectionType = ""           // 未指定（手动模式）
)

// TurnDetection 断句检测配置
type TurnDetection struct {
	Type              ClientTurnDetectionType `json:"type"`                          // 检测类型
	Threshold         float64                 `json:"threshold,omitempty"`           // 音量阈值
	PrefixPaddingMs   int                     `json:"prefix_padding_ms,omitempty"`   // 前缀填充毫秒数
	SilenceDurationMs int                     `json:"silence_duration_ms,omitempty"` // 静音持续毫秒数
}

// ======================== 输入音频转写 ========================

// InputAudioTranscription 输入音频转写配置
type InputAudioTranscription struct {
	Model string `json:"model"` // 转写模型（如 whisper-1）
}

// ======================== 工具/函数调用 ========================

// ToolType 工具类型
type ToolType string

const (
	ToolTypeFunction ToolType = "function" // 函数类型工具
)

// ToolChoiceAuto/None/Required 工具选择策略
const (
	ToolChoiceAuto     = "auto"     // 自动选择
	ToolChoiceNone     = "none"     // 不使用工具
	ToolChoiceRequired = "required" // 强制使用工具
)

// Tool 工具定义（函数调用）
type Tool struct {
	Type        ToolType `json:"type"`        // 工具类型
	Name        string   `json:"name"`        // 工具名称
	Description string   `json:"description"` // 工具描述
	Parameters  any      `json:"parameters"`  // 工具参数 Schema
}

// ======================== 消息类型 ========================

// MessageRole 消息角色
type MessageRole string

const (
	MessageRoleSystem    MessageRole = "system"    // 系统消息
	MessageRoleAssistant MessageRole = "assistant" // 助手消息
	MessageRoleUser      MessageRole = "user"      // 用户消息
)

// MessageItemType 消息条目类型
type MessageItemType string

const (
	MessageItemTypeMessage            MessageItemType = "message"              // 普通消息
	MessageItemTypeFunctionCall       MessageItemType = "function_call"        // 函数调用
	MessageItemTypeFunctionCallOutput MessageItemType = "function_call_output" // 函数调用结果
)

// MessageContentType 消息内容类型
type MessageContentType string

const (
	MessageContentTypeText       MessageContentType = "text"        // 纯文本
	MessageContentTypeAudio      MessageContentType = "audio"       // 音频
	MessageContentTypeInputText  MessageContentType = "input_text"  // 输入文本
	MessageContentTypeInputAudio MessageContentType = "input_audio" // 输入音频
)

// MessageContentPart 消息内容片段
type MessageContentPart struct {
	Type       MessageContentType `json:"type"`                 // 内容类型
	Text       *string            `json:"text,omitempty"`       // 文本内容
	Audio      *string            `json:"audio,omitempty"`      // Base64编码音频
	Transcript *string            `json:"transcript,omitempty"` // 音频转写文本
}

// MessageItem 消息条目（对话中的一条消息）
type MessageItem struct {
	ID      string               `json:"id"`      // 消息唯一ID
	Type    MessageItemType      `json:"type"`    // 消息类型
	Status  ItemStatus           `json:"status"`  // 消息状态
	Role    MessageRole          `json:"role"`    // 角色
	Content []MessageContentPart `json:"content"` // 内容列表
}

// ResponseMessageItem 响应消息条目（带 object 字段）
type ResponseMessageItem struct {
	MessageItem
	Object string `json:"object,omitempty"` // 对象类型（realtime.item）
}

// ======================== 对象类型常量 ========================

const (
	ObjectRealtimeSession string = "realtime.session"      // 会话对象
	ObjectConversation    string = "realtime.conversation" // 对话对象
	ObjectResponse        string = "realtime.response"     // 响应对象
	ObjectItem            string = "realtime.item"         // 条目对象
)

// ======================== 状态枚举 ========================

// ItemStatus 条目状态
type ItemStatus string

const (
	ItemStatusInProgress ItemStatus = "in_progress" // 处理中
	ItemStatusCompleted  ItemStatus = "completed"   // 已完成
	ItemStatusIncomplete ItemStatus = "incomplete"  // 不完整
)

// ResponseStatus 响应状态
type ResponseStatus string

const (
	ResponseStatusInProgress ResponseStatus = "in_progress" // 处理中
	ResponseStatusCompleted  ResponseStatus = "completed"   // 已完成
	ResponseStatusCancelled  ResponseStatus = "cancelled"   // 已取消
	ResponseStatusIncomplete ResponseStatus = "incomplete"  // 不完整
	ResponseStatusFailed     ResponseStatus = "failed"      // 失败
)

// ======================== 错误类型 ========================

// Error OpenAI Realtime API 错误结构体
type Error struct {
	Message string `json:"message,omitempty"`  // 错误消息
	Type    string `json:"type,omitempty"`     // 错误类型（如 invalid_request_error）
	Code    string `json:"code,omitempty"`     // 错误码
	Param   string `json:"param,omitempty"`    // 相关参数
	EventID string `json:"event_id,omitempty"` // 触发错误的客户端事件ID
}

// ======================== 会话配置（服务端返回） ========================

// ServerSession 服务端会话配置（session.created / session.updated 事件返回）
type ServerSession struct {
	ID                      string                   `json:"id"`                                   // 会话ID
	Object                  string                   `json:"object"`                               // 对象类型
	Model                   string                   `json:"model"`                                // 模型名称
	Modalities              []Modality               `json:"modalities,omitempty"`                 // 支持的模态
	Instructions            string                   `json:"instructions,omitempty"`               // 系统指令
	Voice                   Voice                    `json:"voice,omitempty"`                      // 音色
	InputAudioFormat        AudioFormat              `json:"input_audio_format,omitempty"`         // 输入音频格式
	OutputAudioFormat       AudioFormat              `json:"output_audio_format,omitempty"`        // 输出音频格式
	InputAudioTranscription *InputAudioTranscription `json:"input_audio_transcription,omitempty"`  // 输入转写配置
	TurnDetection           *TurnDetection           `json:"turn_detection,omitempty"`             // 断句检测配置
	Tools                   []Tool                   `json:"tools,omitempty"`                      // 可用工具
	Temperature             float32                  `json:"temperature,omitempty"`                // 采样温度
	MaxOutputTokens         IntOrInf                 `json:"max_response_output_tokens,omitempty"` // 最大输出token数
}

// ======================== 对话/响应结构 ========================

// Conversation 对话结构
type Conversation struct {
	ID     string `json:"id"`     // 对话ID
	Object string `json:"object"` // 对象类型（realtime.conversation）
}

// TokenUsageDetails 是 OpenAI usage 中的模态明细。
// 不同模型返回的明细字段可能不完整，未返回时保持 0，由 billing 层标记明细来源。
type TokenUsageDetails struct {
	TextTokens      int `json:"text_tokens,omitempty"`      // 文本 token
	AudioTokens     int `json:"audio_tokens,omitempty"`     // 音频 token
	CachedTokens    int `json:"cached_tokens,omitempty"`    // 命中缓存的输入 token
	ReasoningTokens int `json:"reasoning_tokens,omitempty"` // 推理 token（部分模型可能返回）
}

// Usage Token 使用统计
type Usage struct {
	TotalTokens        int                `json:"total_tokens"`                   // 总 token 数
	InputTokens        int                `json:"input_tokens"`                   // 输入 token 数
	OutputTokens       int                `json:"output_tokens"`                  // 输出 token 数
	InputTokenDetails  *TokenUsageDetails `json:"input_token_details,omitempty"`  // 输入 token 明细
	OutputTokenDetails *TokenUsageDetails `json:"output_token_details,omitempty"` // 输出 token 明细
}

// Response 响应结构
type Response struct {
	ID            string                `json:"id"`                       // 响应ID
	Object        string                `json:"object"`                   // 对象类型
	Status        ResponseStatus        `json:"status"`                   // 响应状态
	StatusDetails any                   `json:"status_details,omitempty"` // 状态详情
	Output        []ResponseMessageItem `json:"output"`                   // 输出条目列表
	Usage         *Usage                `json:"usage,omitempty"`          // Token 使用统计
}

// RateLimit 速率限制信息
type RateLimit struct {
	Name         string  `json:"name"`          // 限制名称（requests/tokens）
	Limit        int     `json:"limit"`         // 最大值
	Remaining    int     `json:"remaining"`     // 剩余值
	ResetSeconds float64 `json:"reset_seconds"` // 重置秒数
}
