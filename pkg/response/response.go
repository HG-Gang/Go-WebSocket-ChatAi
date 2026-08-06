// pkg/response/response.go
// 全局统一响应格式（App 端 JSON 返回结构）
// 所有返回给 App 的消息都必须使用此格式包装
// 文件功能：定义并构造返回给 App 的统一 JSON 响应结构。输入为业务状态码、响应事件
// 类型与内容；输出为 StandardResponse 及其 JSON 字节流，响应 ID 与时间戳在此自动填充。
// 不负责：业务错误码的语义定义（见 pkg/errors）与消息的发送/推送。
package response

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// ResponseEvent 响应事件类型（标识当前消息处于对话流程的哪个阶段）
type ResponseEvent string

const (
	EventBegin               ResponseEvent = "begin"                 // 响应开始
	EventEnd                 ResponseEvent = "end"                   // 响应结束
	EventTextDelta           ResponseEvent = "text_delta"            // 文本增量
	EventAudioDelta          ResponseEvent = "audio_delta"           // 音频增量
	EventTranscriptTextDelta ResponseEvent = "transcript_text_delta" // 音频转写文本增量
	EventError               ResponseEvent = "error"                 // 错误
	EventHeartbeat           ResponseEvent = "heartbeat"             // 心跳
	EventSessionCreated      ResponseEvent = "session_created"       // 会话创建
	EventSessionUpdated      ResponseEvent = "session_updated"       // 会话更新
	EventSessionRestored     ResponseEvent = "session_restored"      // OpenAI 上游重连后会话已恢复
	EventReconnectRequired   ResponseEvent = "reconnect_required"    // 需要重连
)

// StandardResponse 统一响应结构体
// 所有发送给 App 的消息都使用此结构包装
type StandardResponse struct {
	Code           int           `json:"code"`              // 业务状态码（0=成功，非0=错误）
	Response       ResponseEvent `json:"response"`          // 响应事件类型
	ResponseID     string        `json:"responseId"`        // 响应ID（兼容旧 App 的 responseId 字段）
	Content        interface{}   `json:"content,omitempty"` // 内容（文本/音频数据/错误信息等）
	InputTimestamp int64         `json:"input_timestamp"`   // 输入时间戳（毫秒）
	RespTimestamp  int64         `json:"resp_timestamp"`    // 响应时间戳（毫秒）
}

// NewResponse 创建标准响应（自动填充时间戳和响应ID）
// 无失败路径：响应 ID 生成极端失败时降级回退（见 generateResponseID），始终返回可用对象。
func NewResponse(code int, event ResponseEvent, content interface{}) *StandardResponse {
	return &StandardResponse{
		Code:           code,
		Response:       event,
		ResponseID:     generateResponseID(),
		Content:        content,
		InputTimestamp: time.Now().UnixMilli(),
		RespTimestamp:  time.Now().UnixMilli(),
	}
}

// NewResponseWithID 创建标准响应（使用指定的 responseId）
// 无失败路径：沿用调用方传入的响应 ID 与输入时间戳，仅响应时间戳取当前毫秒时间。
func NewResponseWithID(code int, event ResponseEvent, responseID string, content interface{}, inputTs int64) *StandardResponse {
	return &StandardResponse{
		Code:           code,
		Response:       event,
		ResponseID:     responseID,
		Content:        content,
		InputTimestamp: inputTs,
		RespTimestamp:  time.Now().UnixMilli(),
	}
}

// Success 成功响应快捷方法
// 等价于 NewResponse(0, event, content)，无失败路径。
func Success(event ResponseEvent, content interface{}) *StandardResponse {
	return NewResponse(0, event, content)
}

// Error 错误响应快捷方法
// 等价于 NewResponse(code, EventError, {"message": message})，无失败路径。
func Error(code int, message string) *StandardResponse {
	return NewResponse(code, EventError, map[string]string{"message": message})
}

// ToJSON 序列化为 JSON 字节流
// 序列化失败时返回错误；同时输出 responseId 与 response_id 两个字段，兼容新旧 App。
func (r *StandardResponse) ToJSON() ([]byte, error) {
	type alias StandardResponse
	// 通过内嵌 alias 展开全部原有字段，并追加 snake_case 的 response_id：
	// 旧 App 读取 camelCase 的 responseId，新 App 读取 response_id，双字段并存
	// 避免改版过渡期旧端解析失败。
	payload := struct {
		*alias
		ResponseIDSnake string `json:"response_id,omitempty"`
	}{
		alias:           (*alias)(r),
		ResponseIDSnake: r.ResponseID,
	}
	return json.Marshal(payload)
}

// generateResponseID 生成唯一响应ID
// UUIDv7 生成失败时回退到 UUIDv4，保证任何环境下都能返回可用 ID，不向调用方抛错。
func generateResponseID() string {
	v7, err := uuid.NewV7()
	if err != nil {
		return uuid.New().String()
	}
	return v7.String()
}
