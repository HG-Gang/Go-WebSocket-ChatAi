// internal/provider/openairesponses/client.go
// 文件功能：OpenAI Responses API（/v1/responses）的 HTTP 调用封装。输入为接近官方格式的
// payload map 与模型配置，输出为解析后的 Result（含原始响应）或 error；不负责 Realtime
// WebSocket 四协程链路（见 internal/provider/openai）。
//
// 安全边界：API key 仅作为 Authorization Bearer 请求头发送给上游，不写入日志；调试快照
// 与错误信息不回显密钥原文；未初始化、未启用或缺 key 时返回错误失败关闭。
package openairesponses

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/logger"
)

const (
	defaultEndpoint  = "https://api.openai.com/v1" // 上游默认 base URL，配置未指定时使用
	defaultModel     = "gpt-4.1"                   // 请求未指定 model 时的默认模型
	defaultTimeoutMs = 60000                       // 默认请求超时（毫秒）
)

// Client 是 OpenAI Responses API 的轻量 HTTP 客户端。
// 设计目标：让业务层传入接近官方接口的 JSON，网关只补齐模型、指令、超时和鉴权。
type Client struct {
	cfg        *conf.ModelConfig
	httpClient *http.Client
}

// Result 保留 OpenAI 原始响应，同时提取常用字段，方便调试页面直接展示。
type Result struct {
	StatusCode int             `json:"status_code"`
	Raw        json.RawMessage `json:"raw"`
	ID         string          `json:"id,omitempty"`
	Model      string          `json:"model,omitempty"`
	Status     string          `json:"status,omitempty"`
	OutputText string          `json:"output_text,omitempty"`
}

// New 创建 Responses API 客户端。
// cfg 为空时按空配置创建；Timeout 取配置的 timeout_ms（毫秒）或 timeout（秒），
// 均未配置时回落到默认 60 秒。
func New(cfg *conf.ModelConfig) *Client {
	if cfg == nil {
		cfg = &conf.ModelConfig{}
	}
	return &Client{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: time.Duration(timeoutMs(cfg)) * time.Millisecond,
		},
	}
}

// Create 调用 OpenAI Responses API 创建一次模型响应。
// payload 由调用方提供（接近官方请求格式），本方法只补齐默认字段并附加鉴权头；
// 上游返回 2xx 时返回解析后的 Result；非 2xx 时返回带原始响应体的 Result 同时返回
// error，网络/序列化错误时只返回 error。
func (c *Client) Create(ctx context.Context, payload map[string]any) (*Result, error) {
	// 前置校验失败关闭：客户端未初始化、模型未启用或缺 key 都直接返回错误，
	// 避免空 key 请求打到上游。
	if c == nil || c.cfg == nil {
		return nil, fmt.Errorf("Responses API 配置未初始化")
	}
	if !c.cfg.Enabled {
		return nil, fmt.Errorf("Responses API 未启用")
	}
	if strings.TrimSpace(c.cfg.APIKey) == "" {
		return nil, fmt.Errorf("Responses API 缺少 OPENAI_API_KEY")
	}
	if payload == nil {
		payload = map[string]any{}
	}
	// 只补齐调用方未显式传入的默认字段，不覆盖业务请求中的显式值。
	normalizePayload(payload, c.cfg)

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("序列化 Responses API 请求失败: %w", err)
	}

	upstreamURL := responsesURL(c.cfg)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, upstreamURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("创建 Responses API 请求失败: %w", err)
	}
	// 鉴权头只在此处组装：Authorization 为 Bearer + API key，Content-Type 固定 JSON；
	// Organization 仅在配置显式给出时透传。
	req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	req.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(c.cfg.Organization) != "" {
		req.Header.Set("OpenAI-Organization", c.cfg.Organization)
	}

	// 上游网络错误时回传脱敏信息，避免 API key 或完整 URL 进入错误链与日志。
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求 Responses API 失败: endpoint=%s error=%s", logger.SafeURLForDisplay(upstreamURL), logger.RedactField("content", err.Error()))
	}
	defer resp.Body.Close()

	// 限流读取响应体到 16MiB：上限防止超大响应耗尽内存，同时保留足够空间容纳完整输出；
	// 读取失败视为整体请求失败。
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, fmt.Errorf("读取 Responses API 响应失败: %w", err)
	}
	result := parseResult(resp.StatusCode, raw)
	// 非 2xx 时仍返回解析后的 Result（携带原始响应供调试展示），同时返回 error，
	// 调用方应优先处理 error，不能把错误响应当成功结果使用。
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return result, fmt.Errorf("Responses API 返回异常状态码: %d", resp.StatusCode)
	}
	return result, nil
}

// Status 返回可公开展示的配置快照，不包含 API Key 原文。
func Status(cfg *conf.ModelConfig) map[string]any {
	if cfg == nil {
		cfg = &conf.ModelConfig{}
	}
	return map[string]any{
		"enabled":            cfg.Enabled,
		"default_model":      stringOrDefault(cfg.DefaultModel, defaultModel),
		"endpoint":           logger.SafeURLForDisplay(stringOrDefault(cfg.Endpoint, defaultEndpoint)),
		"api_key_configured": strings.TrimSpace(cfg.APIKey) != "",
		"timeout_ms":         timeoutMs(cfg),
		"store_default":      defaultStore(cfg),
		"instructions_set":   strings.TrimSpace(cfg.Instructions) != "",
		"route":              "/api/openai/responses",
		"upstream_path":      "/v1/responses",
	}
}

// normalizePayload 只补齐调用方没有显式传入的默认值（model、instructions、store），
// 避免网关覆盖业务请求；instructions 仅在配置非空时补入。
func normalizePayload(payload map[string]any, cfg *conf.ModelConfig) {
	if _, ok := payload["model"]; !ok {
		payload["model"] = stringOrDefault(cfg.DefaultModel, defaultModel)
	}
	if _, ok := payload["instructions"]; !ok && strings.TrimSpace(cfg.Instructions) != "" {
		payload["instructions"] = cfg.Instructions
	}
	if _, ok := payload["store"]; !ok {
		payload["store"] = defaultStore(cfg)
	}
}

// parseResult 从原始响应中提取常用字段（ID、Model、Status、OutputText）；
// JSON 解析失败时仍返回带 StatusCode 和 Raw 的结果，保证调用方能看到上游原始输出。
func parseResult(statusCode int, raw []byte) *Result {
	result := &Result{
		StatusCode: statusCode,
		Raw:        json.RawMessage(raw),
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return result
	}
	result.ID = asString(obj["id"])
	result.Model = asString(obj["model"])
	result.Status = asString(obj["status"])
	result.OutputText = extractOutputText(obj)
	return result
}

// extractOutputText 优先取顶层 output_text 字段；不存在时遍历 output[].content[].text
// 拼接文本，兼容 Responses API 的不同返回形状；取不到任何文本时返回空串。
func extractOutputText(obj map[string]any) string {
	if value := asString(obj["output_text"]); value != "" {
		return value
	}
	output, ok := obj["output"].([]any)
	if !ok {
		return ""
	}
	var parts []string
	for _, item := range output {
		itemMap, ok := item.(map[string]any)
		if !ok {
			continue
		}
		content, ok := itemMap["content"].([]any)
		if !ok {
			continue
		}
		for _, part := range content {
			partMap, ok := part.(map[string]any)
			if !ok {
				continue
			}
			if text := asString(partMap["text"]); text != "" {
				parts = append(parts, text)
			}
		}
	}
	return strings.Join(parts, "")
}

// responsesURL 拼接上游 base URL 与 /responses 路径；配置未指定 endpoint 时使用默认值。
func responsesURL(cfg *conf.ModelConfig) string {
	endpoint := strings.TrimRight(stringOrDefault(cfg.Endpoint, defaultEndpoint), "/")
	return endpoint + "/responses"
}

// timeoutMs 读取超时配置：timeout_ms 直接按毫秒使用，timeout 按秒换算（×1000）；
// 非法值或未配置时回落到默认超时。
func timeoutMs(cfg *conf.ModelConfig) int {
	if cfg == nil || cfg.Extra == nil {
		return defaultTimeoutMs
	}
	if value, ok := cfg.Extra["timeout_ms"]; ok {
		if n := numberFromAny(value); n > 0 {
			return n
		}
	}
	if value, ok := cfg.Extra["timeout"]; ok {
		if n := numberFromAny(value); n > 0 {
			return n * 1000
		}
	}
	return defaultTimeoutMs
}

// defaultStore 读取 Extra["store"]：支持 bool 与 "true"/"false" 字符串，
// 其他类型或缺失时返回 false（不默认开启存储）。
func defaultStore(cfg *conf.ModelConfig) bool {
	if cfg == nil || cfg.Extra == nil {
		return false
	}
	value, ok := cfg.Extra["store"]
	if !ok {
		return false
	}
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(v, "true")
	default:
		return false
	}
}

// numberFromAny 将配置值统一转为 int；字符串解析失败或类型不识别时返回 0，由调用方视为未配置。
func numberFromAny(value any) int {
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case string:
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return 0
}

// asString 类型断言为 string，类型不符时返回空串。
func asString(value any) string {
	v, _ := value.(string)
	return v
}

// stringOrDefault 值为空白时返回 fallback，否则原样返回。
func stringOrDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
