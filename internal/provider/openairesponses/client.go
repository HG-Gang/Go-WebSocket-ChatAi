// Package openairesponses 封装 OpenAI Responses API 的 HTTP 调用逻辑。
// 这个包只处理 /v1/responses 这类普通 HTTP 请求，不参与 Realtime WebSocket 四协程链路。
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
)

const (
	defaultEndpoint  = "https://api.openai.com/v1"
	defaultModel     = "gpt-4.1"
	defaultTimeoutMs = 60000
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
func (c *Client) Create(ctx context.Context, payload map[string]any) (*Result, error) {
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
	normalizePayload(payload, c.cfg)

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("序列化 Responses API 请求失败: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, responsesURL(c.cfg), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("创建 Responses API 请求失败: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	req.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(c.cfg.Organization) != "" {
		req.Header.Set("OpenAI-Organization", c.cfg.Organization)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求 Responses API 失败: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, fmt.Errorf("读取 Responses API 响应失败: %w", err)
	}
	result := parseResult(resp.StatusCode, raw)
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
		"endpoint":           stringOrDefault(cfg.Endpoint, defaultEndpoint),
		"api_key_configured": strings.TrimSpace(cfg.APIKey) != "",
		"timeout_ms":         timeoutMs(cfg),
		"store_default":      defaultStore(cfg),
		"instructions_set":   strings.TrimSpace(cfg.Instructions) != "",
		"route":              "/api/openai/responses",
		"upstream_path":      "/v1/responses",
	}
}

// normalizePayload 只补齐调用方没有显式传入的默认值，避免网关覆盖业务请求。
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

func responsesURL(cfg *conf.ModelConfig) string {
	endpoint := strings.TrimRight(stringOrDefault(cfg.Endpoint, defaultEndpoint), "/")
	return endpoint + "/responses"
}

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

func asString(value any) string {
	v, _ := value.(string)
	return v
}

func stringOrDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
