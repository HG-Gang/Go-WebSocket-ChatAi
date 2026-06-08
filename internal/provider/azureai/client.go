// Package azureai 封装 Azure OpenAI 的 HTTP 能力代理。
// 本包只处理普通 HTTP 接口：Chat Completions、Completions、图片、TTS、STT、TST。
// Azure Realtime WebSocket 复用 internal/provider/openai 的四协程 Provider。
package azureai

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/logger"
)

const (
	CapabilityChatCompletions     = "chat_completions"
	CapabilityCompletions         = "completions"
	CapabilityImageGenerations    = "image_generations"
	CapabilityImageEdits          = "image_edits"
	CapabilityAudioSpeech         = "audio_speech"
	CapabilityAudioTranscriptions = "audio_transcriptions"
	CapabilityAudioTranslations   = "audio_translations"
	CapabilityTST                 = "tst"

	defaultHTTPTimeoutMs = 60000
)

// Client 是 Azure OpenAI 普通 HTTP 接口的轻量代理客户端。
// 它只负责组装 Azure deployment 路径、api-version 查询参数和 api-key 鉴权头。
type Client struct {
	cfg        *conf.ModelConfig
	httpClient *http.Client
}

// New 创建 Azure HTTP 客户端。
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

// Do 将请求体按 capability 代理到 Azure OpenAI 上游。
// 调用方负责关闭返回的 resp.Body，这样 handler 可以直接流式复制二进制音频/图片响应。
func (c *Client) Do(ctx context.Context, capability string, contentType string, accept string, body io.Reader) (*http.Response, error) {
	if c == nil || c.cfg == nil {
		return nil, fmt.Errorf("Azure OpenAI 配置未初始化")
	}
	if !c.cfg.Enabled {
		return nil, fmt.Errorf("Azure OpenAI 未启用")
	}
	if strings.TrimSpace(c.cfg.APIKey) == "" {
		return nil, fmt.Errorf("Azure OpenAI 缺少 AZURE_OPENAI_API_KEY")
	}

	upstreamURL, err := c.urlFor(capability)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, upstreamURL, body)
	if err != nil {
		return nil, fmt.Errorf("创建 Azure OpenAI 请求失败: %w", err)
	}
	req.Header.Set("api-key", c.cfg.APIKey)
	if strings.TrimSpace(contentType) != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if strings.TrimSpace(accept) != "" {
		req.Header.Set("Accept", accept)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求 Azure OpenAI 失败: endpoint=%s error=%s", logger.SafeURLForDisplay(upstreamURL), logger.RedactField("content", err.Error()))
	}
	return resp, nil
}

// Status 返回可给调试页面展示的安全配置快照，不返回 API Key 原文。
func Status(cfg *conf.ModelConfig) map[string]any {
	if cfg == nil {
		cfg = &conf.ModelConfig{}
	}
	apiVersion := extraString(cfg, "api_version")
	deploymentName := deploymentFor(cfg, "")
	return map[string]any{
		"enabled":            cfg.Enabled,
		"endpoint":           logger.SafeURLForDisplay(cfg.Endpoint),
		"default_model":      cfg.DefaultModel,
		"api_key_configured": strings.TrimSpace(cfg.APIKey) != "",
		"api_version":        apiVersion,
		"deployment_name":    deploymentName,
		"modules": []map[string]any{
			moduleStatus("Realtime", "/ws/realtime/azure", "GET", deploymentFor(cfg, "realtime"), "WebSocket", true),
			moduleStatus("Chat Completions", "/api/azure/chat/completions", "POST", deploymentFor(cfg, "chat"), "/chat/completions", true),
			moduleStatus("Completions", "/api/azure/completions", "POST", deploymentFor(cfg, "completions"), "/completions", true),
			moduleStatus("文生图", "/api/azure/images/generations", "POST", deploymentFor(cfg, "image"), "/images/generations", true),
			moduleStatus("图生图", "/api/azure/images/edits", "POST", deploymentFor(cfg, "image"), "/images/edits", true),
			moduleStatus("TTS", "/api/azure/audio/speech", "POST", deploymentFor(cfg, "tts"), "/audio/speech", true),
			moduleStatus("STT", "/api/azure/audio/transcriptions", "POST", deploymentFor(cfg, "stt"), "/audio/transcriptions", true),
			moduleStatus("TST", "/api/azure/audio/translations", "POST", deploymentFor(cfg, "tst"), "/audio/translations", true),
		},
		"timeout_ms": timeoutMs(cfg),
	}
}

func (c *Client) urlFor(capability string) (string, error) {
	base := normalizeEndpoint(c.cfg.Endpoint)
	if base == "" {
		return "", fmt.Errorf("Azure OpenAI endpoint 为空")
	}
	apiVersion := extraString(c.cfg, "api_version")
	if apiVersion == "" {
		return "", fmt.Errorf("Azure OpenAI api_version 为空")
	}
	deployment := deploymentFor(c.cfg, capabilityGroup(capability))
	if deployment == "" {
		return "", fmt.Errorf("Azure OpenAI deployment 为空: capability=%s", capability)
	}
	pathSuffix, err := pathSuffixFor(capability)
	if err != nil {
		return "", err
	}

	escapedDeployment := url.PathEscape(deployment)
	rawURL := fmt.Sprintf("%s/openai/deployments/%s%s", base, escapedDeployment, pathSuffix)
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("解析 Azure OpenAI URL 失败: %w", err)
	}
	q := parsed.Query()
	q.Set("api-version", apiVersion)
	parsed.RawQuery = q.Encode()
	return parsed.String(), nil
}

func pathSuffixFor(capability string) (string, error) {
	switch capability {
	case CapabilityChatCompletions:
		return "/chat/completions", nil
	case CapabilityCompletions:
		return "/completions", nil
	case CapabilityImageGenerations:
		return "/images/generations", nil
	case CapabilityImageEdits:
		return "/images/edits", nil
	case CapabilityAudioSpeech:
		return "/audio/speech", nil
	case CapabilityAudioTranscriptions:
		return "/audio/transcriptions", nil
	case CapabilityAudioTranslations, CapabilityTST:
		return "/audio/translations", nil
	default:
		return "", fmt.Errorf("未知 Azure OpenAI capability: %s", capability)
	}
}

func capabilityGroup(capability string) string {
	switch capability {
	case CapabilityChatCompletions:
		return "chat"
	case CapabilityCompletions:
		return "completions"
	case CapabilityImageGenerations, CapabilityImageEdits:
		return "image"
	case CapabilityAudioSpeech:
		return "tts"
	case CapabilityAudioTranscriptions:
		return "stt"
	case CapabilityAudioTranslations, CapabilityTST:
		return "tst"
	default:
		return ""
	}
}

func deploymentFor(cfg *conf.ModelConfig, group string) string {
	if cfg == nil {
		return ""
	}
	keys := []string{}
	switch group {
	case "realtime":
		keys = append(keys, "realtime_deployment")
	case "chat":
		keys = append(keys, "chat_deployment")
	case "completions":
		keys = append(keys, "completions_deployment", "chat_deployment")
	case "responses":
		keys = append(keys, "responses_deployment", "chat_deployment")
	case "image":
		keys = append(keys, "image_deployment")
	case "tts":
		keys = append(keys, "tts_deployment")
	case "stt":
		keys = append(keys, "stt_deployment")
	case "tst":
		keys = append(keys, "tst_deployment", "stt_deployment")
	}
	keys = append(keys, "deployment_name")
	for _, key := range keys {
		if value := extraString(cfg, key); value != "" {
			return value
		}
	}
	return strings.TrimSpace(cfg.DefaultModel)
}

func normalizeEndpoint(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return ""
	}
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	endpoint = strings.TrimRight(endpoint, "/")
	for _, suffix := range []string{"/openai/v1", "/openai"} {
		if strings.HasSuffix(strings.ToLower(endpoint), suffix) {
			endpoint = endpoint[:len(endpoint)-len(suffix)]
			break
		}
	}
	return strings.TrimRight(endpoint, "/")
}

func timeoutMs(cfg *conf.ModelConfig) int {
	if cfg == nil {
		return defaultHTTPTimeoutMs
	}
	if n := intFromExtra(cfg, "timeout_ms"); n > 0 {
		return n
	}
	if n := intFromExtra(cfg, "timeout"); n > 0 {
		return n * 1000
	}
	return defaultHTTPTimeoutMs
}

func intFromExtra(cfg *conf.ModelConfig, key string) int {
	value := extraString(cfg, key)
	if value == "" {
		return 0
	}
	var n int
	if _, err := fmt.Sscanf(value, "%d", &n); err == nil {
		return n
	}
	return 0
}

func extraString(cfg *conf.ModelConfig, key string) string {
	if cfg == nil || cfg.Extra == nil {
		return ""
	}
	value, ok := cfg.Extra[key]
	if !ok || value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func moduleStatus(name, route, method, deployment, upstream string, implemented bool) map[string]any {
	return map[string]any{
		"name":        name,
		"route":       route,
		"method":      method,
		"deployment":  deployment,
		"upstream":    upstream,
		"implemented": implemented,
		"configured":  deployment != "",
	}
}
