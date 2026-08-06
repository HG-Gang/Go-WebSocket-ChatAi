// internal/provider/azureai/client.go
// 文件功能：Azure OpenAI 普通 HTTP 接口（Chat Completions、Completions、图片、TTS、STT、
// TST）的轻量代理客户端。输入为 capability 与请求体，输出为上游 *http.Response（调用方
// 负责关闭 resp.Body）；不负责 Realtime WebSocket（复用 internal/provider/openai 的四协程实现）。
//
// 安全边界：API key 仅作为 api-key 请求头发送给上游，不写入日志；调试快照只暴露
// api_key_configured 布尔值，不返回密钥原文；配置缺失、未启用或缺少 key 时返回错误失败关闭。
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
	// 能力标识：每个值决定上游路径后缀与 deployment 分组（见 pathSuffixFor、capabilityGroup），
	// 调用方需保证 capability 与配置中 models.azureai 的分组配置匹配。
	CapabilityChatCompletions     = "chat_completions"
	CapabilityCompletions         = "completions"
	CapabilityImageGenerations    = "image_generations"
	CapabilityImageEdits          = "image_edits"
	CapabilityAudioSpeech         = "audio_speech"
	CapabilityAudioTranscriptions = "audio_transcriptions"
	CapabilityAudioTranslations   = "audio_translations"
	CapabilityTST                 = "tst"

	// 默认请求超时（毫秒），仅在配置未提供 timeout_ms/timeout 时使用。
	defaultHTTPTimeoutMs = 60000
)

// Client 是 Azure OpenAI 普通 HTTP 接口的轻量代理客户端。
// 它只负责组装 Azure deployment 路径、api-version 查询参数和 api-key 鉴权头。
type Client struct {
	cfg        *conf.ModelConfig
	httpClient *http.Client
}

// New 创建 Azure HTTP 客户端。
// cfg 为空时按空配置创建；Timeout 取配置中的 timeout_ms（毫秒）或 timeout（秒），
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

// Do 按 capability 将请求体代理到 Azure OpenAI 上游。
// 参数：capability 接口能力标识；contentType/accept 为透传的请求头值；body 为请求体读取器。
// 成功返回上游 *http.Response，调用方负责关闭 resp.Body，handler 可直接流式复制二进制
// 音频/图片响应；任何配置或上游错误都返回 error 失败关闭，不会带缺 key 或未启用状态发起请求。
func (c *Client) Do(ctx context.Context, capability string, contentType string, accept string, body io.Reader) (*http.Response, error) {
	// 前置校验失败关闭：客户端未初始化、模型未启用或缺少 API key 时直接返回错误，
	// 避免以空 key 或未启用状态打到上游，也避免产生无意义的噪音请求。
	if c == nil || c.cfg == nil {
		return nil, fmt.Errorf("Azure OpenAI 配置未初始化")
	}
	if !c.cfg.Enabled {
		return nil, fmt.Errorf("Azure OpenAI 未启用")
	}
	if strings.TrimSpace(c.cfg.APIKey) == "" {
		return nil, fmt.Errorf("Azure OpenAI 缺少 AZURE_OPENAI_API_KEY")
	}

	// 上游 URL 任一部分（endpoint、api-version、deployment、路径）缺失都会在这里失败关闭。
	upstreamURL, err := c.urlFor(capability)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, upstreamURL, body)
	if err != nil {
		return nil, fmt.Errorf("创建 Azure OpenAI 请求失败: %w", err)
	}
	// 鉴权头组装：api-key 只在此处设置；Content-Type/Accept 仅在调用方显式给出时透传，
	// 未给出时由上游按请求体内容自行识别。
	req.Header.Set("api-key", c.cfg.APIKey)
	if strings.TrimSpace(contentType) != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if strings.TrimSpace(accept) != "" {
		req.Header.Set("Accept", accept)
	}

	// 上游网络错误时回传脱敏信息：endpoint 只保留 scheme/host，error 内容被脱敏，
	// 避免 API key 或完整 URL 出现在错误链与日志中。
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

// urlFor 组装上游完整 URL：base + /openai/deployments/{deployment} + 能力路径后缀 + api-version；
// 任一必需配置缺失时返回错误失败关闭。
func (c *Client) urlFor(capability string) (string, error) {
	// endpoint、api-version、deployment 任一为空都失败关闭：缺 api-version 的请求必然被
	// Azure 拒绝，缺 deployment 则 URL 不指向任何真实模型。
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

	// PathEscape 防止 deployment 中的特殊字符破坏 URL 结构；api-version 走 Query 编码追加。
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

// pathSuffixFor 返回 capability 对应的上游路径后缀；未知 capability 返回错误失败关闭，
// 防止拼出无意义的 URL 后把请求发到错误地址。
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

// capabilityGroup 把 capability 映射为配置中的 deployment 分组名（chat/image/tts/stt/tst），
// 用于 deploymentFor 按分组查配置；未知 capability 返回空串，最终由 urlFor 失败关闭。
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

// deploymentFor 按分组顺序查找 Extra 中的 deployment 配置，全组缺失时回落到 DefaultModel。
// 例如 completions 组优先 completions_deployment、其次 chat_deployment，允许多能力复用同一部署。
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

// normalizeEndpoint 统一上游 base URL：补 https:// 前缀、去尾部斜杠，
// 并剥离 /openai/v1、/openai 后缀——新版 Azure 路径从根开始拼 /openai/deployments/...。
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

// timeoutMs 读取超时配置：timeout_ms 直接按毫秒使用，timeout 按秒换算（×1000）；
// 非法值或未配置时回落到默认超时。
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

// intFromExtra 从 Extra 读取整数值；缺失或解析失败返回 0，由调用方视为未配置。
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

// extraString 从 Extra 读取配置值并转为去除首尾空白的字符串；缺失或 nil 时返回空串，
// 非字符串类型（int/float/bool 等）也统一转字符串，兼容配置来源差异。
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

// moduleStatus 组装调试页面所需的单模块状态条目；configured 表示该分组已配置 deployment。
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
