// internal/provider/openai/config.go
// OpenAI 模型专属配置封装
// 功能：包装 conf.ModelConfig，提供带默认值的便捷读取方法
package openai

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"TozoAI-Chat-Api/conf"
)

// OpenAIConfig OpenAI 专属配置（包装 conf.ModelConfig）
type OpenAIConfig struct {
	*conf.ModelConfig // 嵌入通用模型配置
}

// NewOpenAIConfig 从通用模型配置创建 OpenAI 专属配置
func NewOpenAIConfig(cfg *conf.ModelConfig) *OpenAIConfig {
	return &OpenAIConfig{ModelConfig: cfg}
}

// GetWsURL 获取 OpenAI Realtime WS 地址（带兜底值）
func (c *OpenAIConfig) GetWsURL() string {
	if c.Realtime.WsUrl != "" {
		return c.Realtime.WsUrl
	}
	return "wss://api.openai.com/v1/realtime"
}

func (c *OpenAIConfig) GetDefaultModel() string {
	if c.DefaultModel != "" {
		return c.DefaultModel
	}
	return "gpt-realtime"
}

// ExtraString 从模型 extra 扩展配置中读取字符串。
// 设计原因：Azure OpenAI 的 deployment、api-version、realtime 接入形态都放在 extra 中，
// Provider 层需要统一读取这些字段，避免在业务代码里散落 map[string]interface{} 断言。
func (c *OpenAIConfig) ExtraString(key string) string {
	if c == nil || c.Extra == nil {
		return ""
	}
	value, ok := c.Extra[key]
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

// IsAzure 判断当前配置是否为 Azure OpenAI。
// 这里不依赖包名，而是根据 endpoint、extra.provider 等配置判断，便于未来复用到多套 Azure 资源。
func (c *OpenAIConfig) IsAzure() bool {
	if strings.EqualFold(c.ExtraString("provider"), "azure") {
		return true
	}
	endpoint := strings.ToLower(strings.TrimSpace(c.Endpoint))
	wsURL := strings.ToLower(strings.TrimSpace(c.Realtime.WsUrl))
	return strings.Contains(endpoint, "openai.azure.com") || strings.Contains(wsURL, "openai.azure.com")
}

// GetAzureDeployment 获取 Azure OpenAI 的部署名。
// Realtime 优先读取 realtime_deployment，其次使用通用 deployment_name，最后兜底 default_model。
func (c *OpenAIConfig) GetAzureDeployment(capability string) string {
	keys := []string{}
	switch capability {
	case "realtime":
		keys = append(keys, "realtime_deployment")
	case "chat":
		keys = append(keys, "chat_deployment")
	case "responses":
		keys = append(keys, "responses_deployment")
	case "image":
		keys = append(keys, "image_deployment")
	case "tts":
		keys = append(keys, "tts_deployment")
	case "stt":
		keys = append(keys, "stt_deployment")
	case "tst":
		keys = append(keys, "tst_deployment")
	}
	keys = append(keys, "deployment_name")
	for _, key := range keys {
		if value := c.ExtraString(key); value != "" {
			return value
		}
	}
	return c.GetDefaultModel()
}

// BuildRealtimeURL 生成最终 WebSocket URL。
// OpenAI 使用官方 ?model=xxx；Azure 同时兼容：
//  1. GA 风格：wss://{resource}.openai.azure.com/openai/v1/realtime?model={deployment}
//  2. Preview 风格：wss://{resource}.openai.azure.com/openai/realtime?api-version=...&deployment=...
func (c *OpenAIConfig) BuildRealtimeURL() (string, error) {
	if !c.IsAzure() {
		u, err := url.Parse(c.GetWsURL())
		if err != nil {
			return "", fmt.Errorf("解析 OpenAI realtime ws_url 失败: %w", err)
		}
		q := u.Query()
		if q.Get("model") == "" {
			q.Set("model", c.GetDefaultModel())
		}
		u.RawQuery = q.Encode()
		return u.String(), nil
	}
	return c.buildAzureRealtimeURL()
}

// BuildRealtimeHeader 生成 Realtime 上游握手请求头。
// Azure 使用 api-key；OpenAI 使用 Bearer + OpenAI-Beta。
func (c *OpenAIConfig) BuildRealtimeHeader() http.Header {
	header := http.Header{}
	if c.IsAzure() {
		header.Set("api-key", c.APIKey)
		return header
	}
	header.Set("Authorization", "Bearer "+c.APIKey)
	header.Set("OpenAI-Beta", "realtime=v1")
	if c.Organization != "" {
		header.Set("OpenAI-Organization", c.Organization)
	}
	return header
}

func (c *OpenAIConfig) buildAzureRealtimeURL() (string, error) {
	if strings.TrimSpace(c.Realtime.WsUrl) != "" {
		return c.completeAzureRealtimeQuery(c.Realtime.WsUrl)
	}

	endpoint := strings.TrimSpace(c.Endpoint)
	if endpoint == "" {
		return "", fmt.Errorf("Azure Realtime endpoint 为空")
	}
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("解析 Azure endpoint 失败: %w", err)
	}
	u.Scheme = "wss"

	apiStyle := strings.ToLower(firstNonEmpty(c.ExtraString("realtime_api_style"), c.ExtraString("api_style")))
	if strings.Contains(u.Path, "/openai/v1") || apiStyle == "ga" {
		u.Path = strings.TrimRight(strings.TrimSuffix(u.Path, "/realtime"), "/") + "/realtime"
		q := u.Query()
		if q.Get("model") == "" {
			q.Set("model", c.GetAzureDeployment("realtime"))
		}
		u.RawQuery = q.Encode()
		return u.String(), nil
	}

	u.Path = strings.TrimRight(strings.TrimSuffix(u.Path, "/openai"), "/") + "/openai/realtime"
	q := u.Query()
	if q.Get("api-version") == "" {
		if apiVersion := c.ExtraString("api_version"); apiVersion != "" {
			q.Set("api-version", apiVersion)
		}
	}
	if q.Get("deployment") == "" {
		q.Set("deployment", c.GetAzureDeployment("realtime"))
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (c *OpenAIConfig) completeAzureRealtimeQuery(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("解析 Azure realtime ws_url 失败: %w", err)
	}
	q := u.Query()
	if strings.Contains(u.Path, "/openai/v1/realtime") || q.Get("model") != "" {
		if q.Get("model") == "" {
			q.Set("model", c.GetAzureDeployment("realtime"))
		}
	} else {
		if q.Get("api-version") == "" {
			if apiVersion := c.ExtraString("api_version"); apiVersion != "" {
				q.Set("api-version", apiVersion)
			}
		}
		if q.Get("deployment") == "" {
			q.Set("deployment", c.GetAzureDeployment("realtime"))
		}
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// GetMaxRetries 获取最大重连次数
func (c *OpenAIConfig) GetMaxRetries() int {
	if c.Realtime.ReconnectMaxRetries > 0 {
		return c.Realtime.ReconnectMaxRetries
	}
	return 3
}

// GetReconnectDelay 获取重连延迟时间
func (c *OpenAIConfig) GetReconnectDelay() time.Duration {
	if c.Realtime.ReconnectDelay != "" {
		if d, err := time.ParseDuration(c.Realtime.ReconnectDelay); err == nil {
			return d
		}
	}
	return 1 * time.Second // 默认 1 秒
}

// ======================== 心跳配置读取 ========================

// GetAppPingInterval 获取 Go→App 发送 Ping 的间隔
// 配置项：realtime.app_ping_interval（如 "30s"）
// 默认值：30s
func (c *OpenAIConfig) GetAppPingInterval() time.Duration {
	if c.Realtime.AppPingInterval != "" {
		if d, err := time.ParseDuration(c.Realtime.AppPingInterval); err == nil {
			return d
		}
	}
	return 30 * time.Second
}

// GetAppPongTimeout 获取等待 App Pong 的超时时间
// 配置项：realtime.app_pong_timeout（如 "60s"）
// 默认值：60s
// 语义：超过此时间未收到 App 的 Pong，认为 App 已断开，会话结束
func (c *OpenAIConfig) GetAppPongTimeout() time.Duration {
	if c.Realtime.AppPongTimeout != "" {
		if d, err := time.ParseDuration(c.Realtime.AppPongTimeout); err == nil {
			return d
		}
	}
	return 60 * time.Second
}

// GetApiReadTimeout 获取读取 OpenAI 消息的超时时间
// 配置项：realtime.api_read_timeout（如 "120s"）
// 默认值：120s
// 语义：超过此时间未从 OpenAI 读到任何消息，触发重连
func (c *OpenAIConfig) GetApiReadTimeout() time.Duration {
	if c.Realtime.ApiReadTimeout != "" {
		if d, err := time.ParseDuration(c.Realtime.ApiReadTimeout); err == nil {
			return d
		}
	}
	return 120 * time.Second
}

func (c *OpenAIConfig) GetApiPingInterval() time.Duration {
	if c.Realtime.ApiPingInterval != "" {
		if d, err := time.ParseDuration(c.Realtime.ApiPingInterval); err == nil {
			return d
		}
	}
	return 30 * time.Second
}

func (c *OpenAIConfig) GetApiPongTimeout() time.Duration {
	if c.Realtime.ApiPongTimeout != "" {
		if d, err := time.ParseDuration(c.Realtime.ApiPongTimeout); err == nil {
			return d
		}
	}
	return 90 * time.Second
}

func (c *OpenAIConfig) GetApiWriteTimeout() time.Duration {
	if c.Realtime.ApiWriteTimeout != "" {
		if d, err := time.ParseDuration(c.Realtime.ApiWriteTimeout); err == nil {
			return d
		}
	}
	return 10 * time.Second
}

func (c *OpenAIConfig) ShouldRestoreSession() bool {
	return c.Realtime.RestoreSession
}

func (c *OpenAIConfig) GetRestoreHistoryLimit() int {
	if c.Realtime.RestoreHistoryLimit > 0 {
		return c.Realtime.RestoreHistoryLimit
	}
	return 32
}

func (c *OpenAIConfig) GetSendQueueTimeout() time.Duration {
	if c.Realtime.SendQueueTimeoutMs > 0 {
		return time.Duration(c.Realtime.SendQueueTimeoutMs) * time.Millisecond
	}
	return 250 * time.Millisecond
}
