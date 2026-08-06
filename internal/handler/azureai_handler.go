// internal/handler/azureai_handler.go
// Azure OpenAI 接入处理器：提供 Realtime WebSocket 入口，以及 Chat Completions、
// Completions、文生图、图生图、TTS、STT、语音翻译等 HTTP 反向代理入口。
//
// 文件功能：
//   - AzureRealtimeHandler: 与 OpenAIRealtimeHandler 流程一致，升级 WS 后接入 Azure Realtime 服务。
//   - Azure*Handler 与 handleAzureProxy: 按 capability 把请求体原样转发到 Azure OpenAI 对应接口，
//     并透传 Content-Type/Content-Disposition/Cache-Control 与上游 x-request-id。
//   - AzureStatusHandler: 输出面向 Web 调试页的配置快照，不返回 API Key 原文。
//
// 安全边界：
//   - 请求体与响应体均受 64MB 上限限制，防止无界内存占用。
//   - 上游失败时错误正文只返回脱敏摘要，不携带原始响应内容。
//   - azureai 模型未启用时所有入口直接拒绝，不进入上游。
package handler

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/logger"
	"TozoAI-Chat-Api/internal/provider"
	"TozoAI-Chat-Api/internal/provider/azureai"
	"TozoAI-Chat-Api/internal/service/metrics"
	"TozoAI-Chat-Api/internal/service/session"
)

// azureMaxProxyBody 代理请求体与响应体的最大字节数（64MB），超出时由 MaxBytesReader 直接截断。
const azureMaxProxyBody = 64 << 20

// AzureStatusHandler 输出 Azure OpenAI 的安全配置快照。
// 该接口只给 Web 调试页使用，不返回 API Key 原文。
func AzureStatusHandler(c *gin.Context) {
	cfg := conf.GetModel("azureai")
	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"data": azureai.Status(cfg),
	})
}

// AzureRealtimeHandler Azure OpenAI Realtime WebSocket 入口。
// 逻辑与 OpenAIRealtimeHandler 一致，但 Provider 名称改为 azureai，配置从 conf/models/azureai.yaml 读取。
func AzureRealtimeHandler(c *gin.Context) {
	log := logger.GetModelLogger("azureai")

	// 用户信息由前置鉴权中间件写入 Context；缺失时说明请求未认证，直接拒绝。
	userID, exists := c.Get("user_id")
	if !exists {
		log.Warn("未授权的请求：缺少用户ID")
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized: user_id missing"})
		return
	}
	uid, _ := userID.(string)
	userName := c.GetString("user_name")
	requestID := generateRequestID(uid)

	// 收集客户端上下文（IP、UA、设备标识、位置）后创建带 request_id 的 logger。
	remoteAddr := c.ClientIP()
	if remoteAddr == "" && c.Request != nil {
		remoteAddr = c.Request.RemoteAddr
	}
	userAgent := c.GetHeader("User-Agent")
	deviceID := c.GetHeader("X-Device-ID")
	// Header 缺失时从 query 兜底获取 device_id，保证日志字段有值。
	if deviceID == "" {
		deviceID = c.Query("device_id")
	}
	ipLocation := clientIPLocationFromRequest(c)
	reqLog := log.With(
		zap.String("request_id", requestID),
		zap.String("user_id", uid),
		zap.String("user_name", userName),
		zap.String("device_id", deviceID),
		zap.String("remote_addr", remoteAddr),
		zap.Any("ip_location", ipLocation),
	)
	reqLog.Info("收到 Azure Realtime WS 连接请求")

	// 实例容量不足时拒绝新连接并上报指标，避免已有会话因过载受影响。
	if !session.TryAcquireCapacity() {
		metrics.CapacityRejected()
		activeSessions := session.ActiveCount()
		reqLog.Warn("实例活跃会话数已达上限，拒绝新 Azure WS 连接",
			zap.Int64("active_sessions", activeSessions),
			zap.Int64("max_active_sessions", conf.Global.Capacity.MaxActiveSessions))
		notifyCapacityOverloadAlert(reqLog, "azureai", uid, userName, remoteAddr, ipLocation, activeSessions)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "server overloaded, retry another node"})
		return
	}
	defer session.ReleaseCapacity()

	// 模型未启用或配置丢失时拒绝建立会话。
	modelName := "azureai"
	modelCfg := conf.GetModel(modelName)
	if modelCfg == nil || !modelCfg.Enabled {
		reqLog.Warn("模型请求被拒绝：azureai 未启用或配置丢失")
		c.JSON(http.StatusBadRequest, gin.H{"error": "azureai model is not enabled"})
		return
	}

	// 由工厂按模型名创建 Provider；创建失败说明未注册对应处理器，返回 500。
	prov := provider.Create(modelName)
	if prov == nil {
		reqLog.Error("Azure Provider 创建失败：未找到对应的处理器工厂")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to initialize azureai provider"})
		return
	}

	// 升级失败时原连接已不可复用，直接结束请求。
	appConn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		reqLog.Error("Azure WebSocket 升级失败", zap.Error(err))
		return
	}
	defer appConn.Close()

	// 创建会话并绑定连接与 Provider，阻塞式运行直到客户端断开或出错。
	sess := session.NewSession(uid, userName, modelName, requestID, remoteAddr, userAgent, deviceID, appConn, prov)
	sess.SetClientLocation(ipLocation)
	defer sess.Close()
	sess.Start(c.Request.Context())

	reqLog.Info("Azure WebSocket 会话结束")
}

// AzureChatCompletionsHandler 代理 Azure OpenAI Chat Completions。
func AzureChatCompletionsHandler(c *gin.Context) {
	handleAzureProxy(c, azureai.CapabilityChatCompletions)
}

// AzureCompletionsHandler 代理 Azure OpenAI Completions。
func AzureCompletionsHandler(c *gin.Context) {
	handleAzureProxy(c, azureai.CapabilityCompletions)
}

// AzureImageGenerationsHandler 代理 Azure OpenAI 文生图。
func AzureImageGenerationsHandler(c *gin.Context) {
	handleAzureProxy(c, azureai.CapabilityImageGenerations)
}

// AzureImageEditsHandler 代理 Azure OpenAI 图生图/图片编辑。
func AzureImageEditsHandler(c *gin.Context) {
	handleAzureProxy(c, azureai.CapabilityImageEdits)
}

// AzureAudioSpeechHandler 代理 Azure OpenAI TTS。
func AzureAudioSpeechHandler(c *gin.Context) {
	handleAzureProxy(c, azureai.CapabilityAudioSpeech)
}

// AzureAudioTranscriptionsHandler 代理 Azure OpenAI STT。
func AzureAudioTranscriptionsHandler(c *gin.Context) {
	handleAzureProxy(c, azureai.CapabilityAudioTranscriptions)
}

// AzureAudioTranslationsHandler 代理 Azure OpenAI 语音翻译。
func AzureAudioTranslationsHandler(c *gin.Context) {
	handleAzureProxy(c, azureai.CapabilityAudioTranslations)
}

// AzureTSTHandler 为用户提到的 TST 预留统一入口。
// 当前按 Azure audio translations 代理，后续如接入真正 speech-to-speech 可在这里替换 capability。
func AzureTSTHandler(c *gin.Context) {
	handleAzureProxy(c, azureai.CapabilityTST)
}

// handleAzureProxy 按 capability 把请求体代理转发到 Azure OpenAI 对应接口。
// 请求体限制在 azureMaxProxyBody（64MB）以内；上游失败或超时时返回 502/504，
// 错误正文只含脱敏摘要；成功时透传指定响应头并复制响应体后返回。
func handleAzureProxy(c *gin.Context, capability string) {
	log := logger.GetModelLogger("azureai")
	cfg := conf.GetModel("azureai")
	if cfg == nil || !cfg.Enabled {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "azureai model is not enabled"})
		return
	}

	// 限制请求体大小，超过上限时 MaxBytesReader 使读取失败，避免大文件拖垮进程内存。
	body := http.MaxBytesReader(c.Writer, c.Request.Body, azureMaxProxyBody)
	defer body.Close()

	// 全程受超时约束；超时后按 DeadlineExceeded 映射为 504，而不是无限等待上游。
	ctx, cancel := context.WithTimeout(c.Request.Context(), azureTimeout(cfg))
	defer cancel()

	start := time.Now()
	resp, err := azureai.New(cfg).Do(ctx, capability, c.GetHeader("Content-Type"), c.GetHeader("Accept"), body)
	latency := time.Since(start)
	if err != nil {
		status := http.StatusBadGateway
		if ctx.Err() == context.DeadlineExceeded {
			status = http.StatusGatewayTimeout
		}
		log.Warn("Azure OpenAI 请求失败",
			zap.String("capability", capability),
			zap.Duration("latency", latency),
			zap.Error(err))
		// 错误响应只返回脱敏摘要，不把上游原始错误透传给调用方。
		c.JSON(status, gin.H{
			"code":          status,
			"error":         "azure upstream request failed",
			"error_summary": logger.RedactField("content", err.Error()),
			"capability":    capability,
			"latency_ms":    latency.Milliseconds(),
		})
		return
	}
	defer resp.Body.Close()

	// 透传客户端关心的响应头后复制响应体；响应体同样限制在 64MB 内，防止无界读取。
	copyProxyHeaders(c, resp)
	c.Status(resp.StatusCode)
	if _, err := io.Copy(c.Writer, io.LimitReader(resp.Body, azureMaxProxyBody)); err != nil {
		log.Warn("复制 Azure OpenAI 响应失败",
			zap.String("capability", capability),
			zap.Int("status", resp.StatusCode),
			zap.Error(err))
		return
	}
	log.Info("Azure OpenAI 请求完成",
		zap.String("capability", capability),
		zap.Int("status", resp.StatusCode),
		zap.Duration("latency", latency))
}

// azureTimeout 读取模型配置中的上游超时（毫秒）；配置缺失或非法时兜底为 60 秒。
func azureTimeout(cfg *conf.ModelConfig) time.Duration {
	status := azureai.Status(cfg)
	if value, ok := status["timeout_ms"].(int); ok && value > 0 {
		return time.Duration(value) * time.Millisecond
	}
	return 60 * time.Second
}

// copyProxyHeaders 仅透传调用方需要感知的响应头（Content-Type、Content-Disposition、
// Cache-Control），并把上游 x-request-id 重命名为 X-Upstream-Request-ID，方便调用方回查。
func copyProxyHeaders(c *gin.Context, resp *http.Response) {
	for _, key := range []string{"Content-Type", "Content-Disposition", "Cache-Control"} {
		value := strings.TrimSpace(resp.Header.Get(key))
		if value != "" {
			c.Header(key, value)
		}
	}
	if requestID := strings.TrimSpace(resp.Header.Get("x-request-id")); requestID != "" {
		c.Header("X-Upstream-Request-ID", requestID)
	}
}
