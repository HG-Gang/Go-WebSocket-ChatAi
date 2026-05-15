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

	userID, exists := c.Get("user_id")
	if !exists {
		log.Warn("未授权的请求：缺少用户ID")
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized: user_id missing"})
		return
	}
	uid, _ := userID.(string)
	requestID := generateRequestID(uid)

	remoteAddr := c.ClientIP()
	if remoteAddr == "" && c.Request != nil {
		remoteAddr = c.Request.RemoteAddr
	}
	userAgent := c.GetHeader("User-Agent")
	deviceID := c.GetHeader("X-Device-ID")
	if deviceID == "" {
		deviceID = c.Query("device_id")
	}
	reqLog := log.With(
		zap.String("request_id", requestID),
		zap.String("user_id", uid),
		zap.String("device_id", deviceID),
		zap.String("remote_addr", remoteAddr),
	)
	reqLog.Info("收到 Azure Realtime WS 连接请求")

	if !session.TryAcquireCapacity() {
		metrics.CapacityRejected()
		reqLog.Warn("实例活跃会话数已达上限，拒绝新 Azure WS 连接",
			zap.Int64("active_sessions", session.ActiveCount()),
			zap.Int64("max_active_sessions", conf.Global.Capacity.MaxActiveSessions))
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "server overloaded, retry another node"})
		return
	}
	defer session.ReleaseCapacity()

	modelName := "azureai"
	modelCfg := conf.GetModel(modelName)
	if modelCfg == nil || !modelCfg.Enabled {
		reqLog.Warn("模型请求被拒绝：azureai 未启用或配置丢失")
		c.JSON(http.StatusBadRequest, gin.H{"error": "azureai model is not enabled"})
		return
	}

	prov := provider.Create(modelName)
	if prov == nil {
		reqLog.Error("Azure Provider 创建失败：未找到对应的处理器工厂")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to initialize azureai provider"})
		return
	}

	appConn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		reqLog.Error("Azure WebSocket 升级失败", zap.Error(err))
		return
	}
	defer appConn.Close()

	sess := session.NewSession(uid, modelName, requestID, remoteAddr, userAgent, deviceID, appConn, prov)
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

func handleAzureProxy(c *gin.Context, capability string) {
	log := logger.GetModelLogger("azureai")
	cfg := conf.GetModel("azureai")
	if cfg == nil || !cfg.Enabled {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "azureai model is not enabled"})
		return
	}

	body := http.MaxBytesReader(c.Writer, c.Request.Body, azureMaxProxyBody)
	defer body.Close()

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
		c.JSON(status, gin.H{
			"code":       status,
			"error":      err.Error(),
			"capability": capability,
			"latency_ms": latency.Milliseconds(),
		})
		return
	}
	defer resp.Body.Close()

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

func azureTimeout(cfg *conf.ModelConfig) time.Duration {
	status := azureai.Status(cfg)
	if value, ok := status["timeout_ms"].(int); ok && value > 0 {
		return time.Duration(value) * time.Millisecond
	}
	return 60 * time.Second
}

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
