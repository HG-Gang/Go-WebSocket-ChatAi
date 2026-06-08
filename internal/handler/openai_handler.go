// internal/handler/openai_handler.go
// OpenAI 路由处理器：实现与 OpenAI Realtime API 的 WebSocket 接入与 HTTP 降级逻辑。
//
// 核心职责：
//  1. OpenAIRealtimeHandler: 将普通的 WebSocket 连接升级为实时语音会话，并启动 Provider。
//  2. OpenAIFallbackHandler: 提供标准的 OpenAI Chat Completions 接口，供 WS 故障或降级场景使用。
//
// requestId 机制：
//
//	每次 WS 连接时生成全局唯一 requestId（格式：req_{userID}_{时间戳hex}），
//	贯穿 handler → session → client_ws 全链路日志，便于按单次连接追踪。
package handler

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/logger"
	"TozoAI-Chat-Api/internal/provider"
	"TozoAI-Chat-Api/internal/service/metrics"
	"TozoAI-Chat-Api/internal/service/session"
)

// upgrader WebSocket 升级器配置
var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     checkRealtimeOrigin,
}

func checkRealtimeOrigin(r *http.Request) bool {
	if r == nil {
		return false
	}
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if conf.IsDev() {
		if origin == "" {
			return true
		}
		u, err := url.Parse(origin)
		if err != nil {
			return false
		}
		host := strings.ToLower(u.Hostname())
		return host == "localhost" || host == "127.0.0.1" || host == "::1"
	}
	if conf.Global == nil {
		return false
	}
	for _, allowed := range conf.Global.Security.AllowedOrigins {
		if strings.EqualFold(strings.TrimSpace(allowed), origin) {
			return true
		}
	}
	return false
}

// generateRequestID 生成全局唯一 requestId
// 格式：req_{userID}_{unix毫秒时间戳hex}
// 作用：贯穿当前用户本次连接的所有日志，便于按 requestId 检索完整链路
func generateRequestID(userID string) string {
	return fmt.Sprintf("req_%s_%x", userID, time.Now().UnixMilli())
}

// OpenAIRealtimeHandler OpenAI 实时 WebSocket 接口处理入口
//
// 逻辑流程：
//  1. 从 Context 提取用户 ID
//  2. 生成 requestId（本次连接的全局唯一标识）
//  3. 创建带 request_id + user_id 的 logger（后续所有日志自动携带）
//  4. 检查模型配置 → 创建 Provider → 升级 WS → 创建 Session → 启动
func OpenAIRealtimeHandler(c *gin.Context) {
	log := logger.GetModelLogger("openai")

	// 1. 获取用户信息
	userID, exists := c.Get("user_id")
	if !exists {
		log.Warn("未授权的请求：缺少用户ID")
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized: user_id missing"})
		return
	}
	uid, _ := userID.(string)
	userName := c.GetString("user_name")

	// 2. 生成 requestId（本次连接的全局唯一标识）
	requestID := generateRequestID(uid)

	// 3. 创建带 request_id 和 user_id 的 logger
	//    后续所有通过此 logger 输出的日志都会自动携带这两个字段
	remoteAddr := c.ClientIP()
	if remoteAddr == "" && c.Request != nil {
		remoteAddr = c.Request.RemoteAddr
	}
	userAgent := c.GetHeader("User-Agent")
	deviceID := c.GetHeader("X-Device-ID")
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
	reqLog.Info("收到 WS 连接请求")

	if !session.TryAcquireCapacity() {
		metrics.CapacityRejected()
		activeSessions := session.ActiveCount()
		reqLog.Warn("实例活跃会话数已达上限，拒绝新 WS 连接",
			zap.Int64("active_sessions", activeSessions),
			zap.Int64("max_active_sessions", conf.Global.Capacity.MaxActiveSessions))
		notifyCapacityOverloadAlert(reqLog, "openai", uid, userName, remoteAddr, ipLocation, activeSessions)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "server overloaded, retry another node"})
		return
	}
	defer session.ReleaseCapacity()

	// 4. 检查模型配置状态
	modelName := "openai"
	modelCfg := conf.GetModel(modelName)
	connectionCfg, err := realtimeConfigFromQuery(c, modelCfg)
	if err != nil {
		reqLog.Warn("Realtime override 参数无效", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if !connectionCfg.Enabled {
		reqLog.Warn("模型请求被拒绝：openai 未启用或配置丢失")
		c.JSON(http.StatusBadRequest, gin.H{"error": "openai model is not enabled"})
		return
	}

	// 5. 通过工厂创建 Provider 实例
	prov := provider.CreateWithConfig(modelName, &connectionCfg)
	if prov == nil {
		reqLog.Error("Provider 创建失败：未找到对应的处理器工厂")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to initialize openai provider"})
		return
	}

	// 6. 升级 HTTP 协议至 WebSocket
	appConn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		reqLog.Error("WebSocket 升级失败", zap.Error(err))
		return
	}
	defer appConn.Close()

	// 7. 创建并运行实时会话（传入 requestId，贯穿全链路日志）
	sess := session.NewSession(uid, userName, modelName, requestID, remoteAddr, userAgent, deviceID, appConn, prov)
	sess.SetClientLocation(ipLocation)
	defer sess.Close()

	// 8. 启动会话处理循环（阻塞直到会话退出）
	sess.Start(c.Request.Context())

	reqLog.Info("WebSocket 会话结束")
}

func realtimeConfigFromQuery(c *gin.Context, base *conf.ModelConfig) (conf.ModelConfig, error) {
	cfg := conf.ModelConfig{Enabled: true}
	if base != nil {
		cfg = *base
		if base.Extra != nil {
			cfg.Extra = make(map[string]interface{}, len(base.Extra))
			for key, value := range base.Extra {
				cfg.Extra[key] = value
			}
		}
	}

	wsURL := firstQuery(c, "upstream_ws_url", "upstream_url", "realtime_url")
	apiKey := firstQuery(c, "upstream_api_key", "api_key")
	model := firstQuery(c, "upstream_model", "model")
	endpoint := firstQuery(c, "upstream_endpoint", "endpoint")

	if apiKey != "" && (conf.Global == nil || !conf.Global.Security.AllowUpstreamQueryKey) {
		return cfg, fmt.Errorf("upstream api key query override is disabled")
	}

	hasOverride := wsURL != "" || apiKey != "" || model != "" || endpoint != ""
	if hasOverride {
		cfg.Enabled = true
	}

	if wsURL != "" {
		normalizedURL, err := normalizeRealtimeUpstreamURL(wsURL)
		if err != nil {
			return cfg, err
		}
		cfg.Realtime.WsUrl = normalizedURL
	}
	if apiKey != "" {
		cfg.APIKey = apiKey
	}
	if model != "" {
		cfg.DefaultModel = model
	}
	if endpoint != "" {
		cfg.Endpoint = endpoint
	}

	if strings.TrimSpace(cfg.APIKey) == "" {
		return cfg, fmt.Errorf("upstream API key is required")
	}
	return cfg, nil
}

func normalizeRealtimeUpstreamURL(rawURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Host == "" {
		return "", fmt.Errorf("upstream_ws_url must be a valid ws://, wss://, http://, or https:// URL")
	}

	switch strings.ToLower(parsed.Scheme) {
	case "wss", "ws":
	case "https":
		parsed.Scheme = "wss"
		parsed.Path = normalizeRealtimePath(parsed.Path)
	case "http":
		parsed.Scheme = "ws"
		parsed.Path = normalizeRealtimePath(parsed.Path)
	default:
		return "", fmt.Errorf("upstream_ws_url must use ws://, wss://, http://, or https://")
	}
	return parsed.String(), nil
}

func normalizeRealtimePath(path string) string {
	cleanPath := strings.TrimRight(strings.TrimSpace(path), "/")
	switch cleanPath {
	case "", "/":
		return "/v1/realtime"
	case "/v1":
		return "/v1/realtime"
	default:
		return path
	}
}

func firstQuery(c *gin.Context, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(c.Query(key)); value != "" {
			return value
		}
	}
	return ""
}

// OpenAIFallbackHandler OpenAI HTTP 降级接口
func OpenAIFallbackHandler(c *gin.Context) {
	log := logger.GetModelLogger("openai")

	if !conf.Global.Fallback.Enabled {
		log.Warn("降级接口请求被拦截：Fallback 功能未开启")
		c.JSON(http.StatusForbidden, gin.H{"error": "HTTP fallback feature is disabled in global config"})
		return
	}

	modelName := "openai"
	prov := provider.Create(modelName)
	if prov == nil {
		log.Error("降级 Provider 创建失败")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to initialize provider for fallback"})
		return
	}

	prov.HandleFallback(c)
}
