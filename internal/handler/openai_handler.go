// internal/handler/openai_handler.go
// OpenAI 路由处理器：实现与 OpenAI Realtime API 的 WebSocket 接入与 HTTP 降级逻辑。
//
// 核心职责：
//  1. OpenAIRealtimeHandler: 将普通的 WebSocket 连接升级为实时语音会话，并启动 Provider。
//  2. OpenAIFallbackHandler: 提供标准的 OpenAI Chat Completions 接口，供 WS 故障或降级场景使用。
//
// 安全边界：
//   - 前置鉴权中间件未写入 user_id 的请求直接拒绝，不进入会话创建流程。
//   - WS 升级请求的 Origin 不在允许名单内时拒绝升级（失败关闭）。
//   - 上游 API key 仅当全局配置显式开启 AllowUpstreamQueryKey 时才接受 query 覆盖，否则请求被拒绝。
//
// requestId 机制：
//
//	每次 WS 连接时生成全局唯一 requestId（格式：req_{userID}_{unix毫秒时间戳hex}），
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

// checkRealtimeOrigin 校验 WS 升级请求的 Origin 是否可信。
// 开发模式放行空 Origin 与 localhost/127.0.0.1/::1 主机名；生产模式仅放行全局
// 安全配置 AllowedOrigins 白名单中的来源。返回 false 时 gorilla/websocket 拒绝升级（失败关闭）。
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

	// 用户信息由前置鉴权中间件写入 Context；缺失时说明请求未认证，直接拒绝后续处理。
	userID, exists := c.Get("user_id")
	if !exists {
		log.Warn("未授权的请求：缺少用户ID")
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized: user_id missing"})
		return
	}
	uid, _ := userID.(string)
	userName := c.GetString("user_name")

	// 生成本次连接唯一 requestId，作为全链路日志的检索主键。
	requestID := generateRequestID(uid)

	// 收集客户端上下文（IP、UA、设备标识、位置）后创建带 request_id 的 logger，
	// 后续所有通过此 logger 输出的日志自动携带这些字段。
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
	reqLog.Info("收到 WS 连接请求")

	// 先检查实例容量：超过上限时拒绝新连接并上报指标，避免已有会话因过载受影响。
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

	// 读取模型配置并应用 query 覆盖；模型未启用或配置丢失时拒绝建立会话。
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

	// 由工厂按模型名创建 Provider；创建失败说明该模型未注册处理器，返回 500。
	prov := provider.CreateWithConfig(modelName, &connectionCfg)
	if prov == nil {
		reqLog.Error("Provider 创建失败：未找到对应的处理器工厂")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to initialize openai provider"})
		return
	}

	// 升级 HTTP 协议至 WebSocket；升级失败时原连接已不可复用，直接结束请求。
	appConn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		reqLog.Error("WebSocket 升级失败", zap.Error(err))
		return
	}
	defer appConn.Close()

	// 创建会话并绑定连接与 Provider，写入客户端位置；会话结束时连接随之关闭。
	sess := session.NewSession(uid, userName, modelName, requestID, remoteAddr, userAgent, deviceID, appConn, prov)
	sess.SetClientLocation(ipLocation)
	defer sess.Close()

	// 阻塞式运行会话处理循环，直到客户端断开或会话出错才返回。
	sess.Start(c.Request.Context())

	reqLog.Info("WebSocket 会话结束")
}

// realtimeConfigFromQuery 应用 query 参数对上游连接配置的覆盖（WS 地址、API key、
// 模型、endpoint），返回深拷贝后的副本，避免污染全局配置。query 传入 API key 仅在
// AllowUpstreamQueryKey 显式开启时接受，否则返回错误；最终 API key 必须非空，否则连接无法建立。
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

	// query 传入上游 API key 属于信任边界：未显式开启 AllowUpstreamQueryKey 时整体拒绝请求。
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

// normalizeRealtimeUpstreamURL 将 http(s) 形式的 URL 规整为 ws(s) 并补全 /v1/realtime
// 路径，兼容配置或客户端直接传 HTTP 地址的场景；协议非法或缺少 host 时返回错误，调用方应拒绝请求。
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

// normalizeRealtimePath 为空路径或 /v1 时补全实时接口路径 /v1/realtime，其余路径原样保留。
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

// firstQuery 按参数顺序返回第一个非空的 query 值（去首尾空白），全部为空时返回空串。
func firstQuery(c *gin.Context, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(c.Query(key)); value != "" {
			return value
		}
	}
	return ""
}

// OpenAIFallbackHandler OpenAI HTTP 降级入口
// 仅当全局配置开启 Fallback 功能时可用，否则返回 403（失败关闭）；
// 可用时创建 Provider 并把请求交给其 HTTP 降级实现处理。
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
