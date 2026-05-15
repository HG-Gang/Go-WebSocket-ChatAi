package handler

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/provider/azureai"
	"TozoAI-Chat-Api/internal/provider/openairesponses"
	"TozoAI-Chat-Api/internal/service/metrics"
	redisService "TozoAI-Chat-Api/internal/service/redis"
	"TozoAI-Chat-Api/internal/service/session"
)

var debugProcessStartedAt = time.Now()

// DebugStatusHandler 为本地 Web 调试页面提供只读诊断快照。
//
// 设计原则：
//  1. 只输出排障需要的运行状态，不输出 API Key / JWT Secret / Redis Password。
//  2. 每次请求实时读取 Go runtime、会话容量、Redis 连接池和 OpenAI Realtime 配置。
//  3. 页面可以高频轮询这个接口，所以这里不执行 SCAN 这类重操作；Redis key 明细仍由 /api/redis/keys 单独负责。
func DebugStatusHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"data": gin.H{
			"server":    buildDebugServerStatus(),
			"memory":    buildDebugMemoryStatus(),
			"capacity":  buildDebugCapacityStatus(),
			"features":  buildDebugFeatureStatus(),
			"redis":     buildDebugRedisStatus(),
			"network":   buildDebugNetworkStatus(),
			"openai":    buildDebugOpenAIStatus(),
			"responses": buildDebugResponsesStatus(),
			"azure":     buildDebugAzureStatus(),
			"metrics":   metrics.Snapshot(),
			"routes": []gin.H{
				{"name": "health", "method": "GET", "path": "/health", "purpose": "基础存活检查与活跃会话数"},
				{"name": "debug_status", "method": "GET", "path": "/api/debug/status", "purpose": "Go / Redis / OpenAI 配置与运行时诊断"},
				{"name": "redis_keys", "method": "GET", "path": "/api/redis/keys?pattern=*&count=500", "purpose": "Redis key 明细与类型/TTL/值预览"},
				{"name": "openai_responses_status", "method": "GET", "path": "/api/openai/responses/status", "purpose": "OpenAI Responses API 安全配置快照"},
				{"name": "openai_responses", "method": "POST", "path": "/api/openai/responses", "purpose": "OpenAI Responses API HTTP 代理接口"},
				{"name": "azure_status", "method": "GET", "path": "/api/azure/status", "purpose": "Azure OpenAI 安全配置快照"},
				{"name": "azure_realtime_ws", "method": "GET", "path": "/ws/realtime/azure", "purpose": "Azure OpenAI Realtime WebSocket 网关"},
				{"name": "azure_chat", "method": "POST", "path": "/api/azure/chat/completions", "purpose": "Azure Chat Completions 代理接口"},
				{"name": "azure_images", "method": "POST", "path": "/api/azure/images/generations", "purpose": "Azure 文生图代理接口"},
				{"name": "azure_audio", "method": "POST", "path": "/api/azure/audio/speech", "purpose": "Azure TTS 代理接口"},
				{"name": "token", "method": "GET", "path": "/test/generate-token?userId=1001", "purpose": "本地调试 JWT token 生成"},
				{"name": "realtime_ws", "method": "GET", "path": "/ws/realtime/openai", "purpose": "App / 耳机接入 Go Realtime 网关"},
			},
		},
	})
}

func buildDebugServerStatus() gin.H {
	now := time.Now()
	env, mode, addr := "", "", ""
	if conf.Global != nil {
		env = conf.Global.Env
		mode = conf.Global.Server.Mode
		addr = conf.Global.Server.Addr
	}
	return gin.H{
		"env":            env,
		"mode":           mode,
		"addr":           addr,
		"time":           now.Format(time.RFC3339),
		"uptime":         now.Sub(debugProcessStartedAt).Round(time.Second).String(),
		"uptime_seconds": int64(now.Sub(debugProcessStartedAt).Seconds()),
		"go_version":     runtime.Version(),
		"os":             runtime.GOOS,
		"arch":           runtime.GOARCH,
		"num_cpu":        runtime.NumCPU(),
		"goroutines":     runtime.NumGoroutine(),
	}
}

func buildDebugMemoryStatus() gin.H {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	lastGC := ""
	if mem.LastGC > 0 {
		lastGC = time.Unix(0, int64(mem.LastGC)).Format(time.RFC3339)
	}

	return gin.H{
		"alloc_bytes":         mem.Alloc,
		"total_alloc_bytes":   mem.TotalAlloc,
		"sys_bytes":           mem.Sys,
		"heap_alloc_bytes":    mem.HeapAlloc,
		"heap_inuse_bytes":    mem.HeapInuse,
		"heap_idle_bytes":     mem.HeapIdle,
		"heap_released_bytes": mem.HeapReleased,
		"stack_inuse_bytes":   mem.StackInuse,
		"next_gc_bytes":       mem.NextGC,
		"num_gc":              mem.NumGC,
		"last_gc":             lastGC,
		"gc_pause_total_ns":   mem.PauseTotalNs,
		"gc_cpu_fraction":     mem.GCCPUFraction,
		"alloc_mb":            bytesToMB(mem.Alloc),
		"sys_mb":              bytesToMB(mem.Sys),
		"heap_alloc_mb":       bytesToMB(mem.HeapAlloc),
		"heap_released_mb":    bytesToMB(mem.HeapReleased),
		"stack_inuse_mb":      bytesToMB(mem.StackInuse),
		"next_gc_mb":          bytesToMB(mem.NextGC),
		"gc_pause_total_ms":   float64(mem.PauseTotalNs) / float64(time.Millisecond),
		"gc_cpu_fraction_pct": mem.GCCPUFraction * 100,
	}
}

func buildDebugCapacityStatus() gin.H {
	active := session.ActiveCount()
	maxActive := int64(0)
	if conf.Global != nil {
		maxActive = conf.Global.Capacity.MaxActiveSessions
	}

	usagePct := float64(0)
	if maxActive > 0 {
		usagePct = float64(active) / float64(maxActive) * 100
	}

	return gin.H{
		"active_sessions":     active,
		"max_active_sessions": maxActive,
		"limit_enabled":       maxActive > 0,
		"usage_percent":       usagePct,
	}
}

func buildDebugFeatureStatus() gin.H {
	if conf.Global == nil {
		return gin.H{}
	}
	return gin.H{
		"jwt_enabled":        conf.Global.JWT.Enabled,
		"rate_limit_enabled": conf.Global.RateLimit.Enabled,
		"billing_enabled":    conf.Global.Billing.Enabled,
		"fallback_enabled":   conf.Global.Fallback.Enabled,
		"redis_enabled":      conf.Global.Redis.Enabled,
	}
}

func buildDebugRedisStatus() gin.H {
	status := gin.H{
		"enabled":        false,
		"addr":           "",
		"db":             0,
		"pool_size":      0,
		"min_idle_conns": 0,
		"available":      false,
		"ping_ok":        false,
		"ping_ms":        int64(-1),
		"error":          "",
	}
	if conf.Global != nil {
		status["enabled"] = conf.Global.Redis.Enabled
		status["addr"] = conf.Global.Redis.Addr
		status["db"] = conf.Global.Redis.DB
		status["pool_size"] = conf.Global.Redis.PoolSize
		status["min_idle_conns"] = conf.Global.Redis.MinIdleConns
	}

	client := redisService.GetClient()
	if client == nil {
		status["error"] = "redis client is nil"
		return status
	}

	status["available"] = true

	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := client.Ping(ctx).Err(); err != nil {
		status["error"] = err.Error()
	} else {
		status["ping_ok"] = true
	}
	status["ping_ms"] = time.Since(start).Milliseconds()

	pool := client.PoolStats()
	status["pool"] = gin.H{
		"hits":             pool.Hits,
		"misses":           pool.Misses,
		"timeouts":         pool.Timeouts,
		"wait_count":       pool.WaitCount,
		"unusable":         pool.Unusable,
		"wait_duration_ns": pool.WaitDurationNs,
		"total_conns":      pool.TotalConns,
		"idle_conns":       pool.IdleConns,
		"stale_conns":      pool.StaleConns,
	}
	return status
}

func buildDebugNetworkStatus() gin.H {
	httpProxy := strings.TrimSpace(os.Getenv("HTTP_PROXY"))
	httpsProxy := strings.TrimSpace(os.Getenv("HTTPS_PROXY"))
	allProxy := strings.TrimSpace(os.Getenv("ALL_PROXY"))
	noProxy := strings.TrimSpace(os.Getenv("NO_PROXY"))

	// OpenAI Realtime 默认使用 wss://。
	// gorilla/websocket 会继承 net/http.ProxyFromEnvironment；
	// 因此 wss/https 上游请求实际生效的是 HTTPS_PROXY。
	// HTTP_PROXY 只对 ws/http 目标生效。
	effectiveOpenAIProxy := httpsProxy

	return gin.H{
		"http_proxy":             maskProxyURL(httpProxy),
		"https_proxy":            maskProxyURL(httpsProxy),
		"all_proxy":              maskProxyURL(allProxy),
		"no_proxy":               noProxy,
		"openai_proxy_effective": maskProxyURL(effectiveOpenAIProxy),
		"openai_proxy_enabled":   effectiveOpenAIProxy != "",
		"azure_proxy_effective":  maskProxyURL(effectiveOpenAIProxy),
		"azure_proxy_enabled":    effectiveOpenAIProxy != "",
	}
}

func buildDebugOpenAIStatus() gin.H {
	cfg := conf.GetModel("openai")
	if cfg == nil {
		cfg = &conf.ModelConfig{}
	}

	return gin.H{
		"enabled":                 cfg.Enabled,
		"default_model":           stringOrDefault(cfg.DefaultModel, "gpt-realtime"),
		"endpoint":                cfg.Endpoint,
		"voice":                   cfg.Voice,
		"api_key_configured":      strings.TrimSpace(cfg.APIKey) != "",
		"organization_configured": strings.TrimSpace(cfg.Organization) != "",
		"ws_url":                  stringOrDefault(cfg.Realtime.WsUrl, "wss://api.openai.com/v1/realtime"),
		"realtime": gin.H{
			"name":                  cfg.Realtime.Name,
			"reconnect_max_retries": intOrDefault(cfg.Realtime.ReconnectMaxRetries, 3),
			"reconnect_delay":       durationOrDefault(cfg.Realtime.ReconnectDelay, time.Second),
			"app_ping_interval":     durationOrDefault(cfg.Realtime.AppPingInterval, 30*time.Second),
			"app_pong_timeout":      durationOrDefault(cfg.Realtime.AppPongTimeout, 60*time.Second),
			"api_read_timeout":      durationOrDefault(cfg.Realtime.ApiReadTimeout, 120*time.Second),
			"api_ping_interval":     durationOrDefault(cfg.Realtime.ApiPingInterval, 30*time.Second),
			"api_pong_timeout":      durationOrDefault(cfg.Realtime.ApiPongTimeout, 90*time.Second),
			"api_write_timeout":     durationOrDefault(cfg.Realtime.ApiWriteTimeout, 10*time.Second),
			"restore_session":       cfg.Realtime.RestoreSession,
			"restore_history_limit": intOrDefault(cfg.Realtime.RestoreHistoryLimit, 32),
			"send_queue_timeout_ms": intOrDefault(cfg.Realtime.SendQueueTimeoutMs, 250),
		},
	}
}

func buildDebugResponsesStatus() gin.H {
	cfg := conf.GetModel("openairesponses")
	return openairesponses.Status(cfg)
}

func buildDebugAzureStatus() gin.H {
	cfg := conf.GetModel("azureai")
	if cfg == nil {
		cfg = &conf.ModelConfig{}
	}
	status := azureai.Status(cfg)
	extra := cfg.Extra
	if extra == nil {
		extra = map[string]interface{}{}
	}
	return gin.H{
		"enabled":                 cfg.Enabled,
		"default_model":           cfg.DefaultModel,
		"endpoint":                cfg.Endpoint,
		"voice":                   cfg.Voice,
		"api_key_configured":      strings.TrimSpace(cfg.APIKey) != "",
		"organization_configured": strings.TrimSpace(cfg.Organization) != "",
		"deployment_name":         stringFromExtra(extra, "deployment_name"),
		"api_version":             stringFromExtra(extra, "api_version"),
		"chat_deployment":         stringFromExtra(extra, "chat_deployment"),
		"completions_deployment":  stringFromExtra(extra, "completions_deployment"),
		"responses_deployment":    stringFromExtra(extra, "responses_deployment"),
		"realtime_deployment":     stringFromExtra(extra, "realtime_deployment"),
		"image_deployment":        stringFromExtra(extra, "image_deployment"),
		"tts_deployment":          stringFromExtra(extra, "tts_deployment"),
		"stt_deployment":          stringFromExtra(extra, "stt_deployment"),
		"tst_deployment":          stringFromExtra(extra, "tst_deployment"),
		"modules":                 status["modules"],
		"timeout_ms":              status["timeout_ms"],
		"realtime": gin.H{
			"name":                  cfg.Realtime.Name,
			"ws_url":                cfg.Realtime.WsUrl,
			"reconnect_max_retries": intOrDefault(cfg.Realtime.ReconnectMaxRetries, 3),
			"reconnect_delay":       durationOrDefault(cfg.Realtime.ReconnectDelay, time.Second),
			"app_ping_interval":     durationOrDefault(cfg.Realtime.AppPingInterval, 30*time.Second),
			"app_pong_timeout":      durationOrDefault(cfg.Realtime.AppPongTimeout, 60*time.Second),
			"api_read_timeout":      durationOrDefault(cfg.Realtime.ApiReadTimeout, 120*time.Second),
			"api_ping_interval":     durationOrDefault(cfg.Realtime.ApiPingInterval, 30*time.Second),
			"api_pong_timeout":      durationOrDefault(cfg.Realtime.ApiPongTimeout, 90*time.Second),
			"api_write_timeout":     durationOrDefault(cfg.Realtime.ApiWriteTimeout, 10*time.Second),
			"restore_session":       cfg.Realtime.RestoreSession,
			"restore_history_limit": intOrDefault(cfg.Realtime.RestoreHistoryLimit, 32),
			"send_queue_timeout_ms": intOrDefault(cfg.Realtime.SendQueueTimeoutMs, 250),
		},
		"planned_modules": []string{
			"realtime",
			"chat_completions",
			"completions",
			"responses",
			"text_to_image",
			"image_to_image",
			"tts",
			"stt",
			"tst",
		},
	}
}

func bytesToMB(v uint64) float64 {
	return float64(v) / 1024 / 1024
}

func stringOrDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func intOrDefault(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

func durationOrDefault(value string, fallback time.Duration) string {
	if strings.TrimSpace(value) == "" {
		return fallback.String()
	}
	if d, err := time.ParseDuration(value); err == nil {
		return d.String()
	}
	return value + " (invalid, fallback " + fallback.String() + ")"
}

func stringFromExtra(extra map[string]interface{}, key string) string {
	if extra == nil {
		return ""
	}
	value, ok := extra[key]
	if !ok || value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

func maskProxyURL(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.User == nil {
		return raw
	}
	if username := parsed.User.Username(); username != "" {
		parsed.User = url.UserPassword(username, "******")
	}
	return parsed.String()
}
