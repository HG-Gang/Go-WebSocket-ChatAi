// internal/handler/debug_handler.go
// 本地 Web 调试页的只读诊断快照接口。
//
// 文件功能：
//   - DebugStatusHandler: 实时聚合 Go runtime、会话容量、Redis 连接池、网络代理、
//     三个模型（openai/openairesponses/azureai）的配置快照与路由清单。
//   - buildDebug* 系列: 分别组装 server/process/memory/capacity/features/redis/
//     network/openai/responses/azure 各分片数据。
//
// 安全边界：
//   - 接口只输出排障所需状态：API Key 一律经 maskAPIKey 脱敏（保留可识别前缀与末 4 位），
//     Proxy URL 与 Endpoint 经 SafeURLForDisplay 隐藏凭据部分，任何路径都不输出密钥原文。
//   - 接口可被调试页高频轮询，因此不执行 SCAN 等重操作；Redis key 明细由 redis_handler 单独负责。
package handler

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/logger"
	"TozoAI-Chat-Api/internal/provider/azureai"
	"TozoAI-Chat-Api/internal/provider/openairesponses"
	"TozoAI-Chat-Api/internal/service/monitor"
	redisService "TozoAI-Chat-Api/internal/service/redis"
	"TozoAI-Chat-Api/internal/service/session"
)

// debugProcessStartedAt 进程启动时刻，作为 uptime 与内存快照的基准。
var debugProcessStartedAt = time.Now()

// DebugStatusHandler 为本地 Web 调试页面提供只读诊断快照。
//
// 设计原则：
//  1. 只输出排障需要的运行状态，不输出 API Key / JWT Secret / Redis Password。
//  2. 每次请求实时读取 Go runtime、会话容量、Redis 连接池和 OpenAI Realtime 配置。
//  3. 页面可以高频轮询这个接口，所以这里不执行 SCAN 这类重操作；Redis key 明细仍由 /api/redis/keys 单独负责。
func DebugStatusHandler(c *gin.Context) {
	snapshot := monitor.Collect(debugProcessStartedAt)
	monitor.LogSnapshotThrottled(snapshot, 30*time.Second)
	snapshot.Log = monitor.LogState()

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"data": gin.H{
			"server":    snapshot.Server,
			"process":   snapshot.Process,
			"memory":    snapshot.Memory,
			"capacity":  snapshot.Capacity,
			"monitor":   snapshot,
			"features":  buildDebugFeatureStatus(),
			"redis":     buildDebugRedisStatus(),
			"network":   buildDebugNetworkStatus(),
			"openai":    buildDebugOpenAIStatus(),
			"responses": buildDebugResponsesStatus(),
			"azure":     buildDebugAzureStatus(),
			"metrics":   snapshot.Metrics,
			"routes": []gin.H{
				{"name": "health", "method": "GET", "path": "/health", "purpose": "基础存活检查与活跃会话数"},
				{"name": "debug_status", "method": "GET", "path": "/api/debug/status", "purpose": "Go / Redis / OpenAI 配置与运行时诊断"},
				{"name": "redis_keys", "method": "GET", "path": "/api/redis/keys?pattern=*&count=500", "purpose": "Redis key 明细与类型/TTL/值预览"},
				{"name": "openai_responses_status", "method": "GET", "path": "/api/openai/responses/status", "purpose": "OpenAI Responses API 安全配置快照"},
				{"name": "openai_responses", "method": "POST", "path": "/api/openai/responses", "purpose": "OpenAI Responses API HTTP 代理接口"},
				{"name": "web_models", "method": "GET", "path": "/api/web/models", "purpose": "Web 聊天看板模型配置列表"},
				{"name": "web_metrics", "method": "GET", "path": "/api/web/metrics", "purpose": "Web 聊天看板请求明细与图表数据"},
				{"name": "stats_resources", "method": "GET", "path": "/api/stats/resources", "purpose": "day/week/month 统一资源统计"},
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

// buildDebugServerStatus 组装 server 分片：运行环境、监听地址、进程启动时长与 Go 运行时版本。
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

// buildDebugProcessStatus 组装 process 分片：PID、可执行文件路径、工作目录与进程启动时长。
// 当前未被 DebugStatusHandler 引用，供测试与后续诊断场景复用。
func buildDebugProcessStatus() gin.H {
	executable, _ := os.Executable()
	workingDir, _ := os.Getwd()
	return gin.H{
		"pid":            os.Getpid(),
		"ppid":           os.Getppid(),
		"executable":     executable,
		"working_dir":    workingDir,
		"go_version":     runtime.Version(),
		"os":             runtime.GOOS,
		"arch":           runtime.GOARCH,
		"num_cpu":        runtime.NumCPU(),
		"goroutines":     runtime.NumGoroutine(),
		"gomaxprocs":     runtime.GOMAXPROCS(0),
		"started_at":     debugProcessStartedAt.Format(time.RFC3339),
		"uptime":         time.Since(debugProcessStartedAt).Round(time.Second).String(),
		"uptime_seconds": int64(time.Since(debugProcessStartedAt).Seconds()),
	}
}

// buildDebugMemoryStatus 读取 Go 运行时内存统计并组装 memory 分片，同时给出字节与 MB
// 两种单位（含 GC 暂停总时长），便于直接判断内存水位与 GC 压力。
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

// buildDebugCapacityStatus 组装 capacity 分片：当前与最大活跃会话数、是否启用上限及占用百分比。
// 未配置上限（maxActive 为 0）时按未启用处理，占用百分比为 0。
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

// buildDebugFeatureStatus 输出 jwt/rate_limit/billing/fallback/redis 各功能开关状态；
// 全局配置缺失时返回空对象，避免调试页读取空指针。
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

// buildDebugRedisStatus 组装 redis 分片：配置地址/连接池参数、客户端可用性、
// 800ms 超时的 Ping 结果（ping_ms 单位毫秒）与连接池统计。
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

// buildDebugNetworkStatus 组装 network 分片：环境变量代理与 openai/azureai 两个模型
// 实际生效的拨号代理（优先级 yaml proxy_url > 环境变量 > 直连）；代理地址输出前统一脱敏。
func buildDebugNetworkStatus() gin.H {
	httpProxy := strings.TrimSpace(os.Getenv("HTTP_PROXY"))
	httpsProxy := strings.TrimSpace(os.Getenv("HTTPS_PROXY"))
	allProxy := strings.TrimSpace(os.Getenv("ALL_PROXY"))
	noProxy := strings.TrimSpace(os.Getenv("NO_PROXY"))

	// OpenAI Realtime 默认使用 wss://。
	// gorilla/websocket 会继承 net/http.ProxyFromEnvironment；
	// 因此 wss/https 上游请求实际生效的是 HTTPS_PROXY。
	// HTTP_PROXY 只对 ws/http 目标生效。

	// 优先级：yaml realtime.proxy_url > HTTPS_PROXY 环境变量 > 直连。
	// 这里独立计算 openai / azure 两个模型的实际拨号代理，便于排障。
	openaiProxy, openaiSource := resolveModelProxy("openai", httpsProxy)
	azureProxy, azureSource := resolveModelProxy("azureai", httpsProxy)

	return gin.H{
		"http_proxy":             maskProxyURL(httpProxy),
		"https_proxy":            maskProxyURL(httpsProxy),
		"all_proxy":              maskProxyURL(allProxy),
		"no_proxy":               noProxy,
		"openai_proxy_effective": maskProxyURL(openaiProxy),
		"openai_proxy_enabled":   openaiProxy != "",
		"openai_proxy_source":    openaiSource,
		"azure_proxy_effective":  maskProxyURL(azureProxy),
		"azure_proxy_enabled":    azureProxy != "",
		"azure_proxy_source":     azureSource,
	}
}

// resolveModelProxy 计算指定模型 Realtime 拨号实际使用的代理。
// 返回 (effectiveProxy, source)：source ∈ {"config","env","none"}。
// 与 client_ws.go Connect() 的代理决策保持一致。
func resolveModelProxy(modelKey, envProxy string) (string, string) {
	return resolveProxy(conf.GetModel(modelKey), envProxy)
}

// resolveProxy 是 resolveModelProxy 的纯函数版本，便于单测，不依赖全局 conf 状态。
func resolveProxy(cfg *conf.ModelConfig, envProxy string) (string, string) {
	if cfg != nil {
		if cp := strings.TrimSpace(cfg.Realtime.ProxyURL); cp != "" {
			return cp, "config"
		}
	}
	if env := strings.TrimSpace(envProxy); env != "" {
		return env, "env"
	}
	return "", "none"
}

// buildDebugOpenAIStatus 组装 openai 模型配置快照：功能开关、Realtime 参数、
// instructions 摘要、API key 脱敏值与 WS 地址（隐藏凭据部分）。
func buildDebugOpenAIStatus() gin.H {
	cfg := conf.GetModel("openai")
	if cfg == nil {
		cfg = &conf.ModelConfig{}
	}

	return gin.H{
		"model_key":               "openai",
		"enabled":                 cfg.Enabled,
		"default_model":           stringOrDefault(cfg.DefaultModel, "gpt-realtime"),
		"instructions_configured": strings.TrimSpace(cfg.Instructions) != "",
		"instructions_length":     len(strings.TrimSpace(cfg.Instructions)),
		"instructions_hash":       shortConfigHash(cfg.Instructions),
		"endpoint":                logger.SafeURLForDisplay(cfg.Endpoint),
		"voice":                   cfg.Voice,
		"api_key_configured":      strings.TrimSpace(cfg.APIKey) != "",
		"api_key_masked":          maskAPIKey(cfg.APIKey),
		"organization_configured": strings.TrimSpace(cfg.Organization) != "",
		"organization":            cfg.Organization,
		"rate_rps":                cfg.RateRPS,
		"rate_burst":              cfg.RateBurst,
		"max_session_ttl":         cfg.MaxSessionTTL,
		"ws_url":                  logger.SafeURLForDisplay(stringOrDefault(cfg.Realtime.WsUrl, "wss://api.openai.com/v1/realtime")),
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
			"proxy_url":             maskProxyURL(strings.TrimSpace(cfg.Realtime.ProxyURL)),
			"proxy_configured":      strings.TrimSpace(cfg.Realtime.ProxyURL) != "",
		},
	}
}

// buildDebugResponsesStatus 组装 openairesponses 模型的安全配置快照，
// 直接复用 provider 的 Status 输出，避免两处维护同一脱敏口径。
func buildDebugResponsesStatus() gin.H {
	cfg := conf.GetModel("openairesponses")
	return openairesponses.Status(cfg)
}

// buildDebugAzureStatus 组装 azureai 模型配置快照：各 capability 的 deployment 清单、
// API key 脱敏值、Realtime 参数与 planned_modules 展示。
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
		"model_key":               "azureai",
		"enabled":                 cfg.Enabled,
		"default_model":           cfg.DefaultModel,
		"instructions_configured": strings.TrimSpace(cfg.Instructions) != "",
		"instructions_length":     len(strings.TrimSpace(cfg.Instructions)),
		"instructions_hash":       shortConfigHash(cfg.Instructions),
		"endpoint":                logger.SafeURLForDisplay(cfg.Endpoint),
		"voice":                   cfg.Voice,
		"api_key_configured":      strings.TrimSpace(cfg.APIKey) != "",
		"api_key_masked":          maskAPIKey(cfg.APIKey),
		"organization_configured": strings.TrimSpace(cfg.Organization) != "",
		"organization":            cfg.Organization,
		"rate_rps":                cfg.RateRPS,
		"rate_burst":              cfg.RateBurst,
		"max_session_ttl":         cfg.MaxSessionTTL,
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
			"ws_url":                logger.SafeURLForDisplay(cfg.Realtime.WsUrl),
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
			"proxy_url":             maskProxyURL(strings.TrimSpace(cfg.Realtime.ProxyURL)),
			"proxy_configured":      strings.TrimSpace(cfg.Realtime.ProxyURL) != "",
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

// bytesToMB 字节数转 MB，供调试页内存展示使用。
func bytesToMB(v uint64) float64 {
	return float64(v) / 1024 / 1024
}

// stringOrDefault 空串（去首尾空白后）时返回 fallback，否则原样返回。
func stringOrDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

// intOrDefault 非正值时返回 fallback，保证调试页展示的可读默认值。
func intOrDefault(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

// durationOrDefault 把配置的时间字符串解析为可读形式；解析失败时保留原文并
// 标注实际生效的 fallback，让配置问题在调试页直接可见。
func durationOrDefault(value string, fallback time.Duration) string {
	if strings.TrimSpace(value) == "" {
		return fallback.String()
	}
	if d, err := time.ParseDuration(value); err == nil {
		return d.String()
	}
	return value + " (invalid, fallback " + fallback.String() + ")"
}

// stringFromExtra 从模型配置的 Extra 表中读取字符串值；键缺失、值为 nil 或
// 类型不是 string 时返回空串，避免类型断言 panic。
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

// shortConfigHash 对配置文本取 sha256 前 6 字节摘要（空文本返回空串），
// 用于在不泄露原文的前提下比对两端配置是否一致。
func shortConfigHash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("sha256:%x", sum[:6])
}

// maskProxyURL 代理 URL 展示前统一脱敏（隐藏用户名密码等凭据部分）。
func maskProxyURL(raw string) string {
	return logger.SafeURLForDisplay(raw)
}

// maskAPIKey 用于 /api/debug/status 等只读诊断接口输出 API Key 的脱敏字符串。
// 设计目标：用户能在测试面板上 一眼对照出当前用的是哪一把 Key
// （多账号 / 多项目情况下排障必备），同时不泄漏完整密钥。
//
//	空                                              -> "未配置"
//	<=8 字符                                        -> "***（长度=N）"  // 太短大概率是占位符
//	"sk-proj-1234567890abcdefghijklmn"              -> "sk-proj...klmn（长度=34）"
//	其他形式（如 Azure 32 位 hex）                 -> 前 4 + ... + 末 4 + 长度
func maskAPIKey(raw string) string {
	key := strings.TrimSpace(raw)
	if key == "" {
		return "未配置"
	}
	n := len(key)
	if n <= 8 {
		return fmt.Sprintf("***（长度=%d）", n)
	}
	// 优先保留 OpenAI 风格的可识别前缀（sk-、sk-proj-、sk-svcacct- 等）。
	prefix := key[:4]
	for _, p := range []string{"sk-proj-", "sk-svcacct-", "sk-admin-", "sk-"} {
		if strings.HasPrefix(key, p) && n > len(p)+4 {
			prefix = p
			break
		}
	}
	if len(prefix) > n-4 {
		prefix = key[:n-4]
	}
	suffix := key[n-4:]
	return fmt.Sprintf("%s...%s（长度=%d）", prefix, suffix, n)
}
