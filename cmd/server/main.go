// cmd/server/main.go
// 文件功能：服务进程入口。负责配置加载、gin 路由装配、HTTP 服务启动、周期监控与
// 日志清理调度，以及收到 SIGINT/SIGTERM 时的优雅退出（HTTP 优雅关停 → 后台协程停止 → 资源释放）。
// 输入是 conf 目录配置与环境变量；输出是监听 conf.Global.Server.Addr 的 HTTP 服务。
// 不负责：具体业务处理（见 internal/handler）、WS 协议细节（见 internal/service/session）。
// 安全边界：配置加载或生产校验失败时 panic 终止启动（失败关闭）；未配置可信代理时
// 显式清空（不信任 X-Forwarded-For）；测试 token 与调试接口仅在对应安全开关开启时注册。
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/handler"
	"TozoAI-Chat-Api/internal/logger"
	"TozoAI-Chat-Api/internal/middleware"
	"TozoAI-Chat-Api/internal/service/monitor"
	"TozoAI-Chat-Api/internal/service/redis"
	"TozoAI-Chat-Api/internal/service/requestlog"
	"TozoAI-Chat-Api/internal/service/session"

	_ "TozoAI-Chat-Api/internal/provider/azureai"
	_ "TozoAI-Chat-Api/internal/provider/openai"
)

func main() {
	// 配置加载失败直接 panic，任何缺省配置都不允许带病启动。
	if err := conf.Load(); err != nil {
		panic(fmt.Sprintf("config load failed: %v", err))
	}

	// 生产环境切到 gin ReleaseMode，关闭调试日志输出。
	if conf.Global.Env == "prod" {
		gin.SetMode(gin.ReleaseMode)
	} else {
		gin.SetMode(gin.DebugMode)
	}

	// 初始化顺序：日志 → Redis → 模型配置缓存，随后按需初始化请求明细 DB。
	logger.Init()
	redis.Init()
	conf.InitModelConfig()

	serverLog := logger.GetModelLogger("global")
	if conf.Global != nil && conf.Global.DB.Enabled {
		// DB 初始化失败属于致命错误，日志可用时直接 Fatal 终止启动。
		if err := requestlog.Init(true, conf.Global.DB.Driver, conf.Global.DB.DSN); err != nil {
			serverLog.Fatal("requestlog db init failed", zap.Error(err))
		}
		serverLog.Info("requestlog db ready",
			zap.String("driver", conf.Global.DB.Driver),
			zap.String("dsn", conf.Global.DB.DSN))
	}

	r := buildRouter()
	// HTTP 超时参数用于防慢速连接长期占用：读请求头 5 秒、空闲 120 秒、Header 上限 1MB。
	server := &http.Server{
		Addr:              conf.Global.Server.Addr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	serverLog.Info("service starting",
		zap.String("addr", conf.Global.Server.Addr),
		zap.String("env", conf.Global.Env),
		zap.Bool("jwt_enabled", conf.Global.JWT.Enabled),
		zap.Bool("rate_limit_enabled", conf.Global.RateLimit.Enabled),
		zap.Bool("fallback_enabled", conf.Global.Fallback.Enabled))

	// 启动周期指标日志与日志清理调度器，二者均通过 context 取消实现优雅退出。
	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	monitorDone := monitor.StartPeriodicLogger(monitorCtx, time.Now(), 30*time.Second)
	cleanupInterval, err := time.ParseDuration(conf.Global.Logs.CleanupInterval)
	if err != nil {
		// 清理间隔配置非法属于启动期错误，立即终止启动。
		serverLog.Fatal("log cleanup interval config invalid", zap.String("interval", conf.Global.Logs.CleanupInterval), zap.Error(err))
	}
	cleanupCtx, cleanupCancel := context.WithCancel(context.Background())
	cleanupDone := logger.StartCleanupScheduler(cleanupCtx, conf.Global.Logs.RetentionDays, cleanupInterval)

	// 监听 SIGINT/SIGTERM；收到信号后按依赖逆序关停：先停 HTTP，再停后台协程，最后释放资源。
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		serverLog.Info("shutdown signal received")

		// 最多 10 秒内完成 HTTP 优雅关停，超时未结束的连接被强制断开。
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			serverLog.Fatal("service shutdown failed", zap.Error(err))
		}

		// 停掉周期监控与日志清理调度器，并各自最多等 2 秒退出，避免无限阻塞关停流程。
		monitorCancel()
		select {
		case <-monitorDone:
		case <-time.After(2 * time.Second):
			serverLog.Warn("periodic monitor logger did not stop before shutdown timeout")
		}

		cleanupCancel()
		select {
		case <-cleanupDone:
		case <-time.After(2 * time.Second):
			serverLog.Warn("log cleanup scheduler did not stop before shutdown timeout")
		}

		// 关闭外部资源并同步刷新日志缓冲后再退出。
		redis.Close()
		requestlog.Close()
		serverLog.Info("service shutdown complete")
		logger.SyncAll()
		os.Exit(0)
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		serverLog.Fatal("service start failed", zap.Error(err))
	}
}

// buildRouter 组装 gin 路由：先配置可信代理，再注册公共与受保护路由。
func buildRouter() *gin.Engine {
	r := gin.Default()
	applyTrustedProxies(r)
	registerRoutes(r)
	return r
}

// applyTrustedProxies 应用可信代理白名单。
// 未配置时显式清空可信代理，使 gin 忽略 X-Forwarded-For 等转发头、以 RemoteAddr 为准；
// 配置了非法 IP/CIDR 时 panic，避免带着错误的信任边界启动。
func applyTrustedProxies(r *gin.Engine) {
	if r == nil {
		return
	}
	if conf.Global == nil || len(conf.Global.Security.TrustedProxies) == 0 {
		_ = r.SetTrustedProxies(nil)
		return
	}
	if err := r.SetTrustedProxies(conf.Global.Security.TrustedProxies); err != nil {
		panic(fmt.Sprintf("trusted proxies config invalid: %v", err))
	}
}

// registerRoutes 划分公共与受保护路由组：受保护组按开关挂 JWT 鉴权与全局限流中间件。
func registerRoutes(r *gin.Engine) {
	public := r.Group("/")
	registerPublicRoutes(public)

	auth := r.Group("/")
	if conf.Global != nil && conf.Global.JWT.Enabled {
		auth.Use(middleware.Auth())
	}
	if conf.Global != nil && conf.Global.RateLimit.Enabled {
		auth.Use(middleware.RateLimit())
	}
	registerProtectedRoutes(auth)
}

// registerPublicRoutes 注册无需鉴权的路由。
// 注意：/test/generate-token 与调试接口仅在对应安全开关开启时注册，
// 生产环境必须由 loader 的生产校验强制关闭。
func registerPublicRoutes(public *gin.RouterGroup) {
	public.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":          "ok",
			"time":            time.Now().Format(time.RFC3339),
			"active_sessions": session.ActiveCount(),
		})
	})

	if conf.Global != nil && conf.Global.Security.PublicTokenEnabled {
		// 测试 token 生成入口：仅供联调环境使用，便于在 JWT 鉴权开启时拿到合法 token。
		public.GET("/test/generate-token", func(c *gin.Context) {
			userID := c.DefaultQuery("userId", "test_user_001")
			userName := c.DefaultQuery("userName", "")
			token, err := middleware.GenerateTokenWithUserName(userID, userName)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "error": "failed to generate token: " + err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{
				"code":      200,
				"token":     token,
				"user_id":   userID,
				"user_name": userName,
				"tips":      "Authorization: Bearer " + token,
			})
		})
	}

	if conf.Global != nil && conf.Global.Security.PublicDebugEnabled {
		registerDebugRoutes(public)
	}

	webStatic := handler.WebStaticHandler("./web")
	public.GET("/web", webStatic)
	public.HEAD("/web", webStatic)
	public.GET("/web/*filepath", webStatic)
	public.HEAD("/web/*filepath", webStatic)
}

// registerProtectedRoutes 注册需要鉴权的业务路由（JWT/限流中间件在 registerRoutes 中挂载）。
// 调试路由在未开启公共调试开关时挂到受保护组，保证调试接口至少要求鉴权。
func registerProtectedRoutes(auth *gin.RouterGroup) {
	auth.GET("/ws/realtime/openai", handler.OpenAIRealtimeHandler)
	auth.POST("/api/openai/responses", handler.OpenAIResponsesHandler)
	auth.POST("/api/web/chat", handler.WebChatHandler)
	auth.POST("/api/web/upload", handler.WebUploadHandler)
	auth.GET("/api/web/requests", handler.WebRequestsHandler)
	auth.GET("/api/web/requests/stats", handler.WebRequestStatsHandler)
	auth.GET("/api/workspace/projects", handler.WorkspaceProjectsHandler)
	auth.GET("/api/workspace/list", handler.WorkspaceListHandler)
	auth.GET("/api/workspace/read", handler.WorkspaceReadHandler)
	auth.POST("/api/workspace/write", handler.WorkspaceWriteHandler)
	auth.POST("/api/workspace/write/confirm", handler.WorkspaceWriteConfirmHandler)
	auth.POST("/api/workspace/write/reject", handler.WorkspaceWriteRejectHandler)

	auth.GET("/ws/realtime/azure", handler.AzureRealtimeHandler)
	auth.POST("/api/azure/chat/completions", handler.AzureChatCompletionsHandler)
	auth.POST("/api/azure/completions", handler.AzureCompletionsHandler)
	auth.POST("/api/azure/images/generations", handler.AzureImageGenerationsHandler)
	auth.POST("/api/azure/images/edits", handler.AzureImageEditsHandler)
	auth.POST("/api/azure/audio/speech", handler.AzureAudioSpeechHandler)
	auth.POST("/api/azure/audio/transcriptions", handler.AzureAudioTranscriptionsHandler)
	auth.POST("/api/azure/audio/translations", handler.AzureAudioTranslationsHandler)
	auth.POST("/api/azure/tst", handler.AzureTSTHandler)

	if conf.Global == nil || !conf.Global.Security.PublicDebugEnabled {
		registerDebugRoutes(auth)
	}

	if conf.Global != nil && conf.Global.Fallback.Enabled {
		// 降级开关开启时才注册兼容 /v1/chat/completions 的降级路由。
		auth.POST("/v1/chat/completions", handler.OpenAIFallbackHandler)
	}
}

// registerDebugRoutes 注册调试与状态查看路由（Redis keys、服务状态、指标等）。
// 挂载位置由调用方决定（公共或受保护组），本函数只负责注册。
func registerDebugRoutes(group *gin.RouterGroup) {
	group.GET("/api/redis/keys", handler.RedisKeysHandler)
	group.GET("/api/debug/status", handler.DebugStatusHandler)
	group.GET("/api/openai/responses/status", handler.OpenAIResponsesStatusHandler)
	group.GET("/api/web/models", handler.WebModelsHandler)
	group.GET("/api/web/metrics", handler.WebMetricsHandler)
	group.GET("/api/stats/resources", handler.StatsResourcesHandler)
	group.GET("/api/azure/status", handler.AzureStatusHandler)
}
