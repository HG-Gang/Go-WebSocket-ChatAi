// cmd/server/main.go
// 服务入口文件：负责加载配置、初始化组件、注册路由及中间件，并处理服务生命周期。
//
// 核心逻辑：
//  1. 加载全局配置（loader.go）
//  2. 初始化日志、Redis、模型缓存等单例
//  3. 按需启用 JWT 鉴权、限流、Fallback 降级功能
//  4. 注册路由，区分公开接口与受保护接口
//  5. 启动 HTTP Server 并处理系统信号（优雅关闭）
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
	"TozoAI-Chat-Api/internal/service/redis"
	"TozoAI-Chat-Api/internal/service/session"

	// ========== 关键：空导入触发 Provider 工厂注册 ==========
	// 每个模型包的 init() 函数会调用 provider.Register() 注册工厂
	// 确保 handler.OpenAIRealtimeHandler 能够通过工厂模式创建 Provider 实例
	_ "TozoAI-Chat-Api/internal/provider/azureai"
	_ "TozoAI-Chat-Api/internal/provider/openai"
)

func main() {
	// 1. 初始化配置加载（全局配置 + 环境变量覆盖）
	if err := conf.Load(); err != nil {
		panic(fmt.Sprintf("配置加载失败: %v", err))
	}

	// 2. 环境判断与运行模式设置
	if conf.Global.Env == "prod" {
		gin.SetMode(gin.ReleaseMode)
	} else {
		gin.SetMode(gin.DebugMode)
	}

	// 3. 初始化全局共享组件
	logger.Init()          // 初始化日志系统（zap）
	redis.Init()           // 初始化 Redis 客户端连接
	conf.InitModelConfig() // 初始化模型配置缓存

	// 4. 初始化 Gin 引擎
	r := gin.Default()

	// 5. 注册路由组

	// 5.1 公开路由（无需 JWT 鉴权，无需限流）
	public := r.Group("/")
	{
		// 健康检查接口
		public.GET("/health", func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{
				"status":          "ok",
				"time":            time.Now().Format(time.RFC3339),
				"active_sessions": session.ActiveCount(),
			})
		})

		// 测试接口：生成 JWT Token
		// 支持 ?userId=xxx 指定用户ID（默认 test_user_001）
		public.GET("/test/generate-token", func(c *gin.Context) {
			userID := c.DefaultQuery("userId", "test_user_001")
			token, err := middleware.GenerateToken(userID)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "error": "failed to generate token: " + err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{
				"code":    200,
				"token":   token,
				"user_id": userID,
				"tips":    "请在请求头中携带：Authorization: Bearer " + token,
			})
		})

		// Redis 监控 API：扫描所有 key 并返回类型+值
		// 仅开发环境可用，生产环境应关闭或加鉴权
		public.GET("/api/redis/keys", handler.RedisKeysHandler)
		public.GET("/api/debug/status", handler.DebugStatusHandler)
		public.GET("/api/openai/responses/status", handler.OpenAIResponsesStatusHandler)
		public.GET("/api/azure/status", handler.AzureStatusHandler)

		// Web 测试面板：静态文件服务（开发环境可用）
		// 访问 http://localhost:8080/web/ 打开 WebSocket 测试面板
		// 访问 http://localhost:8080/web/redis.html 打开 Redis 监控面板
		public.Static("/web", "./web")
	}

	// 5.2 受保护路由组（Realtime WS 与 Fallback HTTP）
	auth := r.Group("/")

	// 5.2.1 按需加载中间件：JWT 鉴权校验
	if conf.Global.JWT.Enabled {
		auth.Use(middleware.Auth())
	}

	// 5.2.2 按需加载中间件：全局限流
	if conf.Global.RateLimit.Enabled {
		auth.Use(middleware.RateLimit())
	}

	{
		// OpenAI 实时 WebSocket 接口
		auth.GET("/ws/realtime/openai", handler.OpenAIRealtimeHandler)
		auth.POST("/api/openai/responses", handler.OpenAIResponsesHandler)

		// Azure OpenAI：Realtime + 普通 HTTP 能力代理。
		// 这些接口使用同一个 azureai 模型配置，deployment 与 api-version 从 conf/models/azureai.yaml 读取。
		auth.GET("/ws/realtime/azure", handler.AzureRealtimeHandler)
		auth.POST("/api/azure/chat/completions", handler.AzureChatCompletionsHandler)
		auth.POST("/api/azure/completions", handler.AzureCompletionsHandler)
		auth.POST("/api/azure/images/generations", handler.AzureImageGenerationsHandler)
		auth.POST("/api/azure/images/edits", handler.AzureImageEditsHandler)
		auth.POST("/api/azure/audio/speech", handler.AzureAudioSpeechHandler)
		auth.POST("/api/azure/audio/transcriptions", handler.AzureAudioTranscriptionsHandler)
		auth.POST("/api/azure/audio/translations", handler.AzureAudioTranslationsHandler)
		auth.POST("/api/azure/tst", handler.AzureTSTHandler)

		// 只有在全局降级功能开启时，才暴露 HTTP 降级接口
		if conf.Global.Fallback.Enabled {
			auth.POST("/v1/chat/completions", handler.OpenAIFallbackHandler)
		}
	}

	// 6. 实例化并启动 HTTP 服务器
	serverLog := logger.GetModelLogger("global")
	server := &http.Server{
		Addr:              conf.Global.Server.Addr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	serverLog.Info("服务启动中...",
		zap.String("addr", conf.Global.Server.Addr),
		zap.String("env", conf.Global.Env),
		zap.Bool("jwt_enabled", conf.Global.JWT.Enabled),
		zap.Bool("rate_limit_enabled", conf.Global.RateLimit.Enabled),
		zap.Bool("fallback_enabled", conf.Global.Fallback.Enabled))

	// 6.1 优雅关闭逻辑（监听系统信号）
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		serverLog.Info("收到终止信号，开始优雅关闭...")

		// 设置 10 秒关闭超时
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			serverLog.Fatal("服务强制关闭失败", zap.Error(err))
		}

		redis.Close()    // 断开 Redis 连接
		logger.SyncAll() // 刷新日志缓冲区
		serverLog.Info("服务优雅关闭完成，退出程序")
		os.Exit(0)
	}()

	// 6.2 监听并服务
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		serverLog.Fatal("服务启动失败", zap.Error(err))
	}
}
