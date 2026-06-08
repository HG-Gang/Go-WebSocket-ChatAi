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
	"TozoAI-Chat-Api/internal/service/session"

	_ "TozoAI-Chat-Api/internal/provider/azureai"
	_ "TozoAI-Chat-Api/internal/provider/openai"
)

func main() {
	if err := conf.Load(); err != nil {
		panic(fmt.Sprintf("config load failed: %v", err))
	}

	if conf.Global.Env == "prod" {
		gin.SetMode(gin.ReleaseMode)
	} else {
		gin.SetMode(gin.DebugMode)
	}

	logger.Init()
	redis.Init()
	conf.InitModelConfig()

	r := buildRouter()

	serverLog := logger.GetModelLogger("global")
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

	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	monitorDone := monitor.StartPeriodicLogger(monitorCtx, time.Now(), 30*time.Second)
	cleanupInterval, err := time.ParseDuration(conf.Global.Logs.CleanupInterval)
	if err != nil {
		serverLog.Fatal("log cleanup interval config invalid", zap.String("interval", conf.Global.Logs.CleanupInterval), zap.Error(err))
	}
	cleanupCtx, cleanupCancel := context.WithCancel(context.Background())
	cleanupDone := logger.StartCleanupScheduler(cleanupCtx, conf.Global.Logs.RetentionDays, cleanupInterval)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		serverLog.Info("shutdown signal received")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			serverLog.Fatal("service shutdown failed", zap.Error(err))
		}

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

		redis.Close()
		serverLog.Info("service shutdown complete")
		logger.SyncAll()
		os.Exit(0)
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		serverLog.Fatal("service start failed", zap.Error(err))
	}
}

func buildRouter() *gin.Engine {
	r := gin.Default()
	applyTrustedProxies(r)
	registerRoutes(r)
	return r
}

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

func registerPublicRoutes(public *gin.RouterGroup) {
	public.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":          "ok",
			"time":            time.Now().Format(time.RFC3339),
			"active_sessions": session.ActiveCount(),
		})
	})

	if conf.Global != nil && conf.Global.Security.PublicTokenEnabled {
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

func registerProtectedRoutes(auth *gin.RouterGroup) {
	auth.GET("/ws/realtime/openai", handler.OpenAIRealtimeHandler)
	auth.POST("/api/openai/responses", handler.OpenAIResponsesHandler)
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
		auth.POST("/v1/chat/completions", handler.OpenAIFallbackHandler)
	}
}

func registerDebugRoutes(group *gin.RouterGroup) {
	group.GET("/api/redis/keys", handler.RedisKeysHandler)
	group.GET("/api/debug/status", handler.DebugStatusHandler)
	group.GET("/api/openai/responses/status", handler.OpenAIResponsesStatusHandler)
	group.GET("/api/web/models", handler.WebModelsHandler)
	group.GET("/api/web/metrics", handler.WebMetricsHandler)
	group.GET("/api/stats/resources", handler.StatsResourcesHandler)
	group.GET("/api/azure/status", handler.AzureStatusHandler)
}
