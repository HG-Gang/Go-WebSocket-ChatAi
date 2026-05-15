package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/logger"
	"TozoAI-Chat-Api/internal/provider/openairesponses"
)

// OpenAIResponsesStatusHandler 输出 Responses API 的安全配置快照。
// 该接口只给 Web 诊断页使用，不返回 API Key 原文。
func OpenAIResponsesStatusHandler(c *gin.Context) {
	cfg := conf.GetModel("openairesponses")
	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"data": openairesponses.Status(cfg),
	})
}

// OpenAIResponsesHandler 接入 OpenAI Responses API。
// 请求体尽量贴近官方 /v1/responses JSON，网关只补齐 model、instructions、store 等默认值。
func OpenAIResponsesHandler(c *gin.Context) {
	log := logger.GetModelLogger("openairesponses")
	cfg := conf.GetModel("openairesponses")
	if cfg == nil || !cfg.Enabled {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "openairesponses model is not enabled"})
		return
	}

	var payload map[string]any
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "invalid JSON: " + err.Error()})
		return
	}
	if _, ok := payload["input"]; !ok {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "missing required field: input"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), responseTimeout(cfg))
	defer cancel()

	start := time.Now()
	result, err := openairesponses.New(cfg).Create(ctx, payload)
	latency := time.Since(start)
	if err != nil {
		status := http.StatusBadGateway
		if ctx.Err() == context.DeadlineExceeded {
			status = http.StatusGatewayTimeout
		}
		log.Warn("Responses API 请求失败",
			zap.Error(err),
			zap.Duration("latency", latency),
			zap.Int("upstream_status", upstreamStatus(result)))
		c.JSON(status, gin.H{
			"code":            status,
			"error":           err.Error(),
			"latency_ms":      latency.Milliseconds(),
			"upstream_status": upstreamStatus(result),
			"upstream_raw":    rawJSON(result),
		})
		return
	}

	log.Info("Responses API 请求完成",
		zap.String("response_id", result.ID),
		zap.String("model", result.Model),
		zap.String("status", result.Status),
		zap.Duration("latency", latency))
	c.JSON(http.StatusOK, gin.H{
		"code":       200,
		"latency_ms": latency.Milliseconds(),
		"data":       result,
	})
}

func responseTimeout(cfg *conf.ModelConfig) time.Duration {
	status := openairesponses.Status(cfg)
	if value, ok := status["timeout_ms"].(int); ok && value > 0 {
		return time.Duration(value) * time.Millisecond
	}
	return 60 * time.Second
}

func upstreamStatus(result *openairesponses.Result) int {
	if result == nil {
		return 0
	}
	return result.StatusCode
}

func rawJSON(result *openairesponses.Result) json.RawMessage {
	if result == nil {
		return nil
	}
	return result.Raw
}
