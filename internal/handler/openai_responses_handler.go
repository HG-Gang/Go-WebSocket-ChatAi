// internal/handler/openai_responses_handler.go
// OpenAI Responses API 的 HTTP 接入处理器。
//
// 文件功能：
//   - 接收贴近官方 /v1/responses 结构的 JSON 请求，校验模型启用状态与必填 input 字段。
//   - 调用 openairesponses Provider 转发上游，并以统一 code/latency_ms/data 结构返回。
//   - 提供仅面向 Web 诊断页的配置状态快照接口（不返回 API Key 原文）。
//
// 安全边界：
//   - 错误响应中的上游原文与错误摘要统一经 RedactField 脱敏后再返回，避免泄露敏感内容。
//   - 请求体中的 model_config/_model_config 键在此提取后即从 payload 删除，不参与上游转发。
package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
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
	// 请求体需为合法 JSON 对象；解析失败时不向下游转发，直接返回 400。
	var payload map[string]any
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "invalid JSON: " + err.Error()})
		return
	}
	// 请求可通过 model_config 指定路由到哪个模型配置，未指定时默认 openairesponses。
	modelConfig := responseModelConfigName(payload)
	log := logger.GetModelLogger(modelConfig)
	cfg := conf.GetModel(modelConfig)
	if cfg == nil || !cfg.Enabled {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": modelConfig + " model is not enabled"})
		return
	}
	// 缺少必填 input 字段时直接拒绝，避免非法请求进入上游。
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
		// 上游失败或超时时统一返回 502/504；错误正文中不携带上游原始响应，
		// 只返回脱敏后的摘要与状态码，避免敏感内容泄漏给调用方。
		status := http.StatusBadGateway
		if ctx.Err() == context.DeadlineExceeded {
			status = http.StatusGatewayTimeout
		}
		errorSummary := logger.RedactField("content", err.Error())
		addResponsesMetric(c, modelConfig, cfg, payload, result, "failed", latency, errorSummary)
		log.Warn("Responses API 请求失败",
			zap.Error(err),
			zap.String("error_summary", errorSummary),
			zap.Duration("latency", latency),
			zap.Int("upstream_status", upstreamStatus(result)))
		c.JSON(status, gin.H{
			"code":                 status,
			"error":                "responses upstream request failed",
			"error_summary":        errorSummary,
			"latency_ms":           latency.Milliseconds(),
			"upstream_status":      upstreamStatus(result),
			"upstream_raw_summary": redactedRawJSON(result),
		})
		return
	}

	addResponsesMetric(c, modelConfig, cfg, payload, result, result.Status, latency, "")
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

// responseModelConfigName 从 payload 中提取路由用的模型配置名（键为 model_config 或
// _model_config，后者优先级更高），提取后删除该键，防止其被转发给上游。
func responseModelConfigName(payload map[string]any) string {
	if payload == nil {
		return "openairesponses"
	}
	modelConfig := ""
	for _, key := range []string{"model_config", "_model_config"} {
		value, ok := payload[key]
		delete(payload, key)
		if !ok {
			continue
		}
		if text, ok := value.(string); ok && strings.TrimSpace(text) != "" {
			modelConfig = strings.TrimSpace(text)
		}
	}
	if modelConfig != "" {
		return modelConfig
	}
	return "openairesponses"
}

// responseTimeout 读取模型配置中的上游超时（毫秒）；配置缺失或非法时兜底为 60 秒。
func responseTimeout(cfg *conf.ModelConfig) time.Duration {
	status := openairesponses.Status(cfg)
	if value, ok := status["timeout_ms"].(int); ok && value > 0 {
		return time.Duration(value) * time.Millisecond
	}
	return 60 * time.Second
}

// upstreamStatus 返回上游 HTTP 状态码；上游未返回响应（result 为 nil）时按 0 处理。
func upstreamStatus(result *openairesponses.Result) int {
	if result == nil {
		return 0
	}
	return result.StatusCode
}

// rawJSON 返回上游响应原始字节；无响应时返回 nil，供脱敏与诊断复用。
func rawJSON(result *openairesponses.Result) json.RawMessage {
	if result == nil {
		return nil
	}
	return result.Raw
}

// redactedRawJSON 对上游原始响应做 content 字段脱敏后返回，用于错误诊断输出。
func redactedRawJSON(result *openairesponses.Result) string {
	raw := rawJSON(result)
	if len(raw) == 0 {
		return ""
	}
	return logger.RedactField("content", string(raw))
}
