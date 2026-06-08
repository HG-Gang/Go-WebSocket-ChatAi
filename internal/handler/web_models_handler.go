package handler

import (
	"net/http"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/logger"
)

// WebModelsHandler 返回 Web 面板可安全展示的模型配置列表。
// 不返回 API Key 原文，仅返回是否已配置以及脱敏后的标识。
func WebModelsHandler(c *gin.Context) {
	models := make([]gin.H, 0)
	if conf.Global != nil {
		names := make([]string, 0, len(conf.Global.Models))
		for name := range conf.Global.Models {
			names = append(names, name)
		}
		sort.Strings(names)

		for _, name := range names {
			cfg := conf.Global.Models[name]
			modelType := webModelStringFromExtra(cfg.Extra, "type", inferModelType(name, cfg.Endpoint))
			billingMode := webModelStringFromExtra(cfg.Extra, "billing_mode", "token")
			models = append(models, gin.H{
				"name":               name,
				"enabled":            cfg.Enabled,
				"default_model":      cfg.DefaultModel,
				"endpoint":           logger.SafeURLForDisplay(cfg.Endpoint),
				"api_key_configured": strings.TrimSpace(cfg.APIKey) != "",
				"api_key_masked":     webModelMaskAPIKey(cfg.APIKey),
				"type":               modelType,
				"billing_mode":       billingMode,
				"rate_rps":           cfg.RateRPS,
				"rate_burst":         cfg.RateBurst,
				"organization_set":   strings.TrimSpace(cfg.Organization) != "",
			})
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"data": models,
	})
}

func webModelStringFromExtra(extra map[string]interface{}, key, fallback string) string {
	if extra == nil {
		return fallback
	}
	value, ok := extra[key]
	if !ok || value == nil {
		return fallback
	}
	switch v := value.(type) {
	case string:
		if strings.TrimSpace(v) != "" {
			return v
		}
	case bool:
		if v {
			return "true"
		}
		return "false"
	}
	return fallback
}

func inferModelType(name, endpoint string) string {
	joined := strings.ToLower(name + " " + endpoint)
	switch {
	case strings.Contains(joined, "azure"):
		return "azure"
	case strings.Contains(joined, "openai"):
		return "openai"
	default:
		return "custom"
	}
}

func webModelMaskAPIKey(apiKey string) string {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return "未配置"
	}
	if len(apiKey) <= 8 {
		return "****"
	}
	return apiKey[:4] + "..." + apiKey[len(apiKey)-4:]
}
