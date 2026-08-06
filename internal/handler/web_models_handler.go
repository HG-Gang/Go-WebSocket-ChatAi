// internal/handler/web_models_handler.go
// Web 看板模型配置处理器：返回前端可安全展示的模型配置列表。
//
// 文件功能：
//   - WebModelsHandler：从 conf.Global.Models 输出模型名称、启用状态、脱敏端点、费率与 key 配置情况。
//
// 安全边界：
//   - 不返回 API key 原文，只返回是否已配置与脱敏后的展示值。
//   - 端点经 SafeURLForDisplay 脱敏，避免 URL query 中的密钥随端点一起暴露。
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

// webModelStringFromExtra 从模型 extra 配置读取字符串指标。
// extra 缺失或值为空时使用 fallback；bool 值转换为 "true"/"false" 文本，保证看板字段类型统一。
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

// inferModelType 根据模型名和端点文本推断展示用类型。
// 命中 azure/openai 关键字时返回对应类型，否则归为 custom；只影响看板展示，不参与路由决策。
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

// webModelMaskAPIKey 生成 API key 的脱敏展示值。
// 空 key 返回“未配置”，长度不超过 8 时全部打码，其余只保留首尾各 4 位，避免看板泄漏可用密钥片段。
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
