// internal/handler/stats_handler.go
// 资源统计接口：按 day/week/month 窗口返回各模型资源的用量聚合数据。
//
// 文件功能：
//   - StatsResourcesHandler: 校验 period 参数后，从 stats 服务读取三组窗口数据并返回，
//     同时把聚合结果以 web metrics 事件发出，供实时指标面板展示。
//   - 不负责指标采集与存储，只做参数校验、组装与转发。
package handler

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"TozoAI-Chat-Api/internal/service/stats"
)

// StatsResourcesHandler 返回统一资源统计的 day/week/month 窗口。
// period 只决定 selected 指向哪个窗口；periods 始终返回三组数据，便于前端在柱状图、折线图等视图间切换。
// 非法 period 返回 400；可选过滤参数 source/model/kind 透传给 stats 服务。
func StatsResourcesHandler(c *gin.Context) {
	// period 归一化后必须是 day/week/month 三者之一，否则拒绝请求，避免后续用非法键取数。
	period := strings.ToLower(strings.TrimSpace(c.DefaultQuery("period", "day")))
	if period == "" {
		period = "day"
	}
	if !validStatsPeriod(period) {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":  400,
			"error": "period must be one of day, week, month",
		})
		return
	}

	filter := stats.ResourceFilter{
		Source: c.Query("source"),
		Model:  c.Query("model"),
		Kind:   c.Query("kind"),
	}
	// 以服务器当前时间为基准生成三组窗口；selected 按 period 指向其中一组。
	now := time.Now()
	periods := stats.ResourcePeriodsWithFilter(now, filter)
	emitWebMetricsLog(webMetricsLogEvent{Event: "stats_rollup", Resources: periods})
	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"data": gin.H{
			"generated_at": now.Format(time.RFC3339),
			"period":       period,
			"filters":      statsFilterResponse(filter),
			"selected":     periods[period],
			"periods":      periods,
		},
	})
}

// validStatsPeriod 判断 period 是否为受支持的窗口名（day/week/month）。
func validStatsPeriod(period string) bool {
	switch period {
	case "day", "week", "month":
		return true
	default:
		return false
	}
}

// statsFilterResponse 把过滤条件转成响应结构（去除首尾空白），供前端回显本次筛选。
func statsFilterResponse(filter stats.ResourceFilter) map[string]string {
	return map[string]string{
		"source": strings.TrimSpace(filter.Source),
		"model":  strings.TrimSpace(filter.Model),
		"kind":   strings.TrimSpace(filter.Kind),
	}
}
