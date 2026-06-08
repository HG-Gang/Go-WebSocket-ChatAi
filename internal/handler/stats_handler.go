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
func StatsResourcesHandler(c *gin.Context) {
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

func validStatsPeriod(period string) bool {
	switch period {
	case "day", "week", "month":
		return true
	default:
		return false
	}
}

func statsFilterResponse(filter stats.ResourceFilter) map[string]string {
	return map[string]string{
		"source": strings.TrimSpace(filter.Source),
		"model":  strings.TrimSpace(filter.Model),
		"kind":   strings.TrimSpace(filter.Kind),
	}
}
