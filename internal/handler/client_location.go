package handler

import (
	"strings"

	"github.com/gin-gonic/gin"
)

// clientIPLocationFromRequest 从反向代理/CDN 注入的请求头提取粗粒度所在地。
// 这些字段只用于监控展示和日志排障，不参与鉴权，也不信任其做安全决策。
func clientIPLocationFromRequest(c *gin.Context) map[string]string {
	if c == nil || c.Request == nil {
		return nil
	}
	location := map[string]string{}
	if country := firstHeader(c, "CF-IPCountry", "X-Vercel-IP-Country", "X-Appengine-Country", "X-Geo-Country", "X-Country-Code"); country != "" {
		location["country"] = country
	}
	if region := firstHeader(c, "X-Vercel-IP-Country-Region", "X-Geo-Region", "X-Region", "CF-IPRegion"); region != "" {
		location["region"] = region
	}
	if city := firstHeader(c, "X-Vercel-IP-City", "X-Geo-City", "X-City", "CF-IPCity"); city != "" {
		location["city"] = city
	}
	if len(location) == 0 {
		return nil
	}
	location["source"] = "request_header"
	return location
}

func firstHeader(c *gin.Context, names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(c.GetHeader(name)); value != "" {
			return value
		}
	}
	return ""
}
