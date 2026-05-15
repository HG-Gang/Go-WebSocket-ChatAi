// internal/rate/factory.go
// 作用：限流工厂
// 功能：为每个模型返回独立的 rate.Limiter（基于其专属配置的 rps/burst）

package rate

import (
	"sync"

	"TozoAI-Chat-Api/conf"
	"golang.org/x/time/rate"
)

var (
	limiters = make(map[string]*rate.Limiter)
	mu       sync.Mutex
)

// GetLimiter 工厂方法：获取或创建模型专属限流器
func GetLimiter(model string) *rate.Limiter {
	if model == "" {
		model = "global"
	}

	mu.Lock()
	defer mu.Unlock()

	if l, ok := limiters[model]; ok {
		return l
	}

	// 从模型配置中读取限流参数
	cfg := conf.GetModel(model)
	if cfg == nil || cfg.RateRPS <= 0 {
		// 没有配置或禁用限流 → 无限速
		l := rate.NewLimiter(rate.Inf, 10000)
		limiters[model] = l
		return l
	}

	l := rate.NewLimiter(rate.Limit(cfg.RateRPS), cfg.RateBurst)
	limiters[model] = l
	return l
}
