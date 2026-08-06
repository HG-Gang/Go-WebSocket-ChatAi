// internal/rate/factory.go
// 模型限流器工厂。
// 文件功能：
// - 输入：模型名；输出：该模型进程内共享的 rate.Limiter。
// - 限流参数来自 conf.GetModel 的 RateRPS/RateBurst；模型未配置或参数非法时返回无限速限流器。
// - 不负责实际请求拦截与 Redis 分布式计数（见 internal/middleware/rate.go）。
// 安全边界：
// - 错误配置按"放开"处理（rate.Inf），避免把配置错误放大为全量拦截，由调用方决定后续策略。
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

// GetLimiter 获取或创建模型专属限流器，同一模型进程内复用同一实例。
// 模型名为空时归一到 global；配置缺失或 RateRPS<=0 时返回无限速限流器（rate.Inf），不返回错误。
func GetLimiter(model string) *rate.Limiter {
	// 空模型名归一到 global，保证未指定模型的调用共享同一配额维度。
	if model == "" {
		model = "global"
	}

	mu.Lock()
	defer mu.Unlock()

	if l, ok := limiters[model]; ok {
		return l
	}

	// 从模型配置中读取限流参数。
	cfg := conf.GetModel(model)
	if cfg == nil || cfg.RateRPS <= 0 {
		// 配置缺失或 RPS 非法时返回无限速；rate.Inf 下 burst 参数不生效，10000 仅为占位。
		l := rate.NewLimiter(rate.Inf, 10000)
		limiters[model] = l
		return l
	}

	l := rate.NewLimiter(rate.Limit(cfg.RateRPS), cfg.RateBurst)
	limiters[model] = l
	return l
}
