// internal/middleware/rate.go
// 全局限流中间件。
// 文件功能：
// - 输入：gin Context（依赖 Auth 中间件先注入 user_id）、路由模式与 model 查询参数。
// - 输出：限流通过时放行请求，超限时以 429 中止并返回 rps/burst 配置。
// - 采用双层限流：内存令牌桶（Local，本地快速拦截）+ Redis 原子计数（Global，集群全局配额）。
// - 不负责鉴权；未注入 user_id 的请求按未认证拒绝。
// 安全边界：
// - Redis 未启用或计数失败时降级为仅内存限流，降级期间全局配额无法保证。
// - 内存限流器按空闲时间回收，避免高基数 key 导致内存无界增长。
package middleware

import (
	"crypto/md5"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"golang.org/x/time/rate" // 官方令牌桶实现

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/logger"
	"TozoAI-Chat-Api/internal/service/metrics"
	"TozoAI-Chat-Api/internal/service/redis"
)

const (
	localLimiterTTL             = 10 * time.Minute
	localLimiterCleanupInterval = time.Minute
)

type limiterEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// limiters 存储内存限流器实例；空闲条目会被回收，避免高基数用户导致内存无界增长。
var (
	limiters    = make(map[string]*limiterEntry)
	mu          sync.Mutex
	lastCleanup time.Time
)

// RateLimit 返回全局限流中间件：先本地令牌桶快速拦截，再 Redis 计数做全局配额校验。
func RateLimit() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 全局开关关闭时直接放行，不做任何限流。
		if !conf.Global.RateLimit.Enabled {
			c.Next()
			return
		}

		log := logger.GetModelLogger("global")

		// 限流维度之一为模型名，缺失时默认 openai，与上游默认模型保持一致。
		model := c.Query("model")
		if model == "" {
			model = "openai"
		}

		// 用户维度来自 Auth 中间件注入的 user_id；未注入说明请求未完成鉴权，直接拒绝。
		userID, exists := c.Get("user_id")
		if !exists {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing user_id for rate limiting"})
			return
		}

		// 路由维度使用注册的路由模式（如 /v1/chat/completions），而非带参数的完整 URL，避免 key 爆炸。
		path := c.FullPath()

		// 按模型读取限流配置；模型被禁用时不做限流拦截。
		modelCfg := conf.GetModel(model)
		if !modelCfg.Enabled {
			c.Next()
			return
		}
		// RPS 或 Burst 非法时无法构造令牌桶；跳过限流并告警，避免配置错误误伤正常请求。
		if modelCfg.RateRPS <= 0 || modelCfg.RateBurst <= 0 {
			log.Warn("模型限流参数无效，跳过限流",
				zap.String("model", model),
				zap.Int("rate_rps", modelCfg.RateRPS),
				zap.Int("rate_burst", modelCfg.RateBurst))
			c.Next()
			return
		}

		// 构建唯一限流标识：维度为 用户 + 模型 + 路由；先用 MD5 压缩 key 长度，降低 Redis 存储开销。
		rawKey := fmt.Sprintf("rate_limit:%s:%s:%s", userID, model, path)
		hash := md5.Sum([]byte(rawKey))
		limitKey := fmt.Sprintf("rate_limit:%x", hash)

		// 第一层：本地令牌桶限流，无网络开销，承担绝大部分快速拦截。
		now := time.Now()
		mu.Lock()
		// 定期回收空闲超过 10 分钟的限流器，防止长期运行后内存无界增长。
		if now.Sub(lastCleanup) >= localLimiterCleanupInterval {
			for key, entry := range limiters {
				if now.Sub(entry.lastSeen) > localLimiterTTL {
					delete(limiters, key)
				}
			}
			lastCleanup = now
		}
		entry, ok := limiters[limitKey]
		if !ok {
			// 首次访问创建令牌桶：RateRPS 为每秒补充令牌数，RateBurst 为桶容量（突发上限）。
			entry = &limiterEntry{limiter: rate.NewLimiter(rate.Limit(modelCfg.RateRPS), modelCfg.RateBurst)}
			limiters[limitKey] = entry
		}
		entry.lastSeen = now
		limiter := entry.limiter
		mu.Unlock()

		// 本地桶无可用令牌即拒绝，被拒绝的请求不再进入 Redis 层。
		if !limiter.Allow() {
			metrics.RateLimitRejected(userID.(string), model, path, "local")
			log.Warn("触发内存限流拦截",
				zap.String("user_id", userID.(string)),
				zap.String("model", model),
				zap.String("path", path))
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": "rate limit exceeded (local)",
				"model": model,
				"rps":   modelCfg.RateRPS,
				"burst": modelCfg.RateBurst,
			})
			return
		}

		// 第二层：Redis 原子计数，多实例共用同一 key，保证集群全局配额一致。
		ctx := c.Request.Context()
		redisClient := redis.GetClient() // Redis 未启用时返回 nil，此时降级为仅本地限流
		if redisClient == nil {
			c.Next()
			return
		}

		// 对当前秒的计数原子递增；失败时降级放行并告警，代价是降级期间全局配额约束失效。
		cnt, err := redisClient.Incr(ctx, limitKey).Result()
		if err != nil {
			log.Warn("Redis 限流计数操作失败，已降级为本地限流", zap.Error(err))
			c.Next()
			return
		}

		// 每秒第一次计数时设置 1 秒过期，使计数窗口按自然秒自动重置。
		if cnt == 1 {
			_ = redisClient.Expire(ctx, limitKey, 1*time.Second).Err()
		}

		// 全局每秒计数超过模型 Burst 即拒绝，保证集群总量不突破配置的突发上限。
		if cnt > int64(modelCfg.RateBurst) {
			metrics.RateLimitRejected(userID.(string), model, path, "global")
			log.Warn("触发分布式全局限流拦截",
				zap.String("user_id", userID.(string)),
				zap.String("model", model),
				zap.String("path", path),
				zap.Int64("count", cnt),
				zap.Int("burst", modelCfg.RateBurst))
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": "rate limit exceeded (global)",
				"model": model,
				"rps":   modelCfg.RateRPS,
				"burst": modelCfg.RateBurst,
			})
			return
		}

		// 双层限流均通过，放行业务处理。
		c.Next()
	}
}
