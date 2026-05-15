// internal/middleware/rate.go
// 全局限流中间件：
// 实现基于用户、模型、接口维度的访问控制，防止 API 被滥用。
// 采用双层限流策略：
// 1. 内存令牌桶（Local Rate Limiting）：极高性能，适用于快速拦截。
// 2. Redis 分布式计数（Global Rate Limiting）：集群环境下保证全局配额一致性。
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

// RateLimit 全局限流中间件入口
func RateLimit() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 1. 检查全局限流开关
		// 如果配置中关闭了限流功能，则直接放行
		if !conf.Global.RateLimit.Enabled {
			c.Next()
			return
		}

		log := logger.GetModelLogger("global")

		// 2. 提取限流维度参数
		// 获取模型名称，默认为 "openai"
		model := c.Query("model")
		if model == "" {
			model = "openai"
		}

		// 获取用户 ID（由 Auth 中间件注入）
		userID, exists := c.Get("user_id")
		if !exists {
			// 如果没有用户 ID，说明请求未经过鉴权或鉴权失败，拒绝访问
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing user_id for rate limiting"})
			return
		}

		// 获取完整的接口路径
		path := c.FullPath()

		// 3. 获取该模型的特定限流配置
		modelCfg := conf.GetModel(model)
		// 如果模型被禁用，则不进行限流拦截
		if !modelCfg.Enabled {
			c.Next()
			return
		}
		if modelCfg.RateRPS <= 0 || modelCfg.RateBurst <= 0 {
			log.Warn("模型限流参数无效，跳过限流",
				zap.String("model", model),
				zap.Int("rate_rps", modelCfg.RateRPS),
				zap.Int("rate_burst", modelCfg.RateBurst))
			c.Next()
			return
		}

		// 4. 构建唯一的限流标识 (Key)
		// 维度：用户 + 模型 + 路径
		rawKey := fmt.Sprintf("rate_limit:%s:%s:%s", userID, model, path)
		// 使用 MD5 对 Key 进行哈希，缩短长度并规整化，优化 Redis 存储性能
		hash := md5.Sum([]byte(rawKey))
		limitKey := fmt.Sprintf("rate_limit:%x", hash)

		// 5. 第一层：内存令牌桶限流（快速拦截）
		now := time.Now()
		mu.Lock()
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
			// 首次访问时创建限流器
			// RateRPS: 每秒产生的令牌数
			// RateBurst: 桶的最大容量（突发处理能力）
			entry = &limiterEntry{limiter: rate.NewLimiter(rate.Limit(modelCfg.RateRPS), modelCfg.RateBurst)}
			limiters[limitKey] = entry
		}
		entry.lastSeen = now
		limiter := entry.limiter
		mu.Unlock()

		// 尝试获取令牌，如果没有可用令牌则直接拒绝
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

		// 6. 第二层：Redis 分布式限流（全局同步）
		// 注意：如果 Redis 未启用或连接失败，系统将降级为仅使用内存限流
		ctx := c.Request.Context()
		redisClient := redis.GetClient() // Redis 未启用时返回 nil，安全降级
		if redisClient == nil {
			c.Next()
			return
		}

		// 原子递增当前秒内的请求计数
		cnt, err := redisClient.Incr(ctx, limitKey).Result()
		if err != nil {
			log.Warn("Redis 限流计数操作失败，已降级为本地限流", zap.Error(err))
			c.Next()
			return
		}

		// 如果是该秒内的第一次请求，设置 1 秒的过期时间，确保计数重置
		if cnt == 1 {
			_ = redisClient.Expire(ctx, limitKey, 1*time.Second).Err()
		}

		// 检查分布式计数是否超过了配置的突发上限
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

		// 7. 限流通过，继续业务处理
		c.Next()
	}
}
