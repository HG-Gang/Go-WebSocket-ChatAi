// internal/service/redis/redis.go
// Redis 客户端的统一封装层
//
// 文件功能：
// - 初始化 Redis 连接（使用 conf.Global.Redis 配置）
// - 提供常用操作封装（Get/Set/Expire/HGet/HSet 等）
// - 支持按模型隔离 key 前缀（避免不同模型数据冲突）
// - 连接池、超时、重连由 go-redis 自动管理
//
// 安全边界：
// - 密码只来自 conf.Global.Redis.Password（生产环境从环境变量注入），本文件不落盘密钥
// - Redis 未启用时 GetClient 返回 nil，调用方需自行判断，本包不做失败关闭
package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/logger"
)

var (
	client *redis.Client
	once   sync.Once
)

// Init 初始化 Redis 客户端（单例模式）
// 使用 sync.Once 保证并发调用只初始化一次；Redis 未启用时静默跳过。
func Init() {
	once.Do(func() {
		if !conf.Global.Redis.Enabled {
			logger.GetModelLogger("global").Warn("Redis 未启用，跳过初始化")
			return
		}

		// 连接池配置缺失或非法时回退到保守默认值，避免连接池过小导致请求排队。
		poolSize := conf.Global.Redis.PoolSize
		if poolSize <= 0 {
			poolSize = 128
		}
		minIdleConns := conf.Global.Redis.MinIdleConns
		if minIdleConns < 0 {
			minIdleConns = 0
		}

		client = redis.NewClient(&redis.Options{
			Addr:         conf.Global.Redis.Addr,     // Redis 地址
			Password:     conf.Global.Redis.Password, // 密码（生产环境从环境变量注入）
			DB:           conf.Global.Redis.DB,       // 数据库编号
			PoolSize:     poolSize,                   // 连接池大小（按实例并发配置）
			MinIdleConns: minIdleConns,               // 最小空闲连接
			DialTimeout:  5 * time.Second,            // 连接超时
			ReadTimeout:  3 * time.Second,            // 读取超时
			WriteTimeout: 3 * time.Second,            // 写入超时
		})

		// 启动即 Ping 验证连通性，失败直接 Fatal 失败关闭，避免应用带病启动。
		ctx := context.Background()
		if _, err := client.Ping(ctx).Result(); err != nil {
			logger.GetModelLogger("global").Fatal("Redis 连接失败", zap.Error(err))
		}

		logger.GetModelLogger("global").Info("Redis 初始化成功",
			zap.String("addr", conf.Global.Redis.Addr),
			zap.Int("db", conf.Global.Redis.DB),
			zap.Int("pool_size", poolSize),
			zap.Int("min_idle_conns", minIdleConns))
	})
}

// GetClient 获取 Redis 客户端实例
// 当 Redis 未启用或初始化失败时返回 nil，调用方需自行判断
func GetClient() *redis.Client {
	return client
}

// MustGetClient 获取 Redis 客户端实例（不可用时 panic，仅内部强依赖场景使用）
func MustGetClient() *redis.Client {
	if client == nil {
		panic("Redis 未启用或初始化失败")
	}
	return client
}

// GetWithPrefix 按模型添加 key 前缀（避免冲突）
func GetWithPrefix(model, key string) string {
	// 空模型名回退到 global 前缀，保证所有调用方都有隔离的 Key 空间。
	if model == "" {
		model = "global"
	}
	return fmt.Sprintf("%s:%s", model, key)
}

// Set 设置键值（带过期时间）
// value 必须是 string / []byte / 数值类型，不支持 map/struct（请使用 HSetMap）
func Set(model, key string, value interface{}, expiration time.Duration) error {
	c := GetClient()
	if c == nil {
		return nil // Redis 未启用，静默跳过
	}
	ctx := context.Background()
	return c.Set(ctx, GetWithPrefix(model, key), value, expiration).Err()
}

// HSetMap 批量写入 Hash 字段（适合 map[string]interface{} 场景）
func HSetMap(model, key string, fields map[string]interface{}, expiration time.Duration) error {
	c := GetClient()
	if c == nil {
		return nil
	}
	ctx := context.Background()
	prefixedKey := GetWithPrefix(model, key)

	// map 展开为 HSet 参数列表；用 Pipeline 把写入与过期合并为一次网络往返，TTL 非正数时不设置过期。
	args := make([]interface{}, 0, len(fields)*2)
	for k, v := range fields {
		args = append(args, k, v)
	}
	pipe := c.Pipeline()
	pipe.HSet(ctx, prefixedKey, args...)
	if expiration > 0 {
		pipe.Expire(ctx, prefixedKey, expiration)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Get 获取键值
func Get(model, key string) (string, error) {
	c := GetClient()
	if c == nil {
		return "", nil
	}
	ctx := context.Background()
	return c.Get(ctx, GetWithPrefix(model, key)).Result()
}

// HSet Hash 设置字段值
func HSet(model, key, field string, value interface{}) error {
	c := GetClient()
	if c == nil {
		return nil
	}
	ctx := context.Background()
	return c.HSet(ctx, GetWithPrefix(model, key), field, value).Err()
}

// HGet Hash 获取字段值
func HGet(model, key, field string) (string, error) {
	c := GetClient()
	if c == nil {
		return "", nil
	}
	ctx := context.Background()
	return c.HGet(ctx, GetWithPrefix(model, key), field).Result()
}

// Del 删除键
func Del(model, key string) error {
	c := GetClient()
	if c == nil {
		return nil
	}
	ctx := context.Background()
	return c.Del(ctx, GetWithPrefix(model, key)).Err()
}

// Close 关闭 Redis 连接（程序退出时调用）
func Close() {
	if client != nil {
		client.Close()
		logger.GetModelLogger("global").Info("Redis 连接已关闭")
	}
}
