package handler

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	redisService "TozoAI-Chat-Api/internal/service/redis"
)

// RedisKeyInfo 是 Redis 调试页面每一行返回的数据结构。
// 这里额外返回 category 和 description，方便直接判断 key 属于会话元数据、
// 计费、限流、OpenAI Realtime 状态，还是未知业务区域。
type RedisKeyInfo struct {
	Key         string      `json:"key"`
	Type        string      `json:"type"`
	TTL         int64       `json:"ttl"`
	Category    string      `json:"category"`
	Description string      `json:"description"`
	Value       interface{} `json:"value"`
	ValueSafe   bool        `json:"value_safe"`
}

func RedisKeysHandler(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	pattern := c.DefaultQuery("pattern", "*")
	cursorStr := c.DefaultQuery("cursor", "0")
	countStr := c.DefaultQuery("count", "200")
	fullValue := c.DefaultQuery("full", "0") == "1"

	cursor, _ := strconv.ParseUint(cursorStr, 10, 64)
	count, _ := strconv.ParseInt(countStr, 10, 64)
	if count <= 0 || count > 1000 {
		count = 200
	}

	client := redisService.GetClient()
	if client == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"code":  503,
			"error": "Redis 未启用或客户端不可用",
		})
		return
	}

	keys, nextCursor, err := client.Scan(ctx, cursor, pattern, count).Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":  500,
			"error": "Redis SCAN 失败: " + err.Error(),
		})
		return
	}

	result := make([]RedisKeyInfo, 0, len(keys))
	for _, key := range keys {
		category, description := explainRedisKey(key)
		info := RedisKeyInfo{
			Key:         key,
			Category:    category,
			Description: description,
		}

		keyType, err := client.Type(ctx, key).Result()
		if err != nil {
			info.Type = "unknown"
			info.Value = "error: " + err.Error()
			result = append(result, info)
			continue
		}
		info.Type = keyType

		ttl, err := client.TTL(ctx, key).Result()
		if err != nil {
			info.TTL = -1
		} else {
			info.TTL = int64(ttl.Seconds())
		}

		switch keyType {
		case "string":
			val, err := client.Get(ctx, key).Result()
			if err != nil {
				info.Value = "error: " + err.Error()
			} else {
				info.Value = sanitizeRedisValue(key, "", val)
				info.ValueSafe = true
			}
		case "hash":
			val, err := client.HGetAll(ctx, key).Result()
			if err != nil {
				info.Value = "error: " + err.Error()
			} else {
				info.Value = sanitizeRedisHash(key, val)
				info.ValueSafe = true
			}
		case "list":
			stop := int64(99)
			if fullValue {
				stop = -1
			}
			val, err := client.LRange(ctx, key, 0, stop).Result()
			if err != nil {
				info.Value = "error: " + err.Error()
			} else {
				info.Value = sanitizeRedisStrings(key, "item", val)
				info.ValueSafe = true
			}
		case "set":
			val, err := client.SMembers(ctx, key).Result()
			if err != nil {
				info.Value = "error: " + err.Error()
			} else {
				info.Value = sanitizeRedisStrings(key, "member", val)
				info.ValueSafe = true
			}
		case "zset":
			stop := int64(99)
			if fullValue {
				stop = -1
			}
			val, err := client.ZRangeWithScores(ctx, key, 0, stop).Result()
			if err != nil {
				info.Value = "error: " + err.Error()
			} else {
				items := make([]map[string]interface{}, 0, len(val))
				for _, item := range val {
					items = append(items, map[string]interface{}{
						"score":  item.Score,
						"member": sanitizeRedisValue(key, "member", item.Member),
					})
				}
				info.Value = items
				info.ValueSafe = true
			}
		default:
			info.Value = "(unsupported type)"
			info.ValueSafe = true
		}

		result = append(result, info)
	}

	c.JSON(http.StatusOK, gin.H{
		"code":   200,
		"data":   result,
		"cursor": nextCursor,
		"total":  len(result),
		"full":   fullValue,
	})
}

func sanitizeRedisHash(key string, values map[string]string) map[string]interface{} {
	out := make(map[string]interface{}, len(values))
	for field, value := range values {
		out[field] = sanitizeRedisValue(key, field, value)
	}
	return out
}

func sanitizeRedisStrings(key, field string, values []string) []interface{} {
	out := make([]interface{}, 0, len(values))
	for _, value := range values {
		out = append(out, sanitizeRedisValue(key, field, value))
	}
	return out
}

func sanitizeRedisValue(key, field string, value interface{}) interface{} {
	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "" {
		return value
	}
	if isSafeRedisValue(key, field, text) {
		return value
	}
	return redactedRedisValue(text)
}

func isSafeRedisValue(key, field, value string) bool {
	if isSensitiveRedisKey(key) || isSensitiveRedisField(field) {
		return false
	}
	if looksLikeSecret(value) {
		return false
	}
	if isNumericString(value) {
		return true
	}
	field = strings.ToLower(strings.TrimSpace(field))
	for _, safe := range []string{
		"model",
		"status",
		"type",
		"category",
		"detail_source",
		"token_detail_source",
		"source",
		"provider",
		"request_id",
		"response_id",
		"last_response_id",
		"session_id",
		"last_session_id",
		"user_id",
		"user_name",
		"device_id",
		"remote_addr",
		"ip_location",
		"user_agent",
	} {
		if field == safe {
			return true
		}
	}
	return len(value) <= 80 && !strings.Contains(value, "\n")
}

func isSensitiveRedisKey(key string) bool {
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" {
		return false
	}
	if strings.HasPrefix(key, "billing:") || strings.HasPrefix(key, "session:") || strings.Contains(key, "rate_limit") {
		return false
	}
	for _, marker := range redisSensitiveMarkers() {
		if strings.Contains(key, marker) {
			return true
		}
	}
	return false
}

func isSensitiveRedisField(field string) bool {
	field = strings.ToLower(strings.TrimSpace(field))
	if field == "" {
		return false
	}
	for _, marker := range redisSensitiveMarkers() {
		if strings.Contains(field, marker) {
			return true
		}
	}
	for _, marker := range []string{"content", "diff", "payload", "raw", "body", "arguments", "history"} {
		if strings.Contains(field, marker) {
			return true
		}
	}
	return false
}

func redisSensitiveMarkers() []string {
	return []string{
		"api_key",
		"apikey",
		"access_token",
		"authorization",
		"bearer",
		"jwt",
		"secret",
		"webhook",
		"password",
		"private_key",
		"credential",
	}
}

func looksLikeSecret(value string) bool {
	lower := strings.ToLower(value)
	for _, marker := range redisSensitiveMarkers() {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return strings.HasPrefix(value, "sk-") ||
		strings.HasPrefix(value, "eyJ") ||
		strings.Contains(value, "Bearer ") ||
		strings.Contains(value, "access_token=")
}

func isNumericString(value string) bool {
	if value == "" {
		return false
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return true
	}
	return false
}

func redactedRedisValue(value string) string {
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("[REDACTED redis_value len=%d sha256:%x]", len(value), sum[:6])
}

func explainRedisKey(key string) (category, description string) {
	k := strings.ToLower(key)
	switch {
	case strings.Contains(k, "billing:response:"):
		return "billing", "单次 OpenAI response 的 token 明细 key，包含 response_id、session_id、input/output token、text/audio token 拆分和明细来源。"
	case strings.Contains(k, "billing:daily_detail:"):
		return "billing", "每日 token 明细汇总 key，按模型和日期累计 response 数、输入/输出 token、text/audio token、cached/reasoning token。"
	case strings.Contains(k, "billing:daily:"):
		return "billing", "每日 token 总量 key，按模型和日期累计 total_tokens，用于快速查看当天总体消耗。"
	case strings.Contains(k, "billing:duration:") || strings.Contains(k, "billing:daily_duration:"):
		return "billing", "音频时长计费 key，记录输入音频、输出音频和总音频时长，可按用户/模块/日期聚合。"
	case strings.Contains(k, "session:"):
		return "session", "Go WebSocket 会话元数据，通常包含 user_id、request_id、model、start_time、status、end_time 等字段，用于排查某个 App/耳机会话生命周期。"
	case strings.Contains(k, "billing:") || strings.Contains(k, "usage:") || strings.Contains(k, "duration"):
		return "billing", "计费/用量统计 key，用于累计 session token、response token 明细、输入/输出音频时长或用户/日期维度消耗。"
	case strings.Contains(k, "rate_limit") || strings.Contains(k, "ratelimit") || strings.Contains(k, "limit:"):
		return "rate_limit", "限流计数 key，一般带短 TTL，用于限制用户/模型/接口在当前时间窗口内的请求频率。"
	case strings.Contains(k, "openai") || strings.Contains(k, "realtime"):
		return "openai", "OpenAI Realtime 相关 key，可能记录上游会话、上下文恢复、音频/文本中间状态或调试数据。"
	case strings.Contains(k, "jwt") || strings.Contains(k, "token"):
		return "auth", "鉴权或 token 相关 key，用于调试用户身份、token 状态或登录态缓存。"
	default:
		return "other", "未识别的业务 key。需要结合 key 前缀、TTL、value 内容判断来源；若持续增长，应确认是否需要 TTL 或清理策略。"
	}
}
