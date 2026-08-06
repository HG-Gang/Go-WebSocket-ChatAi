// internal/service/billing/billing.go
// 计费服务：Token 消耗统计 + 音频时长统计
//
// 文件功能：
//   - 记录每个会话的输入/输出 Token 消耗
//   - 记录不同模块（Chat/Meeting/Translation）的音频时长
//   - 按模型、用户、模块累计，数据写入 Redis 持久化存储
//   - 受全局 billing.enabled 开关控制，关闭或 Redis 不可用时静默跳过
//
// 明确不负责：
//   - 鉴权与配额扣减，只做消耗统计
//   - Redis 写入失败时的补偿重试（失败直接返回错误，由调用方决定处理）
//
// Redis Key 设计：
//
//	billing:duration:{model}:{module}:{userID}        → Hash（音频时长）
//	billing:daily_duration:{model}:{date}             → String（每日音频总时长）
//	billing:{model}:{sessionID}                       → Hash（Token 消耗，兼容旧查询）
//	billing:daily:{model}:{date}                      → String（每日 Token 总消耗）
//	billing:response:{model}:{sessionID}:{responseID} → Hash（单次 response 明细）
//	billing:daily_detail:{model}:{date}               → Hash（每日 token/text/audio 汇总）
package billing

import (
	"context"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9" // 别名导入，避免与 internal/service/redis 冲突
	"go.uber.org/zap"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/logger"
	"TozoAI-Chat-Api/internal/service/redis"
)

// ======================== 模块常量 ========================

const (
	ModuleChat        = "chat"        // 聊天模块
	ModuleMeeting     = "meeting"     // 会议模块
	ModuleTranslation = "translation" // 翻译模块
)

// ======================== 音频时长计费 ========================

// RecordAudioDuration 记录音频时长消耗
// 参数：
//
//	model            - 模型名称（如 openai, azureai）
//	module           - 模块名称（ModuleChat / ModuleMeeting / ModuleTranslation）
//	userID           - 用户 ID
//	sessionID        - 会话 ID
//	inputDurationMs  - App 输入音频时长（毫秒）
//	outputDurationMs - OpenAI 输出音频时长（毫秒）
func RecordAudioDuration(model, module, userID, sessionID string, inputDurationMs, outputDurationMs int64) error {
	// 计费未启用或配置缺失时静默返回，保证统计链路故障不影响对话主流程。
	if conf.Global == nil || !conf.Global.Billing.Enabled {
		return nil
	}

	if model == "" {
		model = "openai"
	}

	ctx := context.Background()
	client := redis.GetClient()
	if client == nil {
		return nil // Redis 未启用，静默跳过
	}

	// 用户+模块维度 Hash Key，字段同时承载时长累计与会话统计。
	userKey := fmt.Sprintf("billing:duration:%s:%s:%s", model, module, userID)

	pipe := client.Pipeline()

	// 一次 Pipeline 原子累加输入/输出/总音频时长，并覆盖最近会话统计字段。
	pipe.HIncrBy(ctx, userKey, "input_audio_ms", inputDurationMs)
	pipe.HIncrBy(ctx, userKey, "output_audio_ms", outputDurationMs)
	pipe.HIncrBy(ctx, userKey, "total_audio_ms", inputDurationMs+outputDurationMs)

	// 统计信息
	pipe.HIncrBy(ctx, userKey, "session_count", 1)
	pipe.HSet(ctx, userKey, "last_session_id", sessionID)
	pipe.HSet(ctx, userKey, "last_updated", time.Now().UnixMilli())

	// 每日总时长按模型+日期独立 Key，设置 32 天 TTL，避免只写不读导致内存无限增长。
	today := time.Now().Format("2006-01-02")
	dailyKey := fmt.Sprintf("billing:daily_duration:%s:%s", model, today)
	pipe.IncrBy(ctx, dailyKey, inputDurationMs+outputDurationMs)
	pipe.Expire(ctx, dailyKey, 32*24*time.Hour)

	// Pipeline 任一命令失败即整体返回错误并记日志；不重试，避免阻塞对话主流程。
	_, err := pipe.Exec(ctx)
	if err != nil {
		logger.GetModelLogger(model).Error("记录音频时长失败",
			zap.String("user_id", userID),
			zap.String("session_id", sessionID),
			zap.Error(err))
		return err
	}

	logger.GetModelLogger(model).Debug("音频时长已记录",
		zap.String("user_id", userID),
		zap.String("module", module),
		zap.Int64("input_ms", inputDurationMs),
		zap.Int64("output_ms", outputDurationMs))

	return nil
}

// GetUserModuleDuration 查询用户在指定模型+模块下的累计音频时长
// Redis 未启用时返回全零且不报错，调用方按"尚无数据"处理即可。
func GetUserModuleDuration(model, module, userID string) (inputMs, outputMs, totalMs int64, err error) {
	c := redis.GetClient()
	if c == nil {
		return 0, 0, 0, nil
	}
	ctx := context.Background()
	userKey := fmt.Sprintf("billing:duration:%s:%s:%s", model, module, userID)

	fields, err := c.HGetAll(ctx, userKey).Result()
	if err != nil {
		return 0, 0, 0, err
	}

	// 字段缺失或非数字时 ParseInt 报错被忽略，按 0 累计处理。
	inputMs, _ = strconv.ParseInt(fields["input_audio_ms"], 10, 64)
	outputMs, _ = strconv.ParseInt(fields["output_audio_ms"], 10, 64)
	totalMs, _ = strconv.ParseInt(fields["total_audio_ms"], 10, 64)

	return inputMs, outputMs, totalMs, nil
}

// GetDailyDuration 查询指定模型某天的总音频时长（毫秒）
func GetDailyDuration(model, date string) (int64, error) {
	c := redis.GetClient()
	if c == nil {
		return 0, nil
	}
	ctx := context.Background()
	key := fmt.Sprintf("billing:daily_duration:%s:%s", model, date)
	val, err := c.Get(ctx, key).Result()
	if err != nil {
		// goredis.Nil 表示 Key 不存在，返回 0 而非错误
		if err == goredis.Nil {
			return 0, nil
		}
		return 0, err
	}
	return strconv.ParseInt(val, 10, 64)
}

// ======================== Token 消耗计费 ========================

// TokenUsageDetail 是一次 OpenAI response.done 返回的 token 明细。
// 如果上游没有返回 text/audio 拆分，调用方会把 DetailSource 标记为 usage_total_only，前端据此提示“只有总量”。
type TokenUsageDetail struct {
	ResponseID        string
	InputTokens       int
	OutputTokens      int
	TotalTokens       int
	InputTextTokens   int
	InputAudioTokens  int
	OutputTextTokens  int
	OutputAudioTokens int
	CachedTokens      int
	ReasoningTokens   int
	DetailSource      string
}

// RecordTokenUsage 记录 Token 消耗
func RecordTokenUsage(model, sessionID, userID string, input, output int) error {
	return RecordTokenUsageDetail(model, sessionID, userID, TokenUsageDetail{
		InputTokens:  input,
		OutputTokens: output,
		TotalTokens:  input + output,
		DetailSource: "usage_total_only",
	})
}

// RecordTokenUsageDetail 记录一次 response 维度的 Token 消耗明细。
// 写入三类 Redis key：
//   - billing:{model}:{sessionID}：会话累计，兼容旧逻辑。
//   - billing:response:{model}:{sessionID}:{responseID}：单次 response 明细。
//   - billing:daily_detail:{model}:{date}：每日 token/text/audio 汇总。
func RecordTokenUsageDetail(model, sessionID, userID string, detail TokenUsageDetail) error {
	if conf.Global == nil || !conf.Global.Billing.Enabled {
		return nil
	}

	if model == "" {
		model = "openai"
	}

	ctx := context.Background()
	client := redis.GetClient()
	if client == nil {
		return nil // Redis 未启用，静默跳过
	}
	// 上游未拆分 text/audio 时按输入+输出兜底总量，DetailSource 缺省按 usage_detail 处理。
	if detail.TotalTokens <= 0 {
		detail.TotalTokens = detail.InputTokens + detail.OutputTokens
	}
	if detail.DetailSource == "" {
		detail.DetailSource = "usage_detail"
	}

	// 会话维度累计 Key，与 GetSessionUsage 查询口径一致。
	keyPrefix := fmt.Sprintf("billing:%s:%s", model, sessionID)

	pipe := client.Pipeline()
	// 会话 Hash 累计各 token 分项与请求计数，供会话结束后统计与看板展示。
	pipe.HIncrBy(ctx, keyPrefix, "input_tokens", int64(detail.InputTokens))
	pipe.HIncrBy(ctx, keyPrefix, "output_tokens", int64(detail.OutputTokens))
	pipe.HIncrBy(ctx, keyPrefix, "total_tokens", int64(detail.TotalTokens))
	pipe.HIncrBy(ctx, keyPrefix, "input_text_tokens", int64(detail.InputTextTokens))
	pipe.HIncrBy(ctx, keyPrefix, "input_audio_tokens", int64(detail.InputAudioTokens))
	pipe.HIncrBy(ctx, keyPrefix, "output_text_tokens", int64(detail.OutputTextTokens))
	pipe.HIncrBy(ctx, keyPrefix, "output_audio_tokens", int64(detail.OutputAudioTokens))
	pipe.HIncrBy(ctx, keyPrefix, "cached_tokens", int64(detail.CachedTokens))
	pipe.HIncrBy(ctx, keyPrefix, "reasoning_tokens", int64(detail.ReasoningTokens))
	pipe.HIncrBy(ctx, keyPrefix, "response_count", 1)
	pipe.HSet(ctx, keyPrefix, "user_id", userID)
	pipe.HSet(ctx, keyPrefix, "last_response_id", detail.ResponseID)
	pipe.HSet(ctx, keyPrefix, "token_detail_source", detail.DetailSource)
	pipe.HSet(ctx, keyPrefix, "last_used", time.Now().Unix())

	// 每日总账与每日明细双 Key 同时累计，分别服务总消耗与看板图表，均设 32 天 TTL。
	today := time.Now().Format("2006-01-02")
	dailyKey := fmt.Sprintf("billing:daily:%s:%s", model, today)
	pipe.IncrBy(ctx, dailyKey, int64(detail.TotalTokens))
	pipe.Expire(ctx, dailyKey, 32*24*time.Hour)
	dailyDetailKey := fmt.Sprintf("billing:daily_detail:%s:%s", model, today)
	pipe.HIncrBy(ctx, dailyDetailKey, "input_tokens", int64(detail.InputTokens))
	pipe.HIncrBy(ctx, dailyDetailKey, "output_tokens", int64(detail.OutputTokens))
	pipe.HIncrBy(ctx, dailyDetailKey, "total_tokens", int64(detail.TotalTokens))
	pipe.HIncrBy(ctx, dailyDetailKey, "input_text_tokens", int64(detail.InputTextTokens))
	pipe.HIncrBy(ctx, dailyDetailKey, "input_audio_tokens", int64(detail.InputAudioTokens))
	pipe.HIncrBy(ctx, dailyDetailKey, "output_text_tokens", int64(detail.OutputTextTokens))
	pipe.HIncrBy(ctx, dailyDetailKey, "output_audio_tokens", int64(detail.OutputAudioTokens))
	pipe.HIncrBy(ctx, dailyDetailKey, "cached_tokens", int64(detail.CachedTokens))
	pipe.HIncrBy(ctx, dailyDetailKey, "reasoning_tokens", int64(detail.ReasoningTokens))
	pipe.HIncrBy(ctx, dailyDetailKey, "response_count", 1)
	pipe.HSet(ctx, dailyDetailKey, "last_session_id", sessionID)
	pipe.HSet(ctx, dailyDetailKey, "last_response_id", detail.ResponseID)
	pipe.HSet(ctx, dailyDetailKey, "last_updated", time.Now().Unix())
	pipe.Expire(ctx, dailyDetailKey, 32*24*time.Hour)

	if detail.ResponseID != "" {
		// 单次 response 明细 Key 只写不读，TTL 到期自动清理，防止无界增长。
		responseKey := fmt.Sprintf("billing:response:%s:%s:%s", model, sessionID, detail.ResponseID)
		pipe.HSet(ctx, responseKey, map[string]interface{}{
			"model":               model,
			"session_id":          sessionID,
			"user_id":             userID,
			"response_id":         detail.ResponseID,
			"input_tokens":        detail.InputTokens,
			"output_tokens":       detail.OutputTokens,
			"total_tokens":        detail.TotalTokens,
			"input_text_tokens":   detail.InputTextTokens,
			"input_audio_tokens":  detail.InputAudioTokens,
			"output_text_tokens":  detail.OutputTextTokens,
			"output_audio_tokens": detail.OutputAudioTokens,
			"cached_tokens":       detail.CachedTokens,
			"reasoning_tokens":    detail.ReasoningTokens,
			"detail_source":       detail.DetailSource,
			"created_at":          time.Now().Unix(),
		})
		pipe.Expire(ctx, responseKey, 32*24*time.Hour)
	}

	// Pipeline 任一命令失败即整体返回错误并记日志；不重试，数据缺口由看板感知。
	_, err := pipe.Exec(ctx)
	if err != nil {
		logger.GetModelLogger(model).Error("记录 Token 消耗失败",
			zap.String("session_id", sessionID),
			zap.String("response_id", detail.ResponseID),
			zap.Error(err))
		return err
	}

	logger.GetModelLogger(model).Debug("Token 消耗已记录",
		zap.String("session_id", sessionID),
		zap.String("response_id", detail.ResponseID),
		zap.Int("input", detail.InputTokens),
		zap.Int("output", detail.OutputTokens))

	return nil
}

// GetSessionUsage 查询指定会话的 Token 累计消耗
// Redis 未启用时返回全零且不报错，调用方按"尚无数据"处理即可。
func GetSessionUsage(model, sessionID string) (input, output, total int64, err error) {
	c := redis.GetClient()
	if c == nil {
		return 0, 0, 0, nil
	}
	ctx := context.Background()
	key := fmt.Sprintf("billing:%s:%s", model, sessionID)

	fields, err := c.HGetAll(ctx, key).Result()
	if err != nil {
		return 0, 0, 0, err
	}

	// 字段缺失或非数字时 ParseInt 报错被忽略，按 0 累计处理。
	input, _ = strconv.ParseInt(fields["input_tokens"], 10, 64)
	output, _ = strconv.ParseInt(fields["output_tokens"], 10, 64)
	total, _ = strconv.ParseInt(fields["total_tokens"], 10, 64)

	return input, output, total, nil
}

// GetDailyUsage 查询指定模型某天的总 Token 消耗
func GetDailyUsage(model, date string) (int64, error) {
	c := redis.GetClient()
	if c == nil {
		return 0, nil
	}
	ctx := context.Background()
	key := fmt.Sprintf("billing:daily:%s:%s", model, date)
	val, err := c.Get(ctx, key).Result()
	if err != nil {
		if err == goredis.Nil {
			return 0, nil
		}
		return 0, err
	}
	return strconv.ParseInt(val, 10, 64)
}
