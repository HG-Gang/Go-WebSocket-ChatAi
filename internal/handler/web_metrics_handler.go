package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/logger"
	"TozoAI-Chat-Api/internal/provider/openairesponses"
	"TozoAI-Chat-Api/internal/service/stats"
)

const maxWebRequestRecords = 500

// webRequestStats 是 Web 看板的进程内最近请求窗口。
// 它只用于调试页展示 Responses 请求、token、费用和延迟，不是长期审计账本；
// 服务重启会清空，生产费用结算仍应以 billing/Redis 或上游账单为准。
var webRequestStats = struct {
	sync.Mutex
	NextID  int64
	Records []WebRequestRecord
}{Records: make([]WebRequestRecord, 0, maxWebRequestRecords)}

var webMetricsAudit = struct {
	sync.RWMutex
	sink func(webMetricsLogEvent)
}{sink: writeWebMetricsDailyLog}

// WebRequestRecord 是 Web 聊天/看板展示用的轻量请求明细。
// APIKey 只保存脱敏值；Error 只保存可展示错误文本，不能写入完整上游密钥或原始敏感请求体。
type WebRequestRecord struct {
	ID              int64   `json:"id"`
	RequestID       string  `json:"request_id,omitempty"`
	Time            string  `json:"time"`
	Timestamp       int64   `json:"timestamp"`
	ModelConfig     string  `json:"model_config"`
	Model           string  `json:"model"`
	Provider        string  `json:"provider,omitempty"`
	InputTokens     int64   `json:"input_tokens"`
	OutputTokens    int64   `json:"output_tokens"`
	CachedTokens    int64   `json:"cached_input_tokens"`
	ReasoningTokens int64   `json:"reasoning_tokens"`
	TotalTokens     int64   `json:"total_tokens"`
	TotalCost       float64 `json:"total_cost"`
	Status          string  `json:"status"`
	APIKey          string  `json:"api_key"`
	ReasoningEffort string  `json:"reasoning_effort"`
	Endpoint        string  `json:"endpoint"`
	Type            string  `json:"type"`
	BillingMode     string  `json:"billing_mode"`
	Fee             float64 `json:"fee"`
	FirstTokenMs    int64   `json:"first_token_ms"`
	LatencyMs       int64   `json:"latency_ms"`
	UserAgent       string  `json:"user_agent"`
	Error           string  `json:"error,omitempty"`
}

type webMetricsLogEvent struct {
	Event     string
	Record    WebRequestRecord
	Resources map[string]stats.ResourcePeriodStats
}

// webResponseUsage 是 Responses API usage 字段的归一化结果。
// Cached/Reasoning 兼容不同上游字段名，Total 缺失时按 input+output 兜底。
type webResponseUsage struct {
	Input     int64
	Output    int64
	Cached    int64
	Reasoning int64
	Total     int64
}

// webResourceSummary 是天/周/月资源统计的汇总口径。
// FailedRequests 按 status=failed 或 Error 非空统计，AvgLatencyMs 只对窗口内请求取算术平均。
type webResourceSummary struct {
	Requests        int     `json:"requests"`
	FailedRequests  int     `json:"failed_requests"`
	InputTokens     int64   `json:"input_tokens"`
	OutputTokens    int64   `json:"output_tokens"`
	CachedTokens    int64   `json:"cached_tokens"`
	ReasoningTokens int64   `json:"reasoning_tokens"`
	TotalTokens     int64   `json:"total_tokens"`
	TotalCost       float64 `json:"total_cost"`
	AvgLatencyMs    int64   `json:"avg_latency_ms"`
}

// webResourceTimelinePoint 是诊断页资源条形图的一格。
// Label 由窗口粒度决定：当天按小时，近 7 天和近 30 天按日期。
type webResourceTimelinePoint struct {
	Label           string  `json:"label"`
	Requests        int     `json:"requests"`
	FailedRequests  int     `json:"failed_requests"`
	TotalTokens     int64   `json:"total_tokens"`
	CachedTokens    int64   `json:"cached_tokens"`
	ReasoningTokens int64   `json:"reasoning_tokens"`
	TotalCost       float64 `json:"total_cost"`
	AvgLatencyMs    int64   `json:"avg_latency_ms"`
}

// webResourcePeriodStats 同时返回窗口汇总和时间线。
// 前端用 summary 展示总数，用 timeline 画最近桶的资源趋势。
type webResourcePeriodStats struct {
	Summary  webResourceSummary         `json:"summary"`
	Timeline []webResourceTimelinePoint `json:"timeline"`
}

// addWebRequestRecord 将一条 Web 请求记录写入最近窗口。
// 新记录放在数组头部，超过 maxWebRequestRecords 后裁剪尾部，避免调试看板无限占内存。
func addWebRequestRecord(record WebRequestRecord) WebRequestRecord {
	now := time.Now()
	if record.Timestamp == 0 {
		record.Timestamp = now.UnixMilli()
	}
	if record.Time == "" {
		record.Time = now.Format("2006-01-02 15:04:05")
	}
	webRequestStats.Lock()
	webRequestStats.NextID++
	record.ID = webRequestStats.NextID
	webRequestStats.Records = append([]WebRequestRecord{record}, webRequestStats.Records...)
	if len(webRequestStats.Records) > maxWebRequestRecords {
		webRequestStats.Records = webRequestStats.Records[:maxWebRequestRecords]
	}
	webRequestStats.Unlock()

	emitWebMetricsLog(webMetricsLogEvent{Event: "web_request_metric", Record: record})
	return record
}

// addResponsesMetric 从一次 Responses 调用中提取看板指标。
// cfg 只用于 endpoint、模型类型、计费配置和脱敏 key；payload/result 可能来自失败请求，因此字段都要允许缺失。
func addResponsesMetric(c *gin.Context, modelConfig string, cfg *conf.ModelConfig, payload map[string]any, result *openairesponses.Result, status string, latency time.Duration, errorText string) WebRequestRecord {
	usage := extractResponsesUsage(result)
	model := payloadString(payload, "model")
	if model == "" && result != nil {
		model = result.Model
	}
	if model == "" && cfg != nil {
		model = cfg.DefaultModel
	}

	endpoint := ""
	apiKey := "未配置"
	modelType := inferModelType(modelConfig, "")
	billingMode := "token"
	if cfg != nil {
		endpoint = logger.SafeURLForDisplay(cfg.Endpoint)
		apiKey = maskAPIKey(cfg.APIKey)
		modelType = metricStringFromExtra(cfg.Extra, "type", inferModelType(modelConfig, cfg.Endpoint))
		billingMode = metricStringFromExtra(cfg.Extra, "billing_mode", "token")
	}

	reqID := ""
	if result != nil {
		reqID = result.ID
	}
	totalCost := estimateResponseCost(cfg, usage)
	record := addWebRequestRecord(WebRequestRecord{
		RequestID:       reqID,
		ModelConfig:     modelConfig,
		Model:           model,
		Provider:        modelConfig,
		InputTokens:     usage.Input,
		OutputTokens:    usage.Output,
		CachedTokens:    usage.Cached,
		ReasoningTokens: usage.Reasoning,
		TotalTokens:     usage.Total,
		TotalCost:       totalCost,
		Status:          normalizeWebStatus(status, errorText),
		APIKey:          apiKey,
		ReasoningEffort: reasoningEffortFromPayload(payload),
		Endpoint:        endpoint,
		Type:            modelType,
		BillingMode:     billingMode,
		Fee:             totalCost,
		FirstTokenMs:    latency.Milliseconds(),
		LatencyMs:       latency.Milliseconds(),
		UserAgent:       c.GetHeader("User-Agent"),
		Error:           errorText,
	})
	stats.RecordUsage(stats.UsageRecord{
		Source:          stats.SourceResponses,
		Provider:        modelConfig,
		Model:           record.Model,
		UserID:          c.GetString("user_id"),
		UserName:        c.GetString("user_name"),
		Status:          record.Status,
		Timestamp:       record.Timestamp,
		InputTokens:     record.InputTokens,
		OutputTokens:    record.OutputTokens,
		CachedTokens:    record.CachedTokens,
		ReasoningTokens: record.ReasoningTokens,
		TotalTokens:     record.TotalTokens,
		TotalCost:       record.TotalCost,
		LatencyMs:       record.LatencyMs,
		Error:           record.Error,
	})
	// 双写 DB（若已启用），保证看板跨重启可查
	persistRequestLog(c, record, modelConfig, record.RequestID)
	return record
}

// WebMetricsHandler 返回 Web 看板请求明细、汇总数据和图表数据。
// 这个接口面向调试页，返回最近窗口的明细和 day/week/month 聚合；它不读取 Redis，也不代表全量历史。
func WebMetricsHandler(c *gin.Context) {
	webRequestStats.Lock()
	records := append([]WebRequestRecord(nil), webRequestStats.Records...)
	webRequestStats.Unlock()

	modelCounts := map[string]int{}
	statusCounts := map[string]int{}
	typeCounts := map[string]int{}
	minuteAgg := map[string]struct {
		Count  int
		Tokens int64
		Cost   float64
	}{}

	var totalTokens int64
	var totalCost float64
	var totalLatency int64
	for _, r := range records {
		model := r.Model
		if model == "" {
			model = r.ModelConfig
		}
		modelCounts[model]++
		statusCounts[r.Status]++
		typeCounts[r.Type]++
		totalTokens += r.TotalTokens
		totalCost += r.TotalCost
		totalLatency += r.LatencyMs
		minute := ""
		if t := time.UnixMilli(r.Timestamp); !t.IsZero() {
			minute = t.Format("15:04")
		}
		m := minuteAgg[minute]
		m.Count++
		m.Tokens += r.TotalTokens
		m.Cost += r.TotalCost
		minuteAgg[minute] = m
	}

	avgLatency := int64(0)
	if len(records) > 0 {
		avgLatency = totalLatency / int64(len(records))
	}

	minutes := make([]string, 0, len(minuteAgg))
	for minute := range minuteAgg {
		minutes = append(minutes, minute)
	}
	sort.Strings(minutes)
	timeline := make([]gin.H, 0, len(minutes))
	for _, minute := range minutes {
		m := minuteAgg[minute]
		timeline = append(timeline, gin.H{"time": minute, "count": m.Count, "tokens": m.Tokens, "cost": m.Cost})
	}
	resources := stats.ResourcePeriods(time.Now())
	emitWebMetricsLog(webMetricsLogEvent{Event: "stats_rollup", Resources: resources})

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"data": gin.H{
			"summary": gin.H{
				"requests":       len(records),
				"total_tokens":   totalTokens,
				"total_cost":     totalCost,
				"avg_latency_ms": avgLatency,
			},
			"records": records,
			"charts": gin.H{
				"models":    mapToChart(modelCounts),
				"statuses":  mapToChart(statusCounts),
				"types":     mapToChart(typeCounts),
				"timeline":  timeline,
				"resources": resources,
			},
		},
	})
}

// buildResourcePeriodStats 生成诊断页需要的资源窗口。
// day 使用当天 0 点到当前时间，week/month 使用近 7 天和近 30 天滚动窗口。
func buildResourcePeriodStats(records []WebRequestRecord, now time.Time) map[string]webResourcePeriodStats {
	periods := []struct {
		key          string
		start        time.Time
		bucketLayout string
	}{
		{key: "day", start: startOfDay(now), bucketLayout: "15:00"},
		{key: "week", start: now.AddDate(0, 0, -7), bucketLayout: "2006-01-02"},
		{key: "month", start: now.AddDate(0, 0, -30), bucketLayout: "2006-01-02"},
	}

	out := make(map[string]webResourcePeriodStats, len(periods))
	for _, period := range periods {
		out[period.key] = buildResourcePeriod(records, now, period.start, period.bucketLayout)
	}
	return out
}

// buildResourcePeriod 对一个时间窗口做聚合。
// 只有 Timestamp 在窗口内的记录会被纳入，避免旧请求污染当天/本周/本月统计。
func buildResourcePeriod(records []WebRequestRecord, now, start time.Time, bucketLayout string) webResourcePeriodStats {
	summaryAcc := webResourceAccumulator{}
	buckets := make(map[string]*webResourceAccumulator)

	for _, record := range records {
		if record.Timestamp <= 0 {
			continue
		}
		at := time.UnixMilli(record.Timestamp).In(now.Location())
		if at.Before(start) || at.After(now) {
			continue
		}
		summaryAcc.add(record)
		label := at.Format(bucketLayout)
		if buckets[label] == nil {
			buckets[label] = &webResourceAccumulator{}
		}
		buckets[label].add(record)
	}

	labels := make([]string, 0, len(buckets))
	for label := range buckets {
		labels = append(labels, label)
	}
	sort.Strings(labels)

	timeline := make([]webResourceTimelinePoint, 0, len(labels))
	for _, label := range labels {
		summary := buckets[label].summary()
		timeline = append(timeline, webResourceTimelinePoint{
			Label:           label,
			Requests:        summary.Requests,
			FailedRequests:  summary.FailedRequests,
			TotalTokens:     summary.TotalTokens,
			CachedTokens:    summary.CachedTokens,
			ReasoningTokens: summary.ReasoningTokens,
			TotalCost:       summary.TotalCost,
			AvgLatencyMs:    summary.AvgLatencyMs,
		})
	}

	return webResourcePeriodStats{
		Summary:  summaryAcc.summary(),
		Timeline: timeline,
	}
}

func emitWebMetricsLog(event webMetricsLogEvent) {
	if strings.TrimSpace(event.Event) == "" {
		return
	}
	webMetricsAudit.RLock()
	sink := webMetricsAudit.sink
	webMetricsAudit.RUnlock()
	if sink != nil {
		sink(event)
	}
}

func writeWebMetricsDailyLog(event webMetricsLogEvent) {
	if conf.Global == nil || strings.TrimSpace(conf.Global.Logs.RootDir) == "" {
		return
	}
	fields := []zap.Field{zap.String("event", event.Event)}
	switch event.Event {
	case "web_request_metric":
		r := event.Record
		fields = append(fields,
			zap.Int64("id", r.ID),
			zap.String("model_config", r.ModelConfig),
			zap.String("model", r.Model),
			zap.String("status", r.Status),
			zap.String("type", r.Type),
			zap.String("billing_mode", r.BillingMode),
			zap.Int64("input_tokens", r.InputTokens),
			zap.Int64("output_tokens", r.OutputTokens),
			zap.Int64("cached_tokens", r.CachedTokens),
			zap.Int64("reasoning_tokens", r.ReasoningTokens),
			zap.Int64("total_tokens", r.TotalTokens),
			zap.Float64("total_cost", r.TotalCost),
			zap.Int64("first_token_ms", r.FirstTokenMs),
			zap.Int64("latency_ms", r.LatencyMs),
			zap.String("api_key", logger.RedactField("api_key", r.APIKey)),
			zap.String("endpoint", r.Endpoint),
			zap.String("reasoning_effort", r.ReasoningEffort),
			zap.String("user_agent", r.UserAgent),
			zap.String("error", r.Error),
		)
	case "stats_rollup":
		fields = append(fields, zap.Any("resources", event.Resources))
	}
	logger.GetModelLogger("global").Info("web metrics audit", fields...)
}

func setWebMetricsLogSinkForTest(sink func(webMetricsLogEvent)) func() {
	webMetricsAudit.Lock()
	old := webMetricsAudit.sink
	webMetricsAudit.sink = sink
	webMetricsAudit.Unlock()
	return func() {
		webMetricsAudit.Lock()
		webMetricsAudit.sink = old
		webMetricsAudit.Unlock()
	}
}

// webResourceAccumulator 累加单个窗口或单个时间桶的资源消耗。
// latencySum 单独保存，用于最后按请求数计算平均延迟。
type webResourceAccumulator struct {
	totals     webResourceSummary
	latencySum int64
}

// add 将单条请求合入资源汇总。
// status=failed 和 Error 非空都算失败，避免上游错误但 status 未规范化时漏记。
func (a *webResourceAccumulator) add(record WebRequestRecord) {
	a.totals.Requests++
	if strings.EqualFold(record.Status, "failed") || record.Error != "" {
		a.totals.FailedRequests++
	}
	a.totals.InputTokens += record.InputTokens
	a.totals.OutputTokens += record.OutputTokens
	a.totals.CachedTokens += record.CachedTokens
	a.totals.ReasoningTokens += record.ReasoningTokens
	a.totals.TotalTokens += record.TotalTokens
	a.totals.TotalCost += record.TotalCost
	a.latencySum += record.LatencyMs
}

// summary 输出最终汇总并计算平均延迟。
// 没有请求时 AvgLatencyMs 保持 0，前端显示为 0ms 或空态。
func (a *webResourceAccumulator) summary() webResourceSummary {
	out := a.totals
	if out.Requests > 0 {
		out.AvgLatencyMs = a.latencySum / int64(out.Requests)
	}
	return out
}

// startOfDay 返回本地时区的当天零点，用作 day 资源窗口起点。
func startOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

// mapToChart 把计数字典转换为前端图表数组。
// 空 key 统一归为 unknown，输出按名称排序保证快照和测试稳定。
func mapToChart(values map[string]int) []gin.H {
	normalized := make(map[string]int, len(values))
	for key, value := range values {
		if key == "" {
			key = "unknown"
		}
		normalized[key] += value
	}
	keys := make([]string, 0, len(normalized))
	for key := range normalized {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]gin.H, 0, len(keys))
	for _, key := range keys {
		out = append(out, gin.H{"name": key, "value": normalized[key]})
	}
	return out
}

// extractResponsesUsage 从上游 Responses 原始 JSON 中提取 usage。
// OpenAI 和中转服务可能使用 input_tokens_details/input_token_details 等不同字段名，因此这里做兼容解析。
func extractResponsesUsage(result *openairesponses.Result) webResponseUsage {
	usage := webResponseUsage{}
	if result == nil || len(result.Raw) == 0 {
		return usage
	}
	var obj map[string]any
	if err := json.Unmarshal(result.Raw, &obj); err != nil {
		return usage
	}
	usageMap, _ := obj["usage"].(map[string]any)
	if usageMap == nil {
		return usage
	}
	usage.Input = int64FromAny(usageMap["input_tokens"])
	usage.Output = int64FromAny(usageMap["output_tokens"])
	usage.Total = int64FromAny(usageMap["total_tokens"])
	if usage.Total == 0 {
		usage.Total = usage.Input + usage.Output
	}

	for _, key := range []string{"input_tokens_details", "input_token_details"} {
		if details, ok := usageMap[key].(map[string]any); ok {
			usage.Cached += int64FromAny(details["cached_tokens"])
			usage.Reasoning += int64FromAny(details["reasoning_tokens"])
		}
	}
	for _, key := range []string{"output_tokens_details", "output_token_details"} {
		if details, ok := usageMap[key].(map[string]any); ok {
			usage.Cached += int64FromAny(details["cached_tokens"])
			usage.Reasoning += int64FromAny(details["reasoning_tokens"])
		}
	}
	return usage
}

// estimateResponseCost 按模型配置中的每百万 token 单价估算费用。
// 这是看板估算值，不等同于最终账单；cached 和 reasoning token 如果配置了专门价格会单独计费。
func estimateResponseCost(cfg *conf.ModelConfig, usage webResponseUsage) float64 {
	if cfg == nil || cfg.Extra == nil {
		return 0
	}
	inputPrice := extraFloat(cfg.Extra, "input_price_per_1m", "input_cost_per_1m")
	outputPrice := extraFloat(cfg.Extra, "output_price_per_1m", "output_cost_per_1m")
	cachedPrice := extraFloat(cfg.Extra, "cached_input_price_per_1m", "cached_price_per_1m")
	reasoningPrice := extraFloat(cfg.Extra, "reasoning_price_per_1m", "reasoning_cost_per_1m")

	billableInput := usage.Input - usage.Cached
	if billableInput < 0 {
		billableInput = 0
	}
	cost := float64(billableInput)/1_000_000*inputPrice + float64(usage.Cached)/1_000_000*cachedPrice + float64(usage.Output)/1_000_000*outputPrice
	if reasoningPrice > 0 {
		cost += float64(usage.Reasoning) / 1_000_000 * reasoningPrice
	}
	return cost
}

// metricStringFromExtra 从模型 extra 配置里读取字符串指标。
// extra 常来自 YAML，缺失或空值时使用 fallback，避免看板字段出现 nil。
func metricStringFromExtra(extra map[string]interface{}, key, fallback string) string {
	if extra == nil {
		return fallback
	}
	value, ok := extra[key]
	if !ok || value == nil {
		return fallback
	}
	text := strings.TrimSpace(anyToString(value))
	if text == "" {
		return fallback
	}
	return text
}

// extraFloat 读取模型 extra 中的价格字段。
// 支持数字和字符串两种 YAML 解析结果，读取失败时返回 0 表示不估算该项费用。
func extraFloat(extra map[string]interface{}, keys ...string) float64 {
	for _, key := range keys {
		value, ok := extra[key]
		if !ok || value == nil {
			continue
		}
		switch v := value.(type) {
		case float64:
			return v
		case float32:
			return float64(v)
		case int:
			return float64(v)
		case int64:
			return float64(v)
		case string:
			if n, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
				return n
			}
		}
	}
	return 0
}

// payloadString 从请求 payload 中读取字符串字段。
// payload 可能来自用户输入，空白值统一返回空字符串。
func payloadString(payload map[string]any, key string) string {
	if payload == nil {
		return ""
	}
	return strings.TrimSpace(anyToString(payload[key]))
}

// reasoningEffortFromPayload 提取 reasoning effort。
// 支持顶层 reasoning_effort 和嵌套 reasoning.effort，缺失时显示 default。
func reasoningEffortFromPayload(payload map[string]any) string {
	if effort := payloadString(payload, "reasoning_effort"); effort != "" {
		return effort
	}
	if reasoning, ok := payload["reasoning"].(map[string]any); ok {
		return strings.TrimSpace(anyToString(reasoning["effort"]))
	}
	return "default"
}

// normalizeWebStatus 归一化看板状态。
// 只要 errorText 非空就视为 failed，避免上游失败被默认 completed 掩盖。
func normalizeWebStatus(status string, errorText string) string {
	status = strings.TrimSpace(status)
	if errorText != "" {
		return "failed"
	}
	if status == "" {
		return "completed"
	}
	return status
}

// int64FromAny 把上游 JSON 数字转换为 int64。
// 支持 json.Number、float64 和字符串，无法解析时返回 0。
func int64FromAny(value any) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	case json.Number:
		n, _ := v.Int64()
		return n
	case string:
		n, _ := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		return n
	default:
		return 0
	}
}

// anyToString 把配置或 payload 值转换成展示字符串。
// nil 返回空字符串，其他复杂值只用于调试展示，不参与安全判断。
func anyToString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}
