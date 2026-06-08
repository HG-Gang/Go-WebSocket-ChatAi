// Package stats 提供统一资源统计入口。
// 这个包先实现进程内聚合，用来把 Realtime WebSocket 和 HTTP Responses 的 token、费用、
// 延迟、错误等字段收敛到同一套 day/week/month 口径；后续可以把 collector 的存储层替换为
// Redis、数据库或日志聚合系统，而不需要让各个 handler/provider 继续维护多套统计结构。
package stats

import (
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// SourceRealtime 表示数据来自 OpenAI Realtime WebSocket 链路。
	SourceRealtime = "realtime"
	// SourceResponses 表示数据来自 HTTP Responses API 链路。
	SourceResponses = "responses"
	// SourceSystem 表示数据来自系统级监控、告警或容量保护逻辑。
	SourceSystem = "system"
	// SourceWorkspace 表示数据来自 Workspace 文件读写审计链路。
	SourceWorkspace = "workspace"
)

const (
	// ResourceKindCapacityRejected 表示活跃会话达到容量上限后拒绝新连接。
	ResourceKindCapacityRejected = "capacity_rejected"
	// ResourceKindRateLimitRejected 表示请求被本地或 Redis 限流拒绝。
	ResourceKindRateLimitRejected = "rate_limit_rejected"
	// ResourceKindBusinessCacheHit 表示业务级缓存命中次数，不能和 Redis 连接池命中混用。
	ResourceKindBusinessCacheHit = "business_cache_hits"
	// ResourceKindBusinessCacheMiss 表示业务级缓存未命中次数，用来衡量真实业务缓存效果。
	ResourceKindBusinessCacheMiss = "business_cache_misses"
	// ResourceKindError 表示通用错误事件，通常来自 OpenAI、队列或计费链路。
	ResourceKindError = "error"
	// ResourceKindAlertFiring 表示系统过载告警被触发并进入发送流程。
	ResourceKindAlertFiring = "alert_firing"
	// ResourceKindAlertRecovered 表示过载信号恢复，保留给监控恢复事件使用。
	ResourceKindAlertRecovered = "alert_recovered"
	// ResourceKindWorkspaceWritePending 表示 Workspace 写入已创建待确认记录。
	ResourceKindWorkspaceWritePending = "workspace_write_pending"
	// ResourceKindWorkspaceWriteConfirmed 表示 Workspace 待确认写入已落盘。
	ResourceKindWorkspaceWriteConfirmed = "workspace_write_confirmed"
	// ResourceKindWorkspaceWriteRejected 表示 Workspace 待确认写入已拒绝。
	ResourceKindWorkspaceWriteRejected = "workspace_write_rejected"
	// ResourceKindWorkspaceWriteFailed 表示 Workspace 写入执行失败。
	ResourceKindWorkspaceWriteFailed = "workspace_write_failed"
)

const maxUsageRecords = 10000
const maxResourceEventRecords = 10000

// UsageRecord 是一次业务资源消耗事件。
// Source 区分 Realtime、Responses 等入口；Provider 是本地 provider/model_config 名称；
// Model 是上游实际模型；Timestamp 使用毫秒时间戳，缺失时由 RecordUsage 填当前时间。
type UsageRecord struct {
	Source          string
	Provider        string
	Model           string
	UserID          string
	UserName        string
	Status          string
	Timestamp       int64
	InputTokens     int64
	OutputTokens    int64
	CachedTokens    int64
	ReasoningTokens int64
	TotalTokens     int64
	InputAudioMs    int64
	OutputAudioMs   int64
	TotalCost       float64
	LatencyMs       int64
	Error           string
}

// ResourceEvent 是不一定带 token 的运行资源事件。
// 它用于把容量拒绝、限流拒绝、告警、错误和 Workspace 写入结果纳入 day/week/month 统一统计口径。
type ResourceEvent struct {
	Source    string
	Kind      string
	Provider  string
	Model     string
	UserID    string
	UserName  string
	Status    string
	Timestamp int64
	Count     int
	Error     string
}

// ResourceFilter 描述统计查询的可选过滤条件。
// Source 用于区分 realtime/responses/system/workspace；Model 同时匹配 Model 和 Provider，方便用 openai 过滤 provider。
type ResourceFilter struct {
	Source string
	Model  string
	Kind   string
}

// ResourceSummary 是一个统计窗口的汇总结果。
// BySource 和 ByModel 用来证明不同入口已经进入同一统计口径，同时保留页面后续分组展示能力。
type ResourceSummary struct {
	Requests                int            `json:"requests"`
	FailedRequests          int            `json:"failed_requests"`
	InputTokens             int64          `json:"input_tokens"`
	OutputTokens            int64          `json:"output_tokens"`
	CachedTokens            int64          `json:"cached_tokens"`
	ReasoningTokens         int64          `json:"reasoning_tokens"`
	TotalTokens             int64          `json:"total_tokens"`
	InputAudioMs            int64          `json:"input_audio_ms"`
	OutputAudioMs           int64          `json:"output_audio_ms"`
	TotalCost               float64        `json:"total_cost"`
	AvgLatencyMs            int64          `json:"avg_latency_ms"`
	CapacityRejected        int            `json:"capacity_rejected"`
	RateLimitRejected       int            `json:"rate_limit_rejected"`
	BusinessCacheHits       int            `json:"business_cache_hits"`
	BusinessCacheMisses     int            `json:"business_cache_misses"`
	Errors                  int            `json:"errors"`
	AlertsFiring            int            `json:"alerts_firing"`
	AlertsRecovered         int            `json:"alerts_recovered"`
	WorkspaceWritePending   int            `json:"workspace_write_pending"`
	WorkspaceWriteConfirmed int            `json:"workspace_write_confirmed"`
	WorkspaceWriteRejected  int            `json:"workspace_write_rejected"`
	WorkspaceWriteFailed    int            `json:"workspace_write_failed"`
	BySource                map[string]int `json:"by_source"`
	ByModel                 map[string]int `json:"by_model"`
	ByKind                  map[string]int `json:"by_kind"`
}

// ResourceTimelinePoint 是一个时间桶内的资源消耗。
// day 使用小时粒度，week/month 使用日期粒度，便于诊断面板画趋势图。
type ResourceTimelinePoint struct {
	Label                   string  `json:"label"`
	Requests                int     `json:"requests"`
	FailedRequests          int     `json:"failed_requests"`
	TotalTokens             int64   `json:"total_tokens"`
	CachedTokens            int64   `json:"cached_tokens"`
	ReasoningTokens         int64   `json:"reasoning_tokens"`
	TotalCost               float64 `json:"total_cost"`
	AvgLatencyMs            int64   `json:"avg_latency_ms"`
	CapacityRejected        int     `json:"capacity_rejected"`
	RateLimitRejected       int     `json:"rate_limit_rejected"`
	BusinessCacheHits       int     `json:"business_cache_hits"`
	BusinessCacheMisses     int     `json:"business_cache_misses"`
	Errors                  int     `json:"errors"`
	AlertsFiring            int     `json:"alerts_firing"`
	AlertsRecovered         int     `json:"alerts_recovered"`
	WorkspaceWritePending   int     `json:"workspace_write_pending"`
	WorkspaceWriteConfirmed int     `json:"workspace_write_confirmed"`
	WorkspaceWriteRejected  int     `json:"workspace_write_rejected"`
	WorkspaceWriteFailed    int     `json:"workspace_write_failed"`
}

// ResourcePeriodStats 同时返回窗口汇总和时间线。
type ResourcePeriodStats struct {
	Summary  ResourceSummary         `json:"summary"`
	Timeline []ResourceTimelinePoint `json:"timeline"`
}

type collector struct {
	mu      sync.Mutex
	records []UsageRecord
	events  []ResourceEvent
}

var global = &collector{
	records: make([]UsageRecord, 0, maxUsageRecords),
	events:  make([]ResourceEvent, 0, maxResourceEventRecords),
}

// RecordUsage 记录一次资源消耗，并返回归一化后的记录。
// 归一化规则：Source/Status/Model/Provider 为空时使用 unknown，TotalTokens 缺失时按输入+输出兜底。
func RecordUsage(record UsageRecord) UsageRecord {
	now := time.Now()
	if record.Timestamp <= 0 {
		record.Timestamp = now.UnixMilli()
	}
	record.Source = normalizeText(record.Source, "unknown")
	record.Provider = normalizeText(record.Provider, "unknown")
	record.Model = normalizeText(record.Model, record.Provider)
	record.Status = normalizeText(record.Status, "completed")
	if record.TotalTokens <= 0 {
		record.TotalTokens = record.InputTokens + record.OutputTokens
	}

	global.mu.Lock()
	global.records = append(global.records, record)
	if len(global.records) > maxUsageRecords {
		global.records = append([]UsageRecord(nil), global.records[len(global.records)-maxUsageRecords:]...)
	}
	global.mu.Unlock()

	return record
}

// RecordResourceEvent 记录一次非 token 型资源事件，并返回归一化后的事件。
// 这类事件不会增加 Requests；它们通过 kind 独立计数，避免把容量拒绝、告警等运维事件混成模型请求。
func RecordResourceEvent(event ResourceEvent) ResourceEvent {
	now := time.Now()
	if event.Timestamp <= 0 {
		event.Timestamp = now.UnixMilli()
	}
	event.Source = normalizeText(event.Source, SourceSystem)
	event.Kind = normalizeText(event.Kind, "unknown")
	event.Provider = strings.TrimSpace(event.Provider)
	event.Model = strings.TrimSpace(event.Model)
	if event.Model == "" && event.Provider != "" {
		event.Model = event.Provider
	}
	event.Status = normalizeText(event.Status, "recorded")
	if event.Count <= 0 {
		event.Count = 1
	}

	global.mu.Lock()
	global.events = append(global.events, event)
	if len(global.events) > maxResourceEventRecords {
		global.events = append([]ResourceEvent(nil), global.events[len(global.events)-maxResourceEventRecords:]...)
	}
	global.mu.Unlock()

	return event
}

// ResourcePeriods 按 day/week/month 返回资源统计。
// day 是当天零点到 now，week 是最近 7 天滚动窗口，month 是最近 30 天滚动窗口。
func ResourcePeriods(now time.Time) map[string]ResourcePeriodStats {
	return ResourcePeriodsWithFilter(now, ResourceFilter{})
}

// ResourcePeriodsWithFilter 按过滤条件返回 day/week/month 资源统计。
// 空过滤条件等价于 ResourcePeriods；过滤只影响本次查询，不改变进程内 collector 的原始记录。
func ResourcePeriodsWithFilter(now time.Time, filter ResourceFilter) map[string]ResourcePeriodStats {
	if now.IsZero() {
		now = time.Now()
	}
	filter = normalizeResourceFilter(filter)
	global.mu.Lock()
	records := append([]UsageRecord(nil), global.records...)
	events := append([]ResourceEvent(nil), global.events...)
	global.mu.Unlock()

	periods := []struct {
		key          string
		start        time.Time
		bucketLayout string
	}{
		{key: "day", start: startOfDay(now), bucketLayout: "15:00"},
		{key: "week", start: now.AddDate(0, 0, -7), bucketLayout: "2006-01-02"},
		{key: "month", start: now.AddDate(0, 0, -30), bucketLayout: "2006-01-02"},
	}

	out := make(map[string]ResourcePeriodStats, len(periods))
	for _, period := range periods {
		out[period.key] = buildPeriod(records, events, now, period.start, period.bucketLayout, filter)
	}
	return out
}

func buildPeriod(records []UsageRecord, events []ResourceEvent, now, start time.Time, bucketLayout string, filter ResourceFilter) ResourcePeriodStats {
	summaryAcc := newAccumulator()
	buckets := make(map[string]*accumulator)

	for _, record := range records {
		if !resourceFilterMatchesUsage(filter, record) {
			continue
		}
		if record.Timestamp <= 0 {
			continue
		}
		at := time.UnixMilli(record.Timestamp).In(now.Location())
		if at.Before(start) || at.After(now) {
			continue
		}
		summaryAcc.addUsage(record)
		label := at.Format(bucketLayout)
		if buckets[label] == nil {
			buckets[label] = newAccumulator()
		}
		buckets[label].addUsage(record)
	}

	for _, event := range events {
		if !resourceFilterMatchesEvent(filter, event) {
			continue
		}
		if event.Timestamp <= 0 {
			continue
		}
		at := time.UnixMilli(event.Timestamp).In(now.Location())
		if at.Before(start) || at.After(now) {
			continue
		}
		summaryAcc.addEvent(event)
		label := at.Format(bucketLayout)
		if buckets[label] == nil {
			buckets[label] = newAccumulator()
		}
		buckets[label].addEvent(event)
	}

	labels := make([]string, 0, len(buckets))
	for label := range buckets {
		labels = append(labels, label)
	}
	sort.Strings(labels)

	timeline := make([]ResourceTimelinePoint, 0, len(labels))
	for _, label := range labels {
		summary := buckets[label].summary()
		timeline = append(timeline, ResourceTimelinePoint{
			Label:                   label,
			Requests:                summary.Requests,
			FailedRequests:          summary.FailedRequests,
			TotalTokens:             summary.TotalTokens,
			CachedTokens:            summary.CachedTokens,
			ReasoningTokens:         summary.ReasoningTokens,
			TotalCost:               summary.TotalCost,
			AvgLatencyMs:            summary.AvgLatencyMs,
			CapacityRejected:        summary.CapacityRejected,
			RateLimitRejected:       summary.RateLimitRejected,
			BusinessCacheHits:       summary.BusinessCacheHits,
			BusinessCacheMisses:     summary.BusinessCacheMisses,
			Errors:                  summary.Errors,
			AlertsFiring:            summary.AlertsFiring,
			AlertsRecovered:         summary.AlertsRecovered,
			WorkspaceWritePending:   summary.WorkspaceWritePending,
			WorkspaceWriteConfirmed: summary.WorkspaceWriteConfirmed,
			WorkspaceWriteRejected:  summary.WorkspaceWriteRejected,
			WorkspaceWriteFailed:    summary.WorkspaceWriteFailed,
		})
	}

	return ResourcePeriodStats{Summary: summaryAcc.summary(), Timeline: timeline}
}

type accumulator struct {
	totals     ResourceSummary
	latencySum int64
}

func newAccumulator() *accumulator {
	return &accumulator{
		totals: ResourceSummary{
			BySource: make(map[string]int),
			ByModel:  make(map[string]int),
			ByKind:   make(map[string]int),
		},
	}
}

func (a *accumulator) addUsage(record UsageRecord) {
	a.totals.Requests++
	if isFailed(record) {
		a.totals.FailedRequests++
	}
	a.totals.InputTokens += record.InputTokens
	a.totals.OutputTokens += record.OutputTokens
	a.totals.CachedTokens += record.CachedTokens
	a.totals.ReasoningTokens += record.ReasoningTokens
	a.totals.TotalTokens += record.TotalTokens
	a.totals.InputAudioMs += record.InputAudioMs
	a.totals.OutputAudioMs += record.OutputAudioMs
	a.totals.TotalCost += record.TotalCost
	a.latencySum += record.LatencyMs
	a.totals.BySource[normalizeText(record.Source, "unknown")]++
	a.totals.ByModel[normalizeText(record.Model, "unknown")]++
}

func (a *accumulator) addEvent(event ResourceEvent) {
	count := event.Count
	if count <= 0 {
		count = 1
	}
	source := normalizeText(event.Source, SourceSystem)
	kind := normalizeText(event.Kind, "unknown")

	a.totals.BySource[source] += count
	a.totals.ByKind[kind] += count
	if model := normalizeEventModel(event); model != "" {
		a.totals.ByModel[model] += count
	}

	switch kind {
	case ResourceKindCapacityRejected:
		a.totals.CapacityRejected += count
	case ResourceKindRateLimitRejected:
		a.totals.RateLimitRejected += count
	case ResourceKindBusinessCacheHit:
		a.totals.BusinessCacheHits += count
	case ResourceKindBusinessCacheMiss:
		a.totals.BusinessCacheMisses += count
	case ResourceKindError:
		a.totals.Errors += count
	case ResourceKindAlertFiring:
		a.totals.AlertsFiring += count
	case ResourceKindAlertRecovered:
		a.totals.AlertsRecovered += count
	case ResourceKindWorkspaceWritePending:
		a.totals.WorkspaceWritePending += count
	case ResourceKindWorkspaceWriteConfirmed:
		a.totals.WorkspaceWriteConfirmed += count
	case ResourceKindWorkspaceWriteRejected:
		a.totals.WorkspaceWriteRejected += count
	case ResourceKindWorkspaceWriteFailed:
		a.totals.WorkspaceWriteFailed += count
		a.totals.Errors += count
	}

	if kind != ResourceKindError && kind != ResourceKindWorkspaceWriteFailed && resourceEventFailed(event) {
		a.totals.Errors += count
	}
}

func (a *accumulator) summary() ResourceSummary {
	out := a.totals
	if out.BySource == nil {
		out.BySource = make(map[string]int)
	}
	if out.ByModel == nil {
		out.ByModel = make(map[string]int)
	}
	if out.ByKind == nil {
		out.ByKind = make(map[string]int)
	}
	if out.Requests > 0 {
		out.AvgLatencyMs = a.latencySum / int64(out.Requests)
	}
	return out
}

func isFailed(record UsageRecord) bool {
	if strings.TrimSpace(record.Error) != "" {
		return true
	}
	status := strings.ToLower(strings.TrimSpace(record.Status))
	return status == "failed" || status == "error" || status == "incomplete"
}

func resourceEventFailed(event ResourceEvent) bool {
	if strings.TrimSpace(event.Error) != "" {
		return true
	}
	status := strings.ToLower(strings.TrimSpace(event.Status))
	return status == "failed" || status == "error" || status == "incomplete"
}

func normalizeEventModel(event ResourceEvent) string {
	model := strings.TrimSpace(event.Model)
	if model != "" {
		return model
	}
	return strings.TrimSpace(event.Provider)
}

func normalizeResourceFilter(filter ResourceFilter) ResourceFilter {
	return ResourceFilter{
		Source: strings.ToLower(strings.TrimSpace(filter.Source)),
		Model:  strings.ToLower(strings.TrimSpace(filter.Model)),
		Kind:   strings.ToLower(strings.TrimSpace(filter.Kind)),
	}
}

func resourceFilterMatchesUsage(filter ResourceFilter, record UsageRecord) bool {
	if filter.Source != "" && strings.ToLower(strings.TrimSpace(record.Source)) != filter.Source {
		return false
	}
	if filter.Model != "" && !filterMatchesAny(filter.Model, record.Model, record.Provider) {
		return false
	}
	return true
}

func resourceFilterMatchesEvent(filter ResourceFilter, event ResourceEvent) bool {
	if filter.Source != "" && strings.ToLower(strings.TrimSpace(event.Source)) != filter.Source {
		return false
	}
	if filter.Model != "" && !filterMatchesAny(filter.Model, event.Model, event.Provider) {
		return false
	}
	if filter.Kind != "" && strings.ToLower(strings.TrimSpace(event.Kind)) != filter.Kind {
		return false
	}
	return true
}

func filterMatchesAny(filterValue string, values ...string) bool {
	for _, value := range values {
		if strings.ToLower(strings.TrimSpace(value)) == filterValue {
			return true
		}
	}
	return false
}

func normalizeText(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value != "" {
		return value
	}
	fallback = strings.TrimSpace(fallback)
	if fallback != "" {
		return fallback
	}
	return "unknown"
}

func startOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}

// ResetForTest 清空进程内统计，只给单元测试使用。
func ResetForTest() {
	global.mu.Lock()
	global.records = make([]UsageRecord, 0, maxUsageRecords)
	global.events = make([]ResourceEvent, 0, maxResourceEventRecords)
	global.mu.Unlock()
}
