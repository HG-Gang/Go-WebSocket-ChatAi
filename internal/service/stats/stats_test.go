// internal/service/stats/stats_test.go
// 统一资源统计单元测试：覆盖跨时间窗口聚合、运维事件计数与业务缓存命中统计。
package stats

import (
	"testing"
	"time"
)

func TestRecordUsageAggregatesRealtimeAndResponsesAcrossPeriods(t *testing.T) {
	ResetForTest()
	now := time.Date(2026, 6, 7, 15, 30, 0, 0, time.UTC)

	RecordUsage(UsageRecord{
		Source:          SourceRealtime,
		Provider:        "openai",
		Model:           "gpt-realtime",
		UserID:          "user-1",
		Status:          "completed",
		Timestamp:       now.Add(-1 * time.Hour).UnixMilli(),
		InputTokens:     100,
		OutputTokens:    50,
		CachedTokens:    20,
		ReasoningTokens: 5,
		TotalTokens:     150,
		LatencyMs:       90,
	})
	RecordUsage(UsageRecord{
		Source:          SourceResponses,
		Provider:        "openairesponses",
		Model:           "gpt-4.1",
		UserID:          "user-1",
		Status:          "failed",
		Timestamp:       now.Add(-25 * time.Hour).UnixMilli(),
		InputTokens:     80,
		OutputTokens:    20,
		CachedTokens:    10,
		ReasoningTokens: 3,
		TotalTokens:     100,
		TotalCost:       0.12,
		LatencyMs:       210,
	})
	RecordUsage(UsageRecord{
		Source:      SourceResponses,
		Provider:    "openairesponses",
		Model:       "old",
		Status:      "completed",
		Timestamp:   now.AddDate(0, 0, -40).UnixMilli(),
		TotalTokens: 999,
	})

	periods := ResourcePeriods(now)
	day := periods["day"].Summary
	if day.Requests != 1 || day.FailedRequests != 0 || day.TotalTokens != 150 || day.CachedTokens != 20 || day.ReasoningTokens != 5 {
		t.Fatalf("day summary = %+v, want only realtime usage from today", day)
	}
	if day.BySource[SourceRealtime] != 1 || day.BySource[SourceResponses] != 0 {
		t.Fatalf("day BySource = %+v, want realtime only", day.BySource)
	}

	week := periods["week"].Summary
	if week.Requests != 2 || week.FailedRequests != 1 || week.TotalTokens != 250 || week.TotalCost != 0.12 || week.AvgLatencyMs != 150 {
		t.Fatalf("week summary = %+v, want realtime+responses aggregate", week)
	}
	if week.BySource[SourceRealtime] != 1 || week.BySource[SourceResponses] != 1 {
		t.Fatalf("week BySource = %+v, want both sources", week.BySource)
	}
	if week.ByModel["gpt-realtime"] != 1 || week.ByModel["gpt-4.1"] != 1 {
		t.Fatalf("week ByModel = %+v, want both models", week.ByModel)
	}

	month := periods["month"].Summary
	if month.Requests != 2 || month.TotalTokens != 250 {
		t.Fatalf("month summary = %+v, want old record excluded", month)
	}
	if len(periods["day"].Timeline) == 0 || len(periods["week"].Timeline) == 0 || len(periods["month"].Timeline) == 0 {
		t.Fatalf("timelines should not be empty: %+v", periods)
	}
}

func TestRecordResourceEventAggregatesOperationalCounters(t *testing.T) {
	ResetForTest()
	now := time.Date(2026, 6, 7, 15, 30, 0, 0, time.UTC)

	RecordResourceEvent(ResourceEvent{
		Source:    SourceRealtime,
		Kind:      ResourceKindCapacityRejected,
		UserID:    "user-1",
		Timestamp: now.Add(-10 * time.Minute).UnixMilli(),
	})
	RecordResourceEvent(ResourceEvent{
		Source:    SourceRealtime,
		Kind:      ResourceKindRateLimitRejected,
		UserID:    "user-1",
		Timestamp: now.Add(-9 * time.Minute).UnixMilli(),
	})
	RecordResourceEvent(ResourceEvent{
		Source:    SourceRealtime,
		Kind:      ResourceKindError,
		Status:    "failed",
		Error:     "openai upstream error",
		Timestamp: now.Add(-8 * time.Minute).UnixMilli(),
	})
	RecordResourceEvent(ResourceEvent{
		Source:    SourceSystem,
		Kind:      ResourceKindAlertFiring,
		Timestamp: now.Add(-7 * time.Minute).UnixMilli(),
	})
	RecordResourceEvent(ResourceEvent{
		Source:    SourceSystem,
		Kind:      ResourceKindAlertRecovered,
		Timestamp: now.Add(-6 * time.Minute).UnixMilli(),
	})
	RecordResourceEvent(ResourceEvent{
		Source:    SourceWorkspace,
		Kind:      ResourceKindWorkspaceWritePending,
		UserID:    "user-2",
		Timestamp: now.Add(-5 * time.Minute).UnixMilli(),
	})
	RecordResourceEvent(ResourceEvent{
		Source:    SourceWorkspace,
		Kind:      ResourceKindWorkspaceWriteConfirmed,
		UserID:    "user-2",
		Timestamp: now.Add(-4 * time.Minute).UnixMilli(),
	})
	RecordResourceEvent(ResourceEvent{
		Source:    SourceWorkspace,
		Kind:      ResourceKindWorkspaceWriteRejected,
		UserID:    "user-2",
		Timestamp: now.Add(-3 * time.Minute).UnixMilli(),
	})
	RecordResourceEvent(ResourceEvent{
		Source:    SourceWorkspace,
		Kind:      ResourceKindWorkspaceWriteFailed,
		UserID:    "user-2",
		Timestamp: now.Add(-2 * time.Minute).UnixMilli(),
	})

	day := ResourcePeriods(now)["day"]
	summary := day.Summary
	if summary.CapacityRejected != 1 ||
		summary.RateLimitRejected != 1 ||
		summary.Errors != 2 ||
		summary.AlertsFiring != 1 ||
		summary.AlertsRecovered != 1 ||
		summary.WorkspaceWritePending != 1 ||
		summary.WorkspaceWriteConfirmed != 1 ||
		summary.WorkspaceWriteRejected != 1 ||
		summary.WorkspaceWriteFailed != 1 {
		t.Fatalf("day summary = %+v, want operational resource counters", summary)
	}
	if summary.BySource[SourceRealtime] != 3 || summary.BySource[SourceSystem] != 2 || summary.BySource[SourceWorkspace] != 4 {
		t.Fatalf("BySource = %+v, want resource event sources counted", summary.BySource)
	}
	if summary.ByKind[ResourceKindWorkspaceWriteConfirmed] != 1 || summary.ByKind[ResourceKindError] != 1 {
		t.Fatalf("ByKind = %+v, want resource event kinds counted", summary.ByKind)
	}
	if len(day.Timeline) != 1 || day.Timeline[0].WorkspaceWriteConfirmed != 1 || day.Timeline[0].CapacityRejected != 1 {
		t.Fatalf("timeline = %+v, want operational counters in time bucket", day.Timeline)
	}
}

func TestRecordResourceEventAggregatesBusinessCacheHitAndMissCounters(t *testing.T) {
	ResetForTest()
	now := time.Date(2026, 6, 7, 15, 30, 0, 0, time.UTC)

	RecordResourceEvent(ResourceEvent{
		Source:    SourceResponses,
		Kind:      ResourceKindBusinessCacheHit,
		Provider:  "openairesponses",
		Model:     "gpt-4.1",
		UserID:    "user-1",
		Timestamp: now.Add(-4 * time.Minute).UnixMilli(),
		Count:     3,
	})
	RecordResourceEvent(ResourceEvent{
		Source:    SourceResponses,
		Kind:      ResourceKindBusinessCacheMiss,
		Provider:  "openairesponses",
		Model:     "gpt-4.1",
		UserID:    "user-1",
		Timestamp: now.Add(-3 * time.Minute).UnixMilli(),
		Count:     2,
	})

	day := ResourcePeriodsWithFilter(now, ResourceFilter{Kind: ResourceKindBusinessCacheHit})["day"]
	if day.Summary.BusinessCacheHits != 3 || day.Summary.BusinessCacheMisses != 0 {
		t.Fatalf("hit-filter summary = %+v, want only 3 business cache hits", day.Summary)
	}
	if len(day.Timeline) != 1 || day.Timeline[0].BusinessCacheHits != 3 || day.Timeline[0].BusinessCacheMisses != 0 {
		t.Fatalf("hit-filter timeline = %+v, want only hit bucket", day.Timeline)
	}

	all := ResourcePeriods(now)["day"]
	if all.Summary.BusinessCacheHits != 3 || all.Summary.BusinessCacheMisses != 2 {
		t.Fatalf("all summary = %+v, want business cache hits and misses", all.Summary)
	}
	if all.Summary.ByKind[ResourceKindBusinessCacheHit] != 3 || all.Summary.ByKind[ResourceKindBusinessCacheMiss] != 2 {
		t.Fatalf("ByKind = %+v, want cache hit/miss counters", all.Summary.ByKind)
	}
	if all.Summary.ByModel["gpt-4.1"] != 5 {
		t.Fatalf("ByModel = %+v, want cache counters attributed to model", all.Summary.ByModel)
	}
	if len(all.Timeline) != 1 || all.Timeline[0].BusinessCacheHits != 3 || all.Timeline[0].BusinessCacheMisses != 2 {
		t.Fatalf("timeline = %+v, want cache counters in time bucket", all.Timeline)
	}
}
