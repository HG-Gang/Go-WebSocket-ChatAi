package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"TozoAI-Chat-Api/internal/service/stats"
)

func TestStatsResourcesHandlerReturnsSelectedPeriodAndModelFilter(t *testing.T) {
	stats.ResetForTest()
	t.Cleanup(stats.ResetForTest)
	var events []webMetricsLogEvent
	restoreSink := setWebMetricsLogSinkForTest(func(event webMetricsLogEvent) {
		events = append(events, event)
	})
	defer restoreSink()
	now := time.Now()
	currentDayTimestamp := now.UnixMilli()
	stats.RecordUsage(stats.UsageRecord{
		Source:       stats.SourceRealtime,
		Provider:     "openai",
		Model:        "gpt-realtime",
		Status:       "completed",
		Timestamp:    currentDayTimestamp,
		InputTokens:  40,
		OutputTokens: 60,
		TotalTokens:  100,
	})
	stats.RecordUsage(stats.UsageRecord{
		Source:      stats.SourceResponses,
		Provider:    "openairesponses",
		Model:       "gpt-4.1",
		Status:      "completed",
		Timestamp:   currentDayTimestamp,
		TotalTokens: 300,
	})
	stats.RecordResourceEvent(stats.ResourceEvent{
		Source:    stats.SourceRealtime,
		Kind:      stats.ResourceKindError,
		Provider:  "openai",
		Model:     "gpt-realtime",
		Status:    "failed",
		Timestamp: currentDayTimestamp,
	})
	stats.RecordResourceEvent(stats.ResourceEvent{
		Source:    stats.SourceSystem,
		Kind:      stats.ResourceKindCapacityRejected,
		Timestamp: currentDayTimestamp,
	})
	stats.RecordResourceEvent(stats.ResourceEvent{
		Source:    stats.SourceResponses,
		Kind:      stats.ResourceKindBusinessCacheHit,
		Provider:  "openai",
		Model:     "gpt-realtime",
		Timestamp: currentDayTimestamp,
		Count:     4,
	})
	stats.RecordResourceEvent(stats.ResourceEvent{
		Source:    stats.SourceResponses,
		Kind:      stats.ResourceKindBusinessCacheMiss,
		Provider:  "openai",
		Model:     "gpt-realtime",
		Timestamp: currentDayTimestamp,
		Count:     2,
	})

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/stats/resources?period=day&model=openai", nil)

	StatsResourcesHandler(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", w.Code, w.Body.String())
	}
	var body struct {
		Code int `json:"code"`
		Data struct {
			Period   string                               `json:"period"`
			Filters  map[string]string                    `json:"filters"`
			Selected stats.ResourcePeriodStats            `json:"selected"`
			Periods  map[string]stats.ResourcePeriodStats `json:"periods"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Code != 200 || body.Data.Period != "day" || body.Data.Filters["model"] != "openai" {
		t.Fatalf("body metadata = %+v, want code 200 period day model openai", body)
	}
	summary := body.Data.Selected.Summary
	if summary.Requests != 1 ||
		summary.TotalTokens != 100 ||
		summary.Errors != 1 ||
		summary.CapacityRejected != 0 ||
		summary.BusinessCacheHits != 4 ||
		summary.BusinessCacheMisses != 2 {
		t.Fatalf("selected summary = %+v, want only openai model usage, error and cache counters", summary)
	}
	if summary.BySource[stats.SourceRealtime] != 2 || summary.BySource[stats.SourceResponses] != 6 {
		t.Fatalf("BySource = %+v, want realtime usage+error and responses cache events", summary.BySource)
	}
	if _, ok := body.Data.Periods["week"]; !ok {
		t.Fatalf("periods = %+v, want week period also returned for chart switching", body.Data.Periods)
	}
	if len(events) != 1 || events[0].Event != "stats_rollup" {
		t.Fatalf("audit events = %+v, want one stats_rollup event", events)
	}
	auditDay := events[0].Resources["day"].Summary
	if auditDay.BusinessCacheHits != 4 || auditDay.BusinessCacheMisses != 2 {
		t.Fatalf("audit day summary = %+v, want cache counters in stats_rollup log event", auditDay)
	}
}

func TestStatsResourcesHandlerRejectsInvalidPeriod(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/stats/resources?period=year", nil)

	StatsResourcesHandler(c)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 body=%s", w.Code, w.Body.String())
	}
}
