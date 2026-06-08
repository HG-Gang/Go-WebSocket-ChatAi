package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/logger"
	"TozoAI-Chat-Api/internal/provider/openairesponses"
	"TozoAI-Chat-Api/internal/service/stats"

	"github.com/gin-gonic/gin"
)

func TestBuildResourcePeriodStatsAggregatesDayWeekMonth(t *testing.T) {
	now := time.Date(2026, 6, 7, 15, 30, 0, 0, time.UTC)
	records := []WebRequestRecord{
		{
			Timestamp:       now.Add(-1 * time.Hour).UnixMilli(),
			Status:          "completed",
			InputTokens:     40,
			OutputTokens:    60,
			CachedTokens:    10,
			ReasoningTokens: 5,
			TotalTokens:     100,
			TotalCost:       0.12,
			LatencyMs:       100,
		},
		{
			Timestamp:       now.Add(-25 * time.Hour).UnixMilli(),
			Status:          "failed",
			InputTokens:     80,
			OutputTokens:    120,
			CachedTokens:    20,
			ReasoningTokens: 10,
			TotalTokens:     200,
			TotalCost:       0.24,
			LatencyMs:       300,
		},
		{
			Timestamp:       now.AddDate(0, 0, -10).UnixMilli(),
			Status:          "completed",
			InputTokens:     120,
			OutputTokens:    180,
			CachedTokens:    30,
			ReasoningTokens: 15,
			TotalTokens:     300,
			TotalCost:       0.36,
			LatencyMs:       500,
		},
		{
			Timestamp:   now.AddDate(0, 0, -40).UnixMilli(),
			Status:      "completed",
			TotalTokens: 400,
			TotalCost:   0.48,
			LatencyMs:   700,
		},
	}

	stats := buildResourcePeriodStats(records, now)

	day := stats["day"].Summary
	if day.Requests != 1 || day.TotalTokens != 100 || day.CachedTokens != 10 || day.FailedRequests != 0 {
		t.Fatalf("day summary = %+v, want 1 request, 100 tokens, 10 cached, 0 failures", day)
	}
	week := stats["week"].Summary
	if week.Requests != 2 || week.TotalTokens != 300 || week.FailedRequests != 1 || week.AvgLatencyMs != 200 {
		t.Fatalf("week summary = %+v, want 2 requests, 300 tokens, 1 failure, 200ms avg latency", week)
	}
	month := stats["month"].Summary
	if month.Requests != 3 || month.TotalTokens != 600 || month.CachedTokens != 60 || month.ReasoningTokens != 30 {
		t.Fatalf("month summary = %+v, want 3 requests, 600 tokens, 60 cached, 30 reasoning", month)
	}
	if len(stats["day"].Timeline) == 0 || len(stats["week"].Timeline) == 0 || len(stats["month"].Timeline) == 0 {
		t.Fatalf("timelines should not be empty: %+v", stats)
	}
}

func TestWebMetricsHandlerIncludesResourcePeriodCharts(t *testing.T) {
	resetWebRequestStatsForTest()
	t.Cleanup(resetWebRequestStatsForTest)
	stats.ResetForTest()
	t.Cleanup(stats.ResetForTest)
	stats.RecordUsage(stats.UsageRecord{
		Source:          stats.SourceResponses,
		Provider:        "openairesponses",
		Model:           "gpt-4.1",
		Timestamp:       time.Now().UnixMilli(),
		Status:          "completed",
		InputTokens:     10,
		OutputTokens:    20,
		CachedTokens:    3,
		ReasoningTokens: 2,
		TotalTokens:     30,
		TotalCost:       0.03,
		LatencyMs:       120,
	})

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/web/metrics", nil)

	WebMetricsHandler(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var body struct {
		Data struct {
			Charts struct {
				Resources map[string]stats.ResourcePeriodStats `json:"resources"`
			} `json:"charts"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	day := body.Data.Charts.Resources["day"].Summary
	if day.Requests != 1 || day.TotalTokens != 30 || day.CachedTokens != 3 || day.ReasoningTokens != 2 {
		t.Fatalf("day resource summary = %+v, want request/token aggregates", day)
	}
}

func TestWebMetricsHandlerResourceChartsUseUnifiedStatsService(t *testing.T) {
	resetWebRequestStatsForTest()
	t.Cleanup(resetWebRequestStatsForTest)
	stats.ResetForTest()
	t.Cleanup(stats.ResetForTest)
	stats.RecordUsage(stats.UsageRecord{
		Source:          stats.SourceRealtime,
		Provider:        "openai",
		Model:           "gpt-realtime",
		Status:          "completed",
		Timestamp:       time.Now().UnixMilli(),
		InputTokens:     25,
		OutputTokens:    35,
		CachedTokens:    5,
		ReasoningTokens: 2,
		TotalTokens:     60,
	})

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/web/metrics", nil)

	WebMetricsHandler(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var body struct {
		Data struct {
			Charts struct {
				Resources map[string]stats.ResourcePeriodStats `json:"resources"`
			} `json:"charts"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	day := body.Data.Charts.Resources["day"].Summary
	if day.Requests != 1 || day.TotalTokens != 60 || day.BySource[stats.SourceRealtime] != 1 {
		t.Fatalf("day resource summary = %+v, want unified realtime stats", day)
	}
}

func TestAddWebRequestRecordWritesAuditLogEvent(t *testing.T) {
	resetWebRequestStatsForTest()
	t.Cleanup(resetWebRequestStatsForTest)
	var events []webMetricsLogEvent
	restoreSink := setWebMetricsLogSinkForTest(func(event webMetricsLogEvent) {
		events = append(events, event)
	})
	defer restoreSink()

	addWebRequestRecord(WebRequestRecord{
		Model:           "gpt-4.1",
		Status:          "completed",
		InputTokens:     11,
		OutputTokens:    22,
		CachedTokens:    3,
		ReasoningTokens: 4,
		TotalTokens:     33,
		LatencyMs:       120,
	})

	if len(events) != 1 {
		t.Fatalf("audit events len = %d, want 1", len(events))
	}
	if events[0].Event != "web_request_metric" {
		t.Fatalf("audit event = %q, want web_request_metric", events[0].Event)
	}
	if events[0].Record.ID != 1 || events[0].Record.TotalTokens != 33 || events[0].Record.CachedTokens != 3 || events[0].Record.ReasoningTokens != 4 {
		t.Fatalf("audit record = %+v, want assigned id and token fields", events[0].Record)
	}
}

func TestWebMetricsHandlerWritesStatsRollupAuditEvent(t *testing.T) {
	resetWebRequestStatsForTest()
	t.Cleanup(resetWebRequestStatsForTest)
	stats.ResetForTest()
	t.Cleanup(stats.ResetForTest)
	stats.RecordUsage(stats.UsageRecord{
		Source:          stats.SourceResponses,
		Provider:        "openairesponses",
		Model:           "gpt-4.1",
		Timestamp:       time.Now().UnixMilli(),
		Status:          "completed",
		InputTokens:     10,
		OutputTokens:    20,
		CachedTokens:    3,
		ReasoningTokens: 2,
		TotalTokens:     30,
		TotalCost:       0.03,
		LatencyMs:       120,
	})
	var events []webMetricsLogEvent
	restoreSink := setWebMetricsLogSinkForTest(func(event webMetricsLogEvent) {
		events = append(events, event)
	})
	defer restoreSink()

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/web/metrics", nil)

	WebMetricsHandler(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if len(events) != 1 {
		t.Fatalf("audit events len = %d, want 1", len(events))
	}
	if events[0].Event != "stats_rollup" {
		t.Fatalf("audit event = %q, want stats_rollup", events[0].Event)
	}
	day := events[0].Resources["day"].Summary
	if day.Requests != 1 || day.TotalTokens != 30 || day.CachedTokens != 3 || day.ReasoningTokens != 2 {
		t.Fatalf("stats_rollup day summary = %+v, want request/token aggregates", day)
	}
}

func TestWriteWebMetricsDailyLogRedactsSensitiveAPIKey(t *testing.T) {
	resetWebRequestStatsForTest()
	t.Cleanup(resetWebRequestStatsForTest)
	restoreLogger := logger.ResetForTest()
	t.Cleanup(restoreLogger)
	root := t.TempDir()
	oldGlobal := conf.Global
	conf.Global = &conf.GlobalConfig{
		Env: "test",
		Models: map[string]conf.ModelConfig{
			"openai": {Enabled: true},
		},
	}
	conf.Global.Logs.RootDir = root
	conf.InitModelConfig()
	t.Cleanup(func() {
		logger.SyncAll()
		conf.Global = oldGlobal
	})

	writeWebMetricsDailyLog(webMetricsLogEvent{
		Event: "web_request_metric",
		Record: WebRequestRecord{
			ModelConfig: "openairesponses",
			Model:       "gpt-4.1",
			Status:      "completed",
			APIKey:      "sk-test-plain-secret",
		},
	})
	logger.SyncAll()

	path := filepath.Join(root, "openai", "openai-"+time.Now().Format("2006-01-02")+".log")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	text := string(data)
	if strings.Contains(text, "sk-test-plain-secret") || strings.Contains(text, "plain-secret") {
		t.Fatalf("web metrics audit log leaked API key: %s", text)
	}
	if !strings.Contains(text, "api_key") || !strings.Contains(text, "sha256:") {
		t.Fatalf("web metrics audit log should keep redacted api_key marker: %s", text)
	}
}

func TestAddResponsesMetricRecordsUnifiedStatsUsage(t *testing.T) {
	resetWebRequestStatsForTest()
	t.Cleanup(resetWebRequestStatsForTest)
	stats.ResetForTest()
	t.Cleanup(stats.ResetForTest)

	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/api/openai/responses", nil)
	c.Request.Header.Set("User-Agent", "stats-test")

	result := &openairesponses.Result{
		Model:  "gpt-4.1",
		Status: "completed",
		Raw: json.RawMessage(`{
			"usage": {
				"input_tokens": 40,
				"output_tokens": 60,
				"total_tokens": 100,
				"input_token_details": {"cached_tokens": 9},
				"output_token_details": {"reasoning_tokens": 4}
			}
		}`),
	}
	addResponsesMetric(c, "openairesponses", &conf.ModelConfig{DefaultModel: "gpt-4.1"}, map[string]any{}, result, "completed", 150*time.Millisecond, "")

	periods := stats.ResourcePeriods(time.Now())
	day := periods["day"].Summary
	if day.Requests != 1 || day.TotalTokens != 100 || day.CachedTokens != 9 || day.ReasoningTokens != 4 {
		t.Fatalf("stats day summary = %+v, want Responses usage recorded", day)
	}
	if day.BySource[stats.SourceResponses] != 1 {
		t.Fatalf("stats BySource = %+v, want responses source", day.BySource)
	}
}

func resetWebRequestStatsForTest() {
	webRequestStats.Lock()
	defer webRequestStats.Unlock()
	webRequestStats.NextID = 0
	webRequestStats.Records = make([]WebRequestRecord, 0, maxWebRequestRecords)
}
