package metrics

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"TozoAI-Chat-Api/internal/service/stats"
)

func TestCriticalAppEventQueueTimeoutVisibleInSnapshot(t *testing.T) {
	resetMetricsForTest()
	SessionStarted("session-1", "request-1", "user-1", "张三", "device-1", "openai", "127.0.0.1", "test-agent")

	CriticalAppEventQueueTimeout("session-1", "reconnect_required", 128)

	snapshot := Snapshot()
	goStats := snapshot["go"].(goMetrics)
	if goStats.CriticalAppEventQueueTimeouts != 1 {
		t.Fatalf("CriticalAppEventQueueTimeouts = %d, want 1", goStats.CriticalAppEventQueueTimeouts)
	}

	sessions := snapshot["recent_sessions"].([]map[string]any)
	if len(sessions) != 1 {
		t.Fatalf("recent_sessions len = %d, want 1", len(sessions))
	}
	eventCounts := sessions[0]["event_counts"].(map[string]uint64)
	if eventCounts["app_queue_timeout:reconnect_required"] != 1 {
		t.Fatalf("critical event count = %d, want 1", eventCounts["app_queue_timeout:reconnect_required"])
	}
}

func TestOpenAIPingMetricsVisibleInSnapshot(t *testing.T) {
	resetMetricsForTest()
	SessionStarted("session-1", "request-1", "user-1", "张三", "device-1", "openai", "127.0.0.1", "test-agent")

	OpenAIPingSent("session-1")
	OpenAIPingFailed("session-1", errors.New("write ping failed"))

	snapshot := Snapshot()
	openai := snapshot["openai"].(openAIMetrics)
	if openai.PingSent != 1 {
		t.Fatalf("PingSent = %d, want 1", openai.PingSent)
	}
	if openai.PingFailures != 1 {
		t.Fatalf("PingFailures = %d, want 1", openai.PingFailures)
	}
}

func TestSessionSnapshotIncludesRealIPAndLocation(t *testing.T) {
	resetMetricsForTest()
	SessionStarted("session-1", "request-1", "user-1", "张三", "device-1", "openai", "192.168.1.20:23456", "test-agent")

	snapshot := Snapshot()
	sessions := snapshot["recent_sessions"].([]map[string]any)
	if len(sessions) != 1 {
		t.Fatalf("recent_sessions len = %d, want 1", len(sessions))
	}

	session := sessions[0]
	if got := session["real_ip"]; got != "192.168.1.20" {
		t.Fatalf("real_ip = %v, want 192.168.1.20", got)
	}
	location, ok := session["ip_location"].(map[string]string)
	if !ok {
		t.Fatalf("ip_location type = %T, want map[string]string", session["ip_location"])
	}
	if location["status"] != "private" {
		t.Fatalf("ip_location.status = %q, want private", location["status"])
	}
	if location["display"] == "" {
		t.Fatal("ip_location.display is empty")
	}
}

func TestSessionSnapshotPrefersProvidedIPLocation(t *testing.T) {
	resetMetricsForTest()
	SessionStartedWithLocation(
		"session-1",
		"request-1",
		"user-1",
		"张三",
		"device-1",
		"openai",
		"203.0.113.10:23456",
		"test-agent",
		map[string]string{
			"country": "中国",
			"region":  "广东",
			"city":    "深圳",
			"source":  "request_header",
		},
	)

	sessions := Snapshot()["recent_sessions"].([]map[string]any)
	location := sessions[0]["ip_location"].(map[string]string)
	if location["display"] != "中国 / 广东 / 深圳" {
		t.Fatalf("ip_location.display = %q, want 中国 / 广东 / 深圳", location["display"])
	}
	if location["source"] != "request_header" {
		t.Fatalf("ip_location.source = %q, want request_header", location["source"])
	}
}

func TestHighFrequencyCountersVisibleAfterConcurrentAtomicUpdates(t *testing.T) {
	resetMetricsForTest()

	done := make(chan struct{}, 16)
	for i := 0; i < 16; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 1000; j++ {
				CapacityRejected()
				AppWrite("", 7)
				OpenAITextDelta("", "", "令牌")
				InputAudio("", 320)
			}
		}()
	}
	for i := 0; i < 16; i++ {
		<-done
	}

	snapshot := Snapshot()
	goStats := snapshot["go"].(goMetrics)
	app := snapshot["app"].(appMetrics)
	openai := snapshot["openai"].(openAIMetrics)
	business := snapshot["business"].(businessMetrics)

	if goStats.CapacityRejected != 16000 {
		t.Fatalf("CapacityRejected = %d, want 16000", goStats.CapacityRejected)
	}
	if app.BytesOut != 112000 {
		t.Fatalf("BytesOut = %d, want 112000", app.BytesOut)
	}
	if openai.TextDeltaChars != 32000 {
		t.Fatalf("TextDeltaChars = %d, want 32000", openai.TextDeltaChars)
	}
	if business.InputAudioMs == 0 {
		t.Fatal("InputAudioMs should be visible in snapshot")
	}
}

func TestOpenAIResponseDoneUsageRecordsCachedAndReasoningTokens(t *testing.T) {
	resetMetricsForTest()
	SessionStarted("session-1", "request-1", "user-1", "张三", "device-1", "openai", "127.0.0.1", "test-agent")
	OpenAIResponseDoneUsage("session-1", "resp-1", "completed", ResponseTokenUsage{
		InputTokens:     100,
		OutputTokens:    50,
		TotalTokens:     150,
		CachedTokens:    30,
		ReasoningTokens: 12,
	})

	snapshot := Snapshot()
	business := snapshot["business"].(businessMetrics)
	if business.TotalTokens != 150 {
		t.Fatalf("TotalTokens = %d, want 150", business.TotalTokens)
	}
	if business.CachedTokens != 30 {
		t.Fatalf("CachedTokens = %d, want 30", business.CachedTokens)
	}
	if business.ReasoningTokens != 12 {
		t.Fatalf("ReasoningTokens = %d, want 12", business.ReasoningTokens)
	}

	sessions := snapshot["recent_sessions"].([]map[string]any)
	if sessions[0]["cached_tokens"] != uint64(30) {
		t.Fatalf("session cached_tokens = %#v, want 30", sessions[0]["cached_tokens"])
	}
	if sessions[0]["reasoning_tokens"] != uint64(12) {
		t.Fatalf("session reasoning_tokens = %#v, want 12", sessions[0]["reasoning_tokens"])
	}
	responses := sessions[0]["responses"].([]map[string]any)
	if responses[0]["cached_tokens"] != uint64(30) {
		t.Fatalf("response cached_tokens = %#v, want 30", responses[0]["cached_tokens"])
	}
	if responses[0]["reasoning_tokens"] != uint64(12) {
		t.Fatalf("response reasoning_tokens = %#v, want 12", responses[0]["reasoning_tokens"])
	}
}

func TestOperationalMetricsRecordUnifiedResourceStats(t *testing.T) {
	resetMetricsForTest()
	stats.ResetForTest()

	CapacityRejected()
	RateLimitRejected("user-1", "gpt-realtime", "/ws/realtime/openai", "too_many_requests")
	OpenAIError("session-1", "upstream_error", "openai upstream failed")
	BillingError("session-1", errors.New("billing write failed"))
	APISendQueueTimeout("session-1", "conversation.item.create", "queue full")
	CriticalAppEventQueueTimeout("session-1", "response.done", 128)

	summary := stats.ResourcePeriods(time.Now())["day"].Summary
	if summary.CapacityRejected != 1 || summary.RateLimitRejected != 1 || summary.Errors != 4 {
		t.Fatalf("resource summary = %+v, want capacity/rate/error counters from metrics", summary)
	}
	if summary.BySource[stats.SourceRealtime] != 5 || summary.BySource[stats.SourceSystem] != 1 {
		t.Fatalf("BySource = %+v, want realtime operational errors and system capacity rejection", summary.BySource)
	}
	if summary.ByKind[stats.ResourceKindCapacityRejected] != 1 ||
		summary.ByKind[stats.ResourceKindRateLimitRejected] != 1 ||
		summary.ByKind[stats.ResourceKindError] != 4 {
		t.Fatalf("ByKind = %+v, want metrics resource kinds counted", summary.ByKind)
	}
}

func TestCapacityRejectedDoesNotUseGlobalMutex(t *testing.T) {
	data, err := os.ReadFile("metrics.go")
	if err != nil {
		t.Fatalf("ReadFile metrics.go: %v", err)
	}
	body := functionBody(t, string(data), "CapacityRejected")
	if strings.Contains(body, "global.mu.Lock") || strings.Contains(body, "global.mu.Unlock") {
		t.Fatalf("CapacityRejected should use atomic hot-path counter, got body:\n%s", body)
	}
	if !strings.Contains(body, ".Add(1)") {
		t.Fatalf("CapacityRejected should increment an atomic counter, got body:\n%s", body)
	}
}

func functionBody(t *testing.T, source, name string) string {
	t.Helper()
	start := strings.Index(source, "func "+name+"(")
	if start < 0 {
		t.Fatalf("function %s not found", name)
	}
	open := strings.Index(source[start:], "{")
	if open < 0 {
		t.Fatalf("function %s has no body", name)
	}
	bodyStart := start + open + 1
	depth := 1
	for i := bodyStart; i < len(source); i++ {
		switch source[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return source[bodyStart:i]
			}
		}
	}
	t.Fatalf("function %s body is not closed", name)
	return ""
}

func resetMetricsForTest() {
	global = &collector{
		started:  time.Now(),
		sessions: make(map[string]*sessionMetrics),
		app: appMetrics{
			MessageTypes:      make(map[string]uint64),
			DisconnectReasons: make(map[string]uint64),
		},
		openai: openAIMetrics{
			ClientEventTypes: make(map[string]uint64),
			ServerEventTypes: make(map[string]uint64),
			ReconnectReasons: make(map[string]uint64),
		},
		errors: errorMetrics{
			ByCode:   make(map[string]uint64),
			ByReason: make(map[string]uint64),
		},
		business: businessMetrics{
			TokensByUser:  make(map[string]uint64),
			TokensByModel: make(map[string]uint64),
			TokensByDay:   make(map[string]uint64),
		},
	}
}
