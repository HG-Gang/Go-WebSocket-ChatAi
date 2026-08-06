// tools/wsload/main_test.go
// 文件功能：覆盖参数解析与非法参数拦截、延迟百分位计算、报告聚合与 JSON 输出，
// 以及基于本地 echo 服务器的端到端压测流程。
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestParseConfigReadsLoadFlags(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-url", "ws://127.0.0.1:8096/ws/realtime/openai",
		"-users", "1000",
		"-ramp", "30s",
		"-duration", "5m",
		"-token", "jwt-token",
		"-message", "ping",
		"-debug-url", "http://127.0.0.1:8096/api/debug/status",
		"-report", ".tmp/wsload-report.json",
	})
	if err != nil {
		t.Fatalf("parseConfig returned error: %v", err)
	}
	if cfg.URL != "ws://127.0.0.1:8096/ws/realtime/openai" {
		t.Fatalf("URL = %q", cfg.URL)
	}
	if cfg.Users != 1000 {
		t.Fatalf("Users = %d, want 1000", cfg.Users)
	}
	if cfg.Ramp != 30*time.Second {
		t.Fatalf("Ramp = %s, want 30s", cfg.Ramp)
	}
	if cfg.Duration != 5*time.Minute {
		t.Fatalf("Duration = %s, want 5m", cfg.Duration)
	}
	if cfg.Token != "jwt-token" || cfg.Message != "ping" {
		t.Fatalf("Token/Message not parsed: %+v", cfg)
	}
	if cfg.DebugURL == "" || cfg.ReportPath == "" {
		t.Fatalf("debug/report flags not parsed: %+v", cfg)
	}
}

func TestParseConfigRejectsInvalidLoadShape(t *testing.T) {
	if _, err := parseConfig([]string{"-url", "ws://127.0.0.1/ws", "-users", "0"}); err == nil {
		t.Fatal("parseConfig accepted users=0")
	}
	if _, err := parseConfig([]string{"-url", "http://127.0.0.1/ws", "-users", "1"}); err == nil {
		t.Fatal("parseConfig accepted non-websocket URL")
	}
	if _, err := parseConfig([]string{"-url", "ws://127.0.0.1/ws", "-users", "1", "-duration", "0s"}); err == nil {
		t.Fatal("parseConfig accepted duration=0")
	}
}

func TestPercentileUsesNearestRank(t *testing.T) {
	samples := []time.Duration{400 * time.Millisecond, 100 * time.Millisecond, 300 * time.Millisecond, 200 * time.Millisecond}

	if got := percentile(samples, 50); got != 200*time.Millisecond {
		t.Fatalf("p50 = %s, want 200ms", got)
	}
	if got := percentile(samples, 95); got != 400*time.Millisecond {
		t.Fatalf("p95 = %s, want 400ms", got)
	}
	if got := percentile(samples, 99); got != 400*time.Millisecond {
		t.Fatalf("p99 = %s, want 400ms", got)
	}
}

func TestBuildReportAggregatesLatencyErrorsAndCloseCodes(t *testing.T) {
	cfg := loadConfig{URL: "ws://127.0.0.1:8096/ws/realtime/openai", Users: 3, Duration: time.Minute}
	stats := newRunStats()
	stats.recordConnect(true, 20*time.Millisecond, nil)
	stats.recordConnect(true, 40*time.Millisecond, nil)
	stats.recordConnect(false, 0, errString("dial timeout"))
	stats.recordFirstByte(150 * time.Millisecond)
	stats.recordFirstByte(50 * time.Millisecond)
	stats.recordComplete(500 * time.Millisecond)
	stats.recordCloseCode(1000)
	stats.recordCloseCode(1006)
	stats.recordError("openai_reconnect_required")
	stats.messagesSent.Add(7)

	report := buildReport(cfg, stats)

	if report.Config.Users != 3 {
		t.Fatalf("report users = %d, want 3", report.Config.Users)
	}
	if report.Summary.ConnectSuccess != 2 || report.Summary.ConnectFailed != 1 {
		t.Fatalf("connect summary = %+v, want 2 success / 1 failed", report.Summary)
	}
	if report.Summary.MessagesSent != 7 {
		t.Fatalf("messages_sent = %d, want 7", report.Summary.MessagesSent)
	}
	if report.Latency.FirstByteP50Ms != 50 || report.Latency.FirstByteP95Ms != 150 {
		t.Fatalf("first byte latency = %+v, want p50=50 p95=150", report.Latency)
	}
	if report.CloseCodes["1006"] != 1 || report.Errors["openai_reconnect_required"] != 1 || report.Errors["dial timeout"] != 1 {
		t.Fatalf("close/errors not aggregated: close=%+v errors=%+v", report.CloseCodes, report.Errors)
	}
	if !strings.Contains(report.CapacityConclusion, "当前未实测百万并发") {
		t.Fatalf("capacity conclusion = %q, want unproven million-concurrency statement", report.CapacityConclusion)
	}
}

func TestReportJSONContainsCapacityConclusion(t *testing.T) {
	report := buildReport(loadConfig{URL: "ws://127.0.0.1/ws", Users: 1, Duration: time.Second}, newRunStats())

	data, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("Marshal report: %v", err)
	}
	if !strings.Contains(string(data), "capacity_conclusion") {
		t.Fatalf("report json = %s, want capacity_conclusion", string(data))
	}
}

func TestRunStatsForConfigAgainstLocalEchoServer(t *testing.T) {
	// 本地 echo 服务器作为测试替身：读取客户端消息后回写 "ack"，模拟 Realtime 服务端行为。
	upgrader := websocket.Upgrader{}
	messages := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		messages <- string(payload)
		_ = conn.WriteMessage(messageType, []byte("ack"))
	}))
	defer server.Close()

	cfg := loadConfig{
		URL:      "ws" + strings.TrimPrefix(server.URL, "http"),
		Users:    2,
		Ramp:     0,
		Duration: 20 * time.Millisecond,
		Message:  "ping-{user}",
	}
	stats := runStatsForConfig(context.Background(), cfg)
	report := buildReport(cfg, stats)

	if report.Summary.ConnectSuccess != 2 || report.Summary.ConnectFailed != 0 {
		t.Fatalf("connect summary = %+v, want 2 success / 0 failed", report.Summary)
	}
	if report.Summary.MessagesSent != 2 || report.Summary.MessagesRecv != 2 {
		t.Fatalf("message summary = %+v, want 2 sent / 2 recv", report.Summary)
	}
	if report.CloseCodes["1000"] != 2 {
		t.Fatalf("close codes = %+v, want two normal closures", report.CloseCodes)
	}
	if len(report.Errors) != 0 {
		t.Fatalf("errors = %+v, want none", report.Errors)
	}

	got := map[string]bool{}
	for len(got) < 2 {
		select {
		case message := <-messages:
			got[message] = true
		case <-time.After(time.Second):
			t.Fatalf("messages = %+v, want ping-0 and ping-1", got)
		}
	}
	if !got["ping-0"] || !got["ping-1"] {
		t.Fatalf("messages = %+v, want ping-0 and ping-1", got)
	}
}

// errString 把字符串包装为 error，便于在断言中构造期望的错误原因。
type errString string

func (e errString) Error() string { return string(e) }
