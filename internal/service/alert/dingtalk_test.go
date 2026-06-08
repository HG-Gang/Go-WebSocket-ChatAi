package alert

import (
	"context"
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
	"TozoAI-Chat-Api/internal/service/stats"
)

func TestNotifyOverloadSendsDingTalkMessage(t *testing.T) {
	ResetForTest()
	var received map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	}))
	defer server.Close()

	conf.Global = &conf.GlobalConfig{}
	conf.Global.Alerts.DingTalk.Enabled = true
	conf.Global.Alerts.DingTalk.Webhook = server.URL
	conf.Global.Alerts.DingTalk.CooldownSeconds = 0

	err := NotifyOverload(context.Background(), OverloadEvent{
		Provider:       "openai",
		UserID:         "user-1",
		UserName:       "张三",
		RemoteAddr:     "203.0.113.10",
		ActiveSessions: 100,
		MaxSessions:    100,
		Reason:         "capacity_rejected",
	})
	if err != nil {
		t.Fatalf("NotifyOverload returned error: %v", err)
	}

	if received["msgtype"] != "text" {
		t.Fatalf("msgtype = %v, want text", received["msgtype"])
	}
	text, _ := received["text"].(map[string]any)
	content, _ := text["content"].(string)
	for _, want := range []string{"系统过载预警", "openai", "user-1", "张三", "203.0.113.10", "100/100", "capacity_rejected"} {
		if !strings.Contains(content, want) {
			t.Fatalf("content = %q, want contains %q", content, want)
		}
	}
}

func TestNotifyOverloadRecordsUnifiedStatsAlert(t *testing.T) {
	ResetForTest()
	stats.ResetForTest()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	}))
	defer server.Close()

	conf.Global = &conf.GlobalConfig{}
	conf.Global.Alerts.DingTalk.Enabled = true
	conf.Global.Alerts.DingTalk.Webhook = server.URL
	conf.Global.Alerts.DingTalk.CooldownSeconds = 0

	if err := NotifyOverload(context.Background(), OverloadEvent{
		Provider:       "openai",
		ActiveSessions: 100,
		MaxSessions:    100,
		Reason:         "capacity_rejected",
	}); err != nil {
		t.Fatalf("NotifyOverload returned error: %v", err)
	}

	summary := stats.ResourcePeriods(time.Now())["day"].Summary
	if summary.AlertsFiring != 1 {
		t.Fatalf("AlertsFiring = %d, want 1", summary.AlertsFiring)
	}
	if summary.BySource[stats.SourceSystem] != 1 || summary.ByKind[stats.ResourceKindAlertFiring] != 1 {
		t.Fatalf("stats grouping = source %+v kind %+v, want alert_firing system event", summary.BySource, summary.ByKind)
	}
}

func TestNotifyOverloadRecordsRecoveredStatsAndAuditLog(t *testing.T) {
	ResetForTest()
	stats.ResetForTest()
	restoreLogger := logger.ResetForTest()
	t.Cleanup(restoreLogger)

	var received map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	}))
	defer server.Close()

	logRoot := t.TempDir()
	conf.Global = &conf.GlobalConfig{
		Env: "test",
		Models: map[string]conf.ModelConfig{
			"openai": {Enabled: true},
		},
	}
	conf.Global.Logs.RootDir = logRoot
	conf.Global.Alerts.DingTalk.Enabled = true
	conf.Global.Alerts.DingTalk.Webhook = server.URL + "?access_token=plain-secret"
	conf.Global.Alerts.DingTalk.CooldownSeconds = 0
	conf.InitModelConfig()

	if err := NotifyOverload(context.Background(), OverloadEvent{
		Provider:       "system",
		ActiveSessions: 10,
		MaxSessions:    100,
		Reason:         "capacity_usage_high",
		Status:         OverloadStatusRecovered,
	}); err != nil {
		t.Fatalf("NotifyOverload returned error: %v", err)
	}
	logger.SyncAll()

	summary := stats.ResourcePeriods(time.Now())["day"].Summary
	if summary.AlertsRecovered != 1 || summary.AlertsFiring != 0 {
		t.Fatalf("alert counters = firing %d recovered %d, want only one recovered", summary.AlertsFiring, summary.AlertsRecovered)
	}
	if summary.ByKind[stats.ResourceKindAlertRecovered] != 1 {
		t.Fatalf("ByKind = %+v, want alert_recovered", summary.ByKind)
	}

	textPayload, _ := received["text"].(map[string]any)
	content, _ := textPayload["content"].(string)
	for _, want := range []string{"系统过载恢复", "capacity_usage_high", "10/100"} {
		if !strings.Contains(content, want) {
			t.Fatalf("content = %q, want contains %q", content, want)
		}
	}

	path := filepath.Join(logRoot, "openai", "openai-"+time.Now().Format("2006-01-02")+".log")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	text := string(data)
	for _, want := range []string{`"event": "alert_recovered"`, `"status": "recovered"`, `"reason": "capacity_usage_high"`, `"event": "dingtalk_sent"`} {
		if !strings.Contains(text, want) {
			t.Fatalf("audit log = %s, want contains %s", text, want)
		}
	}
	if strings.Contains(text, "plain-secret") {
		t.Fatalf("audit log leaked webhook token: %s", text)
	}
}

func TestNotifyOverloadWritesDailyAuditLogAndRedactsWebhook(t *testing.T) {
	ResetForTest()
	restoreLogger := logger.ResetForTest()
	t.Cleanup(restoreLogger)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	}))
	defer server.Close()

	logRoot := t.TempDir()
	conf.Global = &conf.GlobalConfig{
		Env: "test",
		Models: map[string]conf.ModelConfig{
			"openai": {Enabled: true},
		},
	}
	conf.Global.Logs.RootDir = logRoot
	conf.Global.Alerts.DingTalk.Enabled = true
	conf.Global.Alerts.DingTalk.Webhook = server.URL + "?access_token=plain-secret"
	conf.Global.Alerts.DingTalk.CooldownSeconds = 0
	conf.InitModelConfig()

	if err := NotifyOverload(context.Background(), OverloadEvent{
		Provider:       "openai",
		UserID:         "user-1",
		UserName:       "张三",
		RemoteAddr:     "203.0.113.10",
		IPLocation:     map[string]string{"display": "中国 / 广东 / 深圳", "source": "request_header"},
		ActiveSessions: 100,
		MaxSessions:    100,
		Reason:         "capacity_rejected",
	}); err != nil {
		t.Fatalf("NotifyOverload returned error: %v", err)
	}
	logger.SyncAll()

	path := filepath.Join(logRoot, "openai", "openai-"+time.Now().Format("2006-01-02")+".log")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	text := string(data)
	for _, want := range []string{`"event": "alert_firing"`, `"event": "dingtalk_sent"`, `"provider": "openai"`, `"user_id": "user-1"`, `"reason": "capacity_rejected"`} {
		if !strings.Contains(text, want) {
			t.Fatalf("audit log = %s, want contains %s", text, want)
		}
	}
	if strings.Contains(text, "plain-secret") || strings.Contains(text, "access_token=plain-secret") {
		t.Fatalf("audit log leaked webhook token: %s", text)
	}
	if !strings.Contains(text, "webhook") || !strings.Contains(text, "sha256:") {
		t.Fatalf("audit log should include redacted webhook marker: %s", text)
	}
}

func TestNotifyOverloadWritesFailedAuditLogAndRedactsWebhook(t *testing.T) {
	ResetForTest()
	restoreLogger := logger.ResetForTest()
	t.Cleanup(restoreLogger)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"errcode":1,"errmsg":"failed"}`))
	}))
	defer server.Close()

	logRoot := t.TempDir()
	conf.Global = &conf.GlobalConfig{
		Env: "test",
		Models: map[string]conf.ModelConfig{
			"openai": {Enabled: true},
		},
	}
	conf.Global.Logs.RootDir = logRoot
	conf.Global.Alerts.DingTalk.Enabled = true
	conf.Global.Alerts.DingTalk.Webhook = server.URL + "?access_token=plain-secret"
	conf.Global.Alerts.DingTalk.CooldownSeconds = 0
	conf.InitModelConfig()

	err := NotifyOverload(context.Background(), OverloadEvent{
		Provider:       "openai",
		ActiveSessions: 100,
		MaxSessions:    100,
		Reason:         "capacity_rejected",
	})
	if err == nil || !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("NotifyOverload error = %v, want status 500", err)
	}
	logger.SyncAll()

	path := filepath.Join(logRoot, "openai", "openai-"+time.Now().Format("2006-01-02")+".log")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	text := string(data)
	for _, want := range []string{`"event": "dingtalk_failed"`, `"status": "failed"`, `"http_status": 500`, `"error": "dingtalk webhook status 500"`} {
		if !strings.Contains(text, want) {
			t.Fatalf("audit log = %s, want contains %s", text, want)
		}
	}
	if strings.Contains(text, "plain-secret") || strings.Contains(text, "access_token=plain-secret") {
		t.Fatalf("audit log leaked webhook token: %s", text)
	}
}

func TestNotifyOverloadMessageIncludesIPLocation(t *testing.T) {
	event := OverloadEvent{
		Provider:       "openai",
		UserID:         "user-1",
		UserName:       "张三",
		RemoteAddr:     "203.0.113.10",
		IPLocation:     map[string]string{"display": "中国 / 广东 / 深圳", "source": "request_header"},
		ActiveSessions: 100,
		MaxSessions:    100,
		Reason:         "capacity_rejected",
	}

	content := formatOverloadMessage(event)

	for _, want := range []string{"ip_location: 中国 / 广东 / 深圳", "ip_location_source: request_header"} {
		if !strings.Contains(content, want) {
			t.Fatalf("content = %q, want contains %q", content, want)
		}
	}
}

func TestNotifyOverloadDoesNotSendWhenDisabled(t *testing.T) {
	ResetForTest()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conf.Global = &conf.GlobalConfig{}
	conf.Global.Alerts.DingTalk.Enabled = false
	conf.Global.Alerts.DingTalk.Webhook = server.URL

	if err := NotifyOverload(context.Background(), OverloadEvent{Provider: "openai"}); err != nil {
		t.Fatalf("NotifyOverload returned error: %v", err)
	}
	if calls != 0 {
		t.Fatalf("server calls = %d, want 0", calls)
	}
}

func TestNotifyOverloadSuppressesDuringCooldown(t *testing.T) {
	ResetForTest()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	}))
	defer server.Close()

	conf.Global = &conf.GlobalConfig{}
	conf.Global.Alerts.DingTalk.Enabled = true
	conf.Global.Alerts.DingTalk.Webhook = server.URL
	conf.Global.Alerts.DingTalk.CooldownSeconds = 60

	event := OverloadEvent{Provider: "openai", ActiveSessions: 10, MaxSessions: 10, Reason: "capacity_rejected"}
	if err := NotifyOverload(context.Background(), event); err != nil {
		t.Fatalf("first NotifyOverload returned error: %v", err)
	}
	if err := NotifyOverload(context.Background(), event); err != nil {
		t.Fatalf("second NotifyOverload returned error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("server calls = %d, want 1", calls)
	}
}
