// internal/handler/debug_handler_test.go
// 调试诊断接口的单元测试。
//
// 测试范围：
//   - resolveProxy: Realtime 拨号代理优先级（config > env > 直连）与空白处理。
//   - maskAPIKey: 空值、短值、OpenAI project/classic、Azure hex 各风格的脱敏结果。
//   - DebugStatusHandler: 统一 monitor 快照结构、字段口径与日志节流（轮询场景不刷日志）。
//
// 注意：API Key 等敏感字段在测试中只使用伪造值，仅验证脱敏形状，不校验真实密钥。
package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/service/monitor"

	"github.com/gin-gonic/gin"
)

func TestResolveProxyPrefersConfigOverEnv(t *testing.T) {
	mc := &conf.ModelConfig{}
	mc.Realtime.ProxyURL = "http://10.0.0.1:8080"
	got, source := resolveProxy(mc, "http://env-proxy:7890")
	if got != "http://10.0.0.1:8080" || source != "config" {
		t.Fatalf("expected config wins, got (%q, %q)", got, source)
	}
}

func TestResolveProxyFallsBackToEnv(t *testing.T) {
	mc := &conf.ModelConfig{}
	got, source := resolveProxy(mc, "http://env-proxy:7890")
	if got != "http://env-proxy:7890" || source != "env" {
		t.Fatalf("expected env fallback, got (%q, %q)", got, source)
	}
}

func TestResolveProxyReturnsNoneWhenUnset(t *testing.T) {
	got, source := resolveProxy(nil, "")
	if got != "" || source != "none" {
		t.Fatalf("expected (\"\", \"none\"), got (%q, %q)", got, source)
	}
}

func TestResolveProxyTrimsWhitespace(t *testing.T) {
	mc := &conf.ModelConfig{}
	mc.Realtime.ProxyURL = "   "
	got, source := resolveProxy(mc, "  http://env:1080  ")
	if got != "http://env:1080" || source != "env" {
		t.Fatalf("expected whitespace-only config to fall through to env, got (%q, %q)", got, source)
	}
}

func TestMaskAPIKeyEmpty(t *testing.T) {
	if got := maskAPIKey(""); got != "未配置" {
		t.Fatalf("expected 未配置, got %q", got)
	}
	if got := maskAPIKey("   "); got != "未配置" {
		t.Fatalf("expected 未配置 for whitespace, got %q", got)
	}
}

func TestMaskAPIKeyShortValue(t *testing.T) {
	got := maskAPIKey("abc12345")
	if !strings.HasPrefix(got, "***") {
		t.Fatalf("expected ***-prefixed mask for short key, got %q", got)
	}
	if !strings.Contains(got, "长度=8") {
		t.Fatalf("expected length info in mask, got %q", got)
	}
}

func TestMaskAPIKeyOpenAIProjStyle(t *testing.T) {
	// 长 OpenAI project key 应保留 sk-proj- 前缀和末 4 字符。
	key := "sk-proj-1234567890abcdefghijklmnopABCDEFGHIJKLMNOP1234"
	got := maskAPIKey(key)
	if !strings.HasPrefix(got, "sk-proj-...") {
		t.Fatalf("expected sk-proj-... prefix, got %q", got)
	}
	if !strings.Contains(got, "1234（长度=") {
		t.Fatalf("expected last-4 chars retained, got %q", got)
	}
	// 不能泄漏中间部分。
	if strings.Contains(got, "abcdef") || strings.Contains(got, "ABCDEFGHIJKL") {
		t.Fatalf("mask leaked middle content: %q", got)
	}
}

func TestMaskAPIKeyOpenAIClassicStyle(t *testing.T) {
	key := "sk-abcdefghijklmnopqrstuvwxyz12345678"
	got := maskAPIKey(key)
	if !strings.HasPrefix(got, "sk-...") {
		t.Fatalf("expected sk-... prefix, got %q", got)
	}
	if !strings.Contains(got, "5678") {
		t.Fatalf("expected last-4 chars in mask, got %q", got)
	}
}

func TestMaskAPIKeyAzureHexStyle(t *testing.T) {
	// Azure 一般是 32 字符 hex；走 fallback 前 4 + ... + 末 4。
	key := "0123456789abcdef0123456789abcdef"
	got := maskAPIKey(key)
	if !strings.HasPrefix(got, "0123...") {
		t.Fatalf("expected first-4-then-ellipsis fallback, got %q", got)
	}
	if !strings.Contains(got, "cdef") {
		t.Fatalf("expected last-4 chars in mask, got %q", got)
	}
}

func TestBuildDebugProcessStatusIncludesProcessIdentity(t *testing.T) {
	status := buildDebugProcessStatus()

	if got := status["pid"]; got != os.Getpid() {
		t.Fatalf("pid = %v, want %d", got, os.Getpid())
	}
	if got := status["executable"]; got == "" {
		t.Fatal("executable is empty")
	}
	if got := status["working_dir"]; got == "" {
		t.Fatal("working_dir is empty")
	}
	if got := status["goroutines"]; got == nil {
		t.Fatal("goroutines is missing")
	}
}

func TestDebugStatusHandlerReturnsUnifiedMonitorSnapshot(t *testing.T) {
	// 诊断接口必须返回统一 monitor 快照，避免前端、日志和 handler 各自维护不同字段口径。
	monitor.ResetForTest()
	old := conf.Global
	t.Cleanup(func() { conf.Global = old })
	conf.Global = &conf.GlobalConfig{}
	conf.Global.Env = "test"
	conf.Global.JWT.Enabled = true
	conf.Global.JWT.Secret = "test-secret"
	conf.Global.RateLimit.Enabled = true
	conf.Global.Billing.Enabled = true
	conf.Global.Capacity.MaxActiveSessions = 100
	conf.Global.Redis.Enabled = false
	conf.Global.Models = map[string]conf.ModelConfig{
		"openai": {
			Enabled:  true,
			APIKey:   "sk-test-openai",
			Endpoint: "https://api.openai.com/v1",
		},
		"openairesponses": {
			Enabled:  true,
			APIKey:   "sk-test-responses",
			Endpoint: "https://api.openai.com/v1/responses",
		},
		"azureai": {
			Enabled:  true,
			APIKey:   "azure-key",
			Endpoint: "https://example.openai.azure.com",
		},
	}
	conf.InitModelConfig()

	payload := requestDebugStatus(t)
	data := payload["data"].(map[string]any)
	monitorPayload, ok := data["monitor"].(map[string]any)
	if !ok {
		t.Fatalf("monitor payload missing or wrong type: %#v", data["monitor"])
	}

	process := monitorPayload["process"].(map[string]any)
	if process["pid"].(float64) <= 0 {
		t.Fatalf("monitor.process.pid = %v, want positive", process["pid"])
	}
	if process["resource_source"] == "" {
		t.Fatal("monitor.process.resource_source is empty")
	}
	if process["socket_source"] == "" {
		t.Fatal("monitor.process.socket_source is empty")
	}
	if runtime.GOOS == "windows" && process["handle_count"].(float64) < 0 {
		t.Fatalf("monitor.process.handle_count = %v, want Windows handle count", process["handle_count"])
	}
	if runtime.GOOS != "windows" && process["fd_count"].(float64) < 0 {
		t.Fatalf("monitor.process.fd_count = %v, want Unix fd count", process["fd_count"])
	}

	modules := monitorPayload["modules"].(map[string]any)
	for _, name := range []string{"redis", "openai", "responses", "azure", "billing", "rate_limit", "jwt", "capacity"} {
		if _, ok := modules[name]; !ok {
			t.Fatalf("monitor.modules[%s] missing in %#v", name, modules)
		}
	}

	processTopLevel := data["process"].(map[string]any)
	if processTopLevel["resource_source"] == "" {
		t.Fatal("top-level process.resource_source is empty")
	}

	routes := data["routes"].([]any)
	hasStatsResources := false
	for _, item := range routes {
		route := item.(map[string]any)
		if route["path"] == "/api/stats/resources" {
			hasStatsResources = true
			break
		}
	}
	if !hasStatsResources {
		t.Fatalf("routes = %+v, want /api/stats/resources", routes)
	}
}

func TestDebugStatusHandlerThrottlesMonitorSnapshotLogs(t *testing.T) {
	// 诊断页默认会轮询 /api/debug/status，日志落点必须节流，避免 3 秒轮询刷爆当天日志。
	monitor.ResetForTest()
	old := conf.Global
	t.Cleanup(func() { conf.Global = old })
	conf.Global = &conf.GlobalConfig{}
	conf.Global.Env = "test"

	var calls int
	restoreSink := monitor.SetLogSinkForTest(func(snapshot monitor.Snapshot) {
		calls++
	})
	defer restoreSink()

	now := time.Date(2026, 6, 7, 10, 0, 0, 0, time.UTC)
	restoreNow := monitor.SetNowForTest(func() time.Time {
		return now
	})
	defer restoreNow()

	_ = requestDebugStatus(t)
	_ = requestDebugStatus(t)
	if calls != 1 {
		t.Fatalf("monitor log calls = %d, want 1 throttled write", calls)
	}
}

func requestDebugStatus(t *testing.T) map[string]any {
	t.Helper()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/api/debug/status", DebugStatusHandler)

	req := httptest.NewRequest(http.MethodGet, "/api/debug/status", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body = %s", w.Code, w.Body.String())
	}

	var payload map[string]any
	if err := json.NewDecoder(w.Body).Decode(&payload); err != nil {
		t.Fatalf("decode debug status: %v", err)
	}
	return payload
}
