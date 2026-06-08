package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/service/alert"
	"TozoAI-Chat-Api/internal/service/session"
)

// 上游 query 覆盖只允许在明确配置允许时使用。
// 这个测试锁定第三方中转 URL、Key、模型都能进入单连接临时配置，方便开发调试但不改全局配置。
func TestRealtimeConfigFromQueryOverridesUpstreamRealtimeConfig(t *testing.T) {
	conf.Global = &conf.GlobalConfig{}
	conf.Global.Env = "dev"
	conf.Global.Security.AllowUpstreamQueryKey = true
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req := httptest.NewRequest(http.MethodGet, "/ws/realtime/openai?upstream_ws_url=wss%3A%2F%2Frelay.example.com%2Fv1%2Frealtime&upstream_api_key=relay-key&upstream_model=relay-model", nil)
	c.Request = req

	base := &conf.ModelConfig{
		Enabled:      false,
		APIKey:       "base-key",
		DefaultModel: "base-model",
	}
	base.Realtime.WsUrl = "wss://api.openai.com/v1/realtime"

	cfg, err := realtimeConfigFromQuery(c, base)
	if err != nil {
		t.Fatalf("realtimeConfigFromQuery returned error: %v", err)
	}
	if !cfg.Enabled {
		t.Fatalf("Enabled = false, want true when upstream override is present")
	}
	if cfg.Realtime.WsUrl != "wss://relay.example.com/v1/realtime" {
		t.Fatalf("WsUrl = %q, want relay URL", cfg.Realtime.WsUrl)
	}
	if cfg.APIKey != "relay-key" {
		t.Fatalf("APIKey = %q, want relay-key", cfg.APIKey)
	}
	if cfg.DefaultModel != "relay-model" {
		t.Fatalf("DefaultModel = %q, want relay-model", cfg.DefaultModel)
	}
}

// 开发者常填 HTTP base URL，这里验证服务端会转换为 Realtime WebSocket URL。
// 这样 index/chat 页面可以填写第三方中转的 HTTPS 地址，不必手写完整 wss realtime 路径。
func TestRealtimeConfigFromQueryConvertsHTTPUpstreamURLToWebSocketURL(t *testing.T) {
	conf.Global = &conf.GlobalConfig{}
	conf.Global.Env = "dev"
	conf.Global.Security.AllowUpstreamQueryKey = true
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req := httptest.NewRequest(http.MethodGet, "/ws/realtime/openai?upstream_ws_url=https%3A%2F%2Frelay.example.com%2Fv1&upstream_api_key=relay-key", nil)
	c.Request = req

	cfg, err := realtimeConfigFromQuery(c, &conf.ModelConfig{Enabled: true, APIKey: "base-key"})
	if err != nil {
		t.Fatalf("realtimeConfigFromQuery returned error: %v", err)
	}
	if cfg.Realtime.WsUrl != "wss://relay.example.com/v1/realtime" {
		t.Fatalf("WsUrl = %q, want converted wss realtime URL", cfg.Realtime.WsUrl)
	}
}

// 非 HTTP/WS scheme 必须拒绝。
// 这个测试防止把 ftp/file 等非上游地址带入 WebSocket dial，扩大 SSRF 或本地文件风险。
func TestRealtimeConfigFromQueryRejectsInvalidUpstreamURL(t *testing.T) {
	conf.Global = &conf.GlobalConfig{}
	conf.Global.Env = "dev"
	conf.Global.Security.AllowUpstreamQueryKey = true
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req := httptest.NewRequest(http.MethodGet, "/ws/realtime/openai?upstream_ws_url=ftp%3A%2F%2Frelay.example.com%2Fv1%2Frealtime&upstream_api_key=relay-key", nil)
	c.Request = req

	_, err := realtimeConfigFromQuery(c, &conf.ModelConfig{Enabled: true, APIKey: "base-key"})
	if err == nil {
		t.Fatalf("realtimeConfigFromQuery error = nil, want invalid URL error")
	}
}

// 生产默认不允许通过 query 传上游 API Key。
// 这个测试防止中转 key 出现在浏览器历史、代理日志或 Referer 后仍被服务端接受。
func TestRealtimeConfigFromQueryRejectsUpstreamKeyWhenDisabled(t *testing.T) {
	conf.Global = &conf.GlobalConfig{}
	conf.Global.Env = "prod"
	conf.Global.Security.AllowUpstreamQueryKey = false

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req := httptest.NewRequest(http.MethodGet, "/ws/realtime/openai?upstream_api_key=relay-key", nil)
	c.Request = req

	_, err := realtimeConfigFromQuery(c, &conf.ModelConfig{Enabled: true, APIKey: "base-key"})
	if err == nil || !strings.Contains(err.Error(), "upstream api key query override is disabled") {
		t.Fatalf("realtimeConfigFromQuery error = %v, want disabled query key error", err)
	}
}

// Realtime Origin 校验必须使用配置白名单。
// 这个测试防止生产环境被任意站点发起 WebSocket 跨站连接。
func TestRealtimeOriginUsesConfiguredAllowedOrigins(t *testing.T) {
	conf.Global = &conf.GlobalConfig{}
	conf.Global.Env = "prod"
	conf.Global.Security.AllowedOrigins = []string{"https://app.example.com"}

	req := httptest.NewRequest(http.MethodGet, "/ws/realtime/openai", nil)
	req.Header.Set("Origin", "https://evil.example.com")
	if checkRealtimeOrigin(req) {
		t.Fatalf("unexpected origin allowed")
	}

	req.Header.Set("Origin", "https://app.example.com")
	if !checkRealtimeOrigin(req) {
		t.Fatalf("configured origin should be allowed")
	}
}

// 容量拒绝不仅要返回 503，还要触发钉钉过载告警。
// 这个测试锁定告警内容包含 provider、用户、容量水位和 capacity_rejected 原因。
func TestOpenAIRealtimeOverloadSendsDingTalkAlert(t *testing.T) {
	alert.ResetForTest()
	resetCapacityForHandlerTest()
	t.Cleanup(resetCapacityForHandlerTest)

	var received map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Fatalf("decode dingtalk body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	}))
	defer server.Close()

	conf.Global = &conf.GlobalConfig{}
	conf.Global.Env = "test"
	conf.Global.Logs.RootDir = filepath.Join(".tmp", "handler-alert-test-"+time.Now().Format("20060102150405.000000000"))
	conf.Global.Capacity.MaxActiveSessions = 1
	conf.Global.Alerts.DingTalk.Enabled = true
	conf.Global.Alerts.DingTalk.Webhook = server.URL
	conf.Global.Alerts.DingTalk.TimeoutMs = 1000

	if !session.TryAcquireCapacity() {
		t.Fatal("failed to pre-fill capacity")
	}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/ws/realtime/openai", func(c *gin.Context) {
		c.Set("user_id", "user-1")
		c.Set("user_name", "张三")
		OpenAIRealtimeHandler(c)
	})

	req := httptest.NewRequest(http.MethodGet, "/ws/realtime/openai", nil)
	req.RemoteAddr = "203.0.113.10:34567"
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	if received == nil {
		t.Fatal("dingtalk server did not receive overload alert")
	}
	text, _ := received["text"].(map[string]any)
	content, _ := text["content"].(string)
	for _, want := range []string{"系统过载预警", "openai", "user-1", "张三", "1/1", "capacity_rejected"} {
		if !strings.Contains(content, want) {
			t.Fatalf("alert content = %q, want contains %q", content, want)
		}
	}
}

// resetCapacityForHandlerTest 清空单进程容量计数。
// handler 测试会主动占用容量，清理不彻底会污染后续 Realtime 准入测试。
func resetCapacityForHandlerTest() {
	for session.ActiveCount() > 0 {
		session.ReleaseCapacity()
	}
}
