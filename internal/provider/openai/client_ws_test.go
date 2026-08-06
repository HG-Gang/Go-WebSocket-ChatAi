// internal/provider/openai/client_ws_test.go
// 文件功能：验证 Realtime WS 客户端的核心行为——重放状态缓存只收录可安全重放的事件、
// App 下行队列满时的关键/非关键事件策略、上游 Ping 保活、token 用量转换与统一统计、
// Close 幂等性。
// 测试不依赖真实 OpenAI 服务：队列类测试使用最小 Client 桩，Ping 测试使用本地 httptest WS server。
package openai

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/service/stats"
	protocol "TozoAI-Chat-Api/pkg/protocol/openai"
	"TozoAI-Chat-Api/pkg/response"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// 会话恢复只缓存可安全重放的事件。
// response.create 和音频 append 不应进入恢复历史，避免上游重连后重复生成响应或重放大音频帧。
func TestReplayStateCachesOnlyRecoverableEvents(t *testing.T) {
	state := newReplayState(2)
	state.remember(string(protocol.ClientEventTypeSessionUpdate), []byte(`{"type":"session.update","session":{"instructions":"one"}}`))
	state.remember(string(protocol.ClientEventTypeConversationItemCreate), []byte(`{"type":"conversation.item.create","item":{"type":"message","role":"user"}}`))
	state.remember(string(protocol.ClientEventTypeResponseCreate), []byte(`{"type":"response.create"}`))
	state.remember(string(protocol.ClientEventTypeConversationItemDelete), []byte(`{"type":"conversation.item.delete","item_id":"old"}`))
	state.remember(string(protocol.ClientEventTypeInputAudioBufferAppend), []byte(`{"type":"input_audio_buffer.append","audio":"AAAA"}`))

	session, history := state.snapshot()
	if string(session) != `{"type":"session.update","session":{"instructions":"one"}}` {
		t.Fatalf("session snapshot = %s", string(session))
	}
	if len(history) != 2 {
		t.Fatalf("history len = %d, want 2", len(history))
	}
	if string(history[0]) != `{"type":"conversation.item.create","item":{"type":"message","role":"user"}}` {
		t.Fatalf("history[0] = %s", string(history[0]))
	}
	if string(history[1]) != `{"type":"conversation.item.delete","item_id":"old"}` {
		t.Fatalf("history[1] = %s", string(history[1]))
	}
}

// 关键 App 下行事件不能像普通 delta 一样静默丢弃。
// 队列满时返回错误，调用方才能记录指标并让用户知道需要重连或本次响应失败。
func TestSendAppEventReturnsErrorForCriticalEventWhenQueueFull(t *testing.T) {
	client := newClientForQueueTest()
	client.sendChan <- []byte(`{"response":"existing"}`)

	err := client.sendAppEvent([]byte(`{"response":"reconnect_required"}`), appEventPolicy{
		eventType: string(response.EventReconnectRequired),
		critical:  true,
	})

	if err == nil {
		t.Fatal("critical app event queue timeout returned nil error")
	}
	if got := len(client.sendChan); got != 1 {
		t.Fatalf("sendChan len = %d, want 1", got)
	}
}

// 普通流式 delta 属于 best-effort 事件。
// 慢客户端队列满时可以丢弃它，避免单个客户端拖垮服务端内存。
func TestSendAppEventDropsBestEffortEventWhenQueueFull(t *testing.T) {
	client := newClientForQueueTest()
	client.sendChan <- []byte(`{"response":"existing"}`)

	err := client.sendAppEvent([]byte(`{"response":"text_delta"}`), appEventPolicy{
		eventType: string(response.EventTextDelta),
		critical:  false,
	})

	if err != nil {
		t.Fatalf("best-effort app event returned error: %v", err)
	}
	if got := len(client.sendChan); got != 1 {
		t.Fatalf("sendChan len = %d, want 1", got)
	}
}

// reconnect_required 这类关键网关响应如果无法发送，forwardToApp 必须向上返回错误。
// 这个测试防止重连失败被吞掉，导致页面一直等待但没有任何错误信号。
func TestForwardToAppReportsCriticalQueueFailure(t *testing.T) {
	client := newClientForQueueTest()
	client.sendChan <- []byte(`{"response":"existing"}`)

	err := client.forwardToApp(response.NewResponse(500, response.EventReconnectRequired, "OpenAI reconnect failed"))

	if err == nil {
		t.Fatal("forwardToApp returned nil for dropped reconnect_required event")
	}
}

// OpenAI 写泵使用 WebSocket Ping 保活上游连接。
// 这里用本地 server 捕获 Ping，证明不是普通文本消息或空写。
func TestWriteOpenAIPingSendsWebSocketPing(t *testing.T) {
	pingReceived := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Upgrade(w, r, nil, 1024, 1024)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer conn.Close()

		conn.SetPingHandler(func(appData string) error {
			pingReceived <- struct{}{}
			return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(time.Second))
		})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + server.URL[len("http"):]
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	client := newClientForQueueTest()
	client.apiConn = conn

	if err := client.writeOpenAIPing(); err != nil {
		t.Fatalf("writeOpenAIPing returned error: %v", err)
	}

	select {
	case <-pingReceived:
	case <-time.After(time.Second):
		t.Fatal("OpenAI server did not receive ping")
	}
}

// metricsUsageFromProtocol 必须把输入/输出两侧的 cached 与 reasoning token 合并计数，
// 且与计费口径一致；这里锁住"总数不变、明细不丢"的转换行为。
func TestMetricsUsageFromProtocolIncludesCachedAndReasoningTokens(t *testing.T) {
	usage := metricsUsageFromProtocol(&protocol.Usage{
		InputTokens:  100,
		OutputTokens: 50,
		TotalTokens:  150,
		InputTokenDetails: &protocol.TokenUsageDetails{
			CachedTokens:    30,
			ReasoningTokens: 3,
		},
		OutputTokenDetails: &protocol.TokenUsageDetails{
			CachedTokens:    4,
			ReasoningTokens: 9,
		},
	})

	if usage.InputTokens != 100 {
		t.Fatalf("InputTokens = %d, want 100", usage.InputTokens)
	}
	if usage.OutputTokens != 50 {
		t.Fatalf("OutputTokens = %d, want 50", usage.OutputTokens)
	}
	if usage.TotalTokens != 150 {
		t.Fatalf("TotalTokens = %d, want 150", usage.TotalTokens)
	}
	if usage.CachedTokens != 34 {
		t.Fatalf("CachedTokens = %d, want 34", usage.CachedTokens)
	}
	if usage.ReasoningTokens != 12 {
		t.Fatalf("ReasoningTokens = %d, want 12", usage.ReasoningTokens)
	}
}

func TestResponseDoneRecordsUnifiedStatsUsage(t *testing.T) {
	stats.ResetForTest()
	t.Cleanup(stats.ResetForTest)
	client := newClientForQueueTest()
	client.providerName = "openai"
	client.userID = "user-1"
	client.sessionID = "session-1"

	client.handleOpenAIMessageGateway([]byte(`{
		"type": "response.done",
		"response": {
			"id": "resp_1",
			"object": "realtime.response",
			"status": "completed",
			"output": [],
			"usage": {
				"input_tokens": 100,
				"output_tokens": 50,
				"total_tokens": 150,
				"input_token_details": {"cached_tokens": 20},
				"output_token_details": {"reasoning_tokens": 5}
			}
		}
	}`))

	day := stats.ResourcePeriods(time.Now())["day"].Summary
	if day.Requests != 1 || day.TotalTokens != 150 || day.CachedTokens != 20 || day.ReasoningTokens != 5 {
		t.Fatalf("stats day summary = %+v, want Realtime response.done usage recorded", day)
	}
	if day.BySource[stats.SourceRealtime] != 1 {
		t.Fatalf("stats BySource = %+v, want realtime source", day.BySource)
	}
}

// Close 可能被 read/write pump、handler defer 或测试清理重复调用；
// 幂等关闭并清空 apiConn 指针能避免 nil panic 与 gorilla 连接残留。
func TestCloseIsIdempotentAndClearsRealtimeConnections(t *testing.T) {
	client := newClientForQueueTest()

	if err := client.Close(); err != nil {
		t.Fatalf("first Close returned error: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
	if client.apiConn != nil {
		t.Fatal("apiConn was not cleared")
	}
}

// newClientForQueueTest 构造最小 Client，只保留队列、gateway、replay 等被测依赖。
// 这样队列语义测试不需要真实 OpenAI 连接，也不会触碰用户本地服务。
func newClientForQueueTest() *Client {
	cfg := &conf.ModelConfig{}
	cfg.Realtime.SendQueueTimeoutMs = 1
	cfg.Realtime.ApiWriteTimeout = "1s"
	return &Client{
		cfg:              NewOpenAIConfig(cfg),
		log:              zap.NewNop(),
		sendChan:         make(chan []byte, 1),
		apiSendChan:      make(chan openAIOutbound, 1),
		apiReconnectChan: make(chan reconnectRequest, 1),
		replay:           newReplayState(1),
		gateway:          newGatewayAdapter(),
		respGate:         newOpenAIResponseGate(),
	}
}

// openAIWritePump 必须按配置周期发送上游 Ping。
// 这个测试防止后续重构误删 api_ping_interval，导致长连接依赖读超时才发现半开。
func TestOpenAIWritePumpSendsConfiguredPing(t *testing.T) {
	client := newClientForQueueTest()
	client.cfg.Realtime.ApiPingInterval = "1ms"

	pingSent := make(chan struct{}, 1)
	client.writeOpenAIPingFunc = func() error {
		select {
		case pingSent <- struct{}{}:
		default:
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go client.openAIWritePump(ctx, cancel, &wg)

	select {
	case <-pingSent:
	case <-time.After(time.Second):
		t.Fatal("openAIWritePump did not send configured OpenAI ping")
	}

	cancel()
	wg.Wait()
}

// 关键/非关键事件策略集中由响应类型决定。
// 这里锁住 reconnect/error/end 必须关键，delta/audio 可以 best-effort 的生产背压语义。
func TestCriticalAppEventPolicyFromResponse(t *testing.T) {
	cases := []struct {
		name     string
		event    response.ResponseEvent
		critical bool
	}{
		{name: "reconnect required", event: response.EventReconnectRequired, critical: true},
		{name: "error", event: response.EventError, critical: true},
		{name: "end", event: response.EventEnd, critical: true},
		{name: "text delta", event: response.EventTextDelta, critical: false},
		{name: "audio delta", event: response.EventAudioDelta, critical: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := response.NewResponse(0, tc.event, "").ToJSON()
			if err != nil {
				t.Fatalf("marshal response: %v", err)
			}
			var raw map[string]json.RawMessage
			if err := json.Unmarshal(payload, &raw); err != nil {
				t.Fatalf("unmarshal response payload: %v", err)
			}

			policy := appEventPolicyFromResponse(raw)

			if policy.critical != tc.critical {
				t.Fatalf("critical = %v, want %v", policy.critical, tc.critical)
			}
			if policy.eventType != string(tc.event) {
				t.Fatalf("eventType = %q, want %q", policy.eventType, string(tc.event))
			}
		})
	}
}
