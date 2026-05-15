package openai

import (
	"encoding/json"
	"strings"
	"testing"

	"TozoAI-Chat-Api/conf"
	protocol "TozoAI-Chat-Api/pkg/protocol/openai"
	"TozoAI-Chat-Api/pkg/response"
)

func TestGatewayAdapterPlansLegacyText(t *testing.T) {
	cfg := NewOpenAIConfig(&conf.ModelConfig{Instructions: "test instructions", Voice: "alloy"})
	adapter := newGatewayAdapter()

	plan, err := adapter.buildClientPlan([]byte(`{"msgType":"text","content":"hello","voice":"ash"}`), cfg, "session-1")
	if err != nil {
		t.Fatalf("buildClientPlan returned error: %v", err)
	}
	if len(plan.appMessages) != 0 {
		t.Fatalf("appMessages len = %d, want 0", len(plan.appMessages))
	}
	if len(plan.openAIEvents) != 3 {
		t.Fatalf("openAIEvents len = %d, want 3", len(plan.openAIEvents))
	}

	var sessionEvent map[string]any
	if err := json.Unmarshal(plan.openAIEvents[0], &sessionEvent); err != nil {
		t.Fatalf("decode session event: %v", err)
	}
	if sessionEvent["type"] != "session.update" {
		t.Fatalf("first event type = %v, want session.update", sessionEvent["type"])
	}

	var itemEvent struct {
		Type string `json:"type"`
		Item struct {
			Role    string `json:"role"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"item"`
	}
	if err := json.Unmarshal(plan.openAIEvents[1], &itemEvent); err != nil {
		t.Fatalf("decode conversation event: %v", err)
	}
	if itemEvent.Type != "conversation.item.create" || itemEvent.Item.Role != "user" {
		t.Fatalf("conversation event = %+v", itemEvent)
	}
	if got := itemEvent.Item.Content[0].Text; got != "hello" {
		t.Fatalf("content text = %q, want hello", got)
	}

	var createEvent struct {
		Type     string `json:"type"`
		Response struct {
			OutputModalities []string `json:"output_modalities"`
		} `json:"response"`
	}
	if err := json.Unmarshal(plan.openAIEvents[2], &createEvent); err != nil {
		t.Fatalf("decode response.create: %v", err)
	}
	if createEvent.Type != "response.create" || len(createEvent.Response.OutputModalities) != 1 || createEvent.Response.OutputModalities[0] != "audio" {
		t.Fatalf("response.create = %+v", createEvent)
	}
}

func TestGatewayAdapterInjectsLegacyToolsForTextCommand(t *testing.T) {
	cfg := NewOpenAIConfig(&conf.ModelConfig{Instructions: "test instructions", Voice: "alloy"})
	adapter := newGatewayAdapter()

	plan, err := adapter.buildClientPlan([]byte(`{"msgType":"text_command","content":"导航到机场","map_sdk":"mapbox"}`), cfg, "session-1")
	if err != nil {
		t.Fatalf("buildClientPlan returned error: %v", err)
	}
	if len(plan.openAIEvents) != 3 {
		t.Fatalf("openAIEvents len = %d, want 3", len(plan.openAIEvents))
	}

	var sessionEvent struct {
		Type    string `json:"type"`
		Session struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"session"`
	}
	if err := json.Unmarshal(plan.openAIEvents[0], &sessionEvent); err != nil {
		t.Fatalf("decode session.update: %v", err)
	}
	names := map[string]bool{}
	for _, tool := range sessionEvent.Session.Tools {
		names[tool.Name] = true
	}
	for _, name := range []string{"get_open_weather", "search_tozo_knowledge", "map_command_to_code", "get_specify_route_navigation", "get_nearby_route_navigation"} {
		if !names[name] {
			t.Fatalf("tool %s not injected, got %+v", name, names)
		}
	}
}

func TestBuildAzureRealtimeURLPreview(t *testing.T) {
	cfg := NewOpenAIConfig(&conf.ModelConfig{
		APIKey:   "key",
		Endpoint: "https://example.openai.azure.com",
		Extra: map[string]interface{}{
			"api_version":         "2024-10-21",
			"realtime_deployment": "rt-deploy",
		},
	})
	got, err := cfg.BuildRealtimeURL()
	if err != nil {
		t.Fatalf("BuildRealtimeURL returned error: %v", err)
	}
	if !strings.Contains(got, "wss://example.openai.azure.com/openai/realtime") {
		t.Fatalf("url path = %s, want Azure preview realtime path", got)
	}
	if !strings.Contains(got, "api-version=2024-10-21") || !strings.Contains(got, "deployment=rt-deploy") {
		t.Fatalf("url query = %s, want api-version and deployment", got)
	}
}

func TestGatewayAdapterPingPong(t *testing.T) {
	cfg := NewOpenAIConfig(&conf.ModelConfig{})
	plan, err := newGatewayAdapter().buildClientPlan([]byte(`{"type":"ping","client_id":"app-1"}`), cfg, "session-1")
	if err != nil {
		t.Fatalf("buildClientPlan returned error: %v", err)
	}
	if len(plan.openAIEvents) != 0 {
		t.Fatalf("openAIEvents len = %d, want 0", len(plan.openAIEvents))
	}
	if len(plan.appMessages) != 1 {
		t.Fatalf("appMessages len = %d, want 1", len(plan.appMessages))
	}
	var pong map[string]any
	if err := json.Unmarshal(plan.appMessages[0], &pong); err != nil {
		t.Fatalf("decode pong: %v", err)
	}
	if pong["type"] != "pong" || pong["client_id"] != "app-1" {
		t.Fatalf("pong = %+v", pong)
	}
}

func TestOpenAIResponseGateQueuesCreateUntilDone(t *testing.T) {
	gate := newOpenAIResponseGate()
	sent := make([]string, 0, 2)
	send := func(data []byte) error {
		sent = append(sent, string(data))
		return nil
	}

	if err := gate.sendClientEvent(string(protocol.ClientEventTypeResponseCreate), []byte(`{"type":"response.create","response":{"instructions":"first"}}`), "first", send); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if err := gate.sendClientEvent(string(protocol.ClientEventTypeResponseCreate), []byte(`{"type":"response.create","response":{"instructions":"second"}}`), "second", send); err != nil {
		t.Fatalf("second create: %v", err)
	}
	if len(sent) != 1 {
		t.Fatalf("sent len before done = %d, want 1", len(sent))
	}

	gate.onServerEvent(&protocol.ResponseCreatedEvent{
		ServerEventBase: protocol.ServerEventBase{Type: protocol.ServerEventTypeResponseCreated},
		Response:        protocol.Response{ID: "resp_1"},
	})
	flush := gate.onServerEvent(&protocol.ResponseDoneEvent{
		ServerEventBase: protocol.ServerEventBase{Type: protocol.ServerEventTypeResponseDone},
		Response:        protocol.Response{ID: "resp_1", Status: protocol.ResponseStatusCompleted},
	})
	if !flush {
		t.Fatalf("flush = false, want true")
	}
	if err := gate.flushPending(send); err != nil {
		t.Fatalf("flushPending: %v", err)
	}
	if len(sent) != 2 {
		t.Fatalf("sent len after flush = %d, want 2", len(sent))
	}
	if !strings.Contains(sent[1], "second") {
		t.Fatalf("flushed payload = %s, want second create", sent[1])
	}
}

func TestStandardResponseJSONIncludesLegacyAndSnakeResponseID(t *testing.T) {
	resp := response.NewResponseWithID(0, response.EventBegin, "resp_1", "", 1)
	data, err := resp.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got["responseId"] != "resp_1" || got["response_id"] != "resp_1" {
		t.Fatalf("response id fields = %+v", got)
	}
}
