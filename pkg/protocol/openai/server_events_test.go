// pkg/protocol/openai/server_events_test.go
// 文件功能：验证 UnmarshalServerEvent 对当前事件名与旧版预览事件名的类型分发——
// 新版事件名解析出对应新事件类型，旧版事件名（response.audio.delta 等）仍能
// 映射到同一结构体，保证升级过渡期透传语义不变。
package openai

import "testing"

func TestUnmarshalServerEvent_CurrentRealtimeEventNames(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want ServerEventType
	}{
		{
			name: "output text delta",
			raw:  `{"type":"response.output_text.delta","event_id":"evt_1","response_id":"resp_1","item_id":"item_1","output_index":0,"content_index":0,"delta":"hi"}`,
			want: ServerEventTypeResponseTextDelta,
		},
		{
			name: "output audio delta",
			raw:  `{"type":"response.output_audio.delta","event_id":"evt_1","response_id":"resp_1","item_id":"item_1","output_index":0,"content_index":0,"delta":"AAAA"}`,
			want: ServerEventTypeResponseAudioDelta,
		},
		{
			name: "output transcript delta",
			raw:  `{"type":"response.output_audio_transcript.delta","event_id":"evt_1","response_id":"resp_1","item_id":"item_1","output_index":0,"content_index":0,"delta":"hello"}`,
			want: ServerEventTypeResponseAudioTranscriptDelta,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evt, err := UnmarshalServerEvent([]byte(tt.raw))
			if err != nil {
				t.Fatalf("UnmarshalServerEvent() error = %v", err)
			}
			if got := evt.ServerEventType(); got != tt.want {
				t.Fatalf("ServerEventType() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestUnmarshalServerEvent_LegacyRealtimeEventNames(t *testing.T) {
	raw := `{"type":"response.audio.delta","event_id":"evt_1","response_id":"resp_1","item_id":"item_1","output_index":0,"content_index":0,"delta":"AAAA"}`
	evt, err := UnmarshalServerEvent([]byte(raw))
	if err != nil {
		t.Fatalf("UnmarshalServerEvent() error = %v", err)
	}
	if got := evt.ServerEventType(); got != ServerEventTypeLegacyResponseAudioDelta {
		t.Fatalf("ServerEventType() = %q, want %q", got, ServerEventTypeLegacyResponseAudioDelta)
	}
	if _, ok := evt.(*ResponseAudioDeltaEvent); !ok {
		t.Fatalf("event type = %T, want *ResponseAudioDeltaEvent", evt)
	}
}
