package openai

import (
	"testing"

	protocol "TozoAI-Chat-Api/pkg/protocol/openai"
)

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
