package openai

import (
	"testing"

	"TozoAI-Chat-Api/conf"
)

func TestGetProxyURLReturnsEmptyWhenUnset(t *testing.T) {
	cfg := NewOpenAIConfig(&conf.ModelConfig{})
	if got := cfg.GetProxyURL(); got != "" {
		t.Fatalf("expected empty proxy url, got %q", got)
	}
}

func TestGetProxyURLReturnsConfiguredValue(t *testing.T) {
	mc := &conf.ModelConfig{}
	mc.Realtime.ProxyURL = "http://127.0.0.1:7890"
	cfg := NewOpenAIConfig(mc)
	if got := cfg.GetProxyURL(); got != "http://127.0.0.1:7890" {
		t.Fatalf("expected http://127.0.0.1:7890, got %q", got)
	}
}

func TestGetProxyURLTrimsWhitespace(t *testing.T) {
	mc := &conf.ModelConfig{}
	mc.Realtime.ProxyURL = "  http://127.0.0.1:7890  "
	cfg := NewOpenAIConfig(mc)
	if got := cfg.GetProxyURL(); got != "http://127.0.0.1:7890" {
		t.Fatalf("expected trimmed value, got %q", got)
	}
}

func TestGetProxyURLOnNilSafe(t *testing.T) {
	var cfg *OpenAIConfig
	if got := cfg.GetProxyURL(); got != "" {
		t.Fatalf("expected empty proxy url on nil receiver, got %q", got)
	}
	cfg = &OpenAIConfig{} // ModelConfig nil
	if got := cfg.GetProxyURL(); got != "" {
		t.Fatalf("expected empty proxy url when ModelConfig is nil, got %q", got)
	}
}
