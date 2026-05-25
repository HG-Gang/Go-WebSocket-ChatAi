package handler

import (
	"testing"

	"TozoAI-Chat-Api/conf"
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
