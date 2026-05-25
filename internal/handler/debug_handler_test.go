package handler

import (
	"strings"
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
