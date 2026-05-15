package conf

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAppliesRootLevelModelOverride(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error = %v", err)
	}
	repoRoot := filepath.Dir(wd)
	if err := os.Chdir(repoRoot); err != nil {
		t.Fatalf("Chdir(%q) error = %v", repoRoot, err)
	}
	defer func() {
		if err := os.Chdir(wd); err != nil {
			t.Fatalf("restore Chdir(%q) error = %v", wd, err)
		}
	}()

	t.Setenv("OPENAI_API_KEY", "test-key")
	if err := Load(); err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	cfg := GetModel("openai")
	if cfg.DefaultModel != "gpt-realtime" {
		t.Fatalf("DefaultModel = %q, want %q", cfg.DefaultModel, "gpt-realtime")
	}
	if cfg.APIKey != "test-key" {
		t.Fatalf("APIKey = %q, want env override", cfg.APIKey)
	}
	if cfg.Endpoint != "https://api.openai.com/v1" {
		t.Fatalf("Endpoint = %q, want base config value preserved", cfg.Endpoint)
	}
	if cfg.Realtime.ApiPingInterval != "30s" {
		t.Fatalf("ApiPingInterval = %q, want %q", cfg.Realtime.ApiPingInterval, "30s")
	}
	if cfg.Instructions == "" {
		t.Fatalf("Instructions should be loaded from root config override")
	}
	if cfg.Voice != "alloy" {
		t.Fatalf("Voice = %q, want %q", cfg.Voice, "alloy")
	}
	if cfg.RateRPS != 10 {
		t.Fatalf("RateRPS = %d, want %d", cfg.RateRPS, 10)
	}
}
