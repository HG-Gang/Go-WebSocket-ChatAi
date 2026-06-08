package logger

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"TozoAI-Chat-Api/conf"
)

func TestStartCleanupSchedulerRemovesExpiredDailyLogsAndStops(t *testing.T) {
	resetLoggerForTest(t)

	root := t.TempDir()
	modelDir := filepath.Join(root, "openai")
	if err := os.MkdirAll(modelDir, 0755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", modelDir, err)
	}

	oldPath := filepath.Join(modelDir, "openai-"+time.Now().AddDate(0, 0, -3).Format("2006-01-02")+".log")
	currentPath := filepath.Join(modelDir, "openai-"+time.Now().Format("2006-01-02")+".log")
	if err := os.WriteFile(oldPath, []byte("old"), 0644); err != nil {
		t.Fatalf("WriteFile old log error = %v", err)
	}
	if err := os.WriteFile(currentPath, []byte("current"), 0644); err != nil {
		t.Fatalf("WriteFile current log error = %v", err)
	}

	conf.Global = &conf.GlobalConfig{
		Env: "test",
		Models: map[string]conf.ModelConfig{
			"openai": {Enabled: true},
		},
	}
	conf.Global.Logs.RootDir = root
	conf.InitModelConfig()

	ctx, cancel := context.WithCancel(context.Background())
	done := StartCleanupScheduler(ctx, 1, 5*time.Millisecond)

	waitUntil(t, 200*time.Millisecond, func() bool {
		_, err := os.Stat(oldPath)
		return os.IsNotExist(err)
	})
	if _, err := os.Stat(currentPath); err != nil {
		t.Fatalf("current log should be kept, stat error = %v", err)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("cleanup scheduler did not stop after context cancellation")
	}
}

func TestCleanupSchedulerWritesAuditSummaryWithoutHoldingFile(t *testing.T) {
	resetLoggerForTest(t)

	root := t.TempDir()
	modelDir := filepath.Join(root, "openai")
	if err := os.MkdirAll(modelDir, 0755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", modelDir, err)
	}

	oldName := "openai-" + time.Now().AddDate(0, 0, -7).Format("2006-01-02") + ".log"
	oldPath := filepath.Join(modelDir, oldName)
	if err := os.WriteFile(oldPath, []byte("old"), 0644); err != nil {
		t.Fatalf("WriteFile old log error = %v", err)
	}

	conf.Global = &conf.GlobalConfig{
		Env: "test",
		Models: map[string]conf.ModelConfig{
			"openai": {Enabled: true},
		},
	}
	conf.Global.Logs.RootDir = root
	conf.InitModelConfig()

	ctx, cancel := context.WithCancel(context.Background())
	done := StartCleanupScheduler(ctx, 1, time.Hour)
	defer func() {
		cancel()
		<-done
	}()

	auditPath := filepath.Join(root, "audit", "audit-"+time.Now().Format("2006-01-02")+".log")
	waitUntil(t, 200*time.Millisecond, func() bool {
		data, err := os.ReadFile(auditPath)
		return err == nil &&
			strings.Contains(string(data), `"event":"log_cleanup"`) &&
			strings.Contains(string(data), `"deleted_count":1`) &&
			strings.Contains(string(data), filepath.ToSlash(filepath.Join("openai", oldName)))
	})
	data, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatalf("ReadFile audit log error = %v", err)
	}
	if strings.Contains(string(data), root) || strings.Contains(string(data), filepath.ToSlash(root)) {
		t.Fatalf("audit log should use relative paths and must not leak absolute root %q: %s", root, string(data))
	}
	if strings.Contains(string(data), `"root_dir"`) {
		t.Fatalf("audit log should not expose absolute root field: %s", string(data))
	}

	if err := os.Remove(auditPath); err != nil {
		t.Fatalf("audit log should not be held open after write, Remove(%q) error = %v", auditPath, err)
	}
}

func TestModelLoggerRotatesFileWhenDateChangesWhileLoggerIsHeld(t *testing.T) {
	resetLoggerForTest(t)

	root := t.TempDir()
	conf.Global = &conf.GlobalConfig{
		Env: "test",
		Models: map[string]conf.ModelConfig{
			"openai": {Enabled: true},
		},
	}
	conf.Global.Logs.RootDir = root
	conf.InitModelConfig()

	nowFunc = func() time.Time {
		return time.Date(2026, 6, 7, 23, 59, 59, 0, time.Local)
	}
	log := GetModelLogger("openai")
	log.Info("before midnight")

	nowFunc = func() time.Time {
		return time.Date(2026, 6, 8, 0, 0, 1, 0, time.Local)
	}
	log.Info("after midnight")
	SyncAll()

	firstPath := filepath.Join(root, "openai", "openai-2026-06-07.log")
	secondPath := filepath.Join(root, "openai", "openai-2026-06-08.log")
	first, err := os.ReadFile(firstPath)
	if err != nil {
		t.Fatalf("ReadFile first day log error = %v", err)
	}
	second, err := os.ReadFile(secondPath)
	if err != nil {
		t.Fatalf("ReadFile second day log error = %v", err)
	}
	if !strings.Contains(string(first), "before midnight") {
		t.Fatalf("first day log should contain before-midnight entry: %s", string(first))
	}
	if strings.Contains(string(first), "after midnight") {
		t.Fatalf("first day log should not contain after-midnight entry: %s", string(first))
	}
	if !strings.Contains(string(second), "after midnight") {
		t.Fatalf("second day log should contain after-midnight entry: %s", string(second))
	}
}

func TestRedactFieldMasksSensitiveLogValues(t *testing.T) {
	cases := []struct {
		name  string
		key   string
		value string
	}{
		{name: "api key", key: "api_key", value: "sk-test-secret-value"},
		{name: "jwt token", key: "token", value: "eyJhbGciOiJIUzI1Ni.secret"},
		{name: "webhook", key: "webhook", value: "https://oapi.dingtalk.com/robot/send?access_token=secret"},
		{name: "secret", key: "jwt_secret", value: "prod-secret"},
		{name: "redis value", key: "redis_value", value: "OPENAI_API_KEY=secret"},
		{name: "workspace content", key: "content", value: "package main\nconst key = \"secret\""},
		{name: "workspace diff", key: "diff", value: "+OPENAI_API_KEY=secret"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RedactField(tc.key, tc.value)
			if strings.Contains(got, tc.value) || strings.Contains(got, "secret") {
				t.Fatalf("RedactField(%q, %q) = %q, want no raw secret", tc.key, tc.value, got)
			}
			if !strings.Contains(got, "sha256:") {
				t.Fatalf("RedactField(%q, %q) = %q, want stable hash marker", tc.key, tc.value, got)
			}
		})
	}
}

func TestRedactFieldKeepsOrdinaryOperationalValues(t *testing.T) {
	for _, tc := range []struct {
		key   string
		value string
	}{
		{key: "model", value: "gpt-realtime"},
		{key: "status", value: "completed"},
		{key: "path", value: "internal/logger/logger.go"},
	} {
		if got := RedactField(tc.key, tc.value); got != tc.value {
			t.Fatalf("RedactField(%q, %q) = %q, want unchanged", tc.key, tc.value, got)
		}
	}
}

func resetLoggerForTest(t *testing.T) {
	t.Helper()

	oldGlobal := conf.Global
	restoreLogger := ResetForTest()

	t.Cleanup(func() {
		restoreLogger()
		conf.Global = oldGlobal
	})
}

func waitUntil(t *testing.T, timeout time.Duration, ok func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}
