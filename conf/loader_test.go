// conf/loader_test.go
// 文件功能：验证配置加载顺序与覆盖规则（根配置覆盖模型文件默认值、环境变量优先于
// 模型文件中的 api_key）、生产配置失败关闭校验，以及模型配置缓存命中/未命中的统计口径。
// 测试依赖仓库根目录下真实的 conf/*.yaml 配置，执行前会 chdir 到仓库根目录。
package conf

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"TozoAI-Chat-Api/internal/service/stats"
)

// 根配置中的 models.openai 会覆盖 conf/models/openai.yaml 的默认值，
// 同时环境变量 OPENAI_API_KEY 必须优先覆盖 YAML，防止生产 key 被旧配置污染。
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
	if cfg.Endpoint != "https://dxb.huifei.net/v1" {
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
	if Global.Alerts.DingTalk.Enabled {
		t.Fatalf("Alerts.DingTalk.Enabled = true, want false by default")
	}
	if Global.Alerts.DingTalk.Webhook != "" {
		t.Fatalf("Alerts.DingTalk.Webhook = %q, want base config webhook", Global.Alerts.DingTalk.Webhook)
	}
	if Global.Alerts.DingTalk.CooldownSeconds != 300 {
		t.Fatalf("Alerts.DingTalk.CooldownSeconds = %d, want 300", Global.Alerts.DingTalk.CooldownSeconds)
	}
	if Global.Alerts.DingTalk.TimeoutMs != 3000 {
		t.Fatalf("Alerts.DingTalk.TimeoutMs = %d, want 3000", Global.Alerts.DingTalk.TimeoutMs)
	}
	if Global.Logs.RetentionDays != 30 {
		t.Fatalf("Logs.RetentionDays = %d, want 30", Global.Logs.RetentionDays)
	}
	if Global.Logs.CleanupInterval != "24h" {
		t.Fatalf("Logs.CleanupInterval = %q, want 24h", Global.Logs.CleanupInterval)
	}
}

// 生产配置校验测试用于防止线上空 JWT 密钥、公开调试接口和缺失 Origin 白名单静默启动。
func TestValidateProductionRejectsEmptyJWTSecret(t *testing.T) {
	cfg := &GlobalConfig{}
	cfg.Env = "prod"
	cfg.JWT.Enabled = true
	cfg.JWT.Secret = ""

	err := validateProductionConfig(cfg)
	if err == nil || !strings.Contains(err.Error(), "jwt.secret") {
		t.Fatalf("validateProductionConfig error = %v, want jwt.secret error", err)
	}
}

func TestValidateProductionRejectsPublicDebugAndTokenRoutes(t *testing.T) {
	cfg := &GlobalConfig{}
	cfg.Env = "prod"
	cfg.JWT.Enabled = true
	cfg.JWT.Secret = "prod-secret"
	cfg.Security.AllowedOrigins = []string{"https://app.example.com"}

	cfg.Security.PublicTokenEnabled = true
	if err := validateProductionConfig(cfg); err == nil || !strings.Contains(err.Error(), "public_token_enabled") {
		t.Fatalf("public token validation error = %v, want public_token_enabled error", err)
	}

	cfg.Security.PublicTokenEnabled = false
	cfg.Security.PublicDebugEnabled = true
	if err := validateProductionConfig(cfg); err == nil || !strings.Contains(err.Error(), "public_debug_enabled") {
		t.Fatalf("public debug validation error = %v, want public_debug_enabled error", err)
	}
}

func TestValidateProductionRequiresAllowedOrigins(t *testing.T) {
	cfg := &GlobalConfig{}
	cfg.Env = "prod"
	cfg.JWT.Enabled = true
	cfg.JWT.Secret = "prod-secret"

	err := validateProductionConfig(cfg)
	if err == nil || !strings.Contains(err.Error(), "allowed_origins") {
		t.Fatalf("validateProductionConfig error = %v, want allowed_origins error", err)
	}
}

func TestGetModelRecordsBusinessCacheHitAndMiss(t *testing.T) {
	stats.ResetForTest()
	t.Cleanup(stats.ResetForTest)

	Global = &GlobalConfig{
		Models: map[string]ModelConfig{
			"openai": {
				Enabled:      true,
				DefaultModel: "gpt-realtime",
			},
		},
	}
	InitModelConfig()

	if got := GetModel("openai"); got.DefaultModel != "gpt-realtime" {
		t.Fatalf("GetModel(openai).DefaultModel = %q, want gpt-realtime", got.DefaultModel)
	}
	if got := GetModel("missing-model"); got.Enabled {
		t.Fatalf("GetModel(missing-model).Enabled = true, want empty config")
	}

	day := stats.ResourcePeriods(time.Now())["day"].Summary
	if day.BusinessCacheHits != 1 || day.BusinessCacheMisses != 1 {
		t.Fatalf("business cache counters = hits %d misses %d, want 1/1", day.BusinessCacheHits, day.BusinessCacheMisses)
	}
	if day.ByKind[stats.ResourceKindBusinessCacheHit] != 1 || day.ByKind[stats.ResourceKindBusinessCacheMiss] != 1 {
		t.Fatalf("ByKind = %+v, want one model config cache hit and miss", day.ByKind)
	}
	if day.ByModel["openai"] != 1 || day.ByModel["missing-model"] != 1 {
		t.Fatalf("ByModel = %+v, want hit and miss attributed to requested model keys", day.ByModel)
	}
}
