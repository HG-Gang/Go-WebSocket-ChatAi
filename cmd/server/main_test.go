// cmd/server/main_test.go
// 文件功能：验证生产环境路由安全（测试 token 与调试接口不可匿名访问）、可信代理配置
// 生效，以及 main 中周期监控与日志清理调度接线存在（通过源码文本断言）。
package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"TozoAI-Chat-Api/conf"
)

// 生产路由测试用于防止测试 token 和诊断接口被匿名暴露到公网。
func TestProductionDisablesPublicTokenAndDebugRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	conf.Global = &conf.GlobalConfig{}
	conf.Global.Env = "prod"
	conf.Global.JWT.Enabled = true
	conf.Global.JWT.Secret = "prod-secret"
	conf.Global.Security.PublicTokenEnabled = false
	conf.Global.Security.PublicDebugEnabled = false

	router := buildRouter()
	for _, path := range []string{
		"/test/generate-token?userId=1001",
		"/api/debug/status",
		"/api/redis/keys",
		"/api/web/models",
		"/api/web/metrics",
		"/api/stats/resources",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code == http.StatusOK {
			t.Fatalf("%s status = %d, want protected or unavailable route", path, w.Code)
		}
	}
}

func TestBuildRouterAppliesTrustedProxies(t *testing.T) {
	gin.SetMode(gin.TestMode)
	conf.Global = &conf.GlobalConfig{}
	conf.Global.Env = "prod"
	conf.Global.JWT.Enabled = false
	conf.Global.Security.TrustedProxies = []string{"10.0.0.1"}

	router := buildRouter()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = "10.0.0.1:12345"
	req.Header.Set("X-Forwarded-For", "203.0.113.10")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("health status = %d, want 200", w.Code)
	}
}

func TestMainStartsPeriodicMonitorLogger(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("ReadFile main.go: %v", err)
	}
	mainSource := string(data)

	for _, want := range []string{
		`"TozoAI-Chat-Api/internal/service/monitor"`,
		`monitor.StartPeriodicLogger(`,
		`context.WithCancel(context.Background())`,
		`monitorCancel()`,
	} {
		if !strings.Contains(mainSource, want) {
			t.Fatalf("main.go missing periodic monitor startup wiring %q", want)
		}
	}
}

func TestMainStartsPeriodicLogCleanupScheduler(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("ReadFile main.go: %v", err)
	}
	mainSource := string(data)

	for _, want := range []string{
		`logger.StartCleanupScheduler(`,
		`cleanupCancel()`,
		`cleanupDone`,
		`conf.Global.Logs.RetentionDays`,
		`conf.Global.Logs.CleanupInterval`,
	} {
		if !strings.Contains(mainSource, want) {
			t.Fatalf("main.go missing log cleanup scheduler wiring %q", want)
		}
	}
}
