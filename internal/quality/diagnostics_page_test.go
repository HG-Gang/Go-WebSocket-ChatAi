package quality_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDiagnosticsPageShowsUserIdentityRealIPAndLocation(t *testing.T) {
	root := repositoryRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "web", "diagnostics.html"))
	if err != nil {
		t.Fatalf("读取 diagnostics.html 失败: %v", err)
	}
	html := string(data)

	for _, want := range []string{
		`用户 / 设备 / 位置`,
		`session.user_name`,
		`session.real_ip`,
		`session.ip_location`,
		`formatIPLocation(`,
		`真实IP`,
		`所在地`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("diagnostics.html 缺少用户身份/IP/所在地展示逻辑 %q", want)
		}
	}
}

func TestDiagnosticsPageShowsCachedAndReasoningTokenSnapshotFields(t *testing.T) {
	root := repositoryRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "web", "diagnostics.html"))
	if err != nil {
		t.Fatalf("读取 diagnostics.html 失败: %v", err)
	}
	html := string(data)

	for _, want := range []string{
		`business.cached_tokens`,
		`business.reasoning_tokens`,
		`缓存命中`,
		`推理`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("diagnostics.html 缺少 cached/reasoning token 快照展示 %q", want)
		}
	}
}

func TestDiagnosticsPageUsesStatsResourcesAPIForResourceCharts(t *testing.T) {
	root := repositoryRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "web", "diagnostics.html"))
	if err != nil {
		t.Fatalf("读取 diagnostics.html 失败: %v", err)
	}
	html := string(data)

	for _, want := range []string{
		`/api/stats/resources`,
		`result.statsResources`,
		`renderResourceStats(result.statsResources)`,
		`payload?.data?.periods`,
		`rs-day-ops`,
		`capacity_rejected`,
		`rate_limit_rejected`,
		`alerts_firing`,
		`workspace_write_confirmed`,
		`business_cache_hits`,
		`business_cache_misses`,
		`rs-day-cache`,
		`rs-week-cache`,
		`rs-month-cache`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("diagnostics.html 缺少 stats resources 资源图数据源 %q", want)
		}
	}
	for _, forbidden := range []string{
		`数据来自 /api/web/metrics 的请求明细聚合`,
		`renderResourceStats(result.webMetrics)`,
	} {
		if strings.Contains(html, forbidden) {
			t.Fatalf("diagnostics.html 资源图不应继续依赖 Web metrics 调试窗口: %q", forbidden)
		}
	}
}

func TestDiagnosticsPageShowsAlertFiringRecoveredAndCurrentReasons(t *testing.T) {
	root := repositoryRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "web", "diagnostics.html"))
	if err != nil {
		t.Fatalf("读取 diagnostics.html 失败: %v", err)
	}
	html := string(data)

	for _, want := range []string{
		`id="m-alert-status"`,
		`id="m-alert-reasons"`,
		`id="m-alert-last-recovered"`,
		`monitor.alerts`,
		`alerts.active_reasons`,
		`alerts.last_recovered_reason`,
		`summary.alerts_recovered`,
		`恢复 ${number(recovered)}`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("diagnostics.html 缺少告警状态展示逻辑 %q", want)
		}
	}
}
