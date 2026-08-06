// internal/quality/chat_page_test.go
// AI 项目助手页（web/chat.html）质量测试：锁定页面三栏职责、token 统计与图表、
// 主题联动等行为，防止页面回归为旧的文件编辑布局或重新暴露上游调试输入。
package quality_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestChatPageUsesEditorAsConversationAndInspectorAsRawEvents(t *testing.T) {
	// 这个测试锁定 AI 项目助手页面的三栏职责：
	// 左侧只做项目目录，中间 #editor 做对话输出，右侧 #chat-log 做原始事件、日志、token 和图表。
	root := repositoryRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "web", "chat.html"))
	if err != nil {
		t.Fatalf("读取 chat.html 失败: %v", err)
	}
	html := string(data)

	if strings.Contains(html, `<textarea id="editor"`) {
		t.Fatal("#editor 不能再是文件编辑 textarea，它必须是聊天输出容器")
	}
	for _, want := range []string{
		`<aside class="pane project-pane">`,
		`<section class="pane conversation-pane">`,
		`<div class="chat-output" id="editor"`,
		`<section class="pane inspector-pane">`,
		`id="event-filter"`,
		`id="token-current-total"`,
		`id="token-current-cached"`,
		`id="token-current-missed"`,
		`id="token-cumulative-total"`,
		`id="chart-shape"`,
		`id="token-chart"`,
		`function recordRawEvent(`,
		`function updateTokenStatsFromEvent(`,
		`function renderTokenChart(`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("chat.html 缺少 %q", want)
		}
	}

	leftStart := strings.Index(html, `<aside class="pane project-pane">`)
	leftEnd := strings.Index(html, `<section class="pane conversation-pane">`)
	if leftStart < 0 || leftEnd <= leftStart {
		t.Fatal("无法定位左侧项目目录区域")
	}
	left := html[leftStart:leftEnd]
	for _, forbidden := range []string{"upstream-api-key", "upstream-ws-url", "上游 API Key 仅限开发调试"} {
		if strings.Contains(left, forbidden) {
			t.Fatalf("左侧项目目录区域不应包含 %q", forbidden)
		}
	}
	if !strings.Contains(left, `id="file-list"`) {
		t.Fatal("左侧项目目录区域必须包含文件列表")
	}

	bootStart := strings.Index(html, `document.addEventListener('DOMContentLoaded'`)
	if bootStart < 0 {
		t.Fatal("chat.html 缺少 DOMContentLoaded 初始化入口")
	}
	bootEnd := strings.Index(html[bootStart:], "});")
	if bootEnd < 0 {
		t.Fatal("无法定位 DOMContentLoaded 初始化块")
	}
	boot := html[bootStart : bootStart+bootEnd]
	for _, want := range []string{
		`renderRawEvents();`,
		`updateTokenDisplay();`,
		`renderTokenChart();`,
	} {
		if !strings.Contains(boot, want) {
			t.Fatalf("DOMContentLoaded 初始化块缺少 %q", want)
		}
	}
}

func TestChatPageDoesNotExposeUpstreamQueryKeyDebugControls(t *testing.T) {
	root := repositoryRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "web", "chat.html"))
	if err != nil {
		t.Fatalf("读取 chat.html 失败: %v", err)
	}
	html := string(data)

	for _, forbidden := range []string{
		`id="upstream-api-key"`,
		`id="upstream-ws-url"`,
		`upstream_api_key`,
		`upstream_ws_url`,
		`上游 API Key 仅限开发调试`,
	} {
		if strings.Contains(html, forbidden) {
			t.Fatalf("AI 项目助手页不应暴露上游 query key 调试入口: %q", forbidden)
		}
	}
	if !strings.Contains(html, `服务端 OpenAI 配置`) {
		t.Fatal("AI 项目助手页应提示使用服务端 OpenAI 配置")
	}
}

func TestChatPageShowsGranularTokenTotalsAndChartShapes(t *testing.T) {
	root := repositoryRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "web", "chat.html"))
	if err != nil {
		t.Fatalf("读取 chat.html 失败: %v", err)
	}
	html := string(data)

	for _, want := range []string{
		`id="token-current-input"`,
		`id="token-current-output"`,
		`id="token-current-cached"`,
		`id="token-current-missed"`,
		`id="token-cumulative-input"`,
		`id="token-cumulative-output"`,
		`id="token-cumulative-cached"`,
		`id="token-cumulative-missed"`,
		`id="token-cumulative-reasoning"`,
		`value="bar"`,
		`value="line"`,
		`value="stacked"`,
		`value="area"`,
		`value="grouped"`,
		`function drawTokenAreaChart(`,
		`function drawGroupedTokenChart(`,
		`prompt_tokens`,
		`completion_tokens`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("chat.html 缺少 token 面板或图表能力 %q", want)
		}
	}
}

func TestChatPageLetsTokenChartSwitchSeriesAndShowsLegend(t *testing.T) {
	root := repositoryRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "web", "chat.html"))
	if err != nil {
		t.Fatalf("读取 chat.html 失败: %v", err)
	}
	html := string(data)

	for _, want := range []string{
		`id="chart-series"`,
		`value="total"`,
		`value="input"`,
		`value="output"`,
		`value="cached"`,
		`value="missed"`,
		`value="reasoning"`,
		`id="chart-legend"`,
		`$('chart-series').addEventListener('change', renderTokenChart);`,
		`function renderTokenLegend(`,
		`function tokenSeriesDefinitions(`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("chat.html 缺少 token 图表维度切换或图例能力 %q", want)
		}
	}
}

func TestChatPageRendersTokenLegendBeforeEmptyChartReturn(t *testing.T) {
	root := repositoryRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "web", "chat.html"))
	if err != nil {
		t.Fatalf("读取 chat.html 失败: %v", err)
	}
	html := string(data)

	chartStart := strings.Index(html, `function renderTokenChart()`)
	if chartStart < 0 {
		t.Fatal("chat.html 缺少 renderTokenChart")
	}
	chartEnd := strings.Index(html[chartStart:], `function renderTokenLegend(`)
	if chartEnd < 0 {
		t.Fatal("无法定位 renderTokenChart 结束位置")
	}
	renderTokenChart := html[chartStart : chartStart+chartEnd]
	legendCall := strings.Index(renderTokenChart, `renderTokenLegend(series);`)
	emptyReturn := strings.Index(renderTokenChart, `if (rows.length === 0)`)
	if legendCall < 0 {
		t.Fatal("renderTokenChart 必须在绘制前刷新图例")
	}
	if emptyReturn < 0 {
		t.Fatal("renderTokenChart 必须保留空数据提示")
	}
	if legendCall > emptyReturn {
		t.Fatal("token 图例必须在空数据 return 前渲染，避免初始页面图例为空")
	}
}

func TestChatPageDrawsAllSelectedTokenSeriesForLineAndAreaCharts(t *testing.T) {
	root := repositoryRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "web", "chat.html"))
	if err != nil {
		t.Fatalf("读取 chat.html 失败: %v", err)
	}
	html := string(data)

	for _, forbidden := range []string{
		`drawTokenLineChart(ctx, rows, padding, plotW, plotH, maxTotal, series[0]);`,
		`drawTokenAreaChart(ctx, rows, padding, plotW, plotH, maxTotal, series[0]);`,
	} {
		if strings.Contains(html, forbidden) {
			t.Fatalf("全部维度切到折线或面积图时不应只绘制第一条 token 序列: %q", forbidden)
		}
	}
	for _, want := range []string{
		`drawTokenLineChart(ctx, rows, padding, plotW, plotH, maxTotal, series);`,
		`drawTokenAreaChart(ctx, rows, padding, plotW, plotH, maxTotal, series);`,
		`function drawSingleTokenLine(`,
		`for (const item of series)`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("chat.html 缺少多 token 序列折线/面积图绘制逻辑 %q", want)
		}
	}
}

func TestChatPageCalculatesCacheMissesFromInputTokens(t *testing.T) {
	root := repositoryRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "web", "chat.html"))
	if err != nil {
		t.Fatalf("读取 chat.html 失败: %v", err)
	}
	html := string(data)

	for _, want := range []string{
		`const currentMissed = Math.max(0, state.tokenCurrent.input - state.tokenCurrent.cached);`,
		`const cumulativeMissed = Math.max(0, state.tokenCumulative.input - state.tokenCumulative.cached);`,
		`return Math.max(0, numeric(row.input) - numeric(row.cached));`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("chat.html 必须用输入 token 减缓存命中计算未命中量，缺少 %q", want)
		}
	}
}

func TestChatPageKeepsSelectedFileAsChatContextOnly(t *testing.T) {
	root := repositoryRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "web", "chat.html"))
	if err != nil {
		t.Fatalf("读取 chat.html 失败: %v", err)
	}
	html := string(data)

	openStart := strings.Index(html, `async function openFile(path)`)
	if openStart < 0 {
		t.Fatal("chat.html 缺少 openFile")
	}
	openEnd := strings.Index(html[openStart:], `async function connectWS()`)
	if openEnd < 0 {
		t.Fatal("无法定位 openFile 结束位置")
	}
	openFile := html[openStart : openStart+openEnd]
	for _, forbidden := range []string{
		`/api/workspace/file`,
		`file.content`,
		`$('editor').value`,
		`$('editor').textContent =`,
	} {
		if strings.Contains(openFile, forbidden) {
			t.Fatalf("选中文件只能设置聊天上下文，不应把文件内容写入 #editor: %q", forbidden)
		}
	}
	for _, want := range []string{
		`state.currentPath = path`,
		`workspace.context_selected`,
		`appendMessage('system'`,
		`updateChatContext();`,
	} {
		if !strings.Contains(openFile, want) {
			t.Fatalf("openFile 缺少聊天上下文逻辑 %q", want)
		}
	}
}

func TestChatPageUsesSharedThemeForConversationAndInspector(t *testing.T) {
	root := repositoryRoot(t)
	chatData, err := os.ReadFile(filepath.Join(root, "web", "chat.html"))
	if err != nil {
		t.Fatalf("读取 chat.html 失败: %v", err)
	}
	themeData, err := os.ReadFile(filepath.Join(root, "web", "theme.js"))
	if err != nil {
		t.Fatalf("读取 theme.js 失败: %v", err)
	}
	chat := string(chatData)
	theme := string(themeData)

	for _, want := range []string{
		`body[class*="theme-"] .connection-panel`,
		`body[class*="theme-"] #editor.chat-output`,
		`body[class*="theme-"] .token-card`,
		`body[class*="theme-"] .chart-box`,
		`body[class*="theme-"] .raw-event`,
	} {
		if !strings.Contains(theme, want) {
			t.Fatalf("theme.js 缺少 chat 页面主题覆盖 %q", want)
		}
	}
	for _, want := range []string{
		`document.addEventListener('tozo-theme-change', renderTokenChart);`,
		`function cssVar(`,
		`cssVar('--surface'`,
		`cssVar('--muted'`,
		`cssVar('--line'`,
		`cssVar('--accent'`,
		`cssVar('--warn'`,
	} {
		if !strings.Contains(chat, want) {
			t.Fatalf("chat.html 缺少主题感知图表逻辑 %q", want)
		}
	}
}
