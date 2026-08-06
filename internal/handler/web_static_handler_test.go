// internal/handler/web_static_handler_test.go
// /web 静态页处理器测试：锁定主题脚本注入幂等、相对路径归一化和路径穿越拒绝。
package handler

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// 这组测试锁定 /web 静态页主题脚本注入行为。
// 目标是防止新增页面漏掉 theme.js，导致测试面板切换颜色后其他页面不同步。
func TestInjectSharedThemeScript(t *testing.T) {
	html := []byte("<!doctype html><html><head><title>x</title></head><body></body></html>")
	got := injectSharedThemeScript(html)
	if !bytes.Contains(got, []byte(sharedThemeScript)) {
		t.Fatalf("theme script was not injected: %s", got)
	}

	again := injectSharedThemeScript(got)
	if bytes.Count(again, []byte("theme.js")) != 1 {
		t.Fatalf("theme script injected more than once: %s", again)
	}
}

// 已经手写相对路径 theme.js 的旧页面也必须被归一化。
// 这样缓存版本、路径和 defer 属性都由服务端统一维护。
func TestInjectSharedThemeScriptNormalizesRelativeThemePath(t *testing.T) {
	html := []byte(`<!doctype html><html><head><script src="theme.js?v=20260606-theme" defer></script></head><body></body></html>`)

	got := injectSharedThemeScript(html)

	if !bytes.Contains(got, []byte(sharedThemeScript)) {
		t.Fatalf("relative theme script was not normalized: %s", got)
	}
	if bytes.Contains(got, []byte(`src="theme.js`)) {
		t.Fatalf("relative theme script still present: %s", got)
	}
	if bytes.Count(got, []byte("theme.js")) != 1 {
		t.Fatalf("theme script count = %d, want 1: %s", bytes.Count(got, []byte("theme.js")), got)
	}
}

// 静态资源路径必须限制在 web 根目录内。
// 这个测试防止通过 /web/../ 读取项目里的配置、日志或密钥文件。
func TestResolveWebStaticPathRejectsTraversal(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "index.html"), []byte("ok"), 0644); err != nil {
		t.Fatalf("write index: %v", err)
	}

	if _, ok := resolveWebStaticPath(root, "/../secret.txt"); ok {
		t.Fatalf("resolveWebStaticPath allowed traversal")
	}
	if path, ok := resolveWebStaticPath(root, "/"); !ok || filepath.Base(path) != "index.html" {
		t.Fatalf("resolve root = %q, %v; want index.html", path, ok)
	}
}
