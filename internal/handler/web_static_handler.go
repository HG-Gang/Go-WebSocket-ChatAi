// internal/handler/web_static_handler.go
// /web 静态页面处理器：提供静态文件并为 HTML 自动注入共享主题脚本。
//
// 文件功能：
//   - WebStaticHandler：把 Gin 路由参数解析为 web 根目录内的静态资源并返回；HTML 文件额外注入 theme.js。
//   - injectSharedThemeScript：统一旧页面的相对路径/缓存版本主题脚本，防止新增页面漏接颜色模式同步。
//
// 安全边界：
//   - resolveWebStaticPath 阻断路径穿越，非法路径返回 403，绝不继续尝试 ServeFile。
package handler

import (
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/gin-gonic/gin"
)

// sharedThemeScript 是所有 /web HTML 页面共享的主题脚本入口。
// WebStaticHandler 会自动注入或替换它，避免新增页面忘记接入颜色模式同步。
const sharedThemeScript = `<script src="/web/theme.js?v=20260607-theme" defer></script>`

// themeScriptPattern 识别旧版本或相对路径形式的 theme.js 标签。
// 匹配后统一替换为 sharedThemeScript，确保缓存版本和路径一致。
var themeScriptPattern = regexp.MustCompile(`(?i)<script\s+src=["'](?:/web/)?theme\.js(?:\?[^"']*)?["']\s+defer\s*>\s*</script>`)

// WebStaticHandler 提供 /web 静态页面，并为 HTML 自动注入共享主题脚本。
// root 是 web 目录路径，filepath 来自 Gin 路由参数；resolveWebStaticPath 会阻断路径穿越。
func WebStaticHandler(root string) gin.HandlerFunc {
	return func(c *gin.Context) {
		filePath, ok := resolveWebStaticPath(root, c.Param("filepath"))
		if !ok {
			c.JSON(http.StatusForbidden, gin.H{"code": 403, "error": "invalid web path"})
			return
		}

		info, err := os.Stat(filePath)
		if err != nil {
			c.Status(http.StatusNotFound)
			return
		}
		if info.IsDir() {
			filePath = filepath.Join(filePath, "index.html")
			if _, err := os.Stat(filePath); err != nil {
				c.Status(http.StatusNotFound)
				return
			}
		}

		if strings.EqualFold(filepath.Ext(filePath), ".html") {
			data, err := os.ReadFile(filePath)
			if err != nil {
				c.Status(http.StatusInternalServerError)
				return
			}
			c.Data(http.StatusOK, "text/html; charset=utf-8", injectSharedThemeScript(data))
			return
		}

		http.ServeFile(c.Writer, c.Request, filePath)
	}
}

// resolveWebStaticPath 将路由路径解析到 web 根目录内。
// 返回 false 表示路径不可信或解析失败，调用方应返回 403，而不是继续尝试 ServeFile。
func resolveWebStaticPath(root, rawPath string) (string, bool) {
	if rawPath == "" || rawPath == "/" {
		rawPath = "/index.html"
	}
	rel := filepath.Clean(strings.TrimPrefix(rawPath, "/"))
	if rel == "." {
		rel = "index.html"
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", false
	}
	fileAbs, err := filepath.Abs(filepath.Join(rootAbs, rel))
	if err != nil {
		return "", false
	}
	if fileAbs != rootAbs && !strings.HasPrefix(fileAbs, rootAbs+string(filepath.Separator)) {
		return "", false
	}
	return fileAbs, true
}

// injectSharedThemeScript 保证 HTML 至少加载一次共享主题脚本。
// 已存在旧 script 时替换，缺少 </head> 时插到文件开头，避免页面因结构不完整而失去主题同步。
func injectSharedThemeScript(data []byte) []byte {
	if themeScriptPattern.Match(data) {
		return themeScriptPattern.ReplaceAll(data, []byte(sharedThemeScript))
	}
	lower := bytes.ToLower(data)
	idx := bytes.Index(lower, []byte("</head>"))
	if idx < 0 {
		return append([]byte(sharedThemeScript+"\n"), data...)
	}
	out := make([]byte, 0, len(data)+len(sharedThemeScript)+1)
	out = append(out, data[:idx]...)
	out = append(out, []byte("    "+sharedThemeScript+"\n")...)
	out = append(out, data[idx:]...)
	return out
}
