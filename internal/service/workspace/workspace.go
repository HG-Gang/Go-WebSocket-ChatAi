package workspace

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Workspace service 是项目文件访问的唯一底层入口。
// 所有 HTTP 和模型工具写文件都必须经过这里，统一执行 project_id 校验、路径逃逸防护、
// 文件大小限制、文本内容限制和敏感路径拦截。
const (
	// maxReadBytes 限制前端/模型一次读取的最大文件大小，避免误读大文件拖垮 WebSocket 响应。
	maxReadBytes = 512 * 1024
	// maxWriteBytes 限制一次完整文件写入的最大内容，写入前还会校验 UTF-8 和敏感路径。
	maxWriteBytes = 512 * 1024
	// maxListItems 限制目录列表长度，避免根目录或依赖目录返回过大 JSON。
	maxListItems = 500
)

// skippedDirs 是目录浏览时跳过的高噪声目录。
// 这些目录不影响直接按路径读取，但不会出现在默认列表里，降低误操作和页面渲染压力。
var skippedDirs = map[string]bool{
	".git": true, ".idea": true, ".tmp": true, "logs": true,
	"node_modules": true, "vendor": true, "dist": true, "build": true,
}

// Project 表示前端可选择的工作区。
// Root 只展示给已鉴权调试页面，后续多项目时 ID 必须保持稳定。
type Project struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Root string `json:"root"`
}

// Entry 是目录列表中的一项。
// Path 始终是项目内 slash 风格相对路径，不能是绝对路径。
type Entry struct {
	Name     string `json:"name"`
	Path     string `json:"path"`
	Type     string `json:"type"`
	Size     int64  `json:"size"`
	Modified string `json:"modified"`
}

// FileContent 是读取或写入后的文件内容快照。
// Content 只承载 UTF-8 文本，Size 是落盘后的字节数。
type FileContent struct {
	Path    string `json:"path"`
	Content string `json:"content"`
	Size    int64  `json:"size"`
}

// Projects 返回当前线程工作区。
// 这里不扫描磁盘上的其他目录，避免模型工具越权选择非当前项目。
func Projects() ([]Project, error) {
	root, err := projectRoot()
	if err != nil {
		return nil, err
	}
	return []Project{{
		ID:   "current",
		Name: filepath.Base(root),
		Root: root,
	}}, nil
}

// ProjectRoot 将 project_id 映射到真实根目录。
// 当前只支持 current；未知 ID 直接报错，避免前端参数被误解释成文件路径。
func ProjectRoot(projectID string) (string, error) {
	if projectID != "" && projectID != "current" {
		return "", fmt.Errorf("unknown project: %s", projectID)
	}
	return projectRoot()
}

// List 返回项目内某个目录的文件列表。
// relPath 会先 resolve 到项目根内，目录项按“目录优先 + 名称排序”返回，便于前端稳定展示。
func List(projectID, relPath string) ([]Entry, error) {
	root, err := ProjectRoot(projectID)
	if err != nil {
		return nil, err
	}
	dir, displayPath, err := resolve(root, relPath)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("not a directory: %s", displayPath)
	}
	items, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, min(len(items), maxListItems))
	for _, item := range items {
		if len(entries) >= maxListItems {
			break
		}
		name := item.Name()
		if item.IsDir() && skippedDirs[name] {
			continue
		}
		info, err := item.Info()
		if err != nil {
			continue
		}
		typ := "file"
		if info.IsDir() {
			typ = "dir"
		}
		entries = append(entries, Entry{
			Name:     name,
			Path:     filepath.ToSlash(filepath.Join(displayPath, name)),
			Type:     typ,
			Size:     info.Size(),
			Modified: info.ModTime().Format("2006-01-02 15:04:05"),
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Type != entries[j].Type {
			return entries[i].Type == "dir"
		}
		return strings.ToLower(entries[i].Name) < strings.ToLower(entries[j].Name)
	})
	return entries, nil
}

// Read 读取项目内文本文件。
// 读取目录、超大文件和不存在的路径会显式报错；调用方不应把错误静默转换成空内容。
func Read(projectID, relPath string) (FileContent, error) {
	root, err := ProjectRoot(projectID)
	if err != nil {
		return FileContent{}, err
	}
	fullPath, displayPath, err := resolve(root, relPath)
	if err != nil {
		return FileContent{}, err
	}
	info, err := os.Stat(fullPath)
	if err != nil {
		return FileContent{}, err
	}
	if info.IsDir() {
		return FileContent{}, fmt.Errorf("cannot read directory: %s", displayPath)
	}
	if info.Size() > maxReadBytes {
		return FileContent{}, fmt.Errorf("file too large to read through web workspace API: %d bytes", info.Size())
	}
	data, err := os.ReadFile(fullPath)
	if err != nil {
		return FileContent{}, err
	}
	return FileContent{Path: displayPath, Content: string(data), Size: info.Size()}, nil
}

// Write 直接写入项目内文本文件。
// 这是兼容路径；生产安全配置应优先走 PreviewWrite/ConfirmPendingWrite，确保用户确认 diff 后才落盘。
func Write(projectID, relPath, content string) (FileContent, error) {
	if err := validateWritableContent(relPath, content); err != nil {
		return FileContent{}, err
	}
	root, err := ProjectRoot(projectID)
	if err != nil {
		return FileContent{}, err
	}
	fullPath, displayPath, err := resolve(root, relPath)
	if err != nil {
		return FileContent{}, err
	}
	if strings.TrimSpace(displayPath) == "" {
		return FileContent{}, errors.New("path is required")
	}
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return FileContent{}, err
	}
	if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
		return FileContent{}, err
	}
	info, err := os.Stat(fullPath)
	if err != nil {
		return FileContent{}, err
	}
	return FileContent{Path: displayPath, Content: content, Size: info.Size()}, nil
}

// projectRoot 使用当前进程工作目录作为项目根。
// Codex 桌面线程会在仓库根目录启动服务，因此这里不再接受外部传入的绝对路径。
func projectRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Abs(wd)
}

// resolve 把项目内相对路径转换为绝对路径，并防止路径逃逸。
// 返回的 displayPath 始终是 slash 风格相对路径，用于 JSON 响应、diff 和审计日志。
func resolve(root, relPath string) (string, string, error) {
	cleanRel := filepath.Clean(strings.TrimSpace(relPath))
	if cleanRel == "." {
		cleanRel = ""
	}
	if filepath.IsAbs(cleanRel) {
		return "", "", errors.New("absolute paths are not allowed")
	}
	fullPath := filepath.Join(root, cleanRel)
	abs, err := filepath.Abs(fullPath)
	if err != nil {
		return "", "", err
	}
	rel, err := filepath.Rel(root, abs)
	if err != nil {
		return "", "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", "", errors.New("path escapes project root")
	}
	if rel == "." {
		rel = ""
	}
	return abs, filepath.ToSlash(rel), nil
}
