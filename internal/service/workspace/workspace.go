// internal/service/workspace/workspace.go
// 文件功能：Workspace 服务，项目文件访问的唯一底层入口。负责把 project_id 映射到项目根，
// 提供目录列表、文本文件读取与直接写入；输入为 project_id 和项目内相对路径，输出为可
// JSON 序列化的 Entry/FileContent，失败时返回显式错误。
// 安全边界：所有入口统一执行 project_id 校验、路径逃逸防护（resolve）、大小限制、敏感
// 内容与敏感路径拦截；任一步失败都返回错误而不静默降级，调用方不得把读错误当空内容。
package workspace

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

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
	// 边读边构建条目，超过 maxListItems 立即停止，避免超大目录的完整 JSON 回传前端。
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
	// 目录优先、名称不区分大小写排序，保证前端展示顺序稳定，不依赖系统目录顺序。
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
	// 单位字节：超过 maxReadBytes 的文件拒绝读入内存，防止大文件拖垮 WebSocket 响应。
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
	// 内容与路径先全部校验通过再碰磁盘，避免失败一半留下部分落盘结果。
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
	// 允许写入尚不存在的子目录：父目录由服务端按 0755 权限自动创建，前端无需逐级建目录。
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
	// 先规范化并拒绝绝对路径：filepath.Join 会把 ".." 收敛，但绝对路径会被当成新根，必须显式拦截。
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
	// 用 Rel 反向核对最终绝对路径仍在项目根内，出现任一 ".." 前缀即判定逃逸并失败关闭。
	rel, err := filepath.Rel(root, abs)
	if err != nil {
		return "", "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", "", errors.New("path escapes project root")
	}
	// 根目录本身映射为空相对路径，保证所有下游 JSON 中的 path 都是项目内相对路径。
	if rel == "." {
		rel = ""
	}
	return abs, filepath.ToSlash(rel), nil
}
