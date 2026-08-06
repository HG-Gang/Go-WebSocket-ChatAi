// internal/service/workspace/pending.go
// 文件功能：待确认写入（pending write）的创建、确认、拒绝与内存队列管理。输入为
// project_id、相对路径、写入内容与操作者信息（WriteActor）；PreviewWrite 只生成 diff 并
// 入队，确认后才真正落盘，每个状态变化都会写审计日志并上报资源统计。
// 安全边界：敏感路径、超大或二进制内容在预览阶段失败关闭且不创建记录；只有 pending
// 状态允许确认或拒绝，重复操作返回明确错误；写盘期间不持有全局锁，避免阻塞其他请求。
package workspace

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

// Pending 写入状态常量：pending（待确认）、confirmed（已落盘）、rejected（已拒绝）、expired（已过期）。
const (
	PendingStatusPending   = "pending"
	PendingStatusConfirmed = "confirmed"
	PendingStatusRejected  = "rejected"
	PendingStatusExpired   = "expired"
)

// WriteActor 描述一次 Workspace 写入操作的操作者来源。
// UserID/UserName 来自 JWT 或服务端会话上下文；RequestID 用于串联 HTTP/WS 日志；
// Source 用于区分 http、model_tool、test 等入口，便于后续审计和告警归因。
type WriteActor struct {
	UserID    string
	UserName  string
	RequestID string
	Source    string
}

// PendingWrite 表示一次尚未落盘的文件写入申请。
// Before/After 用于生成 diff 和确认落盘，返回给前端时可按需要脱敏；
// DiffHash 是日志和审计使用的稳定摘要，避免把完整文件内容写入日志。
type PendingWrite struct {
	ID          string `json:"id"`
	ProjectID   string `json:"project_id"`
	Path        string `json:"path"`
	Before      string `json:"before,omitempty"`
	After       string `json:"after,omitempty"`
	Diff        string `json:"diff"`
	DiffHash    string `json:"diff_hash"`
	Status      string `json:"status"`
	UserID      string `json:"user_id"`
	UserName    string `json:"user_name"`
	RequestID   string `json:"request_id,omitempty"`
	Source      string `json:"source,omitempty"`
	Reason      string `json:"reason,omitempty"`
	CreatedAt   int64  `json:"created_at"`
	UpdatedAt   int64  `json:"updated_at"`
	RollbackRef string `json:"rollback_ref,omitempty"`
}

// pendingStore 是进程内内存队列，互斥锁保护并发访问；不持久化，服务重启后未确认的写入即失效。
var pendingStore = struct {
	sync.Mutex
	items map[string]*PendingWrite
}{items: make(map[string]*PendingWrite)}

// PreviewWrite 只创建待确认写入，不触碰磁盘上的目标文件。
// 调用方拿到 pending id 和 diff 后，必须由用户显式确认才能执行 ConfirmPendingWrite。
func PreviewWrite(projectID, relPath, content string, actor WriteActor) (PendingWrite, error) {
	// 内容校验失败也先记一条失败审计，便于追溯被拦截写入的来源；不创建任何 pending 记录。
	if err := validateWritableContent(relPath, content); err != nil {
		logWorkspaceWriteAudit("workspace_write_failed", PendingWrite{
			ProjectID: projectID,
			Path:      filepath.ToSlash(strings.TrimSpace(relPath)),
			UserID:    actor.UserID,
			UserName:  actor.UserName,
			RequestID: actor.RequestID,
			Source:    actor.Source,
			Status:    PendingStatusPending,
			Reason:    err.Error(),
		})
		return PendingWrite{}, err
	}

	root, err := ProjectRoot(projectID)
	if err != nil {
		return PendingWrite{}, err
	}
	fullPath, displayPath, err := resolve(root, relPath)
	if err != nil {
		return PendingWrite{}, err
	}
	if strings.TrimSpace(displayPath) == "" {
		return PendingWrite{}, errors.New("path is required")
	}

	// 目标文件已存在时读取旧内容作为 diff 的 before；不存在视为新建，其他 stat 错误直接失败，
	// 避免基于不完整信息生成预览。
	before := ""
	if info, err := os.Stat(fullPath); err == nil {
		if info.IsDir() {
			return PendingWrite{}, fmt.Errorf("cannot write directory: %s", displayPath)
		}
		if info.Size() > maxReadBytes {
			return PendingWrite{}, fmt.Errorf("file too large to preview through web workspace API: %d bytes", info.Size())
		}
		data, err := os.ReadFile(fullPath)
		if err != nil {
			return PendingWrite{}, err
		}
		if !isTextContent(data) {
			return PendingWrite{}, fmt.Errorf("binary content is not allowed: %s", displayPath)
		}
		before = string(data)
	} else if !os.IsNotExist(err) {
		return PendingWrite{}, err
	}

	now := time.Now().UnixMilli()
	diff := buildSimpleDiff(displayPath, before, content)
	pending := PendingWrite{
		ID:        uuid.NewString(),
		ProjectID: defaultProjectID(projectID),
		Path:      displayPath,
		Before:    before,
		After:     content,
		Diff:      diff,
		DiffHash:  hashString(diff),
		Status:    PendingStatusPending,
		UserID:    actor.UserID,
		UserName:  actor.UserName,
		RequestID: actor.RequestID,
		Source:    actor.Source,
		CreatedAt: now,
		UpdatedAt: now,
	}

	// 入队后立即发审计；持锁范围仅限 map 写入，磁盘读取与 diff 构建都不占用全局锁。
	pendingStore.Lock()
	pendingStore.items[pending.ID] = &pending
	pendingStore.Unlock()

	logWorkspaceWriteAudit("workspace_write_preview", pending)
	return pending, nil
}

// ConfirmPendingWrite 将指定 pending 写入真正落盘。
// 只有 pending 状态允许确认；重复确认或已拒绝的写入会返回清晰错误，避免误操作。
func ConfirmPendingWrite(pendingID string, actor WriteActor) (FileContent, PendingWrite, error) {
	// 第一段锁：取出记录并校验状态；状态不允许确认时返回当前快照，让调用方明确看到原因。
	pendingStore.Lock()
	pending, ok := pendingStore.items[pendingID]
	if !ok {
		pendingStore.Unlock()
		return FileContent{}, PendingWrite{}, fmt.Errorf("pending write not found: %s", pendingID)
	}
	if pending.Status != PendingStatusPending {
		copied := *pending
		pendingStore.Unlock()
		return FileContent{}, copied, fmt.Errorf("pending write is not pending: %s", copied.Status)
	}
	pending.UserID = firstNonEmpty(actor.UserID, pending.UserID)
	pending.UserName = firstNonEmpty(actor.UserName, pending.UserName)
	pending.RequestID = firstNonEmpty(actor.RequestID, pending.RequestID)
	pending.Source = firstNonEmpty(actor.Source, pending.Source)
	pending.UpdatedAt = time.Now().UnixMilli()
	projectID, relPath, content := pending.ProjectID, pending.Path, pending.After
	// 解锁后再落盘：磁盘 I/O 可能耗时，不能持锁阻塞其他 pending 的确认与拒绝。
	pendingStore.Unlock()

	file, err := Write(projectID, relPath, content)

	// 第二段锁：写盘期间队列可能已被其他请求变更，重新取当前记录再更新状态。
	pendingStore.Lock()
	defer pendingStore.Unlock()
	current := pendingStore.items[pendingID]
	if current == nil {
		// 记录已被移除时磁盘可能已改，此时必须显式报错，不能伪造成功返回。
		return file, PendingWrite{}, fmt.Errorf("pending write disappeared: %s", pendingID)
	}
	current.UserID = firstNonEmpty(actor.UserID, current.UserID)
	current.UserName = firstNonEmpty(actor.UserName, current.UserName)
	current.RequestID = firstNonEmpty(actor.RequestID, current.RequestID)
	current.Source = firstNonEmpty(actor.Source, current.Source)
	current.UpdatedAt = time.Now().UnixMilli()
	if err != nil {
		// 落盘失败时保留原因并写失败审计，状态不推进，调用方可以重试或排查。
		current.Reason = err.Error()
		copied := *current
		logWorkspaceWriteAudit("workspace_write_failed", copied)
		return FileContent{}, copied, err
	}
	// 落盘成功才推进状态机：confirmed 后不可再确认或拒绝，RollbackRef 记录本次 diff 摘要。
	current.Status = PendingStatusConfirmed
	current.RollbackRef = current.DiffHash
	copied := *current
	logWorkspaceWriteAudit("workspace_write_confirmed", copied)
	return file, copied, nil
}

// RejectPendingWrite 标记指定 pending 为拒绝状态，且不会写入磁盘。
func RejectPendingWrite(pendingID string, actor WriteActor, reason string) (PendingWrite, error) {
	// 只在 pending 状态下允许拒绝，已确认或已拒绝的记录不可回退，保证状态机单向推进。
	pendingStore.Lock()
	defer pendingStore.Unlock()

	pending, ok := pendingStore.items[pendingID]
	if !ok {
		return PendingWrite{}, fmt.Errorf("pending write not found: %s", pendingID)
	}
	if pending.Status != PendingStatusPending {
		return *pending, fmt.Errorf("pending write is not pending: %s", pending.Status)
	}
	// 拒绝不落盘、不删除记录，保留原因与操作者信息，供审计追溯用户为何未批准写入。
	pending.Status = PendingStatusRejected
	pending.UserID = firstNonEmpty(actor.UserID, pending.UserID)
	pending.UserName = firstNonEmpty(actor.UserName, pending.UserName)
	pending.RequestID = firstNonEmpty(actor.RequestID, pending.RequestID)
	pending.Source = firstNonEmpty(actor.Source, pending.Source)
	pending.Reason = reason
	pending.UpdatedAt = time.Now().UnixMilli()

	copied := *pending
	logWorkspaceWriteAudit("workspace_write_rejected", copied)
	return copied, nil
}

// ResetPendingWritesForTest 清空内存 pending store。
// 只在单元测试中使用，避免不同测试用例共享 pending id 和状态。
func ResetPendingWritesForTest() {
	pendingStore.Lock()
	defer pendingStore.Unlock()
	pendingStore.items = make(map[string]*PendingWrite)
}

// validateWritableContent 是所有写入口共用的安全校验。
// relPath 用于阻断 .env、私钥、凭据等敏感文件；content 用于阻断超大或二进制内容。
func validateWritableContent(relPath, content string) error {
	// 按字节计数而非字符，避免 UTF-8 多字节内容绕过长度上限。
	if len([]byte(content)) > maxWriteBytes {
		return fmt.Errorf("file too large to write through web workspace API: %d bytes", len([]byte(content)))
	}
	if isSensitivePath(relPath) {
		return fmt.Errorf("sensitive path is not allowed: %s", filepath.ToSlash(strings.TrimSpace(relPath)))
	}
	if !isTextContent([]byte(content)) {
		return errors.New("binary content is not allowed")
	}
	return nil
}

func isSensitivePath(relPath string) bool {
	// 统一转成 slash 小写形式匹配，避免 Windows 反斜杠或大小写差异绕过规则。
	clean := filepath.ToSlash(filepath.Clean(strings.TrimSpace(relPath)))
	base := strings.ToLower(filepath.Base(clean))
	lower := strings.ToLower(clean)
	// .env 及其变体（如 .env.local）一律拦截，防止密钥文件被写入或改写。
	if base == ".env" || strings.HasPrefix(base, ".env.") {
		return true
	}
	sensitiveNames := map[string]bool{
		"id_rsa": true, "id_dsa": true, "id_ecdsa": true, "id_ed25519": true,
		"authorized_keys": true, "known_hosts": true,
		"credentials": true, "credentials.json": true,
	}
	if sensitiveNames[base] {
		return true
	}
	// 私钥/凭据文件名、.ssh 目录及常见密钥后缀命中即拦截；规则只做路径匹配，不读文件内容。
	return strings.Contains(lower, "/.ssh/") ||
		strings.Contains(lower, "private_key") ||
		strings.HasSuffix(lower, ".pem") ||
		strings.HasSuffix(lower, ".key") ||
		strings.HasSuffix(lower, ".p12") ||
		strings.HasSuffix(lower, ".pfx")
}

func isTextContent(data []byte) bool {
	// 空文件视为合法文本，允许创建空文件。
	if len(data) == 0 {
		return true
	}
	// 非 UTF-8 或含 NUL 字节的都按二进制拒绝，防止把二进制 payload 写进文本工作区。
	if !utf8.Valid(data) {
		return false
	}
	return !strings.Contains(string(data), "\x00")
}

// buildSimpleDiff 生成整行粒度的 diff：before 全标 "-"、after 全标 "+"，不做行内合并；
// 文件原本为空时不输出空的 before 行，保证新文件的 diff 只含新增行。
func buildSimpleDiff(path, before, after string) string {
	var b strings.Builder
	b.WriteString("--- ")
	b.WriteString(path)
	b.WriteString("\n+++ ")
	b.WriteString(path)
	b.WriteString("\n@@\n")
	for _, line := range strings.Split(strings.TrimSuffix(before, "\n"), "\n") {
		if line == "" && before == "" {
			continue
		}
		b.WriteString("-")
		b.WriteString(line)
		b.WriteString("\n")
	}
	for _, line := range strings.Split(strings.TrimSuffix(after, "\n"), "\n") {
		if line == "" && after == "" {
			continue
		}
		b.WriteString("+")
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String()
}

// hashString 生成 sha256 的 hex 摘要，用于审计日志与 RollbackRef；摘要不可逆，
// 保证日志和统计中不会出现文件内容原文。
func hashString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// defaultProjectID 把空 project_id 归一化为 current，与 ProjectRoot 的接受范围保持一致。
func defaultProjectID(projectID string) string {
	if strings.TrimSpace(projectID) == "" {
		return "current"
	}
	return projectID
}

// firstNonEmpty 取第一个非空值，用于把确认或拒绝时最新传入的操作者信息回填到 pending 记录。
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
