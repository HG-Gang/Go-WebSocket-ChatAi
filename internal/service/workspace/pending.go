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

var pendingStore = struct {
	sync.Mutex
	items map[string]*PendingWrite
}{items: make(map[string]*PendingWrite)}

// PreviewWrite 只创建待确认写入，不触碰磁盘上的目标文件。
// 调用方拿到 pending id 和 diff 后，必须由用户显式确认才能执行 ConfirmPendingWrite。
func PreviewWrite(projectID, relPath, content string, actor WriteActor) (PendingWrite, error) {
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

	pendingStore.Lock()
	pendingStore.items[pending.ID] = &pending
	pendingStore.Unlock()

	logWorkspaceWriteAudit("workspace_write_preview", pending)
	return pending, nil
}

// ConfirmPendingWrite 将指定 pending 写入真正落盘。
// 只有 pending 状态允许确认；重复确认或已拒绝的写入会返回清晰错误，避免误操作。
func ConfirmPendingWrite(pendingID string, actor WriteActor) (FileContent, PendingWrite, error) {
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
	pendingStore.Unlock()

	file, err := Write(projectID, relPath, content)

	pendingStore.Lock()
	defer pendingStore.Unlock()
	current := pendingStore.items[pendingID]
	if current == nil {
		return file, PendingWrite{}, fmt.Errorf("pending write disappeared: %s", pendingID)
	}
	current.UserID = firstNonEmpty(actor.UserID, current.UserID)
	current.UserName = firstNonEmpty(actor.UserName, current.UserName)
	current.RequestID = firstNonEmpty(actor.RequestID, current.RequestID)
	current.Source = firstNonEmpty(actor.Source, current.Source)
	current.UpdatedAt = time.Now().UnixMilli()
	if err != nil {
		current.Reason = err.Error()
		copied := *current
		logWorkspaceWriteAudit("workspace_write_failed", copied)
		return FileContent{}, copied, err
	}
	current.Status = PendingStatusConfirmed
	current.RollbackRef = current.DiffHash
	copied := *current
	logWorkspaceWriteAudit("workspace_write_confirmed", copied)
	return file, copied, nil
}

// RejectPendingWrite 标记指定 pending 为拒绝状态，且不会写入磁盘。
func RejectPendingWrite(pendingID string, actor WriteActor, reason string) (PendingWrite, error) {
	pendingStore.Lock()
	defer pendingStore.Unlock()

	pending, ok := pendingStore.items[pendingID]
	if !ok {
		return PendingWrite{}, fmt.Errorf("pending write not found: %s", pendingID)
	}
	if pending.Status != PendingStatusPending {
		return *pending, fmt.Errorf("pending write is not pending: %s", pending.Status)
	}
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
	clean := filepath.ToSlash(filepath.Clean(strings.TrimSpace(relPath)))
	base := strings.ToLower(filepath.Base(clean))
	lower := strings.ToLower(clean)
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
	return strings.Contains(lower, "/.ssh/") ||
		strings.Contains(lower, "private_key") ||
		strings.HasSuffix(lower, ".pem") ||
		strings.HasSuffix(lower, ".key") ||
		strings.HasSuffix(lower, ".p12") ||
		strings.HasSuffix(lower, ".pfx")
}

func isTextContent(data []byte) bool {
	if len(data) == 0 {
		return true
	}
	if !utf8.Valid(data) {
		return false
	}
	return !strings.Contains(string(data), "\x00")
}

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

func hashString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func defaultProjectID(projectID string) string {
	if strings.TrimSpace(projectID) == "" {
		return "current"
	}
	return projectID
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
