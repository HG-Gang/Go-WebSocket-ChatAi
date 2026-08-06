// internal/service/workspace/workspace_test.go
// 文件功能：Workspace 服务（pending 写入生命周期、审计与资源统计）的单元测试。覆盖
// 预览不落盘、确认落盘、拒绝不落盘、敏感路径拦截、审计日志不泄露文件内容与统计计数。
// 安全边界：用例统一切换到临时项目根和测试日志目录，并断言审计日志不含文件内容原文。
package workspace

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/logger"
	"TozoAI-Chat-Api/internal/service/stats"
)

// withTempProject 把当前测试进程切到临时项目根目录。
// Workspace 服务默认以 os.Getwd() 作为 current 项目根；测试结束时必须恢复，
// 避免后续测试误把临时目录当成真实工作区。
func withTempProject(t *testing.T) string {
	t.Helper()

	tempDir := t.TempDir()
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	oldConf := conf.Global
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("Chdir temp dir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(oldWD)
		conf.Global = oldConf
	})
	return tempDir
}

func TestPreviewWriteCreatesPendingWithoutWritingFile(t *testing.T) {
	root := withTempProject(t)
	conf.Global = &conf.GlobalConfig{}
	conf.Global.Security.WorkspaceWriteConfirm = true
	ResetPendingWritesForTest()

	pending, err := PreviewWrite("current", "notes/a.txt", "after\n", WriteActor{UserID: "1001", UserName: "张三", Source: "test"})
	if err != nil {
		t.Fatalf("PreviewWrite error = %v", err)
	}
	if pending.ID == "" || pending.Status != PendingStatusPending {
		t.Fatalf("pending = %+v, want id and pending status", pending)
	}
	if pending.Path != "notes/a.txt" || pending.After != "after\n" {
		t.Fatalf("pending path/content = %+v", pending)
	}
	if pending.Diff == "" || !strings.Contains(pending.Diff, "+after") {
		t.Fatalf("diff = %q, want added content", pending.Diff)
	}
	if _, err := os.Stat(filepath.Join(root, "notes", "a.txt")); !os.IsNotExist(err) {
		t.Fatalf("file was written during preview, stat err = %v", err)
	}
}

func TestConfirmPendingWriteWritesFileAndMarksConfirmed(t *testing.T) {
	root := withTempProject(t)
	conf.Global = &conf.GlobalConfig{}
	conf.Global.Security.WorkspaceWriteConfirm = true
	ResetPendingWritesForTest()

	pending, err := PreviewWrite("current", "notes/a.txt", "confirmed\n", WriteActor{UserID: "1001"})
	if err != nil {
		t.Fatalf("PreviewWrite error = %v", err)
	}
	file, confirmed, err := ConfirmPendingWrite(pending.ID, WriteActor{UserID: "1001", UserName: "张三"})
	if err != nil {
		t.Fatalf("ConfirmPendingWrite error = %v", err)
	}
	if confirmed.Status != PendingStatusConfirmed {
		t.Fatalf("confirmed status = %s, want confirmed", confirmed.Status)
	}
	if file.Content != "confirmed\n" {
		t.Fatalf("file content = %q", file.Content)
	}
	data, err := os.ReadFile(filepath.Join(root, "notes", "a.txt"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data) != "confirmed\n" {
		t.Fatalf("disk content = %q", string(data))
	}
}

func TestRejectPendingWriteDoesNotWriteFile(t *testing.T) {
	root := withTempProject(t)
	conf.Global = &conf.GlobalConfig{}
	conf.Global.Security.WorkspaceWriteConfirm = true
	ResetPendingWritesForTest()

	pending, err := PreviewWrite("current", "notes/a.txt", "rejected\n", WriteActor{UserID: "1001"})
	if err != nil {
		t.Fatalf("PreviewWrite error = %v", err)
	}
	rejected, err := RejectPendingWrite(pending.ID, WriteActor{UserID: "1001"}, "user rejected")
	if err != nil {
		t.Fatalf("RejectPendingWrite error = %v", err)
	}
	if rejected.Status != PendingStatusRejected {
		t.Fatalf("rejected status = %s, want rejected", rejected.Status)
	}
	if _, err := os.Stat(filepath.Join(root, "notes", "a.txt")); !os.IsNotExist(err) {
		t.Fatalf("file was written after reject, stat err = %v", err)
	}
}

func TestPreviewWriteRejectsSensitivePath(t *testing.T) {
	withTempProject(t)
	conf.Global = &conf.GlobalConfig{}
	conf.Global.Security.WorkspaceWriteConfirm = true
	ResetPendingWritesForTest()

	_, err := PreviewWrite("current", ".env", "OPENAI_API_KEY=secret", WriteActor{UserID: "1001"})
	if err == nil || !strings.Contains(err.Error(), "sensitive path") {
		t.Fatalf("PreviewWrite error = %v, want sensitive path error", err)
	}
}

func TestWorkspaceWriteAuditLogsPreviewAndConfirm(t *testing.T) {
	workspaceRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	withTempProject(t)
	logRoot := filepath.Join(workspaceRoot, ".tmp", "workspace-audit-test-"+time.Now().Format("20060102150405.000000000"))
	if err := os.MkdirAll(logRoot, 0755); err != nil {
		t.Fatalf("MkdirAll log root: %v", err)
	}
	restoreLogger := logger.ResetForTest()
	t.Cleanup(restoreLogger)
	conf.Global = &conf.GlobalConfig{}
	conf.Global.Logs.RootDir = logRoot
	conf.Global.Security.WorkspaceWriteConfirm = true
	ResetPendingWritesForTest()

	secretContent := "OPENAI_API_KEY=secret\nvisible=false\n"
	pending, err := PreviewWrite("current", "notes/audit.txt", secretContent, WriteActor{UserID: "1001", UserName: "张三", Source: "test"})
	if err != nil {
		t.Fatalf("PreviewWrite error = %v", err)
	}
	if _, _, err := ConfirmPendingWrite(pending.ID, WriteActor{UserID: "1001", UserName: "张三", Source: "test"}); err != nil {
		t.Fatalf("ConfirmPendingWrite error = %v", err)
	}
	logger.SyncAll()

	logPath := filepath.Join(logRoot, "global", "global-"+time.Now().Format("2006-01-02")+".log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile audit log: %v", err)
	}
	text := string(data)
	if !strings.Contains(text, "workspace_write_preview") || !strings.Contains(text, "workspace_write_confirmed") {
		t.Fatalf("audit log = %s, want preview and confirmed events", text)
	}
	// 安全断言：审计日志中出现文件内容原文或 diff 行即视为泄露，测试直接失败。
	for _, forbidden := range []string{"OPENAI_API_KEY=secret", "visible=false", "+OPENAI_API_KEY", secretContent} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("audit log leaked file content or diff %q: %s", forbidden, text)
		}
	}
}

func TestWorkspaceWriteAuditRecordsUnifiedResourceStats(t *testing.T) {
	withTempProject(t)
	conf.Global = &conf.GlobalConfig{}
	conf.Global.Security.WorkspaceWriteConfirm = true
	ResetPendingWritesForTest()
	stats.ResetForTest()

	pending, err := PreviewWrite("current", "notes/confirmed.txt", "confirmed\n", WriteActor{UserID: "1001", Source: "test"})
	if err != nil {
		t.Fatalf("PreviewWrite confirmed error = %v", err)
	}
	if _, _, err := ConfirmPendingWrite(pending.ID, WriteActor{UserID: "1001", Source: "test"}); err != nil {
		t.Fatalf("ConfirmPendingWrite error = %v", err)
	}
	rejected, err := PreviewWrite("current", "notes/rejected.txt", "rejected\n", WriteActor{UserID: "1002", Source: "test"})
	if err != nil {
		t.Fatalf("PreviewWrite rejected error = %v", err)
	}
	if _, err := RejectPendingWrite(rejected.ID, WriteActor{UserID: "1002", Source: "test"}, "user rejected"); err != nil {
		t.Fatalf("RejectPendingWrite error = %v", err)
	}
	if _, err := PreviewWrite("current", ".env", "OPENAI_API_KEY=secret", WriteActor{UserID: "1003", Source: "test"}); err == nil {
		t.Fatal("PreviewWrite sensitive path should fail")
	}

	summary := stats.ResourcePeriods(time.Now())["day"].Summary
	if summary.WorkspaceWritePending != 2 ||
		summary.WorkspaceWriteConfirmed != 1 ||
		summary.WorkspaceWriteRejected != 1 ||
		summary.WorkspaceWriteFailed != 1 ||
		summary.Errors != 1 {
		t.Fatalf("workspace stats summary = %+v, want pending/confirmed/rejected/failed counters", summary)
	}
	if summary.BySource[stats.SourceWorkspace] != 5 {
		t.Fatalf("BySource = %+v, want all workspace audit events counted", summary.BySource)
	}
	if summary.ByKind[stats.ResourceKindWorkspaceWritePending] != 2 ||
		summary.ByKind[stats.ResourceKindWorkspaceWriteConfirmed] != 1 ||
		summary.ByKind[stats.ResourceKindWorkspaceWriteRejected] != 1 ||
		summary.ByKind[stats.ResourceKindWorkspaceWriteFailed] != 1 {
		t.Fatalf("ByKind = %+v, want workspace write event kinds counted", summary.ByKind)
	}
}
