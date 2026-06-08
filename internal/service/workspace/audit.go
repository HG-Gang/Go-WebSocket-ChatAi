package workspace

import (
	"strings"

	"go.uber.org/zap"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/logger"
	"TozoAI-Chat-Api/internal/service/stats"
)

// logWorkspaceWriteAudit 写入 Workspace 文件修改审计事件。
// 审计日志只记录路径、状态、操作者和 diff hash，不记录 Before/After/Diff 原文，
// 避免把源码、密钥或业务内容泄露到日志文件。
func logWorkspaceWriteAudit(event string, pending PendingWrite) {
	if kind := workspaceStatsKind(event); kind != "" {
		stats.RecordResourceEvent(stats.ResourceEvent{
			Source:   stats.SourceWorkspace,
			Kind:     kind,
			UserID:   pending.UserID,
			UserName: pending.UserName,
			Status:   pending.Status,
			Error:    workspaceStatsError(kind, pending),
		})
	}
	if conf.Global == nil || strings.TrimSpace(conf.Global.Logs.RootDir) == "" {
		return
	}
	logger.GetModelLogger("global").Info("workspace write audit",
		zap.String("event", event),
		zap.String("pending_write_id", pending.ID),
		zap.String("project_id", pending.ProjectID),
		zap.String("path", pending.Path),
		zap.String("status", pending.Status),
		zap.String("user_id", pending.UserID),
		zap.String("user_name", pending.UserName),
		zap.String("request_id", pending.RequestID),
		zap.String("source", pending.Source),
		zap.String("reason", pending.Reason),
		zap.String("diff_hash", pending.DiffHash),
		zap.Int64("created_at", pending.CreatedAt),
		zap.Int64("updated_at", pending.UpdatedAt),
	)
}

func workspaceStatsKind(event string) string {
	switch strings.TrimSpace(event) {
	case "workspace_write_preview":
		return stats.ResourceKindWorkspaceWritePending
	case "workspace_write_confirmed":
		return stats.ResourceKindWorkspaceWriteConfirmed
	case "workspace_write_rejected":
		return stats.ResourceKindWorkspaceWriteRejected
	case "workspace_write_failed":
		return stats.ResourceKindWorkspaceWriteFailed
	default:
		return ""
	}
}

func workspaceStatsError(kind string, pending PendingWrite) string {
	if kind == stats.ResourceKindWorkspaceWriteFailed {
		return pending.Reason
	}
	return ""
}
