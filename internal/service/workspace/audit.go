// internal/service/workspace/audit.go
// 文件功能：Workspace 写操作的审计与资源统计上报。输入是事件名（preview/confirmed/
// rejected/failed）和 PendingWrite；输出是 stats 资源事件与结构化审计日志。
// 安全边界：日志只记录路径、状态、操作者和 diff hash，不记录 Before/After/Diff 原文，
// 避免源码或密钥泄露；审计落盘失败不影响写盘主流程，日志目录未配置时静默跳过。
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
	// 先把事件归一为资源统计 kind 并上报，失败事件额外携带原因，便于告警归因。
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
	// 日志目录未配置（如单测或精简部署）时跳过落盘；审计失败不影响写盘主流程。
	if conf.Global == nil || strings.TrimSpace(conf.Global.Logs.RootDir) == "" {
		return
	}
	// 只记录元数据字段，任何文件内容或 diff 原文都不允许出现在审计日志里。
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

// workspaceStatsKind 把审计事件名映射为统一的资源统计 kind；未知事件返回空串，表示只记日志不上报统计。
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

// workspaceStatsError 只在 failed 事件中携带失败原因，其他事件保持空串，避免审批细节进入统计摘要。
func workspaceStatsError(kind string, pending PendingWrite) string {
	if kind == stats.ResourceKindWorkspaceWriteFailed {
		return pending.Reason
	}
	return ""
}
