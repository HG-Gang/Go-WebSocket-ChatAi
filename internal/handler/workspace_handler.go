// internal/handler/workspace_handler.go
// Workspace HTTP 处理器：暴露当前项目的受控文件浏览与写入视图。
//
// 文件功能：
//   - 项目浏览：WorkspaceProjectsHandler/WorkspaceListHandler/WorkspaceReadHandler。
//   - 文件写入：WorkspaceWriteHandler 在安全配置开启时只创建 pending diff，必须再走确认接口才真正落盘；
//     关闭确认的直写路径仅用于兼容旧配置，生产环境不应关闭 WorkspaceWriteConfirm。
//
// 安全边界：
//   - project_id 目前只接受 current，所有 path 都在 service 层解析为项目内相对路径。
//   - 确认/拒绝接口都会写入审计 actor（user_id/user_name/request_id），便于追溯每次落盘来源。
package handler

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/service/workspace"
)

// workspaceFileRequest 是前端或模型工具 HTTP 代理提交的文件写入请求。
// ProjectID 当前只允许 current，Path 是项目内相对路径，Content 是 UTF-8 文本内容。
type workspaceFileRequest struct {
	ProjectID string `json:"project_id"`
	Path      string `json:"path"`
	Content   string `json:"content"`
}

// workspacePendingRequest 用于确认或拒绝一次待写入申请。
// PendingWriteID 来自 WorkspaceWriteHandler 返回的 pending_write_id。
type workspacePendingRequest struct {
	PendingWriteID string `json:"pending_write_id"`
	Reason         string `json:"reason"`
}

// WorkspaceProjectsHandler 返回可选择的项目列表。
// 当前桌面线程只有一个工作区，返回 current 是为了让前端和模型工具保持稳定契约。
func WorkspaceProjectsHandler(c *gin.Context) {
	projects, err := workspace.Projects()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 200, "data": projects})
}

// WorkspaceListHandler 列出项目内目录。
// path 来自 query string，service 层会跳过 .git、vendor、node_modules 等高噪声目录并限制返回数量。
func WorkspaceListHandler(c *gin.Context) {
	entries, err := workspace.List(c.DefaultQuery("project_id", "current"), c.Query("path"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 200, "data": entries})
}

// WorkspaceReadHandler 读取项目内 UTF-8 文本文件。
// 这里只返回内容给已鉴权调用方；二进制、超大文件和目录读取由 service 层拒绝并返回明确错误。
func WorkspaceReadHandler(c *gin.Context) {
	file, err := workspace.Read(c.DefaultQuery("project_id", "current"), c.Query("path"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 200, "data": file})
}

// WorkspaceWriteHandler 接收一次完整文件内容写入。
// 默认安全路径是 PreviewWrite：只生成 diff 和 pending_write_id，前端必须再调用确认接口；
// 兼容旧配置关闭确认时才会直接 Write，生产环境不应关闭 WorkspaceWriteConfirm。
func WorkspaceWriteHandler(c *gin.Context) {
	var req workspaceFileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": err.Error()})
		return
	}
	projectID := defaultProjectID(req.ProjectID)
	// 安全开关开启时只生成 pending diff，落盘必须等待用户确认，防止模型工具绕过确认直接修改项目文件。
	if workspaceWriteConfirmEnabled() {
		pending, err := workspace.PreviewWrite(projectID, req.Path, req.Content, workspaceActor(c, "http"))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"code": 200, "data": pendingWriteResponse(pending)})
		return
	}

	file, err := workspace.Write(projectID, req.Path, req.Content)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 200, "data": file})
}

// WorkspaceWriteConfirmHandler 将 pending 写入真正落盘。
// pending_write_id 是唯一必填参数，actor 会写入审计日志，便于追溯是哪次 HTTP/WS 请求触发了落盘。
func WorkspaceWriteConfirmHandler(c *gin.Context) {
	var req workspacePendingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": err.Error()})
		return
	}
	if strings.TrimSpace(req.PendingWriteID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "pending_write_id is required"})
		return
	}
	file, pending, err := workspace.ConfirmPendingWrite(req.PendingWriteID, workspaceActor(c, "http"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": err.Error(), "data": pendingWriteResponse(pending)})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 200, "data": gin.H{
		"file":    file,
		"pending": pendingWriteResponse(pending),
	}})
}

// WorkspaceWriteRejectHandler 拒绝一次 pending 写入。
// 拒绝不会触碰磁盘；reason 只作为审计说明，不能被当成权限判断依据。
func WorkspaceWriteRejectHandler(c *gin.Context) {
	var req workspacePendingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": err.Error()})
		return
	}
	if strings.TrimSpace(req.PendingWriteID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "pending_write_id is required"})
		return
	}
	pending, err := workspace.RejectPendingWrite(req.PendingWriteID, workspaceActor(c, "http"), req.Reason)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": err.Error(), "data": pendingWriteResponse(pending)})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 200, "data": pendingWriteResponse(pending)})
}

// defaultProjectID 兼容前端未传 project_id 的场景。
// 当前没有多项目注册表，空值统一映射为 current。
func defaultProjectID(projectID string) string {
	if projectID == "" {
		return "current"
	}
	return projectID
}

// workspaceWriteConfirmEnabled 是写入安全开关。
// conf.Global 为空通常只发生在单元测试；真实服务应通过配置明确控制。
func workspaceWriteConfirmEnabled() bool {
	return conf.Global != nil && conf.Global.Security.WorkspaceWriteConfirm
}

// workspaceActor 从 Gin 上下文提取审计身份。
// UserID/UserName 来自 JWT 中间件，RequestID 用于串联日志，Source 区分 http 与模型工具链路。
func workspaceActor(c *gin.Context, source string) workspace.WriteActor {
	return workspace.WriteActor{
		UserID:    c.GetString("user_id"),
		UserName:  c.GetString("user_name"),
		RequestID: c.GetString("request_id"),
		Source:    source,
	}
}

// pendingWriteResponse 统一 pending 写入的前端响应结构。
// 返回 diff 和 diff_hash 供用户确认；Before/After 不直接透出，避免把完整文件内容扩大到响应面。
func pendingWriteResponse(pending workspace.PendingWrite) gin.H {
	return gin.H{
		"pending_write_id": pending.ID,
		"project_id":       pending.ProjectID,
		"path":             pending.Path,
		"diff":             pending.Diff,
		"diff_hash":        pending.DiffHash,
		"status":           pending.Status,
		"user_id":          pending.UserID,
		"user_name":        pending.UserName,
		"reason":           pending.Reason,
		"created_at":       pending.CreatedAt,
		"updated_at":       pending.UpdatedAt,
	}
}
