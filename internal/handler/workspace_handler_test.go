package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/service/workspace"
)

// withWorkspaceHandlerProject 为 handler 测试创建临时 current 项目。
// Workspace handler 最终会调用 service/workspace，后者用 os.Getwd() 定位当前项目根目录。
func withWorkspaceHandlerProject(t *testing.T) string {
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
	conf.Global = &conf.GlobalConfig{}
	conf.Global.Security.WorkspaceWriteConfirm = true
	workspace.ResetPendingWritesForTest()
	gin.SetMode(gin.TestMode)
	t.Cleanup(func() {
		_ = os.Chdir(oldWD)
		conf.Global = oldConf
		workspace.ResetPendingWritesForTest()
	})
	return tempDir
}

// 写入确认开启时，HTTP 写接口只能返回 pending diff，不能直接落盘。
// 这个测试防止模型工具或前端保存按钮绕过用户确认修改项目文件。
func TestWorkspaceWriteHandlerReturnsPendingWhenConfirmEnabled(t *testing.T) {
	root := withWorkspaceHandlerProject(t)
	r := gin.New()
	r.POST("/api/workspace/write", WorkspaceWriteHandler)

	body := bytes.NewBufferString(`{"project_id":"current","path":"notes/a.txt","content":"pending\n"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/workspace/write", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	var payload struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Data["pending_write_id"] == "" || payload.Data["diff"] == "" {
		t.Fatalf("data = %+v, want pending_write_id and diff", payload.Data)
	}
	if _, ok := payload.Data["file"]; ok {
		t.Fatalf("data includes file = %+v, want pending preview only", payload.Data)
	}
	if _, err := os.Stat(filepath.Join(root, "notes", "a.txt")); !os.IsNotExist(err) {
		t.Fatalf("file was written during HTTP preview, stat err = %v", err)
	}
}

// 确认和拒绝是 pending 写入的两个终态。
// 这个测试同时证明确认会写盘，拒绝不会写盘，避免文件修改链路出现静默成功。
func TestWorkspaceConfirmAndRejectHandlers(t *testing.T) {
	root := withWorkspaceHandlerProject(t)
	r := gin.New()
	r.POST("/api/workspace/write", WorkspaceWriteHandler)
	r.POST("/api/workspace/write/confirm", WorkspaceWriteConfirmHandler)
	r.POST("/api/workspace/write/reject", WorkspaceWriteRejectHandler)

	writeBody := bytes.NewBufferString(`{"project_id":"current","path":"notes/confirm.txt","content":"confirmed\n"}`)
	writeReq := httptest.NewRequest(http.MethodPost, "/api/workspace/write", writeBody)
	writeReq.Header.Set("Content-Type", "application/json")
	writeW := httptest.NewRecorder()
	r.ServeHTTP(writeW, writeReq)
	if writeW.Code != http.StatusOK {
		t.Fatalf("write status = %d body = %s", writeW.Code, writeW.Body.String())
	}
	pendingID := responseDataString(t, writeW.Body.Bytes(), "pending_write_id")

	confirmBody := bytes.NewBufferString(`{"pending_write_id":"` + pendingID + `"}`)
	confirmReq := httptest.NewRequest(http.MethodPost, "/api/workspace/write/confirm", confirmBody)
	confirmReq.Header.Set("Content-Type", "application/json")
	confirmW := httptest.NewRecorder()
	r.ServeHTTP(confirmW, confirmReq)
	if confirmW.Code != http.StatusOK {
		t.Fatalf("confirm status = %d body = %s", confirmW.Code, confirmW.Body.String())
	}
	data, err := os.ReadFile(filepath.Join(root, "notes", "confirm.txt"))
	if err != nil {
		t.Fatalf("read confirmed file: %v", err)
	}
	if string(data) != "confirmed\n" {
		t.Fatalf("confirmed content = %q", string(data))
	}

	rejectBody := bytes.NewBufferString(`{"project_id":"current","path":"notes/reject.txt","content":"rejected\n"}`)
	rejectReq := httptest.NewRequest(http.MethodPost, "/api/workspace/write", rejectBody)
	rejectReq.Header.Set("Content-Type", "application/json")
	rejectW := httptest.NewRecorder()
	r.ServeHTTP(rejectW, rejectReq)
	rejectID := responseDataString(t, rejectW.Body.Bytes(), "pending_write_id")

	doRejectBody := bytes.NewBufferString(`{"pending_write_id":"` + rejectID + `","reason":"user rejected"}`)
	doRejectReq := httptest.NewRequest(http.MethodPost, "/api/workspace/write/reject", doRejectBody)
	doRejectReq.Header.Set("Content-Type", "application/json")
	doRejectW := httptest.NewRecorder()
	r.ServeHTTP(doRejectW, doRejectReq)
	if doRejectW.Code != http.StatusOK {
		t.Fatalf("reject status = %d body = %s", doRejectW.Code, doRejectW.Body.String())
	}
	if _, err := os.Stat(filepath.Join(root, "notes", "reject.txt")); !os.IsNotExist(err) {
		t.Fatalf("file was written after reject, stat err = %v", err)
	}
}

func responseDataString(t *testing.T, body []byte, key string) string {
	t.Helper()
	var payload struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode response: %v body=%s", err, string(body))
	}
	value, _ := payload.Data[key].(string)
	if value == "" {
		t.Fatalf("data[%s] missing in %+v", key, payload.Data)
	}
	return value
}
