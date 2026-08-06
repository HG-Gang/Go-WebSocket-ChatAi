// internal/handler/web_chat_handler_test.go
// Web 聊天与附件上传链路的单元测试。
//
// 测试范围：
//   - supportsResponsesChat / buildChatInput / classifyUpload 的纯函数行为。
//   - WebUploadHandler 与请求明细/统计接口的上传-入库-查询闭环（sqlite 临时库）。
//   - WebChatHandler 对 azureai 协议不兼容配置的 501 拒绝。
//
// 注意：测试使用 t.TempDir 隔离上传目录与 sqlite 数据库，不触碰真实数据目录。
package handler

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/service/requestlog"
)

func TestSupportsResponsesChat(t *testing.T) {
	okCfg := &conf.ModelConfig{Enabled: true, Endpoint: "https://example.com/v1", APIKey: "k"}
	if !supportsResponsesChat("openairesponses", "openai", okCfg) {
		t.Fatal("openairesponses should support")
	}
	if supportsResponsesChat("azureai", "azure", okCfg) {
		t.Fatal("azureai should not support")
	}
	if supportsResponsesChat("x", "azure", okCfg) {
		t.Fatal("type=azure should not support")
	}
	if supportsResponsesChat("x", "openai", &conf.ModelConfig{Endpoint: ""}) {
		t.Fatal("empty endpoint should not support")
	}
}

func TestBuildChatInputUserAssistantAndAttachmentText(t *testing.T) {
	// seed text attachment
	dir := t.TempDir()
	path := filepath.Join(dir, "a.txt")
	if err := os.WriteFile(path, []byte("hello file"), 0o644); err != nil {
		t.Fatal(err)
	}
	id := "att-1"
	attachmentStore.mu.Lock()
	attachmentStore.items[id] = &attachmentMeta{
		ID: id, Name: "a.txt", Mime: "text/plain", Kind: "text", Path: path, Text: "hello file",
	}
	attachmentStore.mu.Unlock()
	t.Cleanup(func() {
		attachmentStore.mu.Lock()
		delete(attachmentStore.items, id)
		attachmentStore.mu.Unlock()
	})

	input, err := buildChatInput([]chatMessage{
		{Role: "user", Content: "总结附件", AttachmentIDs: []string{id}},
		{Role: "assistant", Content: "好的"},
		{Role: "user", Content: "继续"},
	})
	if err != nil {
		t.Fatalf("buildChatInput: %v", err)
	}
	items, ok := input.([]map[string]any)
	if !ok || len(items) != 3 {
		t.Fatalf("items type/len=%T %v", input, len(items))
	}
	if items[0]["role"] != "user" {
		t.Fatalf("role0=%v", items[0]["role"])
	}
	parts0, _ := items[0]["content"].([]map[string]any)
	if len(parts0) != 1 || parts0[0]["type"] != "input_text" {
		t.Fatalf("parts0=%v", parts0)
	}
	text, _ := parts0[0]["text"].(string)
	if !strings.Contains(text, "hello file") || !strings.Contains(text, "总结附件") {
		t.Fatalf("text=%s", text)
	}
	if items[1]["role"] != "assistant" {
		t.Fatalf("role1=%v", items[1]["role"])
	}
	parts1, _ := items[1]["content"].([]map[string]any)
	if parts1[0]["type"] != "output_text" {
		t.Fatalf("assistant part type=%v", parts1[0]["type"])
	}
}

func TestBuildChatInputMissingAttachment(t *testing.T) {
	_, err := buildChatInput([]chatMessage{
		{Role: "user", Content: "x", AttachmentIDs: []string{"no-such"}},
	})
	if err == nil || !strings.Contains(err.Error(), "attachment not found") {
		t.Fatalf("err=%v", err)
	}
}

func TestClassifyUpload(t *testing.T) {
	cases := []struct {
		mime, name, want string
		ok               bool
	}{
		{"image/png", "a.png", "image", true},
		{"application/pdf", "a.pdf", "pdf", true},
		{"text/plain", "a.txt", "text", true},
		{"application/octet-stream", "a.pdf", "pdf", true},
		{"application/zip", "a.zip", "", false},
	}
	for _, tc := range cases {
		got, ok := classifyUpload(tc.mime, tc.name)
		if ok != tc.ok || got != tc.want {
			t.Fatalf("%s/%s => %s,%v want %s,%v", tc.mime, tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

func TestWebUploadAndRequestsListClosedLoop(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dir := t.TempDir()
	dsn := filepath.Join(dir, "t.db")
	if err := requestlog.Init(true, "sqlite", dsn); err != nil {
		t.Fatalf("db init: %v", err)
	}
	t.Cleanup(requestlog.Close)

	// 避免测试把上传文件写到仓库相对路径 ./data/uploads
	prevGlobal := conf.Global
	conf.Global = &conf.GlobalConfig{}
	conf.Global.WebChat.UploadDir = filepath.Join(dir, "uploads")
	conf.Global.WebChat.MaxUploadBytes = 1 << 20
	conf.Global.WebChat.MaxPDFChars = 10000
	t.Cleanup(func() { conf.Global = prevGlobal })

	// upload
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	part, err := w.CreateFormFile("file", "note.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write([]byte("token usage test content")); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	r := gin.New()
	r.POST("/upload", WebUploadHandler)
	r.GET("/requests", WebRequestsHandler)
	r.GET("/stats", WebRequestStatsHandler)

	req := httptest.NewRequest(http.MethodPost, "/upload", &body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("upload status=%d body=%s", rec.Code, rec.Body.String())
	}
	var up map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &up); err != nil {
		t.Fatal(err)
	}
	if int(up["code"].(float64)) != 200 {
		t.Fatalf("upload resp=%v", up)
	}
	data := up["data"].(map[string]any)
	if data["kind"] != "text" || data["id"] == "" {
		t.Fatalf("data=%v", data)
	}

	// insert a request log as chat would
	_, err = requestlog.Insert(req.Context(), requestlog.Record{
		RequestID:    "resp_test_1",
		ModelConfig:  "openairesponses",
		Model:        "gpt-4.1",
		Provider:     "openairesponses",
		InputTokens:  11,
		OutputTokens: 22,
		TotalTokens:  33,
		Status:       "completed",
		Timestamp:    1_700_000_000_000,
		Time:         "2026-07-19 12:00:00",
	})
	if err != nil {
		t.Fatal(err)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/requests?page=1&size=10&model=gpt-4.1", nil)
	rec2 := httptest.NewRecorder()
	r.ServeHTTP(rec2, req2)
	if rec2.Code != 200 {
		t.Fatalf("list status=%d body=%s", rec2.Code, rec2.Body.String())
	}
	var list map[string]any
	_ = json.Unmarshal(rec2.Body.Bytes(), &list)
	if int(list["total"].(float64)) < 1 {
		t.Fatalf("list=%v", list)
	}

	req3 := httptest.NewRequest(http.MethodGet, "/stats?period=month", nil)
	rec3 := httptest.NewRecorder()
	r.ServeHTTP(rec3, req3)
	if rec3.Code != 200 {
		t.Fatalf("stats status=%d body=%s", rec3.Code, rec3.Body.String())
	}
}

func TestWebChatRejectsAzure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	// inject fake global model into GetModel cache
	prev := conf.Global
	conf.Global = &conf.GlobalConfig{
		Models: map[string]conf.ModelConfig{
			"azureai": {
				Enabled:  true,
				Endpoint: "https://example.openai.azure.com",
				APIKey:   "x",
				Extra:    map[string]interface{}{"type": "azure"},
			},
		},
	}
	conf.InitModelConfig()
	t.Cleanup(func() {
		conf.Global = prev
		if prev != nil {
			conf.InitModelConfig()
		}
	})

	r := gin.New()
	r.POST("/chat", WebChatHandler)
	body := `{"model_config":"azureai","messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/chat", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}
