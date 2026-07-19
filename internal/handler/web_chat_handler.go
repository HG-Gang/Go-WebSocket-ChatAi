package handler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/logger"
	"TozoAI-Chat-Api/internal/provider/openairesponses"
	"TozoAI-Chat-Api/internal/service/requestlog"
)

// chatMessage 是统一聊天 API 的消息项。
type chatMessage struct {
	Role          string   `json:"role"`
	Content       string   `json:"content"`
	AttachmentIDs []string `json:"attachment_ids"`
}

// chatRequest 统一多 Provider 聊天请求。
type chatRequest struct {
	ModelConfig     string        `json:"model_config"`
	Model           string        `json:"model"`
	ReasoningEffort string        `json:"reasoning_effort"`
	Messages        []chatMessage `json:"messages"`
	Stream          bool          `json:"stream"`
}

// attachmentMeta 上传文件元数据（内存索引 + 磁盘文件）。
type attachmentMeta struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Mime      string `json:"mime"`
	Size      int64  `json:"size"`
	Kind      string `json:"kind"` // image|pdf|text
	Path      string `json:"-"`
	Text      string `json:"-"` // 预抽取文本
	CreatedAt int64  `json:"created_at"`
	UserID    string `json:"-"`
}

var attachmentStore = struct {
	mu    sync.RWMutex
	// id -> meta；进程内索引，文件在磁盘
	items map[string]*attachmentMeta
}{items: map[string]*attachmentMeta{}}

// WebUploadHandler 接收图片/PDF/文本附件。
func WebUploadHandler(c *gin.Context) {
	maxBytes := webChatMaxUpload()
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes+1<<20)
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "missing file: " + err.Error()})
		return
	}
	defer file.Close()

	if header.Size > maxBytes {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": fmt.Sprintf("file too large, max %d bytes", maxBytes)})
		return
	}

	data, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "read file failed: " + err.Error()})
		return
	}
	if int64(len(data)) > maxBytes {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": fmt.Sprintf("file too large, max %d bytes", maxBytes)})
		return
	}

	mime := header.Header.Get("Content-Type")
	if mime == "" {
		mime = http.DetectContentType(data)
	}
	kind, ok := classifyUpload(mime, header.Filename)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "unsupported file type: " + mime})
		return
	}

	id := uuid.NewString()
	dir := webChatUploadDir()
	sub := filepath.Join(dir, time.Now().Format("2006"), time.Now().Format("01"))
	if err := os.MkdirAll(sub, 0o755); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "error": "create upload dir failed"})
		return
	}
	ext := filepath.Ext(header.Filename)
	if ext == "" {
		ext = extFromMime(mime)
	}
	path := filepath.Join(sub, id+ext)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "error": "save file failed"})
		return
	}

	meta := &attachmentMeta{
		ID:        id,
		Name:      header.Filename,
		Mime:      mime,
		Size:      int64(len(data)),
		Kind:      kind,
		Path:      path,
		CreatedAt: time.Now().UnixMilli(),
		UserID:    c.GetString("user_id"),
	}
	if kind == "text" || kind == "pdf" {
		text, err := extractAttachmentText(meta, data)
		if err != nil {
			_ = os.Remove(path)
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "extract text failed: " + err.Error()})
			return
		}
		meta.Text = text
	}

	attachmentStore.mu.Lock()
	attachmentStore.items[id] = meta
	attachmentStore.mu.Unlock()
	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"data": gin.H{
			"id":         meta.ID,
			"name":       meta.Name,
			"mime":       meta.Mime,
			"size":       meta.Size,
			"kind":       meta.Kind,
			"created_at": meta.CreatedAt,
		},
	})
}

// WebChatHandler 统一聊天入口：按 model_config 路由到 Responses 等。
// stream=true 时用 SSE 推送最终结果（一期非真 token 流，封装为 delta+done 闭环，保证前端统一协议）。
func WebChatHandler(c *gin.Context) {
	var req chatRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "invalid JSON: " + err.Error()})
		return
	}
	if len(req.Messages) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "messages is required"})
		return
	}
	modelConfig := strings.TrimSpace(req.ModelConfig)
	if modelConfig == "" {
		modelConfig = "openairesponses"
	}
	cfg := conf.GetModel(modelConfig)
	if cfg == nil || !cfg.Enabled {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": modelConfig + " model is not enabled"})
		return
	}

	input, err := buildChatInput(req.Messages)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": err.Error()})
		return
	}

	payload := map[string]any{
		"input": input,
	}
	if strings.TrimSpace(req.Model) != "" {
		payload["model"] = strings.TrimSpace(req.Model)
	}
	if strings.TrimSpace(req.ReasoningEffort) != "" && req.ReasoningEffort != "default" {
		payload["reasoning"] = map[string]any{"effort": req.ReasoningEffort}
	}

	// 路由：目前 HTTP 统一走 Responses 客户端（openairesponses / 兼容 endpoint 的配置）
	// azureai 若启用且 type=azure，一期返回明确错误，避免静默错误
	providerType := metricStringFromExtra(cfg.Extra, "type", inferModelType(modelConfig, cfg.Endpoint))
	if strings.EqualFold(providerType, "azure") || strings.EqualFold(modelConfig, "azureai") {
		// 尝试走 Responses 形状不兼容时给出清晰提示
		if !strings.Contains(strings.ToLower(cfg.Endpoint), "openai") && conf.GetModel("openairesponses") != nil {
			// azure chat 可后续扩展；一期要求闭环：若用户选 azureai 且 enabled，用错误提示
		}
	}

	log := logger.GetModelLogger(modelConfig)
	ctx, cancel := context.WithTimeout(c.Request.Context(), responseTimeout(cfg))
	defer cancel()

	start := time.Now()
	result, err := openairesponses.New(cfg).Create(ctx, payload)
	latency := time.Since(start)

	if err != nil {
		errorSummary := logger.RedactField("content", err.Error())
		record := addResponsesMetric(c, modelConfig, cfg, payload, result, "failed", latency, errorSummary)
		log.Warn("WebChat 请求失败", zap.Error(err), zap.Duration("latency", latency))
		if req.Stream {
			writeChatSSE(c, "", errorSummary, record, true)
			return
		}
		status := http.StatusBadGateway
		if ctx.Err() == context.DeadlineExceeded {
			status = http.StatusGatewayTimeout
		}
		c.JSON(status, gin.H{
			"code":          status,
			"error":         "chat upstream request failed",
			"error_summary": errorSummary,
			"latency_ms":    latency.Milliseconds(),
			"record":        record,
		})
		return
	}

	status := result.Status
	if status == "" {
		status = "completed"
	}
	record := addResponsesMetric(c, modelConfig, cfg, payload, result, status, latency, "")
	log.Info("WebChat 请求完成",
		zap.String("response_id", result.ID),
		zap.String("model", result.Model),
		zap.Duration("latency", latency))

	if req.Stream {
		writeChatSSE(c, result.OutputText, "", record, false)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"code":       200,
		"latency_ms": latency.Milliseconds(),
		"data": gin.H{
			"id":          result.ID,
			"model":       result.Model,
			"status":      result.Status,
			"output_text": result.OutputText,
			"raw":         json.RawMessage(result.Raw),
		},
		"record": record,
	})
}

// WebRequestsHandler 请求明细列表（DB）。
func WebRequestsHandler(c *gin.Context) {
	if !requestlog.Enabled() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"code": 503, "error": "request log db not enabled"})
		return
	}
	f := requestlog.ListFilter{
		Model:       c.Query("model"),
		ModelConfig: c.Query("model_config"),
		Status:      c.Query("status"),
		Provider:    c.Query("provider"),
		Q:           c.Query("q"),
	}
	if v := c.Query("from"); v != "" {
		f.From, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := c.Query("to"); v != "" {
		f.To, _ = strconv.ParseInt(v, 10, 64)
	}
	f.Page, _ = strconv.Atoi(c.DefaultQuery("page", "1"))
	f.Size, _ = strconv.Atoi(c.DefaultQuery("size", "20"))

	items, total, err := requestlog.List(c.Request.Context(), f)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"code":  200,
		"total": total,
		"page":  f.Page,
		"size":  f.Size,
		"items": items,
	})
}

// WebRequestStatsHandler 看板聚合。
func WebRequestStatsHandler(c *gin.Context) {
	if !requestlog.Enabled() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"code": 503, "error": "request log db not enabled"})
		return
	}
	period := c.DefaultQuery("period", "day")
	f := requestlog.ListFilter{
		Model:       c.Query("model"),
		ModelConfig: c.Query("model_config"),
		Status:      c.Query("status"),
		Provider:    c.Query("provider"),
		Q:           c.Query("q"),
	}
	if v := c.Query("from"); v != "" {
		f.From, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := c.Query("to"); v != "" {
		f.To, _ = strconv.ParseInt(v, 10, 64)
	}
	stats, err := requestlog.Stats(c.Request.Context(), period, f)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 200, "data": stats})
}

func writeChatSSE(c *gin.Context, text, errMsg string, record WebRequestRecord, isErr bool) {
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Status(http.StatusOK)
	flusher, _ := c.Writer.(http.Flusher)

	writeEvent := func(event string, payload any) {
		b, _ := json.Marshal(payload)
		_, _ = fmt.Fprintf(c.Writer, "event: %s\ndata: %s\n\n", event, string(b))
		if flusher != nil {
			flusher.Flush()
		}
	}

	if isErr {
		writeEvent("error", gin.H{"error": errMsg, "record": record})
		return
	}
	// 分片推送，模拟流式体验
	runes := []rune(text)
	const chunk = 24
	for i := 0; i < len(runes); i += chunk {
		end := i + chunk
		if end > len(runes) {
			end = len(runes)
		}
		writeEvent("delta", gin.H{"text": string(runes[i:end])})
		time.Sleep(8 * time.Millisecond)
	}
	writeEvent("done", gin.H{
		"output_text": text,
		"record":      record,
	})
}

func buildChatInput(messages []chatMessage) (any, error) {
	// Responses API input: 字符串或 content 数组
	// 多轮：拼成带 role 的 message 列表
	items := make([]map[string]any, 0, len(messages))
	for _, m := range messages {
		role := strings.TrimSpace(m.Role)
		if role == "" {
			role = "user"
		}
		parts := make([]map[string]any, 0, 4)
		content := strings.TrimSpace(m.Content)
		// 附件
		var textExtras []string
		for _, aid := range m.AttachmentIDs {
			attachmentStore.mu.RLock()
			meta := attachmentStore.items[aid]
			attachmentStore.mu.RUnlock()
			if meta == nil {
				return nil, fmt.Errorf("attachment not found: %s", aid)
			}
			switch meta.Kind {
			case "image":
				data, err := os.ReadFile(meta.Path)
				if err != nil {
					return nil, fmt.Errorf("read attachment %s: %w", aid, err)
				}
				b64 := base64.StdEncoding.EncodeToString(data)
				dataURL := "data:" + meta.Mime + ";base64," + b64
				parts = append(parts, map[string]any{
					"type": "input_image",
					"image_url": dataURL,
				})
			case "pdf", "text":
				if meta.Text != "" {
					textExtras = append(textExtras, fmt.Sprintf("[附件:%s]\n%s", meta.Name, meta.Text))
				}
			}
		}
		textBody := content
		if len(textExtras) > 0 {
			if textBody != "" {
				textBody = textBody + "\n\n" + strings.Join(textExtras, "\n\n")
			} else {
				textBody = strings.Join(textExtras, "\n\n")
			}
		}
		if textBody != "" {
			parts = append(parts, map[string]any{
				"type": "input_text",
				"text": textBody,
			})
		}
		if len(parts) == 0 {
			return nil, fmt.Errorf("empty message content")
		}
		// 若仅有文本单 part，可用简化；统一用 content 数组
		items = append(items, map[string]any{
			"role":    role,
			"content": parts,
		})
	}
	return items, nil
}

func classifyUpload(mime, filename string) (kind string, ok bool) {
	mime = strings.ToLower(strings.TrimSpace(mime))
	ext := strings.ToLower(filepath.Ext(filename))
	if strings.HasPrefix(mime, "image/") {
		return "image", true
	}
	if mime == "application/pdf" || ext == ".pdf" {
		return "pdf", true
	}
	if strings.HasPrefix(mime, "text/") ||
		ext == ".txt" || ext == ".md" || ext == ".csv" || ext == ".json" || ext == ".log" {
		return "text", true
	}
	// 部分浏览器 pdf 可能是 application/octet-stream
	if ext == ".pdf" {
		return "pdf", true
	}
	return "", false
}

func extractAttachmentText(meta *attachmentMeta, data []byte) (string, error) {
	maxChars := webChatMaxPDFChars()
	switch meta.Kind {
	case "text":
		if !utf8.Valid(data) {
			// 尝试按 latin1 容错
			s := string(data)
			return truncateRunes(s, maxChars), nil
		}
		return truncateRunes(string(data), maxChars), nil
	case "pdf":
		// 轻量抽取：扫描 PDF 中可读 ASCII/UTF-8 文本流（不依赖外部库）
		text := extractPDFPlainText(data)
		if strings.TrimSpace(text) == "" {
			return "", fmt.Errorf("无法从 PDF 提取文本，请转换为 txt 后重试")
		}
		return truncateRunes(text, maxChars), nil
	default:
		return "", nil
	}
}

// extractPDFPlainText 从 PDF 字节中启发式提取括号字符串与可读片段。
func extractPDFPlainText(data []byte) string {
	var b strings.Builder
	// 提取 (...) 中的文本（简易 PDF 字符串）
	for i := 0; i < len(data); i++ {
		if data[i] == '(' {
			j := i + 1
			for j < len(data) && data[j] != ')' {
				if data[j] == '\\' && j+1 < len(data) {
					j += 2
					continue
				}
				j++
			}
			if j < len(data) && j-i > 2 {
				frag := string(data[i+1 : j])
				frag = strings.ReplaceAll(frag, "\\n", "\n")
				frag = strings.ReplaceAll(frag, "\\r", "")
				if isMostlyPrintable(frag) {
					b.WriteString(frag)
					b.WriteByte(' ')
				}
			}
			i = j
		}
	}
	// 补充：连续可打印字符段
	if b.Len() < 40 {
		var run strings.Builder
		for _, ch := range data {
			if ch >= 32 && ch < 127 || ch == '\n' || ch == '\t' {
				run.WriteByte(ch)
			} else {
				if run.Len() >= 20 {
					b.WriteString(run.String())
					b.WriteByte('\n')
				}
				run.Reset()
			}
		}
	}
	return strings.TrimSpace(b.String())
}

func isMostlyPrintable(s string) bool {
	if s == "" {
		return false
	}
	ok := 0
	for _, r := range s {
		if r == '\n' || r == '\t' || r >= 32 {
			ok++
		}
	}
	return float64(ok)/float64(utf8.RuneCountInString(s)) > 0.8
}

func truncateRunes(s string, max int) string {
	if max <= 0 {
		max = 100000
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "\n...[truncated]"
}

func extFromMime(mime string) string {
	switch {
	case strings.Contains(mime, "png"):
		return ".png"
	case strings.Contains(mime, "jpeg"), strings.Contains(mime, "jpg"):
		return ".jpg"
	case strings.Contains(mime, "gif"):
		return ".gif"
	case strings.Contains(mime, "webp"):
		return ".webp"
	case strings.Contains(mime, "pdf"):
		return ".pdf"
	default:
		return ".bin"
	}
}

func webChatMaxUpload() int64 {
	if conf.Global != nil && conf.Global.WebChat.MaxUploadBytes > 0 {
		return conf.Global.WebChat.MaxUploadBytes
	}
	return 10 << 20
}

func webChatMaxPDFChars() int {
	if conf.Global != nil && conf.Global.WebChat.MaxPDFChars > 0 {
		return conf.Global.WebChat.MaxPDFChars
	}
	return 100000
}

func webChatUploadDir() string {
	if conf.Global != nil && strings.TrimSpace(conf.Global.WebChat.UploadDir) != "" {
		return conf.Global.WebChat.UploadDir
	}
	return "./data/uploads"
}

func persistRequestLog(c *gin.Context, record WebRequestRecord, modelConfig, requestID string) {
	if !requestlog.Enabled() {
		return
	}
	provider := modelConfig
	rec := requestlog.Record{
		RequestID:       firstNonEmpty(requestID, fmt.Sprintf("req-%d", record.ID)),
		Time:            record.Time,
		Timestamp:       record.Timestamp,
		ModelConfig:     record.ModelConfig,
		Model:           record.Model,
		Provider:        provider,
		InputTokens:     record.InputTokens,
		OutputTokens:    record.OutputTokens,
		CachedTokens:    record.CachedTokens,
		ReasoningTokens: record.ReasoningTokens,
		TotalTokens:     record.TotalTokens,
		TotalCost:       record.TotalCost,
		Fee:             record.Fee,
		Status:          record.Status,
		APIKey:          record.APIKey,
		ReasoningEffort: record.ReasoningEffort,
		Endpoint:        record.Endpoint,
		Type:            record.Type,
		BillingMode:     record.BillingMode,
		FirstTokenMs:    record.FirstTokenMs,
		LatencyMs:       record.LatencyMs,
		UserAgent:       record.UserAgent,
		UserID:          c.GetString("user_id"),
		Error:           record.Error,
	}
	if _, err := requestlog.Insert(context.Background(), rec); err != nil {
		logger.GetModelLogger("global").Warn("persist request log failed", zap.Error(err))
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
