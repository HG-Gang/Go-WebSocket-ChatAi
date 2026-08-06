// internal/logger/logger.go
// 日志系统核心逻辑：
// 1. 按当前模型分目录存储日志，例如 logs/openai/openai-2026-05-14.log。
// 2. 按日期切分日志文件，每天一个文件。
// 3. 文件日志不输出 ANSI 颜色控制字符，避免出现 [34mINFO[0m 这类内容。
// 4. 时间格式统一为 “年-月-日 时:分:秒”，不带 +0800 时区后缀。
// 5. 日志实例按模型缓存，文件写入时再按当前日期选择目标文件，避免长连接跨零点后继续写旧文件。
// 安全边界：
// - RedactField 对高风险字段只输出长度与 sha256 摘要，原始密钥/凭据不落日志。
// - 日志清理审计只记录根目录内相对路径，不把服务器绝对目录写入审计文件。
package logger

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"TozoAI-Chat-Api/conf"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// 日志实例缓存，key 为模型名；具体日期文件由 dailyFileWriteSyncer 在写入时决定。
var (
	loggers = make(map[string]*zap.Logger)
	mu      sync.Mutex
	nowFunc = time.Now
)

// testSnapshot 保存测试开始前的 logger 全局状态。
// 该类型只供 ResetForTest 使用，生产路径不应直接依赖。
type testSnapshot struct {
	loggers map[string]*zap.Logger
	nowFunc func() time.Time
}

// LogCleanupSummary 是一次过期日志清理的审计摘要。
// Paths 使用日志根目录内的相对路径，避免把服务器绝对目录写入审计日志。
type LogCleanupSummary struct {
	Event         string   `json:"event"`
	EventDate     string   `json:"event_date"`
	RetentionDays int      `json:"retention_days"`
	Model         string   `json:"model"`
	ScannedCount  int      `json:"scanned_count"`
	DeletedCount  int      `json:"deleted_count"`
	FailedCount   int      `json:"failed_count"`
	DeletedPaths  []string `json:"deleted_paths,omitempty"`
	FailedPaths   []string `json:"failed_paths,omitempty"`
	CreatedAt     string   `json:"created_at"`
}

// Init 初始化日志系统。
// 当前只负责创建日志根目录；具体模型目录在 GetModelLogger 中按需创建。
func Init() {
	if conf.Global == nil {
		panic("全局配置未加载，无法初始化日志系统")
	}
	if err := os.MkdirAll(conf.Global.Logs.RootDir, 0755); err != nil {
		panic(fmt.Sprintf("创建日志根目录失败: %v", err))
	}
}

// GetModelLogger 获取模型专属日志实例。
// 如果传入空字符串或 global，会自动解析为当前启用的模型名称，避免继续生成 global-日期.log。
func GetModelLogger(model string) *zap.Logger {
	model = normalizeModelName(model)
	cacheKey := model

	mu.Lock()
	defer mu.Unlock()

	if l, ok := loggers[cacheKey]; ok {
		return l
	}

	writer := buildLogWriter(model)
	core := zapcore.NewCore(zapcore.NewConsoleEncoder(buildEncoderConfig()), writer, zapcore.DebugLevel)
	options := []zap.Option{
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
		zap.ErrorOutput(buildErrorWriter(model)),
	}
	if conf.IsDev() {
		options = append(options, zap.Development())
	}

	l := zap.New(core, options...).Named(model)
	loggers[cacheKey] = l
	return l
}

// buildLogWriter 构建业务日志输出。
// 开发环境同时输出到文件和 stdout；生产环境只输出到按天轮换的文件。
func buildLogWriter(model string) zapcore.WriteSyncer {
	fileWriter := zapcore.WriteSyncer(&dailyFileWriteSyncer{model: model})
	if conf.IsDev() {
		return zapcore.NewMultiWriteSyncer(fileWriter, zapcore.Lock(os.Stdout))
	}
	return fileWriter
}

// buildErrorWriter 构建 zap 内部错误输出。
// 这里复用按天文件写入器，避免跨日期后内部错误仍落到旧文件。
func buildErrorWriter(model string) zapcore.WriteSyncer {
	fileWriter := zapcore.WriteSyncer(&dailyFileWriteSyncer{model: model})
	if conf.IsDev() {
		return zapcore.NewMultiWriteSyncer(fileWriter, zapcore.Lock(os.Stderr))
	}
	return fileWriter
}

// dailyFileWriteSyncer 在写入时按当前日期定位日志文件。
// 它只持有当天文件句柄，跨日会关闭旧文件并打开新文件；Sync 会主动关闭句柄，便于 Windows 环境测试清理。
type dailyFileWriteSyncer struct {
	model string
	mu    sync.Mutex
	date  string
	file  *os.File
}

// Write 实现 zapcore.WriteSyncer：写入前按当天日期切换文件，跨日自动打开新文件。
func (w *dailyFileWriteSyncer) Write(p []byte) (int, error) {
	if conf.Global == nil || conf.Global.Logs.RootDir == "" {
		return 0, fmt.Errorf("日志配置未初始化")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.rotateLocked(nowFunc().Format("2006-01-02")); err != nil {
		return 0, err
	}
	return w.file.Write(p)
}

// Sync 刷盘并主动关闭当天文件句柄，便于 Windows 环境及时释放文件（测试与清理场景需要）。
func (w *dailyFileWriteSyncer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}
	err := w.file.Sync()
	closeErr := w.file.Close()
	w.file = nil
	w.date = ""
	if err != nil {
		return err
	}
	return closeErr
}

// rotateLocked 在调用方已持锁的前提下切换日期文件：同一天复用句柄，跨天先同步关闭旧文件再建新文件。
func (w *dailyFileWriteSyncer) rotateLocked(dateStr string) error {
	if w.file != nil && w.date == dateStr {
		return nil
	}
	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			return err
		}
		if err := w.file.Close(); err != nil {
			return err
		}
		w.file = nil
		w.date = ""
	}

	logDir := filepath.Join(conf.Global.Logs.RootDir, w.model)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	path := filepath.Join(logDir, fmt.Sprintf("%s-%s.log", w.model, dateStr))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	w.file = file
	w.date = dateStr
	return nil
}

func buildEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		TimeKey:          "ts",
		LevelKey:         "level",
		NameKey:          "logger",
		CallerKey:        "caller",
		MessageKey:       "msg",
		StacktraceKey:    "stacktrace",
		LineEnding:       zapcore.DefaultLineEnding,
		EncodeLevel:      zapcore.CapitalLevelEncoder,
		EncodeTime:       localSecondTimeEncoder,
		EncodeDuration:   zapcore.StringDurationEncoder,
		EncodeCaller:     zapcore.ShortCallerEncoder,
		EncodeName:       zapcore.FullNameEncoder,
		ConsoleSeparator: "\t",
	}
}

// localSecondTimeEncoder 输出本地时间到秒，去掉毫秒和 +0800 时区后缀。
func localSecondTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Local().Format("2006-01-02 15:04:05"))
}

// normalizeModelName 把空模型名或 global 归一为当前启用模型。
// 这样启动、Redis、限流等全局日志也会进入 openai-日期.log 这类模型日志文件。
func normalizeModelName(model string) string {
	if model != "" && model != "global" {
		return model
	}
	return defaultLogModelName()
}

// defaultLogModelName 根据配置选择默认日志模型名。
// 优先使用 openai，其次 azureai；如果都不存在，再选择第一个启用模型。
func defaultLogModelName() string {
	if conf.Global == nil || len(conf.Global.Models) == 0 {
		return "global"
	}
	if cfg, ok := conf.Global.Models["openai"]; ok && cfg.Enabled {
		return "openai"
	}
	if cfg, ok := conf.Global.Models["azureai"]; ok && cfg.Enabled {
		return "azureai"
	}
	for name, cfg := range conf.Global.Models {
		if cfg.Enabled {
			return name
		}
	}
	return "global"
}

// SyncAll 刷盘所有日志，程序退出时调用，避免日志缓冲未写入磁盘。
func SyncAll() {
	mu.Lock()
	defer mu.Unlock()

	for _, l := range loggers {
		_ = l.Sync()
	}
}

// ResetForTest 清空 logger 全局缓存，并返回恢复函数。
// 跨包测试如果会替换 conf.Global.Logs.RootDir，必须调用它，避免复用其他测试创建的 writer。
func ResetForTest() func() {
	mu.Lock()
	snapshot := testSnapshot{loggers: loggers, nowFunc: nowFunc}
	loggers = make(map[string]*zap.Logger)
	nowFunc = time.Now
	mu.Unlock()

	return func() {
		SyncAll()
		mu.Lock()
		loggers = snapshot.loggers
		nowFunc = snapshot.nowFunc
		mu.Unlock()
	}
}

// StartCleanupScheduler 启动按天日志清理后台任务。
// retentionDays 是保留天数，interval 是两次扫描之间的间隔；返回的 done channel 会在 ctx 取消后关闭。
func StartCleanupScheduler(ctx context.Context, retentionDays int, interval time.Duration) <-chan struct{} {
	done := make(chan struct{})
	if ctx == nil || conf.Global == nil || retentionDays <= 0 || interval <= 0 {
		close(done)
		return done
	}

	go func() {
		defer close(done)

		WriteLogCleanupAudit(CleanExpiredLogs(retentionDays, ""))
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				WriteLogCleanupAudit(CleanExpiredLogs(retentionDays, ""))
			}
		}
	}()

	return done
}

// CleanExpiredLogs 清理过期日志。
// days 表示保留天数；model 为空时扫描所有模型目录。
func CleanExpiredLogs(days int, model string) LogCleanupSummary {
	summary := newLogCleanupSummary(days, model)
	if conf.Global == nil || days <= 0 || conf.Global.Logs.RootDir == "" {
		return summary
	}
	expireTime := nowFunc().AddDate(0, 0, -days)

	rootDir := conf.Global.Logs.RootDir
	if model != "" {
		rootDir = filepath.Join(rootDir, normalizeModelName(model))
	}

	_ = filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil {
			summary.FailedCount++
			summary.FailedPaths = append(summary.FailedPaths, relativeLogPath(path))
			return nil
		}
		if info.IsDir() {
			return nil
		}
		summary.ScannedCount++
		// 文件名形如 <model>-2006-01-02.log，以其中日期段判定过期（与按天轮转的文件名约定一致），
		// 而不是依赖修改时间；日期段缺失或解析失败的文件不参与删除。
		filename := filepath.Base(path)
		if len(filename) < len("x-2006-01-02.log") {
			return nil
		}
		datePart := filename[len(filename)-14 : len(filename)-4]
		fileDate, err := time.Parse("2006-01-02", datePart)
		if err != nil {
			return nil
		}
		if fileDate.Before(expireTime) {
			if removeErr := os.Remove(path); removeErr != nil {
				summary.FailedCount++
				summary.FailedPaths = append(summary.FailedPaths, relativeLogPath(path))
				return nil
			}
			summary.DeletedCount++
			summary.DeletedPaths = append(summary.DeletedPaths, relativeLogPath(path))
		}
		return nil
	})
	return summary
}

// WriteLogCleanupAudit 用短生命周期文件句柄写入清理摘要。
// 这里不复用 zap logger，避免清理测试和 Windows 生产环境中额外持有待清理目录的文件句柄。
func WriteLogCleanupAudit(summary LogCleanupSummary) {
	if conf.Global == nil || conf.Global.Logs.RootDir == "" || summary.Event == "" {
		return
	}
	auditDir := filepath.Join(conf.Global.Logs.RootDir, "audit")
	if err := os.MkdirAll(auditDir, 0755); err != nil {
		return
	}
	path := filepath.Join(auditDir, "audit-"+nowFunc().Format("2006-01-02")+".log")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer file.Close()

	_ = json.NewEncoder(file).Encode(summary)
}

// RedactField 根据日志字段名决定是否脱敏。
// 对 key、token、secret、webhook、value、content、diff 等高风险字段，只保留长度和短 sha256 摘要；
// 普通运行字段保持原样，避免影响按模型、状态、路径等维度检索。
func RedactField(key, value string) string {
	if !isSensitiveLogKey(key) {
		return value
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	// 只输出 6 字节摘要前缀，兼顾可追溯性且不泄露原文。
	return fmt.Sprintf("[REDACTED len=%d sha256:%x]", len(value), sum[:6])
}

// isSensitiveLogKey 判断字段名是否命中敏感词表；命中即代表该字段值不得原样落盘。
// 使用包含匹配（如 value、content）是为了覆盖未知前缀的同类字段，宁严勿松。
func isSensitiveLogKey(key string) bool {
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" {
		return false
	}
	for _, marker := range []string{
		"api_key",
		"apikey",
		"access_token",
		"authorization",
		"bearer",
		"jwt",
		"token",
		"secret",
		"webhook",
		"password",
		"sign",
		"signature",
		"redis_value",
		"value",
		"content",
		"diff",
		"private_key",
		"credential",
	} {
		if strings.Contains(key, marker) {
			return true
		}
	}
	return false
}

// SafeURLForDisplay 保留 URL 的路由上下文，同时把凭据与 token 类查询参数脱敏后再展示到页面或日志。
func SafeURLForDisplay(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return RedactField("access_token", raw)
	}
	if parsed.User != nil {
		username := parsed.User.Username()
		if username == "" {
			parsed.User = url.UserPassword("redacted", "******")
		} else {
			parsed.User = url.UserPassword(username, "******")
		}
	}
	query := parsed.Query()
	for key := range query {
		if isSensitiveLogKey(key) || strings.Contains(strings.ToLower(strings.TrimSpace(key)), "key") {
			query.Set(key, RedactField("api_key", query.Get(key)))
		}
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func newLogCleanupSummary(days int, model string) LogCleanupSummary {
	return LogCleanupSummary{
		Event:         "log_cleanup",
		EventDate:     nowFunc().Format("2006-01-02"),
		RetentionDays: days,
		Model:         model,
		CreatedAt:     nowFunc().Format(time.RFC3339),
	}
}

// relativeLogPath 把日志文件路径转为相对日志根目录的斜杠路径；转换失败时原样返回，保证审计仍可写入。
func relativeLogPath(path string) string {
	if conf.Global == nil || conf.Global.Logs.RootDir == "" || path == "" {
		return filepath.ToSlash(path)
	}
	rel, err := filepath.Rel(conf.Global.Logs.RootDir, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(rel)
}
