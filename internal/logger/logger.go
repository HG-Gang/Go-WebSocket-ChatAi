// internal/logger/logger.go
// 日志系统核心逻辑：
// 1. 按当前模型分目录存储日志，例如 logs/openai/openai-2026-05-14.log。
// 2. 按日期切分日志文件，每天一个文件。
// 3. 文件日志不输出 ANSI 颜色控制字符，避免出现 [34mINFO[0m 这类内容。
// 4. 时间格式统一为 “年-月-日 时:分:秒”，不带 +0800 时区后缀。
// 5. 日志实例按“模型+日期”缓存，避免重复创建。
package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"TozoAI-Chat-Api/conf"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// 日志实例缓存，key 格式为“模型名:日期”。
var (
	loggers = make(map[string]*zap.Logger)
	mu      sync.Mutex
)

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
	dateStr := time.Now().Format("2006-01-02")
	cacheKey := fmt.Sprintf("%s:%s", model, dateStr)

	mu.Lock()
	defer mu.Unlock()

	if l, ok := loggers[cacheKey]; ok {
		return l
	}

	logDir := filepath.Join(conf.Global.Logs.RootDir, model)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		panic(fmt.Sprintf("创建模型日志目录失败: %v", err))
	}

	fullPath := filepath.Join(logDir, fmt.Sprintf("%s-%s.log", model, dateStr))
	cfg := buildZapConfig(fullPath)

	l, err := cfg.Build(
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
	)
	if err != nil {
		panic(fmt.Sprintf("初始化 zap logger 失败: %v", err))
	}

	l = l.Named(model)
	loggers[cacheKey] = l
	return l
}

// buildZapConfig 构建统一日志格式。
// 开发环境同时输出到文件和 stdout；生产环境只输出到文件。
func buildZapConfig(fullPath string) zap.Config {
	outputPaths := []string{fullPath}
	errorOutputPaths := []string{fullPath}
	if conf.IsDev() {
		outputPaths = append(outputPaths, "stdout")
		errorOutputPaths = append(errorOutputPaths, "stderr")
	}

	return zap.Config{
		Level:            zap.NewAtomicLevelAt(zap.DebugLevel),
		Development:      conf.IsDev(),
		Encoding:         "console",
		OutputPaths:      outputPaths,
		ErrorOutputPaths: errorOutputPaths,
		EncoderConfig: zapcore.EncoderConfig{
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
		},
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

// CleanExpiredLogs 清理过期日志。
// days 表示保留天数；model 为空时扫描所有模型目录。
func CleanExpiredLogs(days int, model string) {
	expireTime := time.Now().AddDate(0, 0, -days)

	rootDir := conf.Global.Logs.RootDir
	if model != "" {
		rootDir = filepath.Join(rootDir, normalizeModelName(model))
	}

	_ = filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
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
			_ = os.Remove(path)
			GetModelLogger(model).Info("删除过期日志文件", zap.String("path", path))
		}
		return nil
	})
}
