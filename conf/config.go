// conf/config.go
// 文件功能：定义全局配置（GlobalConfig）与模型配置（ModelConfig）结构体，
// 提供全局单例 conf.Global、模型配置缓存（GetModel/InitModelConfig/热更新）与环境判断工具。
// 输入是 loader.go 解码并校验后的配置，输出是供各包并发只读查询的配置对象。
// 不负责：配置文件的读取、合并与生产校验（见 loader.go）。
// 安全边界：缓存未初始化或模型不存在时 GetModel 返回空配置而非 nil，避免调用方空指针；
// 计时字段单位：MaxSessionTTL 为秒、SendQueueTimeoutMs 为毫秒、MaxUploadBytes 为字节。
package conf

import (
	"sync"

	"TozoAI-Chat-Api/internal/service/stats"
)

// Global 全局配置单例（通过 loader.go 加载配置后初始化）
var Global *GlobalConfig

// ModelConfig 模型专属配置结构体
// 每个模型（如 openai, azureai）独立配置，支持差异化的 API 密钥、限流规则及 WebSocket 属性。
type ModelConfig struct {
	Enabled       bool                   `mapstructure:"enabled"`         // 是否启用该模型
	APIKey        string                 `mapstructure:"api_key"`         // 模型 API Key（支持环境变量占位符）
	DefaultModel  string                 `mapstructure:"default_model"`   // 默认模型版本（如 gpt-4o-realtime）
	Endpoint      string                 `mapstructure:"endpoint"`        // API 基础端点地址（HTTP/HTTPS）
	Instructions  string                 `mapstructure:"instructions"`    // 系统指令（模型的默认提示词/System Prompt）
	Voice         string                 `mapstructure:"voice"`           // 语音音色（仅 Realtime API 使用）
	RateRPS       int                    `mapstructure:"rate_rps"`        // 每秒请求数限制（Rate Limit RPS）
	RateBurst     int                    `mapstructure:"rate_burst"`      // 突发请求数限制（Rate Limit Burst）
	MaxSessionTTL int                    `mapstructure:"max_session_ttl"` // 会话最大存活时间（秒）
	Organization  string                 `mapstructure:"organization"`    // 模型组织 ID（可选，如 OpenAI Org ID）
	Extra         map[string]interface{} `mapstructure:"extra"`           // 扩展字段（存储各模型特有的非通用配置）

	// Realtime 专属配置（用于 WebSocket 实时语音/对话连接）
	Realtime struct {
		Name                string `mapstructure:"name"`                  // Realtime 模型内部标识名称
		WsUrl               string `mapstructure:"ws_url"`                // WebSocket 基础连接地址
		ReconnectMaxRetries int    `mapstructure:"reconnect_max_retries"` // 最大重连尝试次数
		ReconnectDelay      string `mapstructure:"reconnect_delay"`       // 重连延迟（如 "1s", "0s" 表示立即重连）

		// ========== 心跳配置（App↔Go 和 Go↔OpenAI 分别配置） ==========
		// App↔Go 心跳：Go 服务向 App 发 Ping，App 回 Pong
		AppPingInterval string `mapstructure:"app_ping_interval"` // Go→App 发送 Ping 的间隔（如 "30s"）
		AppPongTimeout  string `mapstructure:"app_pong_timeout"`  // 等待 App Pong 的超时时间（如 "60s"，超时则认为会话结束）

		// Go↔OpenAI 心跳：通过持续读取消息来检测连接状态
		ApiReadTimeout  string `mapstructure:"api_read_timeout"`  // 读取 OpenAI 消息的超时时间（如 "120s"，超时则触发重连）
		ApiPingInterval string `mapstructure:"api_ping_interval"` // Go→OpenAI WebSocket Ping 间隔
		ApiPongTimeout  string `mapstructure:"api_pong_timeout"`  // 等待 OpenAI Pong/任意消息的超时
		ApiWriteTimeout string `mapstructure:"api_write_timeout"` // 写入 OpenAI WebSocket 的超时

		RestoreSession      bool `mapstructure:"restore_session"`       // OpenAI 重连后是否自动恢复 session.update 和最近上下文
		RestoreHistoryLimit int  `mapstructure:"restore_history_limit"` // 重连恢复时最多重放的 conversation.item.* 事件数
		SendQueueTimeoutMs  int  `mapstructure:"send_queue_timeout_ms"` // App 下行队列满时最多等待毫秒数

		// ProxyURL 显式指定 Realtime WS 拨号代理（如 "http://127.0.0.1:7890" 或 "socks5://127.0.0.1:1080"）。
		// 留空时回退到 HTTPS_PROXY / HTTP_PROXY / NO_PROXY 等系统环境变量。
		// 优先级：配置 > 环境变量。设计目的：在国内无法直连 api.openai.com 时无需依赖
		// 进程环境变量（GoLand 重启、setx 持久化等容易出错），改由 yaml 直接控制。
		ProxyURL string `mapstructure:"proxy_url"`
	} `mapstructure:"realtime"`
}

// GlobalConfig 系统全局配置结构体
// 聚合了服务运行所需的所有模块化配置，支持通过 mapstructure 标签从 YAML/环境变量自动注入。
type GlobalConfig struct {
	Env string `mapstructure:"env"` // 当前运行环境：dev (开发), test (测试), prod (生产)

	// Server 服务运行配置
	Server struct {
		Addr string `mapstructure:"addr"` // 服务监听地址与端口（如 ":8080"）
		Mode string `mapstructure:"mode"` // 运行模式（debug/release/test）
	} `mapstructure:"server"`

	// JWT 鉴权配置
	JWT struct {
		Enabled     bool   `mapstructure:"enabled"`      // 是否开启 JWT 鉴权校验
		Secret      string `mapstructure:"secret"`       // JWT 签名密钥（生产环境严禁明文，建议环境变量注入）
		Issuer      string `mapstructure:"issuer"`       // JWT 令牌签发者标识
		ExpireHours int    `mapstructure:"expire_hours"` // 令牌有效期（单位：小时）
	} `mapstructure:"jwt"`

	Security struct {
		PublicDebugEnabled      bool     `mapstructure:"public_debug_enabled"`           // 是否开放公开调试接口（生产环境必须关闭）
		PublicTokenEnabled      bool     `mapstructure:"public_token_enabled"`           // 是否开放测试 token 生成接口（生产环境必须关闭）
		AllowedOrigins          []string `mapstructure:"allowed_origins"`                // CORS 允许的 Origin 白名单（生产环境必须非空）
		TrustedProxies          []string `mapstructure:"trusted_proxies"`                // 可信反向代理 IP 列表；为空时不信任任何代理，忽略 X-Forwarded-For
		AllowUpstreamQueryKey   bool     `mapstructure:"allow_upstream_query_key"`       // 是否允许上游请求携带 query key（生产环境必须关闭）
		WorkspaceWriteConfirm   bool     `mapstructure:"workspace_write_confirm"`        // 工作区写操作是否需要人工确认
		WorkspaceAuditRetention int      `mapstructure:"workspace_audit_retention_days"` // 工作区审计日志保留天数
	} `mapstructure:"security"`

	// Redis 缓存与限流配置
	Redis struct {
		Enabled      bool   `mapstructure:"enabled"`        // 是否启用 Redis 功能
		Addr         string `mapstructure:"addr"`           // Redis 服务器地址（host:port）
		Password     string `mapstructure:"password"`       // Redis 认证密码
		DB           int    `mapstructure:"db"`             // Redis 数据库索引（默认 0）
		PoolSize     int    `mapstructure:"pool_size"`      // Redis 连接池大小
		MinIdleConns int    `mapstructure:"min_idle_conns"` // Redis 最小空闲连接数
	} `mapstructure:"redis"`

	// Logs 日志存储配置
	Logs struct {
		RootDir         string `mapstructure:"root_dir"`         // 日志存储根路径（如 "./logs"）
		RetentionDays   int    `mapstructure:"retention_days"`   // 日志保留天数，超过该天数的按天日志会被后台清理
		CleanupInterval string `mapstructure:"cleanup_interval"` // 日志清理调度间隔（如 "24h"）
	} `mapstructure:"logs"`

	// DB 数据库基础配置（Web 请求明细等）
	DB struct {
		Enabled bool   `mapstructure:"enabled"` // 是否启用数据库连接
		Driver  string `mapstructure:"driver"`  // 数据库驱动：sqlite / mysql
		DSN     string `mapstructure:"dsn"`     // 连接串；sqlite 默认 ./data/tozoai.db
	} `mapstructure:"db"`

	// WebChat 聊天看板上传与附件限制
	WebChat struct {
		MaxUploadBytes int64  `mapstructure:"max_upload_bytes"` // 单文件上限，默认 10MB
		MaxPDFChars    int    `mapstructure:"max_pdf_chars"`    // PDF 抽取文本上限
		UploadDir      string `mapstructure:"upload_dir"`       // 上传目录
	} `mapstructure:"web_chat"`

	// 全局功能开关
	RateLimit struct {
		Enabled bool `mapstructure:"enabled"` // 是否启用全局 API 限流功能
	} `mapstructure:"rate_limit"`

	Fallback struct {
		Enabled bool `mapstructure:"enabled"` // 是否开启 HTTP 降级切换功能（WS 失败时自动降级）
	} `mapstructure:"fallback"`

	Billing struct {
		Enabled bool `mapstructure:"enabled"` // 是否启用计费/额度统计功能
	} `mapstructure:"billing"`

	Capacity struct {
		MaxActiveSessions int64 `mapstructure:"max_active_sessions"` // 单实例最大活跃 WS 会话数；0 表示不限制
	} `mapstructure:"capacity"`

	Alerts struct {
		DingTalk struct {
			Enabled         bool     `mapstructure:"enabled"`          // 是否启用钉钉机器人告警
			Webhook         string   `mapstructure:"webhook"`          // 钉钉机器人 Webhook 地址，建议用环境变量注入
			Secret          string   `mapstructure:"secret"`           // 钉钉加签密钥，可选
			CooldownSeconds int      `mapstructure:"cooldown_seconds"` // 同类告警冷却秒数，避免过载时刷屏
			TimeoutMs       int      `mapstructure:"timeout_ms"`       // 发送超时毫秒数
			AtMobiles       []string `mapstructure:"at_mobiles"`       // 需要 @ 的手机号列表
			IsAtAll         bool     `mapstructure:"is_at_all"`        // 是否 @ 所有人
		} `mapstructure:"dingtalk"`
	} `mapstructure:"alerts"`

	// Models 模型配置字典（Key 为模型标识，Value 为具体配置）
	Models map[string]ModelConfig `mapstructure:"models"`
}

// 模型配置内部缓存与并发控制
var (
	modelConfigCache map[string]*ModelConfig // 高效查询缓存
	modelConfigMu    sync.RWMutex            // 保护缓存并发安全的读写锁
)

// GetModel 并发安全地按名称获取模型配置。
// 缓存未初始化或模型不存在时返回空的 ModelConfig 指针而非 nil，
// 防止调用方空指针异常，同时仍按未命中口径记录缓存统计。
func GetModel(modelName string) *ModelConfig {
	modelConfigMu.RLock()
	if modelConfigCache == nil {
		modelConfigMu.RUnlock()
		recordModelConfigCacheStat(false, modelName)
		return &ModelConfig{}
	}

	cfg, ok := modelConfigCache[modelName]
	modelConfigMu.RUnlock()
	if !ok {
		recordModelConfigCacheStat(false, modelName)
		return &ModelConfig{}
	}

	recordModelConfigCacheStat(true, modelName)
	return cfg
}

// recordModelConfigCacheStat 将模型配置缓存读取写入统一 stats。
// 这里记录的是业务配置缓存命中/未命中，和 Redis 连接池 hits/misses 是两套口径。
func recordModelConfigCacheStat(hit bool, modelName string) {
	kind := stats.ResourceKindBusinessCacheMiss
	status := "miss"
	if hit {
		kind = stats.ResourceKindBusinessCacheHit
		status = "hit"
	}
	stats.RecordResourceEvent(stats.ResourceEvent{
		Source:   stats.SourceSystem,
		Kind:     kind,
		Provider: "model_config_cache",
		Model:    modelName,
		Status:   status,
	})
}

// GetModelConfig 为 GetModel 的别名，用于兼容旧版本的调用习惯。
func GetModelConfig(modelName string) *ModelConfig {
	return GetModel(modelName)
}

// IsProd 检查当前环境是否为生产环境（prod）。
func IsProd() bool {
	if Global == nil {
		return false
	}
	return Global.Env == "prod"
}

// IsDev 检查当前环境是否为开发环境（dev）。
func IsDev() bool {
	if Global == nil {
		return true // 默认视为开发环境
	}
	return Global.Env == "dev"
}

// InitModelConfig 将全局配置中的 Models 映射同步到内部高效缓存中。
// 该方法应在系统启动加载完配置文件后被立即调用。
func InitModelConfig() {
	modelConfigMu.Lock()
	defer modelConfigMu.Unlock()

	modelConfigCache = make(map[string]*ModelConfig)

	for name, cfg := range Global.Models {
		// 拷贝后取地址：range 变量会被复用，直接取 &cfg 会导致缓存项指向同一个变量。
		tmp := cfg
		modelConfigCache[name] = &tmp
	}
}

// ReloadModelConfig 重新加载并同步模型配置缓存。
// 用于支持在线配置热更新或手动触发刷新。
func ReloadModelConfig() {
	InitModelConfig()
}

// UpdateModelConfig 运行时动态更新单个模型的配置，并同步更新缓存。
// 参数 modelName 为模型标识符，cfg 为新的配置内容。
func UpdateModelConfig(modelName string, cfg ModelConfig) {
	modelConfigMu.Lock()
	defer modelConfigMu.Unlock()

	if Global.Models == nil {
		Global.Models = make(map[string]ModelConfig)
	}
	Global.Models[modelName] = cfg

	// 拷贝后存指针，让缓存与 Global.Models 各持独立副本，避免共享同一地址被意外修改。
	tmp := cfg
	modelConfigCache[modelName] = &tmp
}
