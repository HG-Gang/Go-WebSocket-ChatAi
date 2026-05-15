module TozoAI-Chat-Api

go 1.25.7 // Go版本：1.25.7（最新稳定版，支持所有现代特性）

// 核心业务依赖（按功能分类）
require (
	// 高性能JSON序列化库（字节跳动开源）：替代标准库encoding/json，提升高并发下的JSON解析/序列化性能
	github.com/bytedance/sonic v1.15.0
	// Gin HTTP框架：轻量级高性能Web框架，支持REST API、中间件、WebSocket升级，作为服务核心入口
	github.com/gin-gonic/gin v1.12.0
	// JWT v5鉴权库：生成/解析JWT Token，用于WS/HTTP接口的身份认证（当前版本v5.3.1）
	github.com/golang-jwt/jwt/v5 v5.3.1
	// UUID生成库：生成唯一标识符，用于sessionID/requestID等场景，保证全局唯一性
	github.com/google/uuid v1.6.0
	// WebSocket核心库：实现WebSocket协议，用于App↔Go、Go↔OpenAI Realtime的双向通信
	github.com/gorilla/websocket v1.5.3
	// Redis客户端v9：用于session存储、分布式限流、Token计费统计、分布式锁等场景
	github.com/redis/go-redis/v9 v9.17.3
	// 配置管理库：支持YAML/ENV/命令行参数等多源配置，统一管理服务配置
	github.com/spf13/viper v1.21.0
	// 高性能日志库：结构化日志输出，支持分级、按文件/控制台输出，高并发下性能优异
	go.uber.org/zap v1.27.0
	// 时间工具库：提供限流、时间计算等功能，防止语音流消耗过快导致封号
	golang.org/x/time v0.15.0
)

// 间接依赖（核心库的依赖）
require (
	// Sonic的加载器：辅助sonic库加载CPU指令集优化
	github.com/bytedance/sonic/loader v0.5.0 // indirect
	// Viper的结构体映射库：Viper v2版本的mapstructure依赖
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
)

// 底层系统依赖（核心库的间接依赖）
require (
	// CPU指令集检测：sonic库用于检测CPU是否支持AVX2等指令集，优化JSON序列化
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	// Go汇编优化：sonic库的汇编代码依赖，提升性能
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	// CPU架构检测：sonic库依赖，适配不同CPU架构（x86/arm）
	golang.org/x/arch v0.22.0 // indirect
	// 网络库：WebSocket/HTTP2的核心依赖，处理网络连接
	golang.org/x/net v0.51.0 // indirect
	// 系统调用库：用于限流、日志、性能监控等场景的系统级操作
	golang.org/x/sys v0.41.0 // indirect
	// 文本处理库：JWT鉴权、国际化等场景的字符串处理
	golang.org/x/text v0.34.0 // indirect
	// 协议缓冲区：Google GenAI SDK的底层依赖，处理protobuf数据
	google.golang.org/protobuf v1.36.10 // indirect
)

require (
	github.com/bytedance/gopkg v0.1.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cloudwego/base64x v0.1.6 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/gabriel-vasile/mimetype v1.4.12 // indirect
	github.com/gin-contrib/sse v1.1.0 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator/v10 v10.30.1 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pelletier/go-toml/v2 v2.2.4 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/quic-go/quic-go v0.59.0 // indirect
	github.com/sagikazarmark/locafero v0.11.0 // indirect
	github.com/sourcegraph/conc v0.3.1-0.20240121214520-5f936abd7ae8 // indirect
	github.com/spf13/afero v1.15.0 // indirect
	github.com/spf13/cast v1.10.0 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/ugorji/go/codec v1.3.1 // indirect
	go.mongodb.org/mongo-driver/v2 v2.5.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/crypto v0.48.0 // indirect
)
