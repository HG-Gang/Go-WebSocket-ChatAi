// conf/loader.go
// 文件功能：按固定顺序加载全部配置：config.yaml → config_{env}.yaml → conf/models/*.yaml，
// 替换 ${ENV_VAR} 环境变量占位符，校验生产配置并初始化模型配置缓存；
// 输入是 conf 目录下的 YAML 文件与环境变量，输出是全局配置 conf.Global。
// 不负责：日志、Redis、路由等运行时资源的初始化。
// 安全边界：生产环境校验失败时 Load 返回错误、进程拒绝启动（失败关闭）；
// api_key 占位符解析为空时不覆盖模型文件中的真实密钥（见 applyRootModelOverride），
// 防止启动后得到空 key 导致 OpenAI WebSocket 握手失败。
package conf

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/spf13/viper" // 配置读取核心库
)

// Load 加载全部配置并写入全局 conf.Global，供 main 启动时调用。
// 加载顺序（后加载覆盖先加载）：config.yaml → config_{env}.yaml → conf/models/*.yaml →
// 环境变量占位符替换；Viper 中环境变量（TOZO_ 前缀）始终优先于 YAML 值。
// 成功返回 nil；基础配置缺失、解码失败或生产配置校验失败时返回错误，调用方应终止启动。
func Load() error {
	v := viper.New() // 独立 Viper 实例，避免污染包级默认实例的搜索路径

	// 环境变量以 TOZO_ 为前缀且始终优先于 YAML；key 中的点号映射为下划线，
	// 例如 redis.addr 对应 TOZO_REDIS_ADDR。
	v.SetEnvPrefix("TOZO")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// 基础配置 config.yaml 必须存在，缺失即返回错误，保证配置默认值有明确来源。
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath("./conf")
	v.AddConfigPath(".")
	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("读取主配置失败: %w", err)
	}

	// 确定运行环境，缺省为 dev；回写 Viper 让后续阶段能读取该值。
	env := v.GetString("env")
	if env == "" {
		env = "dev"
	}
	v.Set("env", env)

	// 环境专属配置 config_{env}.yaml 可选：存在则合并覆盖基础配置，缺失不影响启动。
	// 搜索路径 ./conf → ./，兼容不同工作目录启动。
	envConfigName := fmt.Sprintf("config_%s", env)
	v.SetConfigName(envConfigName)
	v.SetConfigType("yaml")
	if mergeErr := v.MergeInConfig(); mergeErr == nil {
		fmt.Printf("已加载环境配置: %s.yaml\n", envConfigName)
	} else {
		fmt.Printf("未找到环境配置 %s.yaml，跳过\n", envConfigName)
	}

	// 解码基础配置到全局结构体。conf/models/*.yaml 是“单模型根配置”，
	// 必须在解码后写入 Global.Models[modelName]，不能直接 Merge 到全局根节点；
	// 同时快照根配置中的模型覆盖项，供后续模型文件加载时做最终覆盖。
	Global = &GlobalConfig{}
	if err := v.Unmarshal(Global); err != nil {
		return fmt.Errorf("配置解码失败: %w", err)
	}
	rootModelOverrides := cloneModelConfigMap(Global.Models)

	// 加载模型专属配置（conf/models/*.yaml），单模型文件失败只告警，不影响整体启动。
	if err := loadModelOverrides("./conf/models", rootModelOverrides); err != nil {
		fmt.Printf("加载模型专属配置失败: %v\n", err)
	}

	// 替换配置中的环境变量占位符（${ENV_VAR} → 实际值）
	replaceEnvPlaceholders()

	// 生产配置校验失败时立即返回错误，进程在启动前退出（失败关闭）。
	if err := validateProductionConfig(Global); err != nil {
		return err
	}

	// 初始化模型配置缓存，加速并发环境下的 GetModel 读取。
	InitModelConfig()

	// 打印启动信息，便于排查环境与模型加载结果。
	fmt.Printf("[TozoAI] 环境: %s | 服务地址: %s | 模型数: %d\n",
		Global.Env, Global.Server.Addr, len(Global.Models))

	return nil
}

// loadModelOverrides 把 conf/models/{modelName}.yaml 加载到 Global.Models[modelName]。
// 模型文件本身就是一个独立的 ModelConfig 结构，不再额外包一层 models.openai；
// 根配置中同名的 models.{name} 覆盖项在此应用（优先级规则见 applyRootModelOverride）。
// 目录缺失或单个模型文件损坏时仅打印告警并跳过，不影响其余模型与整体启动。
func loadModelOverrides(modelDir string, rootModelOverrides map[string]ModelConfig) error {
	files, err := os.ReadDir(modelDir)
	if err != nil {
		fmt.Printf("模型配置目录 %s 不存在，跳过加载模型配置\n", modelDir)
		return nil
	}
	if Global.Models == nil {
		Global.Models = make(map[string]ModelConfig)
	}

	for _, file := range files {
		// 只处理模型目录下的 .yaml 文件，忽略子目录与其他后缀。
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".yaml") {
			continue
		}

		modelName := strings.TrimSuffix(file.Name(), ".yaml")
		modelCfg := Global.Models[modelName]

		mv := viper.New()
		mv.SetConfigName(modelName)
		mv.SetConfigType("yaml")
		mv.AddConfigPath(modelDir)
		if err := mv.ReadInConfig(); err != nil {
			fmt.Printf("加载模型配置 %s.yaml 失败: %v\n", modelName, err)
			continue
		}
		if err := mv.Unmarshal(&modelCfg); err != nil {
			fmt.Printf("解码模型配置 %s.yaml 失败: %v\n", modelName, err)
			continue
		}
		// 主配置中同名模型的覆盖项在解码后应用，保证主配置为最终覆盖值。
		if rootOverride, ok := rootModelOverrides[modelName]; ok {
			applyRootModelOverride(&modelCfg, rootOverride)
		}

		Global.Models[modelName] = modelCfg
		fmt.Printf("已加载模型配置: %s.yaml\n", modelName)
	}

	return nil
}

// cloneModelConfigMap 快照根配置中的模型覆盖项，供加载模型文件后做最终覆盖，
// 避免模型加载过程改动全局 map 影响后续阶段使用的覆盖快照。
func cloneModelConfigMap(src map[string]ModelConfig) map[string]ModelConfig {
	dst := make(map[string]ModelConfig, len(src))
	for name, cfg := range src {
		dst[name] = cfg
	}
	return dst
}

// validateProductionConfig 校验生产环境（prod）必须满足的安全配置，任一不满足即返回错误，
// 使进程拒绝启动（失败关闭）：如空 JWT 密钥、开放公开调试/令牌接口、缺失 Origin 白名单、
// 允许上游 query key、Redis 与日志配置缺失等。非 prod 环境不做此校验。
func validateProductionConfig(cfg *GlobalConfig) error {
	if cfg == nil || cfg.Env != "prod" {
		return nil
	}
	if cfg.JWT.Enabled && strings.TrimSpace(cfg.JWT.Secret) == "" {
		return fmt.Errorf("prod requires jwt.secret")
	}
	if cfg.Security.PublicTokenEnabled {
		return fmt.Errorf("prod cannot enable security.public_token_enabled")
	}
	if cfg.Security.PublicDebugEnabled {
		return fmt.Errorf("prod cannot enable security.public_debug_enabled")
	}
	if len(cfg.Security.AllowedOrigins) == 0 {
		return fmt.Errorf("prod requires security.allowed_origins")
	}
	if cfg.Security.AllowUpstreamQueryKey {
		return fmt.Errorf("prod cannot enable security.allow_upstream_query_key")
	}
	if cfg.Redis.Enabled && strings.TrimSpace(cfg.Redis.Addr) == "" {
		return fmt.Errorf("prod requires redis.addr when redis.enabled is true")
	}
	if strings.TrimSpace(cfg.Logs.RootDir) == "" {
		return fmt.Errorf("prod requires logs.root_dir")
	}
	if cfg.Logs.RetentionDays <= 0 {
		return fmt.Errorf("prod requires logs.retention_days > 0")
	}
	if _, err := time.ParseDuration(cfg.Logs.CleanupInterval); err != nil {
		return fmt.Errorf("prod requires valid logs.cleanup_interval: %w", err)
	}
	return nil
}

// applyRootModelOverride 合并根配置里的模型覆盖项。
// 优先级规则：conf/models/openai.yaml 提供模型级默认值，conf/config.yaml 的 models.openai 提供最终覆盖值；
// 因此主配置里写了 default_model、instructions、voice、限流或 realtime 参数时，启动后应以主配置为准。
// 根配置中为空或为零值的字段保留模型文件的值，避免空值抹掉模型级默认值。
// 其中 api_key 允许写成 ${OPENAI_API_KEY}，但环境变量为空时不能覆盖 conf/models/openai.yaml 中的真实 key，
// 否则启动后会得到空 key，最终导致 OpenAI WebSocket 握手失败。
func applyRootModelOverride(dst *ModelConfig, root ModelConfig) {
	if root.APIKey != "" {
		if apiKey := resolveEnvPlaceholder(root.APIKey); apiKey != "" {
			dst.APIKey = apiKey
		}
	}
	if root.Enabled {
		dst.Enabled = true
	}
	if root.DefaultModel != "" {
		dst.DefaultModel = root.DefaultModel
	}
	if root.Endpoint != "" {
		dst.Endpoint = root.Endpoint
	}
	if root.Instructions != "" {
		dst.Instructions = root.Instructions
	}
	if root.Voice != "" {
		dst.Voice = root.Voice
	}
	if root.RateRPS > 0 {
		dst.RateRPS = root.RateRPS
	}
	if root.RateBurst > 0 {
		dst.RateBurst = root.RateBurst
	}
	if root.MaxSessionTTL > 0 {
		dst.MaxSessionTTL = root.MaxSessionTTL
	}
	if root.Organization != "" {
		dst.Organization = root.Organization
	}
	if len(root.Extra) > 0 {
		if dst.Extra == nil {
			dst.Extra = make(map[string]interface{}, len(root.Extra))
		}
		for key, value := range root.Extra {
			dst.Extra[key] = value
		}
	}
	applyRootRealtimeOverride(dst, root)
}

// applyRootRealtimeOverride 合并根配置中的 realtime 子配置。
// 这里按“非空值覆盖”的方式处理，避免空字符串或 0 把模型文件中的有效兜底配置抹掉。
func applyRootRealtimeOverride(dst *ModelConfig, root ModelConfig) {
	if root.Realtime.Name != "" {
		dst.Realtime.Name = root.Realtime.Name
	}
	if root.Realtime.WsUrl != "" {
		dst.Realtime.WsUrl = root.Realtime.WsUrl
	}
	if root.Realtime.ReconnectMaxRetries > 0 {
		dst.Realtime.ReconnectMaxRetries = root.Realtime.ReconnectMaxRetries
	}
	if root.Realtime.ReconnectDelay != "" {
		dst.Realtime.ReconnectDelay = root.Realtime.ReconnectDelay
	}
	if root.Realtime.AppPingInterval != "" {
		dst.Realtime.AppPingInterval = root.Realtime.AppPingInterval
	}
	if root.Realtime.AppPongTimeout != "" {
		dst.Realtime.AppPongTimeout = root.Realtime.AppPongTimeout
	}
	if root.Realtime.ApiReadTimeout != "" {
		dst.Realtime.ApiReadTimeout = root.Realtime.ApiReadTimeout
	}
	if root.Realtime.ApiPingInterval != "" {
		dst.Realtime.ApiPingInterval = root.Realtime.ApiPingInterval
	}
	if root.Realtime.ApiPongTimeout != "" {
		dst.Realtime.ApiPongTimeout = root.Realtime.ApiPongTimeout
	}
	if root.Realtime.ApiWriteTimeout != "" {
		dst.Realtime.ApiWriteTimeout = root.Realtime.ApiWriteTimeout
	}
	if root.Realtime.RestoreSession {
		dst.Realtime.RestoreSession = true
	}
	if root.Realtime.RestoreHistoryLimit > 0 {
		dst.Realtime.RestoreHistoryLimit = root.Realtime.RestoreHistoryLimit
	}
	if root.Realtime.SendQueueTimeoutMs > 0 {
		dst.Realtime.SendQueueTimeoutMs = root.Realtime.SendQueueTimeoutMs
	}
}

// resolveEnvPlaceholder 只解析完整的环境变量占位符，例如 ${OPENAI_API_KEY}。
// 返回空字符串表示环境变量不存在或值为空，调用方可据此决定是否保留原配置。
func resolveEnvPlaceholder(value string) string {
	if strings.HasPrefix(value, "${") && strings.HasSuffix(value, "}") {
		envKey := strings.TrimSuffix(strings.TrimPrefix(value, "${"), "}")
		return os.Getenv(envKey)
	}
	return value
}

// replaceEnvPlaceholders 用反射遍历所有配置结构体（模型与全局配置），把字符串字段中
// 完整的 ${ENV_VAR} 占位符替换为环境变量值；环境变量未设置时替换为空字符串。
// 支持嵌套结构体与 map 字段（如 ModelConfig.Extra）。
func replaceEnvPlaceholders() {
	// 单个占位符替换：仅当值整体形如 ${ENV_VAR} 时才替换，避免误伤普通字符串。
	replaceSingle := func(value string) string {
		if strings.HasPrefix(value, "${") && strings.HasSuffix(value, "}") {
			envKey := strings.TrimSuffix(strings.TrimPrefix(value, "${"), "}")
			return os.Getenv(envKey)
		}
		return value
	}

	// 递归遍历结构体，替换所有字符串字段中的占位符。
	var walkStruct func(v interface{}) // 先声明函数，支持递归调用
	walkStruct = func(v interface{}) {
		val := reflect.ValueOf(v)
		// 指针先解引用；非结构体直接返回（map 字段单独处理）。
		if val.Kind() == reflect.Ptr {
			val = val.Elem()
		}
		if val.Kind() != reflect.Struct {
			return
		}

		// 遍历结构体字段，跳过不可修改（未导出）字段。
		for i := 0; i < val.NumField(); i++ {
			field := val.Field(i)
			if !field.CanSet() {
				continue
			}

			// 嵌套结构体递归处理。
			if field.Kind() == reflect.Struct || (field.Kind() == reflect.Ptr && field.Elem().Kind() == reflect.Struct) {
				walkStruct(field.Interface())
				continue
			}

			// 字符串字段整体替换占位符。
			if field.Kind() == reflect.String {
				oldVal := field.String()
				newVal := replaceSingle(oldVal)
				if newVal != oldVal {
					field.SetString(newVal)
				}
			}

			// map 字段（如 ModelConfig.Extra）逐个替换字符串值。
			if field.Kind() == reflect.Map {
				for _, key := range field.MapKeys() {
					val := field.MapIndex(key)
					if val.Kind() == reflect.String {
						oldStr := val.String()
						newStr := replaceSingle(oldStr)
						if newStr != oldStr {
							field.SetMapIndex(key, reflect.ValueOf(newStr))
						}
					}
				}
			}
		}
	}

	// 遍历所有模型配置，替换占位符
	for modelName, modelCfg := range Global.Models {
		walkStruct(&modelCfg)
		Global.Models[modelName] = modelCfg
	}

	// 替换全局配置中的占位符（如jwt.secret）
	walkStruct(Global)
}

// LoadModelConfig 重新加载单个模型配置并同步到全局配置与缓存（用于热更新）。
// 从 ./conf/models/{modelName}.yaml 读取并解码；文件缺失或解码失败时返回错误，
// 且不修改现有配置。
func LoadModelConfig(modelName string) error {
	v := viper.New()
	v.SetConfigName(fmt.Sprintf("models/%s", modelName))
	v.SetConfigType("yaml")
	v.AddConfigPath("./conf")
	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("加载模型 %s 配置失败: %w", modelName, err)
	}

	// 解码到模型配置结构体
	var modelCfg ModelConfig
	if err := v.Unmarshal(&modelCfg); err != nil {
		return fmt.Errorf("解码模型 %s 配置失败: %w", modelName, err)
	}

	// 只对 APIKey 做占位符替换；模型文件其余字段按 YAML 原值使用。
	replaceSingle := func(value string) string {
		if strings.HasPrefix(value, "${") && strings.HasSuffix(value, "}") {
			envKey := strings.TrimSuffix(strings.TrimPrefix(value, "${"), "}")
			return os.Getenv(envKey)
		}
		return value
	}
	modelCfg.APIKey = replaceSingle(modelCfg.APIKey)

	// 写入全局配置并重建缓存，使热更新对 GetModel 立即生效。
	Global.Models[modelName] = modelCfg
	InitModelConfig()

	return nil
}
