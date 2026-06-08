// Package conf 提供配置文件加载核心逻辑
//  1. 加载基础配置（config.yaml）
//  2. 加载环境专属配置（config_dev.yaml / config_prod.yaml）
//     文件名格式为 config_{env}.yaml（下划线分隔），使用 SetConfigName() 直接搜索。
//  3. 加载模型专属配置（conf/models/*.yaml）
//  4. 替换环境变量占位符（${ENV_VAR}）
//  5. 初始化模型配置缓存
package conf

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/spf13/viper" // 配置读取核心库
)

// Load 加载所有配置（入口方法，main中调用）
// 加载顺序（后加载的覆盖先加载的）：
// 1. 基础配置（config.yaml）
// 2. 环境配置（config_{env}.yaml，如 config_dev.yaml / config_prod.yaml）
// 3. 模型配置（conf/models/*.yaml）
// 4. 环境变量（替换占位符）
// 返回：错误信息（nil表示成功）
func Load() error {
	v := viper.New() // 创建Viper实例（避免全局污染）

	// 1. 环境变量配置（前缀TOZO，支持嵌套key转换）
	v.SetEnvPrefix("TOZO")                             // 环境变量前缀：TOZO_
	v.AutomaticEnv()                                   // 自动读取环境变量
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // 嵌套key转换：redis.addr → TOZO_REDIS_ADDR

	// 2. 读取基础配置（config.yaml）
	v.SetConfigName("config") // 配置文件名（无后缀）
	v.SetConfigType("yaml")   // 配置文件类型
	v.AddConfigPath("./conf") // 配置文件路径1
	v.AddConfigPath(".")      // 配置文件路径2（兜底）
	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("读取主配置失败: %w", err)
	}

	// 3. 确定运行环境（默认dev）
	env := v.GetString("env")
	if env == "" {
		env = "dev"
	}
	v.Set("env", env) // 写入Viper，确保后续配置能读取

	// 4. 加载环境专属配置（config_{env}.yaml，如 config_dev.yaml / config_prod.yaml）
	// 文件名格式：config_{env}.yaml（下划线分隔，与 Viper SetConfigName 兼容）
	// 搜索路径：./conf → ./（兼容不同工作目录启动）
	envConfigName := fmt.Sprintf("config_%s", env)
	v.SetConfigName(envConfigName)
	v.SetConfigType("yaml")
	if mergeErr := v.MergeInConfig(); mergeErr == nil {
		fmt.Printf("已加载环境配置: %s.yaml\n", envConfigName)
	} else {
		fmt.Printf("未找到环境配置 %s.yaml，跳过\n", envConfigName)
	}

	// 5. 解码基础配置到全局结构体。conf/models/*.yaml 是“单模型根配置”，
	// 必须在解码后写入 Global.Models[modelName]，不能直接 Merge 到全局根节点。
	Global = &GlobalConfig{}
	if err := v.Unmarshal(Global); err != nil {
		return fmt.Errorf("配置解码失败: %w", err)
	}
	rootModelOverrides := cloneModelConfigMap(Global.Models)

	// 6. 加载模型专属配置（conf/models/*.yaml）
	if err := loadModelOverrides("./conf/models", rootModelOverrides); err != nil {
		fmt.Printf("加载模型专属配置失败: %v\n", err)
	}

	// 7. 替换配置中的环境变量占位符（${ENV_VAR} → 实际值）
	replaceEnvPlaceholders()

	if err := validateProductionConfig(Global); err != nil {
		return err
	}

	// 8. 初始化模型配置缓存（加速GetModel调用）
	InitModelConfig()

	// 9. 打印启动信息（便于排查）
	fmt.Printf("[TozoAI] 环境: %s | 服务地址: %s | 模型数: %d\n",
		Global.Env, Global.Server.Addr, len(Global.Models))

	return nil
}

// loadModelOverrides 将 conf/models/openai.yaml 这类模型文件加载到 Global.Models["openai"]。
// 模型文件本身就是一个独立的 ModelConfig 结构，不再额外包一层 models.openai。
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
		if rootOverride, ok := rootModelOverrides[modelName]; ok {
			applyRootModelOverride(&modelCfg, rootOverride)
		}

		Global.Models[modelName] = modelCfg
		fmt.Printf("已加载模型配置: %s.yaml\n", modelName)
	}

	return nil
}

func cloneModelConfigMap(src map[string]ModelConfig) map[string]ModelConfig {
	dst := make(map[string]ModelConfig, len(src))
	for name, cfg := range src {
		dst[name] = cfg
	}
	return dst
}

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

// replaceEnvPlaceholders 替换配置中的环境变量占位符（通用实现）
// 支持所有模型的所有字段中的 ${ENV_VAR} 占位符
func replaceEnvPlaceholders() {
	// 单个占位符替换函数
	replaceSingle := func(value string) string {
		// 检查是否是占位符格式：${ENV_VAR}
		if strings.HasPrefix(value, "${") && strings.HasSuffix(value, "}") {
			envKey := strings.TrimSuffix(strings.TrimPrefix(value, "${"), "}")
			return os.Getenv(envKey) // 替换为环境变量值
		}
		return value // 非占位符直接返回
	}

	// 递归遍历结构体，替换所有字符串字段中的占位符
	var walkStruct func(v interface{}) // 先声明函数，支持递归调用
	walkStruct = func(v interface{}) {
		val := reflect.ValueOf(v)
		// 处理指针：取指向的值
		if val.Kind() == reflect.Ptr {
			val = val.Elem()
		}
		// 仅处理结构体
		if val.Kind() != reflect.Struct {
			return
		}

		// 遍历结构体字段
		for i := 0; i < val.NumField(); i++ {
			field := val.Field(i)
			// 跳过不可修改的字段
			if !field.CanSet() {
				continue
			}

			// 递归处理嵌套结构体
			if field.Kind() == reflect.Struct || (field.Kind() == reflect.Ptr && field.Elem().Kind() == reflect.Struct) {
				walkStruct(field.Interface())
				continue
			}

			// 替换字符串字段中的占位符
			if field.Kind() == reflect.String {
				oldVal := field.String()
				newVal := replaceSingle(oldVal)
				if newVal != oldVal {
					field.SetString(newVal)
				}
			}

			// 处理map字段（如ModelConfig.Extra）
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

// LoadModelConfig 加载单个模型配置（扩展方法，用于热加载）
// 参数：modelName - 模型名称
// 返回：错误信息
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

	// 替换占位符
	replaceSingle := func(value string) string {
		if strings.HasPrefix(value, "${") && strings.HasSuffix(value, "}") {
			envKey := strings.TrimSuffix(strings.TrimPrefix(value, "${"), "}")
			return os.Getenv(envKey)
		}
		return value
	}
	modelCfg.APIKey = replaceSingle(modelCfg.APIKey)

	// 更新全局配置
	Global.Models[modelName] = modelCfg
	// 更新缓存
	InitModelConfig()

	return nil
}
