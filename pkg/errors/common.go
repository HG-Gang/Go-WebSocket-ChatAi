// pkg/errors/common.go
// 文件功能：定义全局唯一业务错误码常量与统一业务错误结构体 BusinessError。
// 输入：业务侧的错误码与用户可见消息；输出：可被上层直接序列化为 JSON 的错误结构。
// 不负责：各上游（OpenAI/Azure AI）的特有错误类型，其扩展文件见同包
// openai_errors.go、azureai_error.go；也不负责错误日志的记录与上报。
package errors

// 通用业务错误码（全局唯一）
// 命名规则：ERR_CODE_模块_错误类型
const (
	// 通用错误
	ErrCodeSuccess      = "0"             // 成功
	ErrCodeUnknown      = "UNKNOWN"       // 未知错误
	ErrCodeInvalidParam = "INVALID_PARAM" // 参数无效
	ErrCodeUnauthorized = "UNAUTHORIZED"  // 未授权
	ErrCodeForbidden    = "FORBIDDEN"     // 禁止访问
	ErrCodeRateLimit    = "RATE_LIMIT"    // 限流
	ErrCodeNotFound     = "NOT_FOUND"     // 资源不存在

	// 会话错误
	ErrCodeSessionCreate  = "SESSION_CREATE"  // 会话创建失败
	ErrCodeSessionTimeout = "SESSION_TIMEOUT" // 会话超时
	ErrCodeSessionClosed  = "SESSION_CLOSED"  // 会话已关闭

	// Redis错误
	ErrCodeRedisConn = "REDIS_CONN" // Redis连接失败
	ErrCodeRedisOp   = "REDIS_OP"   // Redis操作失败

	// Provider错误
	ErrCodeProviderConnect = "PROVIDER_CONNECT" // Provider连接失败
	ErrCodeProviderOp      = "PROVIDER_OP"      // Provider操作失败

	// JWT错误
	ErrCodeJWTInvalid = "JWT_INVALID" // JWT无效
	ErrCodeJWTExpired = "JWT_EXPIRED" // JWT过期
)

// BusinessError 业务错误结构体
// 只承载业务错误码与用户可见消息，不包含原始错误和调用栈；
// 排查所需的原始错误链由调用方在日志侧自行记录。
type BusinessError struct {
	Code    string `json:"code"` // 业务错误码（如 SESSION_CREATE）
	Message string `json:"message"`
}
