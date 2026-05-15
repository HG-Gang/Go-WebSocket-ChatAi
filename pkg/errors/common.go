// pkg/errors/common.go
// 通用错误处理：
// 1. 定义业务错误码
// 2. 业务错误结构体（包含错误码、消息、原始错误）
// 3. 错误包装/解包工具方法
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
// 包含错误码、用户可见消息、原始错误、调用栈（便于排查）
type BusinessError struct {
	Code    string `json:"code"` // 业务错误码（如SESSION_CREATE）
	Message string `json:"message"`
} // 用户
