// internal/middleware/auth.go
// JWT Bearer 鉴权中间件。
// 文件功能：
// - 输入：Authorization 请求头（Bearer <token>）或 token 查询参数，以及 conf 中的 JWT 配置。
// - 输出：验证通过后把 user_id、user_name 写入 gin Context 并放行；验证失败以 401/500 中止请求。
// - 提供 GenerateToken/GenerateTokenWithUserName 签发 token（测试与内部调用场景）。
// - 不负责 token 刷新、吊销与用户角色（权限）校验。
// 安全边界：
//   - 信任边界：服务端配置属于可信输入；客户端提供的 Authorization 头、query token 与
//     token 内容全部不可信，必须通过签名验证后才可使用。
//   - 密钥必须由配置显式提供，缺失时不回退默认密钥，直接失败关闭。
//   - 只接受 HMAC 签名算法，拒绝 none 与非对称算法，防止算法混淆攻击。
package middleware

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"TozoAI-Chat-Api/conf"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

// Auth 返回 JWT 鉴权中间件。
// 流程：开关检查 → 密钥解析 → Bearer token 提取 → 验签 → claims 校验 → 注入上下文；
// 任何一步失败都以 401（密钥配置故障为 500）中止请求，不放行。
func Auth() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 全局配置未加载或鉴权开关关闭时按匿名用户放行，由下游业务决定是否允许访问。
		if conf.Global == nil || !conf.Global.JWT.Enabled {
			c.Set("user_id", "anonymous")
			c.Set("user_name", "anonymous")
			c.Next()
			return
		}

		// 密钥缺失属于服务端配置故障：失败关闭并返回 500，禁止回退到固定默认密钥。
		jwtSecret, err := resolveJWTSecret()
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"code":    500,
				"message": "JWT 配置错误",
			})
			return
		}

		// 提取 Bearer token，缺失或格式错误时由 bearerToken 以 401 中止请求。
		tokenStr, ok := bearerToken(c)
		if !ok {
			return
		}

		// 验签回调：只接受 HMAC 家族算法（HS256 等），其他算法一律拒绝，防止算法混淆。
		token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unsupported signing method: %s", token.Header["alg"])
			}
			return jwtSecret, nil
		})
		// 验签失败时按错误文本区分过期、签名、格式等常见原因，给出可读提示后拒绝访问。
		if err != nil {
			errMsg := "Token 验证失败"
			if strings.Contains(err.Error(), "expired") {
				errMsg = "Token 已过期"
			} else if strings.Contains(err.Error(), "signature") {
				errMsg = "Token 签名无效"
			} else if strings.Contains(err.Error(), "malformed") {
				errMsg = "Token 格式错误"
			}

			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"code":    401,
				"message": errMsg,
			})
			return
		}
		// 已通过验签但 token 未被标记有效（如 exp 已过期）时同样拒绝，不进入业务。
		if !token.Valid {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"code":    401,
				"message": "Token 无效",
			})
			return
		}

		// claims 必须是 jwt.MapClaims，结构不符视为 token 与预期协议不一致，失败关闭。
		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"code":    401,
				"message": "Token 中身份信息格式错误",
			})
			return
		}

		// 只使用已验签 claims 中的 sub 作为用户 ID；缺失、非字符串或去空白后为空都拒绝，
		// 避免匿名或伪造请求获得身份。
		userID, ok := claims["sub"].(string)
		userID = strings.TrimSpace(userID)
		if !ok || userID == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"code":    401,
				"message": "Token 中缺少有效的用户 ID",
			})
			return
		}

		// user_name 是可选声明，缺失时保持空字符串，不影响鉴权通过。
		userName := ""
		if claimName, ok := claims["user_name"].(string); ok {
			userName = strings.TrimSpace(claimName)
		}
		c.Set("user_id", userID)
		c.Set("user_name", userName)
		c.Next()
	}
}

// bearerToken 从 Authorization 请求头或 token 查询参数提取 token 字符串。
// 提取失败（格式错误或两处均缺失）时以 401 中止请求并返回 false。
func bearerToken(c *gin.Context) (string, bool) {
	// 请求头只接受 "Bearer <token>" 双段格式，前缀不区分大小写，其他格式一律拒绝。
	authHeader := c.GetHeader("Authorization")
	if authHeader != "" {
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"code":    401,
				"message": "Token 格式错误，正确格式：Bearer <token>",
			})
			return "", false
		}
		return strings.TrimSpace(parts[1]), true
	}
	// 兼容旧客户端经查询参数传 token；query 内容完全不可信，仍须走完整验签流程。
	if queryToken := strings.TrimSpace(c.Query("token")); queryToken != "" {
		return queryToken, true
	}
	// 两个来源均缺失时明确返回 401，不静默放行。
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
		"code":    401,
		"message": "缺少 Authorization 请求头或 token 参数",
	})
	return "", false
}

// resolveJWTSecret 读取配置中的 JWT 密钥并去除首尾空白。
// 全局配置缺失或密钥为空时返回错误，调用方必须失败关闭，禁止回退到固定默认密钥。
func resolveJWTSecret() ([]byte, error) {
	if conf.Global == nil {
		return nil, fmt.Errorf("jwt secret is not configured")
	}
	secret := strings.TrimSpace(conf.Global.JWT.Secret)
	if secret == "" {
		return nil, fmt.Errorf("jwt secret is not configured")
	}
	return []byte(secret), nil
}

// GenerateToken 仅用用户 ID 签发 JWT，user_name 声明为空。
func GenerateToken(userID string) (string, error) {
	return GenerateTokenWithUserName(userID, "")
}

// GenerateTokenWithUserName 使用配置的 HMAC 密钥签发 HS256 JWT。
// 成功返回签名后的 token 字符串；用户 ID 为空或 JWT 密钥缺失时返回错误。
func GenerateTokenWithUserName(userID, userName string) (string, error) {
	// 去除空白后仍为空说明调用方未提供有效用户，拒绝签发无法关联用户的 token。
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return "", fmt.Errorf("user id is required")
	}

	jwtSecret, err := resolveJWTSecret()
	if err != nil {
		return "", err
	}

	// 有效期未配置时默认 24 小时，过期后 token 在验签环节自动失效。
	expireHours := conf.Global.JWT.ExpireHours
	if expireHours <= 0 {
		expireHours = 24
	}
	now := time.Now()
	claims := jwt.MapClaims{
		"sub":     userID,
		"user_id": userID,
		"exp":     now.Add(time.Duration(expireHours) * time.Hour).Unix(),
		"iat":     now.Unix(),
	}
	// iss 与 user_name 仅在配置或入参非空时写入，避免向 token 注入空声明。
	if issuer := strings.TrimSpace(conf.Global.JWT.Issuer); issuer != "" {
		claims["iss"] = issuer
	}
	if userName = strings.TrimSpace(userName); userName != "" {
		claims["user_name"] = userName
	}

	// 签发并签名，返回的字符串即客户端后续在 Authorization 头携带的 Bearer token。
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(jwtSecret)
}
