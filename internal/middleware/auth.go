// internal/middleware/auth.go
// JWT 鉴权中间件：负责用户身份验证与令牌管理
package middleware

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"TozoAI-Chat-Api/conf" // 导入项目配置包

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

// Auth JWT 鉴权中间件
// 逻辑流程：
// 1. 检查全局配置，若未启用 JWT 则设为匿名用户直接通过。
// 2. 从 Authorization 请求头获取 Bearer Token。
// 3. 解析并验证 Token 的签名与有效期。
// 4. 提取用户 ID 并存入上下文供后续业务逻辑使用。
func Auth() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 1. 检查 JWT 功能开关
		// 如果在配置中禁用了 JWT 验证，则跳过验证逻辑
		if !conf.Global.JWT.Enabled {
			// 设置默认用户 ID 为 "anonymous"（匿名用户）
			c.Set("user_id", "anonymous")
			c.Next()
			return
		}

		// 2. 从项目全局配置读取 JWT 密钥
		jwtSecretStr := conf.Global.JWT.Secret
		if jwtSecretStr == "" {
			jwtSecretStr = "default-jwt-secret-123456" // 安全兜底默认值
		}
		jwtSecret := []byte(jwtSecretStr)

		// 3. 获取 Token（优先 Authorization Header，其次 URL 参数 token）
		// 浏览器 WebSocket API 不支持自定义 Header，因此 WS 连接需通过 URL 参数传递 Token
		var tokenStr string
		authHeader := c.GetHeader("Authorization")
		if authHeader != "" {
			// 标准格式: "Bearer <token>"
			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
					"code":    401,
					"message": "Token 格式错误，正确格式：Bearer <token>",
				})
				return
			}
			tokenStr = parts[1]
		} else if queryToken := c.Query("token"); queryToken != "" {
			// 兼容 WebSocket 场景：从 URL 参数获取 Token
			// 使用方式: ws://host:port/ws/realtime/openai?token=eyJhbGciOiJI...
			tokenStr = queryToken
		} else {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"code":    401,
				"message": "缺少 Authorization 请求头或 token 参数",
			})
			return
		}

		// 5. 解析并验证 JWT Token
		token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
			// 验证签名算法，防止算法混淆攻击（如使用 None 算法）
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("不支持的签名算法: %s", token.Header["alg"])
			}
			return jwtSecret, nil
		})

		// 6. 处理解析或验证错误
		if err != nil {
			errMsg := "Token 验证失败"
			// 细化错误提示
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

		// 7. 检查 Token 有效性
		if !token.Valid {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"code":    401,
				"message": "Token 无效",
			})
			return
		}

		// 8. 提取 Claims 中的用户身份信息
		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"code":    401,
				"message": "Token 中身份信息格式错误",
			})
			return
		}

		// 9. 提取 sub 字段（用户唯一标识 ID）
		userID, ok := claims["sub"].(string)
		if !ok || userID == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"code":    401,
				"message": "Token 中缺少有效的用户 ID",
			})
			return
		}

		// 10. 将解析出的用户 ID 存入 Gin 上下文，供下游 Handler 使用
		c.Set("user_id", userID)

		// 11. 验证通过，继续执行后续中间件或处理器
		c.Next()
	}
}

// GenerateToken 为指定用户生成 JWT Token
// 参数：userID - 用户的唯一标识
// 返回：生成的 Token 字符串及可能发生的错误
func GenerateToken(userID string) (string, error) {
	// 1. 获取 JWT 密钥
	jwtSecretStr := conf.Global.JWT.Secret
	if jwtSecretStr == "" {
		jwtSecretStr = "default-jwt-secret-123456"
	}
	jwtSecret := []byte(jwtSecretStr)

	// 2. 获取过期时间（单位：小时）
	expireHours := conf.Global.JWT.ExpireHours
	if expireHours <= 0 {
		expireHours = 24 // 默认 24 小时过期
	}

	// 3. 构建负载 (Claims)
	claims := jwt.MapClaims{
		"sub": userID,                                                 // 用户 ID
		"exp": time.Now().Add(time.Duration(expireHours) * time.Hour).Unix(), // 设置过期时间
		"iat": time.Now().Unix(),                                      // 签发时间
	}

	// 4. 创建 Token 实例并进行签名
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(jwtSecret)
}
