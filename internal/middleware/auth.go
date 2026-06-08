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

func Auth() gin.HandlerFunc {
	return func(c *gin.Context) {
		if conf.Global == nil || !conf.Global.JWT.Enabled {
			c.Set("user_id", "anonymous")
			c.Set("user_name", "anonymous")
			c.Next()
			return
		}

		jwtSecret, err := resolveJWTSecret()
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"code":    500,
				"message": "JWT 配置错误",
			})
			return
		}

		tokenStr, ok := bearerToken(c)
		if !ok {
			return
		}

		token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unsupported signing method: %s", token.Header["alg"])
			}
			return jwtSecret, nil
		})
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
		if !token.Valid {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"code":    401,
				"message": "Token 无效",
			})
			return
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"code":    401,
				"message": "Token 中身份信息格式错误",
			})
			return
		}

		userID, ok := claims["sub"].(string)
		userID = strings.TrimSpace(userID)
		if !ok || userID == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"code":    401,
				"message": "Token 中缺少有效的用户 ID",
			})
			return
		}

		userName := ""
		if claimName, ok := claims["user_name"].(string); ok {
			userName = strings.TrimSpace(claimName)
		}
		c.Set("user_id", userID)
		c.Set("user_name", userName)
		c.Next()
	}
}

func bearerToken(c *gin.Context) (string, bool) {
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
	if queryToken := strings.TrimSpace(c.Query("token")); queryToken != "" {
		return queryToken, true
	}
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
		"code":    401,
		"message": "缺少 Authorization 请求头或 token 参数",
	})
	return "", false
}

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

func GenerateToken(userID string) (string, error) {
	return GenerateTokenWithUserName(userID, "")
}

func GenerateTokenWithUserName(userID, userName string) (string, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return "", fmt.Errorf("user id is required")
	}

	jwtSecret, err := resolveJWTSecret()
	if err != nil {
		return "", err
	}

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
	if issuer := strings.TrimSpace(conf.Global.JWT.Issuer); issuer != "" {
		claims["iss"] = issuer
	}
	if userName = strings.TrimSpace(userName); userName != "" {
		claims["user_name"] = userName
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(jwtSecret)
}
