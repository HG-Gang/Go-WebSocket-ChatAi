// internal/middleware/auth_test.go
// Auth 中间件与 token 签发测试。
// 覆盖两个安全点：空密钥必须拒绝签发（禁止回退到固定默认密钥）、
// user_name claim 必须注入 gin Context 供后续监控与限流使用。
package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"TozoAI-Chat-Api/conf"
)

// JWT 测试用于防止空密钥回退到固定默认值，并确保用户名称能进入后续监控上下文。
func TestGenerateTokenRejectsEmptySecret(t *testing.T) {
	conf.Global = &conf.GlobalConfig{}
	conf.Global.JWT.Enabled = true
	conf.Global.JWT.Secret = ""

	_, err := GenerateToken("1001")
	if err == nil || !strings.Contains(err.Error(), "jwt secret") {
		t.Fatalf("GenerateToken error = %v, want jwt secret error", err)
	}
}

func TestAuthSetsUserNameFromClaims(t *testing.T) {
	gin.SetMode(gin.TestMode)
	conf.Global = &conf.GlobalConfig{}
	conf.Global.JWT.Enabled = true
	conf.Global.JWT.Secret = "test-secret"

	token, err := GenerateTokenWithUserName("1001", "张三")
	if err != nil {
		t.Fatalf("GenerateTokenWithUserName error = %v", err)
	}

	router := gin.New()
	router.Use(Auth())
	router.GET("/me", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"user_id":   c.GetString("user_id"),
			"user_name": c.GetString("user_name"),
		})
	})

	req := httptest.NewRequest(http.MethodGet, "/me", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"user_name":"张三"`) {
		t.Fatalf("body = %s, want user_name claim in context", w.Body.String())
	}
}
