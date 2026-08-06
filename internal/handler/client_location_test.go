// internal/handler/client_location_test.go
// 客户端地理位置提取测试：验证代理地理 Header 的解析优先级与无 Header 时返回 nil。
package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestClientIPLocationFromRequestUsesProxyGeoHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req := httptest.NewRequest(http.MethodGet, "/ws/realtime/openai", nil)
	req.Header.Set("CF-IPCountry", "CN")
	req.Header.Set("X-Geo-Region", "广东")
	req.Header.Set("X-Geo-City", "深圳")
	c.Request = req

	location := clientIPLocationFromRequest(c)

	if location["country"] != "CN" {
		t.Fatalf("country = %q, want CN", location["country"])
	}
	if location["region"] != "广东" {
		t.Fatalf("region = %q, want 广东", location["region"])
	}
	if location["city"] != "深圳" {
		t.Fatalf("city = %q, want 深圳", location["city"])
	}
	if location["source"] != "request_header" {
		t.Fatalf("source = %q, want request_header", location["source"])
	}
}

func TestClientIPLocationFromRequestReturnsNilWithoutGeoHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/ws/realtime/openai", nil)

	if location := clientIPLocationFromRequest(c); location != nil {
		t.Fatalf("location = %#v, want nil", location)
	}
}
