// internal/service/session/manager_test.go
// 会话管理器单元测试：验证所在地信息序列化与空值边界。
package session

import (
	"encoding/json"
	"testing"
)

func TestFormatClientLocationForRedisReturnsJSONString(t *testing.T) {
	raw := map[string]string{
		"display": "中国 / 广东 / 深圳",
		"source":  "request_header",
	}

	got := formatClientLocationForRedis(raw)

	var decoded map[string]string
	if err := json.Unmarshal([]byte(got), &decoded); err != nil {
		t.Fatalf("formatClientLocationForRedis returned non-JSON %q: %v", got, err)
	}
	if decoded["display"] != raw["display"] || decoded["source"] != raw["source"] {
		t.Fatalf("decoded location = %#v, want %#v", decoded, raw)
	}
}

func TestFormatClientLocationForRedisReturnsEmptyForEmptyLocation(t *testing.T) {
	if got := formatClientLocationForRedis(nil); got != "" {
		t.Fatalf("empty location = %q, want empty string", got)
	}
}
