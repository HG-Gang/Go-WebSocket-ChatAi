package utils

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"unsafe"

	"github.com/bytedance/sonic"
	"github.com/google/uuid"
)

func UniqueID() string {
	uuidRequest, err := uuid.NewV7()
	if err != nil {
		return strings.ReplaceAll(uuid.NewString(), "-", "")
	}
	return strings.ReplaceAll(uuidRequest.String(), "-", "")
}

func GetLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", fmt.Errorf("failed to get interface addresses: %w", err)
	}

	for _, addr := range addrs {
		// 判断当前地址是否为 IP 网络地址。
		if ipnet, ok := addr.(*net.IPNet); ok {
			// 跳过本机回环地址。
			if ipnet.IP.IsLoopback() {
				continue
			}
			// 这里只返回 IPv4 地址。
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", fmt.Errorf("no valid local IP address found")
}

// Marshal 对象转JSON字符串
func Marshal(v interface{}) ([]byte, error) {
	return sonic.Marshal(v)
}

// Unmarshal JSON字符串转interface{}
func Unmarshal(in []byte, body interface{}) error {
	return sonic.Unmarshal(in, body)
}

// MustToJSON 对象转JSON字符串
func MustToJSON(obj interface{}) string {
	str, _ := sonic.Marshal(obj)
	return Bytes2Str(str)
}

// MustToJSON 对象转JSON字符串
func MustToJSONByte(obj interface{}) string {
	str, _ := sonic.Marshal(obj)
	return Bytes2Str(str)
}

// MustFromJSON JSON字符串转interface{}
func MustFromJSON(data string) (m interface{}) {
	sonic.Unmarshal(Str2Bytes(data), &m)
	return m
}

func AnyToMap(data any) map[string]any {
	tmp, _ := Marshal(data)
	var m map[string]any
	_ = Unmarshal(tmp, &m)
	return m
}

func MapToAny[T any](data map[string]any) T {
	var m T
	tmp, _ := Marshal(data)
	_ = Unmarshal(tmp, &m)
	return m
}

/*// Bytes2Str 把字节切片转换为字符串。
func Bytes2StrV2(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// Str2Bytes 把字符串转换为字节切片。
func Str2BytesV2(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&struct {
		string
		Cap int
	}{s, len(s)}))
}*/

// Bytes2Str 零拷贝字节转字符串
// 警告：转换后的字符串严禁修改，因为底层共享内存
func Bytes2Str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// Str2Bytes 零拷贝字符串转字节切片
// 分析：此处的 struct 强转是为了手动构造切片的 Cap 字段
func Str2Bytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&struct {
		string
		Cap int
	}{s, len(s)}))
}

func WriteResp(w http.ResponseWriter, httpStatus int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	json.NewEncoder(w).Encode(body)
}

func WriteRespWithHttpStatus(w http.ResponseWriter, httpStatus int) {
	w.WriteHeader(httpStatus)
	fmt.Fprint(w, http.StatusText(httpStatus))
}
