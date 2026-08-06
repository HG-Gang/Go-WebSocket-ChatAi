// pkg/utils/utils.go
// 文件功能：通用工具函数集合——UUID 生成、本机 IP 获取、基于 sonic 的 JSON
// 序列化/反序列化、零拷贝字符串与字节切片互转、HTTP 响应写入。各函数输入输出
// 见函数级注释。
// 不负责：业务逻辑、鉴权校验与协议解析。
// 安全边界：Bytes2Str/Str2Bytes 零拷贝返回的结果与源数据共享底层内存，只允许
// 读取；修改字符串或切片会破坏共享数据，可能引发内存错误。
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

// UniqueID 生成不带连字符的 UUIDv7 字符串；v7 生成失败时回退到 UUIDv4，不返回错误。
func UniqueID() string {
	uuidRequest, err := uuid.NewV7()
	if err != nil {
		return strings.ReplaceAll(uuid.NewString(), "-", "")
	}
	return strings.ReplaceAll(uuidRequest.String(), "-", "")
}

// GetLocalIP 返回第一个非回环 IPv4 地址；多网卡环境下返回顺序不确定，
// 无可用 IPv4 地址时返回错误。
func GetLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", fmt.Errorf("failed to get interface addresses: %w", err)
	}

	for _, addr := range addrs {
		// 只考虑 IP 网络地址；回环地址不能用于对外通信，直接跳过。
		if ipnet, ok := addr.(*net.IPNet); ok {
			if ipnet.IP.IsLoopback() {
				continue
			}
			// 优先返回 IPv4，保证客户端连接语义明确；无 IPv4 时该函数整体失败。
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", fmt.Errorf("no valid local IP address found")
}

// Marshal 使用 sonic 将对象序列化为 JSON 字节序列；序列化失败时返回错误。
func Marshal(v interface{}) ([]byte, error) {
	return sonic.Marshal(v)
}

// Unmarshal 将 JSON 字节序列解析到 body 指向的对象；JSON 不合法时返回错误。
func Unmarshal(in []byte, body interface{}) error {
	return sonic.Unmarshal(in, body)
}

// MustToJSON 将对象序列化为 JSON 字符串，忽略序列化错误；
// 仅适用于可保证序列化成功的数据，失败时返回空字符串。
func MustToJSON(obj interface{}) string {
	str, _ := sonic.Marshal(obj)
	return Bytes2Str(str)
}

// MustToJSONByte 与 MustToJSON 行为一致，序列化失败时返回空字符串；
// 保留该副本以兼容既有调用方。
func MustToJSONByte(obj interface{}) string {
	str, _ := sonic.Marshal(obj)
	return Bytes2Str(str)
}

// MustFromJSON 将 JSON 字符串解析为任意值，忽略解析错误；
// 仅适用于数据来源可信的场景，解析失败时返回 nil。
func MustFromJSON(data string) (m interface{}) {
	sonic.Unmarshal(Str2Bytes(data), &m)
	return m
}

// AnyToMap 通过 JSON 中转将任意对象转换为 map[string]any；
// 转换失败时返回空 map。注意 JSON 中转会丢失原始类型信息，数值统一按 float64 读取。
func AnyToMap(data any) map[string]any {
	tmp, _ := Marshal(data)
	var m map[string]any
	_ = Unmarshal(tmp, &m)
	return m
}

// MapToAny 通过 JSON 中转将 map[string]any 转换为 T；
// 转换失败或字段类型不符时，对应字段返回 T 的零值。
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
// 通过临时结构体手动构造切片的 Cap 字段，避免零长度字符串得到的切片容量为 0
// 而在后续写入时越界；结果与源字符串共享底层内存，只允许读取。
func Str2Bytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&struct {
		string
		Cap int
	}{s, len(s)}))
}

// WriteResp 以 JSON Content-Type 写入 HTTP 响应；序列化失败时忽略错误，
// 可能写出不完整响应体，调用方应确保入参可正常序列化。
func WriteResp(w http.ResponseWriter, httpStatus int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	json.NewEncoder(w).Encode(body)
}

// WriteRespWithHttpStatus 仅写入状态码与对应状态文本，不输出 JSON 响应体。
func WriteRespWithHttpStatus(w http.ResponseWriter, httpStatus int) {
	w.WriteHeader(httpStatus)
	fmt.Fprint(w, http.StatusText(httpStatus))
}
