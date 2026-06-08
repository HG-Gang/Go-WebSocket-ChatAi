// internal/service/session/manager.go
// 会话管理核心逻辑：
// 1. 创建/启动/关闭会话
// 2. 会话元数据存储（Redis）
// 3. 计费统计（Token 消耗）
//
// ======================== requestId 贯穿日志 ========================
//
// 每次 WS 连接时，handler 层生成 requestId，传入 NewSession。
// Session 和 Provider（client_ws）的所有日志都自动携带 request_id + user_id，
// 方便按单次连接检索完整链路日志。
//
// ======================== 心跳职责划分 ========================
//
// 心跳检测完全由 Provider 层（client_ws.go 的 writePump/readPump）负责。
// Session 层不参与心跳，只负责生命周期管理、Redis 元数据和计费。
package session

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/logger"
	"TozoAI-Chat-Api/internal/provider"
	"TozoAI-Chat-Api/internal/service/billing"
	"TozoAI-Chat-Api/internal/service/metrics"
	"TozoAI-Chat-Api/internal/service/redis"
)

// Session 会话结构体
type Session struct {
	ID         string            // 会话唯一ID（UUID V7）
	UserID     string            // 用户ID（JWT解析得到）
	UserName   string            // 用户名称（JWT解析得到，可为空）
	DeviceID   string            // App/耳机侧上报的设备ID（可选，用于排查单设备断链）
	RequestID  string            // 本次连接的全局唯一请求ID（贯穿全链路日志）
	Model      string            // 模型名称（openai/azureai）
	RemoteAddr string            // App 连接来源地址
	IPLocation map[string]string // 反向代理/CDN 提供的粗粒度所在地（仅用于监控展示）
	UserAgent  string            // App/调试页面 User-Agent
	StartTime  time.Time         // 会话开始时间
	AppConn    *websocket.Conn   // 与App的WS连接
	Provider   provider.Provider // 模型Provider实例
	mu         sync.Mutex        // 会话操作互斥锁
	closed     bool              // 关闭标记
	log        *zap.Logger       // 会话专属日志（带 request_id/session_id/user_id）
}

// NewSession 创建新会话
// 参数：
//   - userID: 用户ID
//   - model: 模型名称
//   - requestID: 本次连接的全局请求ID（由 handler 生成）
//   - remoteAddr/userAgent/deviceID: 连接来源元数据，只用于日志、Redis 和调试指标
//   - appConn: 与App的WS连接
//   - prov: 模型Provider实例
func NewSession(userID, userName, model, requestID, remoteAddr, userAgent, deviceID string, appConn *websocket.Conn, prov provider.Provider) *Session {
	// 生成会话ID（优先UUID V7，降级UUID V4）
	var sessionID string
	uuidV7, err := uuid.NewV7()
	if err != nil {
		sessionID = uuid.New().String()
	} else {
		sessionID = uuidV7.String()
	}

	// 创建会话专属日志（自动携带 request_id + session_id + user_id）
	// 后续所有通过 s.log 输出的日志都会带上这三个字段
	log := logger.GetModelLogger(model).With(
		zap.String("request_id", requestID),
		zap.String("session_id", sessionID),
		zap.String("user_id", userID),
		zap.String("user_name", userName),
	)

	return &Session{
		ID:         sessionID,
		UserID:     userID,
		UserName:   userName,
		DeviceID:   deviceID,
		RequestID:  requestID,
		Model:      model,
		RemoteAddr: remoteAddr,
		UserAgent:  userAgent,
		StartTime:  time.Now(),
		AppConn:    appConn,
		Provider:   prov,
		log:        log,
	}
}

// SetClientLocation 设置请求入口解析出的所在地信息。
// 该信息只用于监控与日志排障，不参与鉴权或安全策略。
func (s *Session) SetClientLocation(location map[string]string) {
	if s == nil || len(location) == 0 {
		return
	}
	s.IPLocation = make(map[string]string, len(location))
	for key, value := range location {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key != "" && value != "" {
			s.IPLocation[key] = value
		}
	}
}

// Start 启动会话
func (s *Session) Start(ctx context.Context) {
	s.log.Info("会话启动",
		zap.String("model", s.Model),
		zap.Time("start_time", s.StartTime))
	remoteAddr := ""
	if s.RemoteAddr != "" {
		remoteAddr = s.RemoteAddr
	}
	if remoteAddr == "" && s.AppConn != nil && s.AppConn.RemoteAddr() != nil {
		remoteAddr = s.AppConn.RemoteAddr().String()
	}
	metrics.SessionStartedWithLocation(s.ID, s.RequestID, s.UserID, s.UserName, s.DeviceID, s.Model, remoteAddr, s.UserAgent, s.IPLocation)

	// 1. 记录会话元数据到Redis（使用 HSetMap 支持 map 批量写入）
	redisKey := fmt.Sprintf("session:%s", s.ID)
	modelCfg := conf.GetModel(s.Model)
	ttl := time.Duration(modelCfg.MaxSessionTTL) * time.Second
	_ = redis.HSetMap(s.Model, redisKey, map[string]interface{}{
		"user_id":     s.UserID,
		"user_name":   s.UserName,
		"device_id":   s.DeviceID,
		"request_id":  s.RequestID,
		"model":       s.Model,
		"remote_addr": remoteAddr,
		"ip_location": formatClientLocationForRedis(s.IPLocation),
		"user_agent":  s.UserAgent,
		"start_time":  s.StartTime.Unix(),
		"status":      "active",
		"max_ttl":     modelCfg.MaxSessionTTL,
	}, ttl)

	// 2. 将带 request_id 的 logger 传递给 Provider（贯穿四协程日志）
	//    Provider.SetLogger 是可选方法，通过接口断言调用
	if lp, ok := s.Provider.(provider.LoggerProvider); ok {
		lp.SetLogger(s.log)
	}
	if cp, ok := s.Provider.(provider.SessionContextProvider); ok {
		cp.SetSessionContext(s.UserID, s.ID)
	}

	// 3. 启动Provider的WS处理逻辑（阻塞直到会话结束）
	if err := s.Provider.HandleWS(ctx, s.AppConn); err != nil {
		s.log.Error("Provider WS 处理异常退出", zap.Error(err))
	}

	// 4. HandleWS 返回 = 会话结束
	metrics.SessionEnded(s.ID, "provider_return", time.Since(s.StartTime))
	s.log.Info("会话正常结束",
		zap.Float64("duration_seconds", time.Since(s.StartTime).Seconds()))
}

func formatClientLocationForRedis(location map[string]string) string {
	if len(location) == 0 {
		return ""
	}
	data, err := json.Marshal(location)
	if err != nil {
		return ""
	}
	return string(data)
}

// Close 优雅关闭会话
func (s *Session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}
	s.closed = true

	// 1. 关闭与App的WS连接
	if s.AppConn != nil {
		_ = s.AppConn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "session closed"),
		)
		_ = s.AppConn.Close()
	}

	// 2. 关闭Provider连接
	if s.Provider != nil {
		_ = s.Provider.Close()
	}

	// 3. 更新Redis会话状态
	redisKey := fmt.Sprintf("session:%s", s.ID)
	_ = redis.HSet(s.Model, redisKey, "status", "closed")
	_ = redis.HSet(s.Model, redisKey, "end_time", time.Now().Unix())

	// 4. 统计会话耗时
	duration := time.Since(s.StartTime).Seconds()
	metrics.SessionEnded(s.ID, "session_close", time.Since(s.StartTime))
	s.log.Info("会话已关闭", zap.Float64("duration_seconds", duration))

	// 5. Token消耗统计
	input, output, total, err := billing.GetSessionUsage(s.Model, s.ID)
	if err != nil {
		s.log.Warn("获取会话 Token 消耗失败", zap.Error(err))
	} else if total > 0 {
		s.log.Info("会话 Token 消耗统计",
			zap.Int64("input_tokens", input),
			zap.Int64("output_tokens", output),
			zap.Int64("total_tokens", total))
	}
}
