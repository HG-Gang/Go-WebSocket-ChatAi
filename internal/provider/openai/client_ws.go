// internal/provider/openai/client_ws.go
// 文件功能：OpenAI Realtime WS 双向转发客户端核心。
// 输入：App 端 WebSocket 连接（文本/二进制帧，兼容旧 msgType 协议与 OpenAI 原生事件）、
// 上游 OpenAI Realtime WS 地址。
// 输出：转发给 OpenAI 的客户端事件、回传给 App 的 StandardResponse（begin/end/text_delta/
// audio_delta/错误/重连通知等）。
// 负责：四协程双向转发（readPump/openAIWritePump/recvPump/writePump）、双端心跳检测、
// OpenAI 断线重连与最小会话状态恢复、response.create/cancel 状态机串行化。
// 不负责：API Key 读取与连接 URL/Header 构造（config.go）、旧协议到 OpenAI 事件的转换细节
// （gateway_protocol.go）、function tool 的具体执行（tool_execution.go）、HTTP 降级
// （HandleFallback 仅返回固定状态码）。
//
// 安全边界（fail-closed）：
//   - API Key 不落日志；URL 与错误信息统一经 SafeURLForDisplay/RedactField 脱敏后记录。
//   - App 心跳超时视为 App 已离线：readPump 调 cancel()，全部协程退出并关闭 OpenAI 连接。
//   - OpenAI 写失败、重连失败均先向 App 下发 reconnect_required 再结束会话，不静默吞错。
//   - 上行单条消息 64KB、下行单帧 16MB 限制；sendChan/apiSendChan 满时按事件关键性
//     丢弃 best-effort 事件或返回错误，防止慢客户端耗尽服务端内存。
//
// ======================== 架构设计 ========================
//
//	四协程模型：
//
//	  readPump:        App → Go              只读 App WS，兼容旧 msgType 协议，产出 OpenAI 事件
//	  openAIWritePump: Go → OpenAI           只写 OpenAI WS，执行 response 状态机、上游写入、重连请求
//	  recvPump:        OpenAI → Go           只读 OpenAI WS，处理流式响应、状态机推进、触发重连
//	  writePump:       Go → App              只写 App WS，发心跳 Ping，下推音频/text/错误
//
//	协程生命周期：所有 channel 只在初始化时创建、从不 close；协程退出统一由 context cancel
//	驱动——任一协程因致命错误退出都会在 defer 中调 cancel()，其余协程随即从 select 中退出，
//	从根上避免"发送方已退出但接收方仍在等 channel"的双向关闭竞态。HandleWS 在 wg.Wait()
//	返回后由 session 层关闭连接。
//
// ======================== 双端心跳机制 ========================
//
//	App↔Go 心跳（WS Ping/Pong 协议层）：
//	  - writePump 按 app_ping_interval 配置定时发 Ping
//	  - readPump 设 SetReadDeadline = app_pong_timeout
//	    → 超时未收到 App 任何消息（包括 Pong）→ ReadMessage 返回 error
//	    → readPump 调 cancel() → 所有协程退出 → session.Close() 关闭 OpenAI 连接
//	  - 语义：超时 = App 已离线/崩溃，当前会话结束，服务端主动断开 OpenAI
//
//	Go↔OpenAI 心跳（通过读超时检测）：
//	  - recvPump 对 apiConn 设 SetReadDeadline = api_read_timeout
//	    → 超时说明 OpenAI 连接可能断了 → 触发 reconnect()
//	    → 重连失败 → 发 reconnect_required 给 App 并退出
//	  - OpenAI Realtime API 本身会持续推送事件，正常不会超时
//
// ======================== 断线后的重连流程 ========================
//
//	场景 A：App→Go 断开
//	  1. readPump 检测到 ReadMessage error → cancel() → 所有协程退出
//	  2. HandleWS 返回 → session.Close() → 关闭 OpenAI WS
//	  3. App 重新发起 WS 连接 → handler 创建全新 Session + Provider
//	  4. App 发送 conversation.item.create 事件恢复历史 → Go 透传给新 OpenAI 连接
//
//	场景 B：Go→OpenAI 断开
//	  1. recvPump 检测到 ReadMessage error → 请求 openAIWritePump 执行 reconnect()
//	  2. 重连成功 → continue 继续读取
//	  3. 重连失败 → 发 reconnect_required 给 App → cancel() → 所有协程退出
//	  4. App 收到 reconnect_required 后由客户端决定是否重新发起连接
//
// ======================== 心跳配置来源 ========================
//
//	所有心跳秒数均从配置文件读取（conf/config.yaml → models.openai.realtime）：
//	  - app_ping_interval: Go→App 发 Ping 间隔（默认 30s）
//	  - app_pong_timeout:  等待 App Pong 超时（默认 60s，超时=会话结束）
//	  - api_read_timeout:  读取 OpenAI 超时（默认 120s，超时=触发重连）
//
//	配置读取路径：config.yaml → conf.ModelConfig.Realtime → OpenAIConfig.GetXxx()
package openai

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/logger"
	"TozoAI-Chat-Api/internal/service/billing"
	"TozoAI-Chat-Api/internal/service/metrics"
	"TozoAI-Chat-Api/internal/service/stats"
	protocol "TozoAI-Chat-Api/pkg/protocol/openai"
	"TozoAI-Chat-Api/pkg/response"
)

// ======================== 常量 ========================

const (
	// writeWait 每次写 App WS 的超时（防止写阻塞）
	writeWait = 10 * time.Second
	// maxMessageSize App 端单条消息最大 64KB
	maxMessageSize = 64 * 1024
	// sendChanSize 发往 App 的 channel 缓冲
	sendChanSize = 512
	// apiSendChanSize 发往 OpenAI 的 channel 缓冲。它隔离 App 读协程和上游写协程，
	// 避免 OpenAI 写阻塞直接卡住 App WS 读取和心跳检测。
	apiSendChanSize = 512
	// apiMaxMessageSize 限制单个 OpenAI 事件帧大小。
	// 音频 delta 可能较大，所以这里明显大于 App 上行帧限制。
	apiMaxMessageSize = 16 * 1024 * 1024
)

// ======================== 消息包装 ========================

// wsMessage 内部 channel 传递的消息（携带 WS 消息类型）
type wsMessage struct {
	messageType int    // websocket.TextMessage / BinaryMessage
	data        []byte // 原始数据
}

// openAIOutbound 是 readPump/recvPump 投递给 openAIWritePump 的上游写任务。
// eventType 用于 responseGate 判断是否需要拦截 response.create/cancel；
// bypassGate 只用于 flush pending response.create，因为状态机已经在取出 pending 时完成了状态切换。
type openAIOutbound struct {
	eventType  string
	data       []byte
	reason     string
	bypassGate bool
}

// reconnectRequest 由 recvPump 发送给 openAIWritePump。
// 这样 OpenAI 重连和恢复写入都在上游写协程内完成，避免多个 goroutine 同时写 OpenAI WS。
type reconnectRequest struct {
	reason string
	done   chan error
}

// replayState 保存 OpenAI 上游重连后可安全恢复的最小状态。
// 只缓存 session.update 和 conversation.item.*，避免重放音频 buffer 或 response.create
// 导致重复语音输入、重复响应或 conversation_already_has_active_response。
type replayState struct {
	mu                sync.Mutex
	lastSessionUpdate []byte
	history           [][]byte
	limit             int
}

// newReplayState 创建重连恢复缓存；limit 非正数时回退默认 32 条，保证历史事件数量有界。
func newReplayState(limit int) *replayState {
	if limit <= 0 {
		limit = 32
	}
	return &replayState{limit: limit}
}

// remember 缓存可安全重放的上游状态：仅记录 session.update 与 conversation.item.* 事件，
// 忽略 response.create、input_audio_buffer.append 等，避免重连重放导致重复响应或重复语音输入。
// 写入前深拷贝 data，防止调用方复用底层切片篡改缓存。
func (s *replayState) remember(eventType string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	copied := append([]byte(nil), data...)
	switch eventType {
	case string(protocol.ClientEventTypeSessionUpdate):
		s.lastSessionUpdate = copied
	case string(protocol.ClientEventTypeConversationItemCreate),
		string(protocol.ClientEventTypeConversationItemTruncate),
		string(protocol.ClientEventTypeConversationItemDelete):
		s.history = append(s.history, copied)
		if len(s.history) > s.limit {
			s.history = append([][]byte(nil), s.history[len(s.history)-s.limit:]...)
		}
	}
}

// snapshot 返回深拷贝的最新 session.update 与最近 limit 条会话历史，供重连后重放。
// 深拷贝保证与 remember 并发读写安全：remember 发生在 openAIWritePump 投递前，
// snapshot 发生在重连恢复时。
func (s *replayState) snapshot() (session []byte, history [][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lastSessionUpdate != nil {
		session = append([]byte(nil), s.lastSessionUpdate...)
	}
	history = make([][]byte, 0, len(s.history))
	for _, item := range s.history {
		history = append(history, append([]byte(nil), item...))
	}
	return session, history
}

// ======================== Client 结构体 ========================

// Client OpenAI Realtime WS 客户端
// 线程安全说明：
//   - appConn 写操作全部由 writePump 负责（通过 sendChan 串行化）
//   - apiConn 写操作全部由 openAIWritePump 负责（通过 apiSendChan 串行化）
//   - apiConn 读操作由 recvPump 独占
//   - OpenAI 重连请求由 recvPump 发给 openAIWritePump 执行，重连恢复写入不跨协程抢写
//   - session 层不直接操作 appConn，避免并发写
type Client struct {
	providerName        string                // 当前 Provider 名称（openai/azureai），用于日志、计费和配置识别
	cfg                 *OpenAIConfig         // 配置（含心跳参数，从配置文件读取）
	log                 *zap.Logger           // 带 providerName 命名空间的日志
	apiConn             *websocket.Conn       // Go→OpenAI WS 连接
	connMu              sync.Mutex            // 保护 apiConn 指针替换；读协程读取指针时也经过它
	reconnMu            sync.Mutex            // 串行化 OpenAI 重连，避免重复重建连接
	appConn             *websocket.Conn       // App→Go WS 连接
	sendChan            chan []byte           // readPump/recvPump → writePump → App
	apiSendChan         chan openAIOutbound   // readPump/recvPump → openAIWritePump → OpenAI
	apiReconnectChan    chan reconnectRequest // recvPump → openAIWritePump，集中执行上游重连
	retryCnt            int                   // 当前重连计数
	replay              *replayState          // OpenAI 上游重连后的最小会话恢复状态
	userID              string                // 当前业务用户，用于计费和链路观测
	sessionID           string                // 当前业务会话，用于计费和链路观测
	gateway             *gatewayAdapter
	respGate            *openAIResponseGate
	writeOpenAIPingFunc func() error // 测试注入点；生产环境为空时走真实 WebSocket Ping
}

// NewClient 创建客户端（实现 provider.Provider 接口）
func NewClient(cfg *conf.ModelConfig) *Client {
	return NewClientWithName(cfg, "openai")
}

// NewClientWithName 创建 Realtime 客户端。
// 同一套四协程链路同时服务 openai 和 azureai：差异只在配置生成的 URL 与鉴权 Header。
func NewClientWithName(cfg *conf.ModelConfig, providerName string) *Client {
	if providerName == "" {
		providerName = "openai"
	}
	openaiCfg := NewOpenAIConfig(cfg)
	return &Client{
		providerName:     providerName,
		cfg:              openaiCfg,
		log:              logger.GetModelLogger(providerName),
		sendChan:         make(chan []byte, sendChanSize),
		apiSendChan:      make(chan openAIOutbound, apiSendChanSize),
		apiReconnectChan: make(chan reconnectRequest, 1),
		replay:           newReplayState(openaiCfg.GetRestoreHistoryLimit()),
		gateway:          newGatewayAdapter(),
		respGate:         newOpenAIResponseGate(),
	}
}

// ======================== Provider 接口 ========================

func (c *Client) Name() string { return c.providerName }

// SetLogger 注入带 request_id 的 logger（实现 provider.LoggerProvider 接口）
// 由 session.Start 调用，使四个主协程日志都携带 request_id + user_id + session_id
func (c *Client) SetLogger(log *zap.Logger) {
	c.log = log
}

func (c *Client) SetSessionContext(userID, sessionID string) {
	c.userID = userID
	c.sessionID = sessionID
}

// Connect 建立 Go→OpenAI WS 连接
func (c *Client) Connect(ctx context.Context) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	metrics.OpenAIConnectAttempt(c.sessionID)

	// 关闭旧连接（重连场景）
	if c.apiConn != nil {
		_ = c.apiConn.Close()
		c.apiConn = nil
	}

	if c.cfg.APIKey == "" {
		err := fmt.Errorf("%s api key is empty", c.Name())
		metrics.OpenAIConnectResult(c.sessionID, err)
		return err
	}

	fullURL, err := c.cfg.BuildRealtimeURL()
	if err != nil {
		metrics.OpenAIConnectResult(c.sessionID, err)
		return err
	}
	header := c.cfg.BuildRealtimeHeader()

	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = c.cfg.GetApiWriteTimeout()

	// 显式代理：当 realtime.proxy_url 在 yaml 中配置时强制走该代理，避免依赖进程环境变量。
	// 留空时维持 websocket.DefaultDialer.Proxy = http.ProxyFromEnvironment 的默认行为。
	proxySource := "env"
	if configuredProxy := c.cfg.GetProxyURL(); configuredProxy != "" {
		parsed, perr := url.Parse(configuredProxy)
		if perr != nil {
			err := fmt.Errorf("解析 realtime.proxy_url 失败: %w", perr)
			c.log.Error("无效的 realtime.proxy_url 配置",
				zap.String("provider", c.Name()),
				zap.String("proxy_url", logger.SafeURLForDisplay(configuredProxy)),
				zap.Error(perr))
			metrics.OpenAIConnectResult(c.sessionID, err)
			return err
		}
		dialer.Proxy = http.ProxyURL(parsed)
		proxySource = "config"
	}

	c.log.Info("正在连接 Realtime 上游",
		zap.String("provider", c.Name()),
		zap.String("url", logger.SafeURLForDisplay(fullURL)),
		zap.String("proxy_source", proxySource))
	conn, _, err := dialer.DialContext(ctx, fullURL, header)
	if err != nil {
		safeErr := fmt.Errorf("connect %s realtime failed: url=%s error=%s", c.Name(), logger.SafeURLForDisplay(fullURL), logger.RedactField("content", err.Error()))
		c.log.Error("连接 Realtime 上游失败",
			zap.String("provider", c.Name()),
			zap.String("url", logger.SafeURLForDisplay(fullURL)),
			zap.String("error_summary", logger.RedactField("content", err.Error())))
		metrics.OpenAIConnectResult(c.sessionID, safeErr)
		return safeErr
	}

	conn.SetReadLimit(apiMaxMessageSize) // 单帧上限 16MB：音频 delta 可能较大，同时防止恶意上游耗尽内存
	apiPongTimeout := c.cfg.GetApiPongTimeout()
	_ = conn.SetReadDeadline(time.Now().Add(apiPongTimeout))
	// 握手成功即注册 Pong 处理器：上游回 Pong 时重置读超时，与 recvPump 的读超时检测配合保活
	conn.SetPongHandler(func(appData string) error {
		_ = conn.SetReadDeadline(time.Now().Add(apiPongTimeout))
		c.log.Debug("收到 OpenAI Pong，重置上游读超时", zap.Duration("timeout", apiPongTimeout))
		return nil
	})

	c.apiConn = conn
	c.retryCnt = 0 // 连接成功即重置重连计数，让 reconnect 的上限只统计连续失败次数
	metrics.OpenAIConnectResult(c.sessionID, nil)
	c.log.Info("Realtime 上游连接成功", zap.String("provider", c.Name()))
	return nil
}

// HandleWS 双向 WS 转发入口（阻塞直到会话结束）
//
// 生命周期：
//  1. 连接 OpenAI
//  2. 读取心跳配置（全部从配置文件）并打印
//  3. 启动四个主协程
//  4. wg.Wait() 等待所有协程退出
//  5. 返回 → session.Close() → 释放资源
func (c *Client) HandleWS(ctx context.Context, appConn *websocket.Conn) error {
	if appConn == nil {
		return fmt.Errorf("appConn is nil")
	}
	c.appConn = appConn

	if err := c.Connect(ctx); err != nil {
		return err
	}

	// 读取心跳配置值（全部从 config.yaml 的 realtime 节读取）
	appPing := c.cfg.GetAppPingInterval() // App↔Go: Ping 发送间隔
	appPong := c.cfg.GetAppPongTimeout()  // App↔Go: Pong 等待超时
	apiRead := c.cfg.GetApiReadTimeout()  // Go↔OpenAI: 读超时
	c.log.Info("心跳配置（从配置文件读取）",
		zap.Duration("app_ping_interval", appPing),
		zap.Duration("app_pong_timeout", appPong),
		zap.Duration("api_read_timeout", apiRead),
		zap.Duration("api_pong_timeout", c.cfg.GetApiPongTimeout()),
		zap.Duration("api_write_timeout", c.cfg.GetApiWriteTimeout()),
		zap.Int("reconnect_max_retries", c.cfg.GetMaxRetries()),
		zap.Duration("reconnect_delay", c.cfg.GetReconnectDelay()))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// 四个 channel 只在初始化时创建、从不 close，协程退出统一由 cancel 驱动：
	// 任一协程致命错误退出都会在 defer 中调 cancel()，其余协程随即从 select 中退出，
	// 从根上避免"发送方已退出但接收方仍在等 channel"的双向关闭竞态

	var wg sync.WaitGroup
	wg.Add(4)
	go c.readPump(ctx, cancel, &wg)        // App -> Go: 读取 App 消息并投递 OpenAI 写队列
	go c.openAIWritePump(ctx, cancel, &wg) // Go -> OpenAI: 唯一 OpenAI 写者，执行上游重连
	go c.recvPump(ctx, cancel, &wg)        // OpenAI -> Go: 唯一 OpenAI 读者，处理流式响应
	go c.writePump(ctx, cancel, &wg)       // Go -> App: 唯一 App 写者，推送音频/text/心跳

	wg.Wait()
	c.log.Info("HandleWS 结束，所有协程已退出")
	return nil
}

// HandleFallback HTTP 降级入口：fallback 未启用时拒绝访问（403），启用后返回 501，
// 具体降级逻辑尚未实现，仅保证不把降级请求漏进 WS 链路。
func (c *Client) HandleFallback(ctx *gin.Context) {
	if !conf.Global.Fallback.Enabled {
		ctx.JSON(http.StatusForbidden, gin.H{"error": "fallback disabled"})
		return
	}
	ctx.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
}

// Close 关闭 OpenAI 连接并清空指针，可重复调用（幂等）。
// 只处理上游连接；App 连接的生命周期由 session 层负责，避免双方重复关闭。
func (c *Client) Close() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.apiConn != nil {
		_ = c.apiConn.Close()
		c.apiConn = nil
	}
	c.log.Info("OpenAI Client 已关闭")
	return nil
}

// ======================== 协程1: readPump ========================
// 职责：读取 App WS 消息，兼容旧 msgType 协议，并转发到 OpenAI
//
// App→Go 心跳检测原理：
//   1. SetReadDeadline(now + app_pong_timeout) 设置读超时
//      → app_pong_timeout 从配置文件读取（realtime.app_pong_timeout）
//   2. writePump 定时发 Ping → App 回 Pong → PongHandler 重置 ReadDeadline
//   3. 如果 app_pong_timeout 内没收到任何消息（Pong/Text/Binary）
//      → ReadMessage 超时返回 i/o timeout error
//      → readPump 退出 → cancel() → 所有协程退出
//      → 会话结束 → session.Close() → OpenAI 连接关闭
//
// 这是"App 心跳超时=会话结束，Go 主动断开 OpenAI"的核心实现。

func (c *Client) readPump(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup) {
	defer func() {
		cancel() // 通知所有协程退出
		wg.Done()
		c.log.Debug("readPump 退出")
	}()

	// 从配置文件读取 App→Go 的 Pong 超时时间
	pongTimeout := c.cfg.GetAppPongTimeout()

	c.appConn.SetReadLimit(maxMessageSize)
	// 初始读超时 = pongTimeout（配置值）
	_ = c.appConn.SetReadDeadline(time.Now().Add(pongTimeout))

	// PongHandler：收到 App 的 Pong 时重置读超时
	c.appConn.SetPongHandler(func(appData string) error {
		c.log.Debug("收到 App Pong，重置读超时", zap.Duration("timeout", pongTimeout))
		metrics.AppPongReceived(c.sessionID)
		_ = c.appConn.SetReadDeadline(time.Now().Add(pongTimeout))
		return nil
	})

	for {
		msgType, data, err := c.appConn.ReadMessage()
		if err != nil {
			// 区分正常关闭和异常关闭，使用不同日志级别
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				c.log.Info("App 正常关闭连接")
				metrics.AppDisconnectReason(c.sessionID, "normal")
			} else if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				// App 异常断开（如崩溃、网络断开），用 Warn 级别
				c.log.Warn("App 连接异常断开", zap.Error(err))
				metrics.AppDisconnectReason(c.sessionID, "unexpected_close")
			} else {
				// 超时或其他 I/O 错误 → App 心跳超时，会话结束
				c.log.Info("App 心跳超时，会话结束（Go 将主动断开 OpenAI 连接）",
					zap.Duration("pong_timeout", pongTimeout),
					zap.Error(err))
				metrics.AppDisconnectReason(c.sessionID, "heartbeat_timeout")
			}
			return
		}

		// 收到任何数据都重置读超时（App 仍然活跃）
		_ = c.appConn.SetReadDeadline(time.Now().Add(pongTimeout))
		appEventType := classifyAppMessage(msgType, data)
		metrics.AppMessage(c.sessionID, appEventType, len(data), msgType == websocket.BinaryMessage)

		if err := c.handleAppMessage(wsMessage{messageType: msgType, data: data}); err != nil {
			if errors.Is(err, errGatewaySessionClose) {
				c.log.Info("App 请求关闭 OpenAI 会话")
				return
			}
			c.log.Error("处理 App 消息失败",
				zap.Int("msg_type", msgType),
				zap.Int("msg_len", len(data)),
				zap.Error(err))
			if msgType == websocket.TextMessage {
				metrics.AppJSONParseError(c.sessionID, err)
			}
		}

		// 每处理完一条消息检查取消信号，保证 cancel() 后最多再阻塞在 ReadMessage 上一次
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// ======================== 协程4: writePump ========================
// 职责：sendChan → 写入 App WS + 定时发 Ping
//
// 心跳发送：按 app_ping_interval 配置值（从配置文件读取）定时发 Ping
// 是 App WS 的唯一写入者（避免并发写 panic）
//
// 重要：session 层的 heartbeatLoop 已删除，
// 心跳完全由 writePump 统一负责，确保 AppConn 单写入者。

func (c *Client) writePump(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup) {
	// 从配置文件读取 Ping 发送间隔
	pingInterval := c.cfg.GetAppPingInterval()
	ticker := time.NewTicker(pingInterval)
	defer func() {
		ticker.Stop()
		cancel()
		wg.Done()
		c.log.Debug("writePump 退出")
	}()

	for {
		select {
		case msg, ok := <-c.sendChan:
			if !ok {
				// 防御性分支：sendChan 从不被 close；若出现关闭则向 App 发 WS Close 帧后退出
				_ = c.appConn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			_ = c.appConn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.appConn.WriteMessage(websocket.TextMessage, msg); err != nil {
				c.log.Warn("写入 App 失败", zap.Error(err))
				return
			}
			metrics.AppWrite(c.sessionID, len(msg))

		case <-ticker.C:
			// 定时发 Ping（按配置间隔）
			_ = c.appConn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.appConn.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.log.Warn("发送 Ping 给 App 失败（App 可能已断开）", zap.Error(err))
				return
			}
			metrics.AppPingSent(c.sessionID)
			c.log.Debug("已发送 Ping 给 App", zap.Duration("interval", pingInterval))

		case <-ctx.Done():
			return
		}
	}
}

// ======================== 协程2: openAIWritePump ========================
// 职责：apiSendChan → OpenAI WS，是 OpenAI 上游连接的唯一写入者。
//
// 为什么单独拆成一个协程：
//  1. Gorilla WebSocket 要求同一个连接最多一个并发 writer。
//  2. App 读协程不能被 OpenAI 写阻塞，否则 App Pong/断线检测会被拖住。
//  3. response.create / response.cancel 必须经过同一个状态机串行处理，避免长聊时
//     出现 conversation_already_has_active_response 或 response_cancel_not_active。
//  4. OpenAI 重连和重连后的 session/history 恢复会写上游连接，因此也统一放在这里执行。
func (c *Client) openAIWritePump(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup) {
	defer func() {
		cancel()
		wg.Done()
		c.log.Debug("openAIWritePump 退出")
	}()

	apiPingInterval := c.cfg.GetApiPingInterval()
	apiPingTicker := time.NewTicker(apiPingInterval)
	defer apiPingTicker.Stop()

	for {
		select {
		case outbound := <-c.apiSendChan:
			// 普通事件写失败说明上游连接可能半开：writeOpenAIOutbound 内部会重连后重试一次，
			// 仍失败则通知 App 并退出整个会话
			if err := c.writeOpenAIOutbound(ctx, outbound); err != nil {
				c.log.Error("OpenAI 上游写入失败，会话需要重连或结束",
					zap.String("event_type", outbound.eventType),
					zap.String("reason", outbound.reason),
					zap.Error(err))
				metrics.OpenAIWriteError(c.sessionID, outbound.eventType, outbound.reason, err)
				if sendErr := c.forwardToApp(response.NewResponse(500, response.EventReconnectRequired, "OpenAI write failed")); sendErr != nil {
					c.log.Warn("OpenAI 写失败通知无法送达 App", zap.Error(sendErr))
				}
				return
			}

		case req := <-c.apiReconnectChan:
			// 重连请求来自 recvPump（唯一 reader）：重连与恢复写入都在本协程执行，
			// 保证上游连接始终只有这一个 writer；结果经 req.done 回传，失败则退出会话
			c.log.Info("收到 OpenAI 重连请求", zap.String("reason", req.reason))
			err := c.reconnect(ctx)
			metrics.ReconnectResult(c.sessionID, req.reason, err)
			select {
			case req.done <- err:
			default:
			}
			if err != nil {
				c.log.Error("OpenAI 重连失败", zap.String("reason", req.reason), zap.Error(err))
				return
			}

		case <-apiPingTicker.C:
			// 周期 Ping 保活上游；失败说明连接半开，同样通知 App 并结束会话
			if err := c.writeOpenAIPing(); err != nil {
				metrics.OpenAIPingFailed(c.sessionID, err)
				c.log.Warn("发送 Ping 给 OpenAI 失败", zap.Duration("interval", apiPingInterval), zap.Error(err))
				if sendErr := c.forwardToApp(response.NewResponse(500, response.EventReconnectRequired, "OpenAI ping failed")); sendErr != nil {
					c.log.Warn("OpenAI ping 失败通知无法送达 App", zap.Error(sendErr))
				}
				return
			}
			metrics.OpenAIPingSent(c.sessionID)
			c.log.Debug("已发送 Ping 给 OpenAI", zap.Duration("interval", apiPingInterval))

		case <-ctx.Done():
			return
		}
	}
}

// writeOpenAIOutbound 执行一次上游写入。
// 普通事件先经过 responseGate；flush pending 的 response.create 已经在取出 pending 时
// 完成状态切换，因此通过 bypassGate 直接写入，避免被状态机再次拦截。
func (c *Client) writeOpenAIOutbound(ctx context.Context, outbound openAIOutbound) error {
	writeOnce := func() error {
		if outbound.bypassGate {
			return c.writeToOpenAI(outbound.data)
		}
		return c.respGate.sendClientEvent(outbound.eventType, outbound.data, outbound.reason, c.writeToOpenAI)
	}

	// 首次写入失败先不放弃：覆盖"连接半开但尚未被读侧发现"的场景，重连后重试一次。
	// 重连本身已恢复会话状态，因此重试不需要额外恢复动作
	if err := writeOnce(); err == nil {
		return nil
	} else {
		c.log.Warn("写 OpenAI 失败，准备重连后重试一次",
			zap.String("event_type", outbound.eventType),
			zap.String("reason", outbound.reason),
			zap.Error(err))
	}

	metrics.ReconnectRequested(c.sessionID, "api_write_error")
	if err := c.reconnect(ctx); err != nil {
		metrics.ReconnectResult(c.sessionID, "api_write_error", err)
		return err
	}
	metrics.ReconnectResult(c.sessionID, "api_write_error", nil)
	return writeOnce()
}

// requestOpenAIReconnect 让 recvPump 把重连交给 openAIWritePump 执行。
// recvPump 是 OpenAI 唯一 reader；openAIWritePump 是 OpenAI 唯一 writer。
// 读侧只负责发现异常并等待结果，不直接写 session.restore，避免并发写上游连接。
func (c *Client) requestOpenAIReconnect(ctx context.Context, reason string) error {
	metrics.ReconnectRequested(c.sessionID, reason)
	req := reconnectRequest{
		reason: reason,
		done:   make(chan error, 1),
	}

	// 发送请求与等待结果都带 ctx 取消：recvPump 是唯一 reader，
	// 会话结束时不能被阻塞在 channel 操作上，否则 wg.Wait 永远等不到它退出
	select {
	case c.apiReconnectChan <- req:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ======================== 协程3: recvPump ========================
// 职责：读取 OpenAI → 包装为 StandardResponse → sendChan → App
//
// Go↔OpenAI 心跳检测（配置值来自 realtime.api_read_timeout）：
//   1. 对 apiConn 设 SetReadDeadline = api_read_timeout（从配置文件读取）
//   2. OpenAI Realtime API 会持续推送事件（心跳/响应），正常不会超时
//   3. 超时说明连接异常 → 触发 reconnect()
//   4. 重连成功 → 重置计数，continue 继续读取
//   5. 重连失败 → 发 reconnect_required 给 App 并退出
//
// 重连策略：
//   - 最大重连次数：reconnect_max_retries（配置文件，默认 3）
//   - 重连间隔：reconnect_delay（配置文件，默认 1s）
//   - 每次成功连接后重置计数器

func (c *Client) recvPump(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup) {
	defer func() {
		cancel()
		wg.Done()
		c.log.Debug("recvPump 退出")
	}()

	// 从配置文件读取 OpenAI 读超时；若单独配置了更短的 API Pong 超时则取较小值，
	// 让半开连接更早被读侧发现（OpenAI 正常会持续推送事件和 Pong，不会误杀健康连接）
	apiReadTimeout := c.cfg.GetApiReadTimeout()
	if apiPongTimeout := c.cfg.GetApiPongTimeout(); apiPongTimeout > 0 && apiPongTimeout < apiReadTimeout {
		apiReadTimeout = apiPongTimeout
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// 获取当前连接引用
			c.connMu.Lock()
			conn := c.apiConn
			c.connMu.Unlock()

			if conn == nil {
				c.log.Warn("OpenAI 连接为 nil，尝试重连")
				if err := c.requestOpenAIReconnect(ctx, "api_connection_nil"); err != nil {
					c.log.Error("OpenAI 重连失败，通知 App",
						zap.Int("total_attempts", c.retryCnt),
						zap.Error(err))
					c.forwardToApp(response.NewResponse(500, response.EventReconnectRequired, "OpenAI connection lost"))
					return
				}
				continue
			}

			// 设置 OpenAI 读超时（检测 Go↔OpenAI 连接存活）
			_ = conn.SetReadDeadline(time.Now().Add(apiReadTimeout))

			_, msg, err := conn.ReadMessage()
			if err != nil {
				c.connMu.Lock()
				staleConn := c.apiConn != conn
				c.connMu.Unlock()
				if staleConn {
					c.log.Debug("忽略已被替换的 OpenAI 旧连接读错误", zap.Error(err))
					continue
				}

				// 区分正常关闭和超时
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					c.log.Info("OpenAI 正常关闭连接")
				} else {
					c.log.Warn("读取 OpenAI 失败，尝试重连",
						zap.Duration("read_timeout", apiReadTimeout),
						zap.Error(err))
				}

				if err := c.requestOpenAIReconnect(ctx, "api_read_error"); err != nil {
					c.log.Error("OpenAI 重连失败，通知 App",
						zap.Int("total_attempts", c.retryCnt),
						zap.Error(err))
					c.forwardToApp(response.NewResponse(500, response.EventReconnectRequired, "OpenAI reconnect failed"))
					return
				}
				continue
			}

			c.handleOpenAIMessage(msg)
		}
	}
}

// ======================== App 消息处理 ========================

// handleAppMessage 按 WS 帧类型分发 App 消息，未知类型直接忽略（防呆，不中断会话）。
func (c *Client) handleAppMessage(msg wsMessage) error {
	switch msg.messageType {
	case websocket.TextMessage:
		return c.handleAppTextMessage(msg.data)
	case websocket.BinaryMessage:
		return c.handleAppBinaryMessage(msg.data)
	default:
		c.log.Debug("忽略未知 WS 消息类型", zap.Int("type", msg.messageType))
		return nil
	}
}

// handleAppTextMessage 按 gateway 计划处理 App 文本消息：先向 App 回确认消息（pong/ack），
// 再按计划打断上游活跃响应（新一轮输入），最后把 OpenAI 事件投递到写队列。
// 计划要求结束会话时返回 errGatewaySessionClose，由 readPump 据此正常退出。
func (c *Client) handleAppTextMessage(data []byte) error {
	plan, err := c.gateway.buildClientPlan(data, c.cfg, c.sessionID)
	if err != nil {
		return err
	}
	for _, msg := range plan.appMessages {
		c.safeSend(msg)
	}
	if plan.closeSession {
		return errGatewaySessionClose
	}
	if plan.interruptActive {
		if err := c.interruptActiveResponse("app_interrupt:" + plan.reason); err != nil {
			return err
		}
	}
	for _, event := range plan.openAIEvents {
		if err := c.forwardClientEvent(event, plan.reason); err != nil {
			return err
		}
	}
	return nil
}

// interruptActiveResponse 构造 response.cancel 并投递到 OpenAI 写队列，
// 用于在用户新一轮输入时打断当前响应；reason 只进日志和指标，不参与协议。
func (c *Client) interruptActiveResponse(reason string) error {
	payload, err := marshalJSON(map[string]any{"type": "response.cancel"})
	if err != nil {
		return err
	}
	return c.enqueueOpenAIOutbound(openAIOutbound{
		eventType: string(protocol.ClientEventTypeResponseCancel),
		data:      payload,
		reason:    reason,
	})
}

// handleAppBinaryMessage 将二进制音频包装为 input_audio_buffer.append
func (c *Client) handleAppBinaryMessage(data []byte) error {
	event := &protocol.InputAudioBufferAppendEvent{
		ClientEventBase: protocol.ClientEventBase{
			Type: protocol.ClientEventTypeInputAudioBufferAppend,
		},
		Audio: base64.StdEncoding.EncodeToString(data),
	}
	eventData, err := protocol.MarshalClientEvent(event)
	if err != nil {
		return fmt.Errorf("marshal audio event: %w", err)
	}
	return c.forwardClientEvent(eventData, "binary_audio")
}

// forwardClientEvent 把 App 发起的 OpenAI 客户端事件投递到上游写队列。
// 先解析事件类型（协议解析失败时回退到裸 JSON 的 type 字段）供 responseGate 拦截与指标统计，
// 事件类型缺失时返回错误（fail-closed，不向未知事件透传）；
// 投递前先写入重连恢复缓存，保证断线重连后可重放该事件。
func (c *Client) forwardClientEvent(data []byte, reason string) error {
	eventType := ""
	if evt, err := protocol.UnmarshalClientEvent(data); err == nil {
		eventType = string(evt.ClientEventType())
	} else {
		var raw struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(data, &raw) == nil {
			eventType = raw.Type
		}
	}
	if eventType == "" {
		return fmt.Errorf("missing OpenAI client event type")
	}

	metrics.OpenAIClientEvent(c.sessionID, eventType, reason, len(data))
	if eventType == string(protocol.ClientEventTypeInputAudioBufferAppend) {
		metrics.InputAudio(c.sessionID, extractClientAudioPayloadLen(data))
	}
	c.log.Debug("App->OpenAI event", zap.String("type", eventType), zap.String("reason", reason))
	c.replay.remember(eventType, data)
	return c.enqueueOpenAIOutbound(openAIOutbound{
		eventType: eventType,
		data:      data,
		reason:    reason,
	})
}

// enqueueOpenAIOutbound 把上游写任务投递给 openAIWritePump。
// 这里使用有界队列和短超时：App/耳机如果持续快于 OpenAI 上游写入，服务端不能无限堆内存。
func (c *Client) enqueueOpenAIOutbound(outbound openAIOutbound) error {
	outbound.data = append([]byte(nil), outbound.data...)
	metrics.QueueDepth(c.sessionID, len(c.sendChan), cap(c.sendChan), len(c.apiSendChan), cap(c.apiSendChan))
	select {
	case c.apiSendChan <- outbound:
		metrics.QueueDepth(c.sessionID, len(c.sendChan), cap(c.sendChan), len(c.apiSendChan), cap(c.apiSendChan))
		return nil
	case <-time.After(c.cfg.GetSendQueueTimeout()):
		metrics.APISendQueueTimeout(c.sessionID, outbound.eventType, outbound.reason)
		return fmt.Errorf("openai outbound queue full: event_type=%s reason=%s", outbound.eventType, outbound.reason)
	}
}

// writeToOpenAI 只允许 openAIWritePump 调用。
// connMu 的作用不是让多个 writer 并发写，而是保护 reconnect 时 apiConn 指针的关闭/替换。
func (c *Client) writeToOpenAI(data []byte) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	return c.writeToOpenAILocked(data)
}

// writeToOpenAILocked 在持有 connMu 的前提下执行单次上游写：连接为 nil 直接失败（fail-closed），
// 每次写设置写超时，防止上游不消费时写协程无限阻塞。
func (c *Client) writeToOpenAILocked(data []byte) error {
	if c.apiConn == nil {
		return fmt.Errorf("openai connection is nil")
	}
	_ = c.apiConn.SetWriteDeadline(time.Now().Add(c.cfg.GetApiWriteTimeout()))
	return c.apiConn.WriteMessage(websocket.TextMessage, data)
}

// ======================== OpenAI 消息处理 ========================

// handleOpenAIMessage 解析 OpenAI 事件 → 包装为 StandardResponse → sendChan
func (c *Client) handleOpenAIMessage(data []byte) {
	c.handleOpenAIMessageGateway(data)
}

// handleOpenAIMessageGateway 对齐旧 PHP OpenAIResponseHandler 的核心行为：
// 它把 OpenAI 服务端事件转换为旧 App 响应信封，并推进本地响应状态机。
func (c *Client) handleOpenAIMessageGateway(data []byte) {
	evt, err := protocol.UnmarshalServerEvent(data)
	if err != nil {
		c.log.Debug("unknown OpenAI event, pass through", zap.Int("data_len", len(data)))
		metrics.OpenAIServerEvent(c.sessionID, "unknown", "", len(data))
		c.safeSend(data)
		return
	}

	eventType := evt.ServerEventType()
	responseID := c.extractResponseID(evt)
	metrics.OpenAIServerEvent(c.sessionID, string(eventType), responseID, len(data))
	// 先推进本地响应状态机：返回值表示该事件是否释放了挂起的 response.create，
	// 需要在事件处理完后冲刷补发，避免 create/cancel 的串行化留下积压
	flushPending := c.respGate.onServerEvent(evt)

	var stdResp *response.StandardResponse
	switch v := evt.(type) {
	case *protocol.ResponseCreatedEvent:
		// 会话开始：下推 begin 事件；若此前 cancel 早于 created 到达（状态机记录的延迟取消意图），
		// 此时补发 response.cancel，解决上游返回 response_cancel_not_active 的问题
		metrics.OpenAIResponseCreated(c.sessionID, v.Response.ID)
		stdResp = response.NewResponseWithID(0, response.EventBegin, v.Response.ID, "", time.Now().UnixMilli())
		if reason, ok := c.respGate.takeCancelAfterCreated(v.Response.ID); ok {
			c.enqueueCancelAfterCreated(v.Response.ID, reason)
		}
	case *protocol.ResponseDoneEvent:
		// 响应结束：先按 token 明细记账与统计（usage 为空或记账失败只记指标、不阻断转发），
		// 再按状态决定下推的收尾事件
		usage := metricsUsageFromProtocol(v.Response.Usage)
		if v.Response.Usage != nil && c.sessionID != "" {
			if err := billing.RecordTokenUsageDetail(c.Name(), c.sessionID, c.userID, billingDetailFromUsage(v.Response.ID, v.Response.Usage)); err != nil {
				metrics.BillingError(c.sessionID, err)
				c.log.Warn("record OpenAI token usage failed", zap.Error(err))
			}
		}
		metrics.OpenAIResponseDoneUsage(c.sessionID, v.Response.ID, string(v.Response.Status), usage)
		c.recordRealtimeUsageStats(string(v.Response.Status), usage)
		switch v.Response.Status {
		case protocol.ResponseStatusCancelled:
			// 用户打断导致的取消：以旧协议 stop_success 收尾，App 据此停止播放
			stdResp = response.NewResponseWithID(0, gatewayResponseStopSuccess, v.Response.ID, "", time.Now().UnixMilli())
		case protocol.ResponseStatusCompleted:
			// 正常完成：取输出中首个非空文本/转写作为最终内容一并下推
			stdResp = response.NewResponseWithID(0, response.EventEnd, v.Response.ID, extractDoneContent(v.Response), time.Now().UnixMilli())
		default:
			// failed/incomplete 等状态视为失败：下推 500 结束事件，并清空输入音频缓冲，
			// 避免残留半截音频干扰用户下一轮输入
			stdResp = response.NewResponseWithID(500, response.EventEnd, v.Response.ID, "", time.Now().UnixMilli())
			clearEvent, _ := marshalJSON(map[string]any{"type": "input_audio_buffer.clear"})
			if clearEvent != nil {
				_ = c.forwardClientEvent(clearEvent, "response_done_not_completed")
			}
		}
	case *protocol.ResponseTextDeltaEvent:
		// 流式文本增量：直接转发给 App 实时渲染
		metrics.OpenAITextDelta(c.sessionID, v.ResponseID, v.Delta)
		stdResp = response.NewResponseWithID(0, response.EventTextDelta, v.ResponseID, v.Delta, time.Now().UnixMilli())
	case *protocol.ResponseAudioDeltaEvent:
		// 流式音频增量：直接转发给 App 实时播放
		metrics.OpenAIAudioDelta(c.sessionID, v.ResponseID, len(v.Delta))
		stdResp = response.NewResponseWithID(0, response.EventAudioDelta, v.ResponseID, v.Delta, time.Now().UnixMilli())
	case *protocol.ResponseAudioTranscriptDeltaEvent:
		// 语音转写增量：仅在非 speaker（麦克风直通）场景下推文本，
		// 避免 App 本地已播放语音时出现重复转写文本
		metrics.OpenAITranscriptDelta(c.sessionID, v.ResponseID, v.Delta)
		if c.gateway.lastMessageType() != gatewayMsgSpeaker {
			stdResp = response.NewResponseWithID(0, response.EventTextDelta, v.ResponseID, v.Delta, time.Now().UnixMilli())
		}
	case *protocol.ConversationItemInputAudioTranscriptionCompletedEvent:
		// 输入音频转写完成：下推转写结果（旧 audioTransCompleted 兼容协议），
		// 并自动续发 response.create 生成语音回复，保证用户说完即可得到答复
		metrics.OpenAITranscriptDelta(c.sessionID, "", v.Transcript)
		stdResp = response.NewResponseWithID(0, gatewayResponseAudioTranslateCompleted, "", v.Transcript, time.Now().UnixMilli())
		create, err := responseCreateAudio()
		if err != nil {
			c.log.Error("build response.create after transcription failed", zap.Error(err))
		} else if err := c.forwardClientEvent(create, "audio_transcription_completed"); err != nil {
			c.log.Warn("send response.create after transcription failed", zap.Error(err))
		}
	case *protocol.ResponseFunctionCallArgumentsDoneEvent:
		// 工具参数齐全：交给函数调用处理逻辑分派执行（可能取消/续发响应）
		stdResp = c.handleFunctionCallArgumentsDone(v)
	case *protocol.ErrorEvent:
		// 上游错误事件：带 500 下推错误信封（消息体经脱敏后落日志）；
		// 若错误表明存在未知活跃响应（conversation_already_has_active_response），
		// 登记延迟取消意图，等 created 到达后自动取消，恢复状态机
		metrics.OpenAIError(c.sessionID, v.Error.Code, v.Error.Message)
		stdResp = response.NewResponseWithID(500, response.EventError, "", v.Error, time.Now().UnixMilli())
		c.log.Warn("OpenAI error event",
			zap.String("code", v.Error.Code),
			zap.String("message", logger.RedactField("content", v.Error.Message)))
		if c.respGate.takeCancelUnknownActive() {
			c.enqueueCancelAfterCreated("", "conversation_already_has_active_response")
		}
	default:
		switch eventType {
		case protocol.ServerEventTypeSessionCreated:
			// 会话建立/更新：原样包装事件对象下推，让 App 拿到完整 session 状态
			stdResp = response.NewResponseWithID(0, response.EventSessionCreated, c.extractResponseID(evt), evt, time.Now().UnixMilli())
		case protocol.ServerEventTypeSessionUpdated:
			stdResp = response.NewResponseWithID(0, response.EventSessionUpdated, c.extractResponseID(evt), evt, time.Now().UnixMilli())
		default:
			// 其余未知事件不做协议假设：原始帧透传给 App；若 gate 因该事件释放了挂起
			// 的 response.create（如 cancelled 类事件），在透传后立即冲刷补发
			c.safeSend(data)
			if flushPending {
				c.flushPendingResponseCreate()
			}
			return
		}
	}

	if stdResp != nil {
		jsonData, err := stdResp.ToJSON()
		if err != nil {
			c.log.Error("serialize response failed", zap.Error(err))
			return
		}
		c.safeSend(jsonData)
	}
	// 标准事件下推完成后，冲刷 gate 释放的挂起 response.create，保持上游串行语义
	if flushPending {
		c.flushPendingResponseCreate()
	}
}

// enqueueCancelAfterCreated 在 response.created 到达后补发此前记录的延迟取消。
// 直接发 response.cancel 若早于 created 到达会得到 response_cancel_not_active，
// 因此先把取消意图存在状态机里，等 created 到了再投递。
func (c *Client) enqueueCancelAfterCreated(responseID, reason string) {
	payload := map[string]any{"type": "response.cancel"}
	if responseID != "" {
		payload["response_id"] = responseID
	}
	cancelEvent, err := marshalJSON(payload)
	if err != nil {
		c.log.Warn("构造延迟取消事件失败", zap.String("reason", reason), zap.Error(err))
		return
	}
	if err := c.enqueueOpenAIOutbound(openAIOutbound{
		eventType: string(protocol.ClientEventTypeResponseCancel),
		data:      cancelEvent,
		reason:    "delayed_cancel:" + reason,
	}); err != nil {
		c.log.Warn("投递延迟取消事件失败", zap.String("reason", reason), zap.Error(err))
	}
}

// handleFunctionCallArgumentsDone 处理工具参数就绪事件：解析 arguments JSON 后按函数名分派，
// 已注册函数交给对应工具执行器；未识别函数回退为 command_app 事件，把 name/call_id/arguments
// 原样下推给 App 自行处理。返回的 StandardResponse 最终由调用方序列化下发。
func (c *Client) handleFunctionCallArgumentsDone(evt *protocol.ResponseFunctionCallArgumentsDoneEvent) *response.StandardResponse {
	args := map[string]any{}
	if evt.Arguments != "" {
		_ = json.Unmarshal([]byte(evt.Arguments), &args)
	}
	switch evt.Name {
	case "map_command_to_code":
		// 耳机控制命令归一化：退出聊天/结束聊天两种代码统一为 code_quit，同时取消当前响应
		commandCode, _ := args["command_code"].(string)
		if commandCode == "code_exit_chat" || commandCode == "code_end_chat" {
			commandCode = "code_quit"
		}
		cancelEvent, _ := marshalJSON(map[string]any{
			"type":        "response.cancel",
			"response_id": evt.ResponseID,
		})
		if cancelEvent != nil {
			_ = c.forwardClientEvent(cancelEvent, "map_command_to_code")
		}
		return response.NewResponseWithID(0, gatewayResponseCommandApp, evt.ResponseID, map[string]string{
			"command_code": commandCode,
		}, time.Now().UnixMilli())
	case "get_open_weather":
		return c.handleWeatherFunctionCall(evt, args)
	case "search_tozo_knowledge":
		return c.handleKnowledgeFunctionCall(evt, args)
	case "get_specify_route_navigation", "get_nearby_route_navigation":
		return c.handleNavigationFunctionCall(evt, args)
	case "workspace_list_files", "workspace_read_file", "workspace_write_file":
		return c.handleWorkspaceFunctionCall(evt, args)
	}

	return response.NewResponseWithID(0, gatewayResponseCommandApp, evt.ResponseID, map[string]any{
		"name":      evt.Name,
		"call_id":   evt.CallID,
		"arguments": args,
	}, time.Now().UnixMilli())
}

// 四个 handle*FunctionCall 结构同构：在独立超时 context 内执行工具（默认 8s，可配置 tool_timeout），
// 超时按失败结果处理，避免工具卡住阻塞 OpenAI 读协程；结果统一交给 applyFunctionToolResult 落地。
func (c *Client) handleWeatherFunctionCall(evt *protocol.ResponseFunctionCallArgumentsDoneEvent, args map[string]any) *response.StandardResponse {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.GetToolTimeout())
	defer cancel()
	return c.applyFunctionToolResult(evt, c.executeWeatherFunctionTool(ctx, evt, args))
}

func (c *Client) handleKnowledgeFunctionCall(evt *protocol.ResponseFunctionCallArgumentsDoneEvent, args map[string]any) *response.StandardResponse {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.GetToolTimeout())
	defer cancel()
	return c.applyFunctionToolResult(evt, c.executeKnowledgeFunctionTool(ctx, evt, args))
}

func (c *Client) handleNavigationFunctionCall(evt *protocol.ResponseFunctionCallArgumentsDoneEvent, args map[string]any) *response.StandardResponse {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.GetToolTimeout())
	defer cancel()
	return c.applyFunctionToolResult(evt, c.executeNavigationFunctionTool(ctx, evt, args))
}

func (c *Client) handleWorkspaceFunctionCall(evt *protocol.ResponseFunctionCallArgumentsDoneEvent, args map[string]any) *response.StandardResponse {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.GetToolTimeout())
	defer cancel()
	return c.applyFunctionToolResult(evt, c.executeWorkspaceFunctionTool(ctx, evt, args))
}

// applyFunctionToolResult 统一落地工具执行结果：需要打断时先取消当前活跃响应；
// 需要继续对话时回填 function_call_output 并续发 response.create；最终把工具结论下推 App。
func (c *Client) applyFunctionToolResult(evt *protocol.ResponseFunctionCallArgumentsDoneEvent, result realtimeToolResult) *response.StandardResponse {
	if result.cancelActive {
		_ = c.cancelActiveFunctionResponse(evt.ResponseID, result.cancelReason)
	}
	if result.continueResponse && result.output != nil {
		c.sendFunctionOutputAndCreate(evt, result.output, result.reason, result.textResponse)
	}
	return result.appResponse
}

// sendFunctionOutputAndCreate 把工具输出作为 function_call_output 写入会话，再按输出模式
// （音频/文本）续发 response.create，让模型基于工具结果继续回答用户。
func (c *Client) sendFunctionOutputAndCreate(evt *protocol.ResponseFunctionCallArgumentsDoneEvent, output map[string]any, reason string, textResponse bool) {
	outputJSON, err := json.Marshal(output)
	if err != nil {
		c.log.Warn("序列化函数调用输出失败", zap.String("function", evt.Name), zap.Error(err))
		return
	}
	item, err := marshalJSON(map[string]any{
		"type": "conversation.item.create",
		"item": map[string]any{
			"type":    "function_call_output",
			"call_id": evt.CallID,
			"output":  string(outputJSON),
		},
	})
	if err != nil {
		c.log.Warn("构造函数调用输出事件失败", zap.String("function", evt.Name), zap.Error(err))
		return
	}
	if err := c.forwardClientEvent(item, reason+"_function_output"); err != nil {
		c.log.Warn("发送函数调用输出失败", zap.String("function", evt.Name), zap.Error(err))
		return
	}
	var create []byte
	if textResponse {
		create, err = responseCreateTextWithInstructions("Use the workspace tool output to answer in the user's language. If a file was changed, mention the changed path. Keep the answer concise.")
	} else {
		create, err = responseCreateAudioWithInstructions("Use the function output to answer the user briefly. If the output reports a missing backend provider, explain that the service is temporarily unavailable.")
	}
	if err != nil {
		c.log.Warn("构造函数调用后续 response.create 失败", zap.String("function", evt.Name), zap.Error(err))
		return
	}
	if err := c.forwardClientEvent(create, reason+"_response_create"); err != nil {
		c.log.Warn("发送函数调用后续 response.create 失败", zap.String("function", evt.Name), zap.Error(err))
	}
}

// cancelActiveFunctionResponse 构造 response.cancel 取消工具执行期间仍活跃的上游响应，
// 优先携带上游返回的 responseID 精确定位目标响应。
func (c *Client) cancelActiveFunctionResponse(responseID, reason string) error {
	payload := map[string]any{"type": "response.cancel"}
	if responseID != "" {
		payload["response_id"] = responseID
	}
	cancelEvent, err := marshalJSON(payload)
	if err != nil {
		return err
	}
	return c.forwardClientEvent(cancelEvent, reason)
}

// flushPendingResponseCreate 在 gate 空闲后取出挂起的 response.create 并投递到写队列。
// 投递失败时把 gate 复位为 idle，避免挂起事件残留导致后续 response.create 被无限拦截。
func (c *Client) flushPendingResponseCreate() {
	payload, ok := c.respGate.takePendingCreate("server_event_released")
	if !ok {
		return
	}
	if err := c.enqueueOpenAIOutbound(openAIOutbound{
		eventType:  string(protocol.ClientEventTypeResponseCreate),
		data:       payload,
		reason:     "flush_pending_response_create",
		bypassGate: true,
	}); err != nil {
		c.respGate.setIdle("", "flush_pending_enqueue_failed")
		c.log.Warn("enqueue pending response.create failed", zap.Error(err))
	}
}

// extractDoneContent 从 response.output 各 content 中取第一个非空文本或转写作为结束内容；
// 无内容时返回空串（completed 响应也允许不带文字）。
func extractDoneContent(resp protocol.Response) string {
	for _, item := range resp.Output {
		for _, part := range item.Content {
			if part.Text != nil && *part.Text != "" {
				return *part.Text
			}
			if part.Transcript != nil && *part.Transcript != "" {
				return *part.Transcript
			}
		}
	}
	return ""
}

// billingDetailFromUsage 把协议 Usage 转换为计费明细：TotalTokens 缺失时用 input+output 求和兜底，
// 输入/输出两侧的 cached 与 reasoning 明细合并计数；两侧明细都缺失时标记为只按总数记账。
func billingDetailFromUsage(responseID string, usage *protocol.Usage) billing.TokenUsageDetail {
	if usage == nil {
		return billing.TokenUsageDetail{ResponseID: responseID}
	}
	detail := billing.TokenUsageDetail{
		ResponseID:   responseID,
		InputTokens:  usage.InputTokens,
		OutputTokens: usage.OutputTokens,
		TotalTokens:  usage.TotalTokens,
	}
	if detail.TotalTokens <= 0 {
		detail.TotalTokens = detail.InputTokens + detail.OutputTokens
	}
	if usage.InputTokenDetails != nil {
		detail.InputTextTokens = usage.InputTokenDetails.TextTokens
		detail.InputAudioTokens = usage.InputTokenDetails.AudioTokens
		detail.CachedTokens += usage.InputTokenDetails.CachedTokens
		detail.ReasoningTokens += usage.InputTokenDetails.ReasoningTokens
	}
	if usage.OutputTokenDetails != nil {
		detail.OutputTextTokens = usage.OutputTokenDetails.TextTokens
		detail.OutputAudioTokens = usage.OutputTokenDetails.AudioTokens
		detail.CachedTokens += usage.OutputTokenDetails.CachedTokens
		detail.ReasoningTokens += usage.OutputTokenDetails.ReasoningTokens
	}
	if usage.InputTokenDetails == nil && usage.OutputTokenDetails == nil {
		detail.DetailSource = "usage_total_only"
	} else {
		detail.DetailSource = "usage_detail"
	}
	return detail
}

// metricsUsageFromProtocol 复用计费转换逻辑提取指标所需 token 数，保证计费与指标口径一致。
func metricsUsageFromProtocol(usage *protocol.Usage) metrics.ResponseTokenUsage {
	if usage == nil {
		return metrics.ResponseTokenUsage{}
	}
	detail := billingDetailFromUsage("", usage)
	return metrics.ResponseTokenUsage{
		InputTokens:     detail.InputTokens,
		OutputTokens:    detail.OutputTokens,
		TotalTokens:     detail.TotalTokens,
		CachedTokens:    detail.CachedTokens,
		ReasoningTokens: detail.ReasoningTokens,
	}
}

// recordRealtimeUsageStats 把单次 response 的 token 用量写入统一统计，
// 携带模型/用户/会话维度，供运营侧按 SourceRealtime 聚合。
func (c *Client) recordRealtimeUsageStats(status string, usage metrics.ResponseTokenUsage) {
	model := ""
	if c.cfg != nil {
		model = c.cfg.GetDefaultModel()
	}
	stats.RecordUsage(stats.UsageRecord{
		Source:          stats.SourceRealtime,
		Provider:        c.Name(),
		Model:           model,
		UserID:          c.userID,
		Status:          status,
		InputTokens:     int64(usage.InputTokens),
		OutputTokens:    int64(usage.OutputTokens),
		CachedTokens:    int64(usage.CachedTokens),
		ReasoningTokens: int64(usage.ReasoningTokens),
		TotalTokens:     int64(usage.TotalTokens),
	})
}

// extractResponseID 从 OpenAI 流式服务端事件中提取 response_id。
func (c *Client) extractResponseID(evt protocol.ServerEvent) string {
	switch v := evt.(type) {
	case *protocol.ResponseTextDeltaEvent:
		return v.ResponseID
	case *protocol.ResponseAudioDeltaEvent:
		return v.ResponseID
	case *protocol.ResponseDoneEvent:
		return v.Response.ID
	case *protocol.ResponseCreatedEvent:
		return v.Response.ID
	case *protocol.ResponseAudioTranscriptDeltaEvent:
		return v.ResponseID
	default:
		return ""
	}
}

// ======================== 工具方法 ========================

// appEventPolicy 描述下行 App 事件的关键性：critical 事件在 sendChan 满时返回错误，
// 普通流式 delta 允许丢弃，防止慢客户端阻塞 OpenAI 读协程。
type appEventPolicy struct {
	eventType string
	critical  bool
}

// sendAppEvent 按事件重要性写入 App 下行队列。
// 普通流式 delta 可按 best-effort 丢弃，避免慢客户端阻塞 OpenAI 读协程；
// 错误、重连、结束和工具结果属于关键事件，队列超时时必须返回错误并写入指标。
func (c *Client) sendAppEvent(data []byte, policy appEventPolicy) error {
	metrics.QueueDepth(c.sessionID, len(c.sendChan), cap(c.sendChan), len(c.apiSendChan), cap(c.apiSendChan))
	select {
	case c.sendChan <- data:
		metrics.QueueDepth(c.sessionID, len(c.sendChan), cap(c.sendChan), len(c.apiSendChan), cap(c.apiSendChan))
		return nil
	case <-time.After(c.cfg.GetSendQueueTimeout()):
		if policy.critical {
			c.log.Warn("关键 App 下行事件队列超时",
				zap.String("event_type", policy.eventType),
				zap.Int("data_len", len(data)),
				zap.Int("chan_cap", cap(c.sendChan)),
				zap.Duration("waited", c.cfg.GetSendQueueTimeout()))
			metrics.CriticalAppEventQueueTimeout(c.sessionID, policy.eventType, len(data))
			return fmt.Errorf("critical app event queue timeout: event_type=%s bytes=%d", policy.eventType, len(data))
		}
		c.log.Warn("sendChan 已满，丢弃消息",
			zap.String("event_type", policy.eventType),
			zap.Int("data_len", len(data)),
			zap.Int("chan_cap", cap(c.sendChan)),
			zap.Duration("waited", c.cfg.GetSendQueueTimeout()))
		metrics.SlowConsumerDrop(c.sessionID, len(data))
		return nil
	}
}

// safeSend 保留给无法解析事件类型的透传消息；它们默认按 best-effort 处理。
func (c *Client) safeSend(data []byte) bool {
	return c.sendAppEvent(data, appEventPolicy{eventType: "raw", critical: false}) == nil
}

// forwardToApp 将 StandardResponse 发送给 App。
func (c *Client) forwardToApp(resp *response.StandardResponse) error {
	data, err := resp.ToJSON()
	if err != nil {
		c.log.Error("序列化 forwardToApp 失败", zap.Error(err))
		return err
	}
	return c.sendAppEvent(data, appEventPolicyFromStandardResponse(resp))
}

func appEventPolicyFromStandardResponse(resp *response.StandardResponse) appEventPolicy {
	if resp == nil {
		return appEventPolicy{eventType: "unknown", critical: false}
	}
	return appEventPolicy{
		eventType: string(resp.Response),
		critical:  isCriticalAppResponse(resp.Response),
	}
}

// appEventPolicyFromResponse 从已序列化的响应 JSON 中提取事件类型（兼容旧字段 type）
// 并映射为关键性，供透传/未知事件的队列策略复用。
func appEventPolicyFromResponse(raw map[string]json.RawMessage) appEventPolicy {
	eventType := rawString(raw, "response")
	if eventType == "" {
		eventType = rawString(raw, "type")
	}
	if eventType == "" {
		eventType = "unknown"
	}
	return appEventPolicy{
		eventType: eventType,
		critical:  isCriticalAppResponse(response.ResponseEvent(eventType)),
	}
}

// isCriticalAppResponse 判定事件是否必须可靠送达：重连/错误/结束/会话事件或 workspace 工具
// 结果一旦丢失，App 会卡在等待状态或丢失上下文，因此必须按 critical 处理。
func isCriticalAppResponse(event response.ResponseEvent) bool {
	switch event {
	case response.EventReconnectRequired,
		response.EventError,
		response.EventEnd,
		response.EventSessionCreated,
		response.EventSessionUpdated,
		response.EventSessionRestored,
		response.ResponseEvent("workspace_tool"):
		return true
	default:
		return false
	}
}

// writeOpenAIPing 发送上游 WebSocket Ping 保活。
// 测试注入点优先；生产环境用 WriteControl 而非 WriteMessage，
// 控制帧可与数据帧并发写入，无需等待写锁。
func (c *Client) writeOpenAIPing() error {
	if c.writeOpenAIPingFunc != nil {
		return c.writeOpenAIPingFunc()
	}

	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.apiConn == nil {
		return fmt.Errorf("openai connection is nil")
	}
	deadline := time.Now().Add(c.cfg.GetApiWriteTimeout())
	return c.apiConn.WriteControl(websocket.PingMessage, nil, deadline)
}

// classifyAppMessage 给 App 消息做指标分类：二进制帧记为 audio；文本帧尝试解析 type，
// 解析失败记为 invalid_json；仅含旧协议 msgType 时加 legacy: 前缀，便于观测新旧协议占比。
func classifyAppMessage(messageType int, data []byte) string {
	if messageType == websocket.BinaryMessage {
		return "binary_audio"
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return "invalid_json"
	}
	if typ := rawString(raw, "type"); typ != "" {
		return typ
	}
	if msgType := rawString(raw, "msgType"); msgType != "" {
		return "legacy:" + msgType
	}
	return "unknown_json"
}

// extractClientAudioPayloadLen 提取 input_audio_buffer.append 中 base64 音频长度，
// 用于输入音频流量指标；解析失败返回 0。
func extractClientAudioPayloadLen(data []byte) int {
	var raw struct {
		Audio string `json:"audio"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return 0
	}
	return len(raw.Audio)
}

// restoreRealtimeState 在重连成功后重放缓存的 session.update 与 conversation.item.* 历史，
// 让上游会话恢复到断线前的最小可用状态。全程持 connMu 且每次写入前检查 ctx，
// 保证与 openAIWritePump 互斥、会话取消时及时中止。
func (c *Client) restoreRealtimeState(ctx context.Context) error {
	sessionUpdate, history := c.replay.snapshot()
	if sessionUpdate == nil && len(history) == 0 {
		return nil
	}

	c.connMu.Lock()
	defer c.connMu.Unlock()

	if sessionUpdate != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := c.writeToOpenAILocked(sessionUpdate); err != nil {
			return fmt.Errorf("restore session.update: %w", err)
		}
	}

	for i, event := range history {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := c.writeToOpenAILocked(event); err != nil {
			return fmt.Errorf("restore history event %d/%d: %w", i+1, len(history), err)
		}
	}

	c.log.Info("OpenAI 重连后已恢复最小会话状态",
		zap.Bool("session_update_restored", sessionUpdate != nil),
		zap.Int("history_events_restored", len(history)))
	metrics.SessionRestore(c.sessionID, len(history), nil)
	c.forwardToApp(response.NewResponse(0, response.EventSessionRestored, map[string]interface{}{
		"history_events": len(history),
	}))
	return nil
}

// reconnect 重连 OpenAI（带重试限制）
//
// 重连策略（配置值从 config.yaml 读取）：
//   - 最大次数：reconnect_max_retries（默认 3）
//   - 每次间隔：reconnect_delay（默认 1s）
//   - 成功后自动重置 retryCnt
func (c *Client) reconnect(ctx context.Context) error {
	c.reconnMu.Lock()
	defer c.reconnMu.Unlock()

	maxRetries := c.cfg.GetMaxRetries()
	// 重连计数每次请求都累加（无论成败），超过上限即失败关闭并结束会话；
	// Connect 成功时已重置计数，因此这里只统计连续失败
	c.retryCnt++
	if c.retryCnt > maxRetries {
		return fmt.Errorf("exceeded max retries: %d/%d", c.retryCnt, maxRetries)
	}

	delay := c.cfg.GetReconnectDelay()
	c.log.Info("尝试重连 OpenAI",
		zap.Int("attempt", c.retryCnt),
		zap.Int("max", maxRetries),
		zap.Duration("delay", delay))

	if delay > 0 {
		// 等待期间检查 context 是否已取消（避免无效等待）
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return fmt.Errorf("context cancelled during reconnect delay")
		}
	}
	if err := c.Connect(ctx); err != nil {
		return err
	}
	// 连接成功且开启会话恢复时重放最小状态；恢复失败同样视为重连失败（fail-closed）
	if c.cfg.ShouldRestoreSession() {
		if err := c.restoreRealtimeState(ctx); err != nil {
			metrics.SessionRestore(c.sessionID, 0, err)
			return err
		}
	}
	return nil
}
