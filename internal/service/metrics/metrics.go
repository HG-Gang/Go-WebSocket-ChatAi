// internal/service/metrics/metrics.go
// 文件功能：为 /api/debug/status 调试页面收集当前 Go 进程的轻量级内存运行指标。
// 输入是各链路协程上报的会话事件、计数器与业务用量，输出是 Snapshot() 返回的可 JSON 化快照 map。
// 指标只属于当前进程；Redis 持久化状态由其他模块负责，本包不会在 WebSocket 热路径上引入额外网络访问。
// 本包不参与鉴权与业务决策，指标丢失不影响业务链路。
// 安全边界：快照不包含密钥、token 或完整对话内容；响应文本、事件线与会话数量均有内存上限；
// 对外导出前复制 map 与 slice，避免调用方修改全局收集器内部状态。
package metrics

import (
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"TozoAI-Chat-Api/internal/service/stats"
)

const (
	// maxRecentSessions 限制内存中保留的最近会话数量。
	maxRecentSessions = 50

	// maxSessionEvents 限制每个会话事件线和全局错误事件线的保留条数。
	maxSessionEvents = 120

	// maxRecentResponses 限制每个会话保留的响应摘要数量。
	maxRecentResponses = 30

	// maxResponseTextChars 限制每个响应在内存中保留的文本/转写长度。
	maxResponseTextChars = 64 * 1024

	// maxSnapshotResponseChars 限制 /api/debug/status 返回的文本长度，避免轮询响应过大。
	maxSnapshotResponseChars = 8 * 1024
)

// collector 是当前进程级别的指标容器。
// 单个用户会话会有四条协程并发更新指标：读 App、写 App、写 OpenAI、读 OpenAI。
// 因此所有写入都必须通过 mu 串行保护。
type collector struct {
	mu       sync.Mutex
	started  time.Time
	counters hotCounters
	app      appMetrics
	goStats  goMetrics
	openai   openAIMetrics
	errors   errorMetrics
	business businessMetrics
	sessions map[string]*sessionMetrics
}

// hotCounters 保存 WebSocket 热路径上的纯累计指标。
// 这些字段只做自增或最近值覆盖，不依赖 map、slice 或会话时间线，因此使用 atomic 可避免所有连接争用 global.mu。
type hotCounters struct {
	appBytesOut           atomic.Uint64
	appSlowConsumerDrops  atomic.Uint64
	goCapacityRejected    atomic.Uint64
	goAPISendTimeouts     atomic.Uint64
	goSendQueueTimeouts   atomic.Uint64
	goCriticalTimeouts    atomic.Uint64
	lastSendQueueLen      atomic.Int64
	lastSendQueueCap      atomic.Int64
	lastAPISendQueueLen   atomic.Int64
	lastAPISendQueueCap   atomic.Int64
	openAIPingSent        atomic.Uint64
	openAIPingFailures    atomic.Uint64
	openAIClientEvents    atomic.Uint64
	openAIServerEvents    atomic.Uint64
	openAIResponseCreated atomic.Uint64
	openAIResponseDone    atomic.Uint64
	openAIResponseOK      atomic.Uint64
	openAIResponseCancel  atomic.Uint64
	openAIResponseFailed  atomic.Uint64
	openAITextChars       atomic.Uint64
	openAITranscriptChars atomic.Uint64
	openAIAudioBytes      atomic.Uint64
	openAIAudioPackets    atomic.Uint64
	businessInputAudioMs  atomic.Uint64
	businessOutputAudioMs atomic.Uint64
	businessInputTokens   atomic.Uint64
	businessOutputTokens  atomic.Uint64
	businessTotalTokens   atomic.Uint64
	businessCachedTokens  atomic.Uint64
	businessReasoning     atomic.Uint64
	businessRateRejected  atomic.Uint64
	businessBillingErrors atomic.Uint64
}

// appMetrics 保存 App、耳机或浏览器这一侧的链路计数。
type appMetrics struct {
	ConnectionsTotal    uint64            `json:"connections_total"`
	DisconnectsTotal    uint64            `json:"disconnects_total"`
	NormalDisconnects   uint64            `json:"normal_disconnects"`
	AbnormalDisconnects uint64            `json:"abnormal_disconnects"`
	HeartbeatTimeouts   uint64            `json:"heartbeat_timeouts"`
	MessagesTotal       uint64            `json:"messages_total"`
	TextMessages        uint64            `json:"text_messages"`
	BinaryMessages      uint64            `json:"binary_messages"`
	BytesIn             uint64            `json:"bytes_in"`
	BytesOut            uint64            `json:"bytes_out"`
	PingSent            uint64            `json:"ping_sent"`
	PongReceived        uint64            `json:"pong_received"`
	PongLatencyMs       latencySummary    `json:"pong_latency_ms"`
	SlowConsumerDrops   uint64            `json:"slow_consumer_drops"`
	JSONParseErrors     uint64            `json:"json_parse_errors"`
	MessageTypes        map[string]uint64 `json:"message_types"`
	DisconnectReasons   map[string]uint64 `json:"disconnect_reasons"`
}

// goMetrics 保存 Go 网关本地压力指标，这些指标不直接归属于 App 或 OpenAI。
type goMetrics struct {
	CapacityRejected              uint64 `json:"capacity_rejected"`
	APISendQueueTimeouts          uint64 `json:"api_send_queue_timeouts"`
	SendQueueTimeouts             uint64 `json:"send_queue_timeouts"`
	CriticalAppEventQueueTimeouts uint64 `json:"critical_app_event_queue_timeouts"`
	LastSendQueueLen              int    `json:"last_send_queue_len"`
	LastSendQueueCap              int    `json:"last_send_queue_cap"`
	LastAPISendQueueLen           int    `json:"last_api_send_queue_len"`
	LastAPISendQueueCap           int    `json:"last_api_send_queue_cap"`
}

// openAIMetrics 保存 OpenAI Realtime 上游连接、事件、流式输出和响应计数。
type openAIMetrics struct {
	ConnectAttempts       uint64            `json:"connect_attempts"`
	ConnectSuccess        uint64            `json:"connect_success"`
	ConnectFailures       uint64            `json:"connect_failures"`
	PingSent              uint64            `json:"ping_sent"`
	PingFailures          uint64            `json:"ping_failures"`
	ReconnectRequests     uint64            `json:"reconnect_requests"`
	ReconnectSuccess      uint64            `json:"reconnect_success"`
	ReconnectFailures     uint64            `json:"reconnect_failures"`
	SessionRestoreSuccess uint64            `json:"session_restore_success"`
	SessionRestoreFailure uint64            `json:"session_restore_failure"`
	ClientEventsTotal     uint64            `json:"client_events_total"`
	ServerEventsTotal     uint64            `json:"server_events_total"`
	ClientEventTypes      map[string]uint64 `json:"client_event_types"`
	ServerEventTypes      map[string]uint64 `json:"server_event_types"`
	ResponseCreated       uint64            `json:"response_created"`
	ResponseDone          uint64            `json:"response_done"`
	ResponseCompleted     uint64            `json:"response_completed"`
	ResponseCancelled     uint64            `json:"response_cancelled"`
	ResponseFailed        uint64            `json:"response_failed"`
	TextDeltaChars        uint64            `json:"text_delta_chars"`
	TranscriptDeltaChars  uint64            `json:"transcript_delta_chars"`
	AudioDeltaBytes       uint64            `json:"audio_delta_bytes"`
	AudioDeltaPackets     uint64            `json:"audio_delta_packets"`
	FirstDeltaLatencyMs   latencySummary    `json:"first_delta_latency_ms"`
	ResponseLatencyMs     latencySummary    `json:"response_latency_ms"`
	ReconnectReasons      map[string]uint64 `json:"reconnect_reasons"`
}

// errorMetrics 为诊断页面保留一份精简错误摘要。
type errorMetrics struct {
	Total    uint64            `json:"total"`
	ByCode   map[string]uint64 `json:"by_code"`
	ByReason map[string]uint64 `json:"by_reason"`
	Recent   []eventRecord     `json:"recent"`
}

// businessMetrics 保存业务用量数据，用于把技术问题和 token、音频、限流、计费行为关联起来。
type businessMetrics struct {
	InputTokens       uint64            `json:"input_tokens"`
	OutputTokens      uint64            `json:"output_tokens"`
	TotalTokens       uint64            `json:"total_tokens"`
	CachedTokens      uint64            `json:"cached_tokens"`
	ReasoningTokens   uint64            `json:"reasoning_tokens"`
	InputAudioMs      uint64            `json:"input_audio_ms"`
	OutputAudioMs     uint64            `json:"output_audio_ms"`
	SessionDurationMs uint64            `json:"session_duration_ms"`
	RateLimitRejected uint64            `json:"rate_limit_rejected"`
	BillingErrors     uint64            `json:"billing_errors"`
	TokensByUser      map[string]uint64 `json:"tokens_by_user"`
	TokensByModel     map[string]uint64 `json:"tokens_by_model"`
	TokensByDay       map[string]uint64 `json:"tokens_by_day"`
}

// latencySummary 是不保存全量样本的最小值、最大值、平均值累加器，统计值单位均为毫秒。
type latencySummary struct {
	Count uint64  `json:"count"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
	Avg   float64 `json:"avg"`
}

// sessionMetrics 是单个 App WebSocket 会话的保留时间线。
// 结构保持轻量，便于 /api/debug/status 高频轮询。
type sessionMetrics struct {
	SessionID         string
	RequestID         string
	UserID            string
	UserName          string
	DeviceID          string
	Model             string
	RemoteAddr        string
	IPLocation        map[string]string
	UserAgent         string
	StartedAt         time.Time
	EndedAt           time.Time
	Active            bool
	EndReason         string
	AppMessages       uint64
	AppBytesIn        uint64
	AppBytesOut       uint64
	AppPingSent       uint64
	AppPongReceived   uint64
	OpenAIPingSent    uint64
	OpenAIPingFailed  uint64
	LastAppPingAt     time.Time
	LastPongLatencyMs float64
	AppDisconnects    uint64
	OpenAIEventsUp    uint64
	OpenAIEventsDown  uint64
	OpenAIReconnects  uint64
	SlowConsumerDrops uint64
	LastSendQueueLen  int
	LastSendQueueCap  int
	LastAPIQueueLen   int
	LastAPIQueueCap   int
	InputAudioMs      uint64
	OutputAudioMs     uint64
	InputTokens       uint64
	OutputTokens      uint64
	CachedTokens      uint64
	ReasoningTokens   uint64
	EventCounts       map[string]uint64
	Events            []eventRecord
	Responses         map[string]*responseMetrics
	RecentResponses   []string
}

// eventRecord 表示会话事件线或错误事件线中的一条记录。
type eventRecord struct {
	At         string `json:"at"`
	Kind       string `json:"kind"`
	Detail     string `json:"detail,omitempty"`
	ResponseID string `json:"response_id,omitempty"`
	Bytes      int    `json:"bytes,omitempty"`
	Code       string `json:"code,omitempty"`
}

// responseMetrics 保存单个 OpenAI 响应生命周期摘要。
// 文本与转写内容按字节受 maxResponseTextChars 限制，只保留最新尾部片段。
type responseMetrics struct {
	ResponseID      string
	Status          string
	CreatedAt       time.Time
	FirstDeltaAt    time.Time
	DoneAt          time.Time
	Text            string
	Transcript      string
	AudioBytes      uint64
	AudioPackets    uint64
	InputTokens     uint64
	OutputTokens    uint64
	CachedTokens    uint64
	ReasoningTokens uint64
	FirstDeltaMs    float64
	DurationMs      float64
}

// ResponseTokenUsage 是一次 OpenAI response.done 的实时 token 明细。
// 它只服务于进程内监控快照；Redis 持久化仍由 billing.TokenUsageDetail 负责。
type ResponseTokenUsage struct {
	InputTokens     int
	OutputTokens    int
	TotalTokens     int
	CachedTokens    int
	ReasoningTokens int
}

// global 是当前 Go 进程使用的唯一指标收集器。
// map 字段在这里初始化，热路径自增时不需要反复判空。
var global = &collector{
	started:  time.Now(),
	sessions: make(map[string]*sessionMetrics),
	app: appMetrics{
		MessageTypes:      make(map[string]uint64),
		DisconnectReasons: make(map[string]uint64),
	},
	openai: openAIMetrics{
		ClientEventTypes: make(map[string]uint64),
		ServerEventTypes: make(map[string]uint64),
		ReconnectReasons: make(map[string]uint64),
	},
	errors: errorMetrics{
		ByCode:   make(map[string]uint64),
		ByReason: make(map[string]uint64),
	},
	business: businessMetrics{
		TokensByUser:  make(map[string]uint64),
		TokensByModel: make(map[string]uint64),
		TokensByDay:   make(map[string]uint64),
	},
}

// SessionStarted 记录 App WebSocket 生命周期的第一个事件。
// session.Start 在 handler 接受 App 连接后、OpenAI Provider 启动四协程流水线前调用它。
func SessionStarted(sessionID, requestID, userID, userName, deviceID, model, remoteAddr, userAgent string) {
	SessionStartedWithLocation(sessionID, requestID, userID, userName, deviceID, model, remoteAddr, userAgent, nil)
}

// SessionStartedWithLocation 记录会话开始，并允许 handler 传入代理/CDN 解析出的所在地。
// location 只接受展示字段，不参与鉴权；缺失时会回退到本地 IP 类型分类。
func SessionStartedWithLocation(sessionID, requestID, userID, userName, deviceID, model, remoteAddr, userAgent string, location map[string]string) {
	// sessionID 为空说明调用点不关心会话维度，直接放弃登记，避免污染全局连接计数。
	if sessionID == "" {
		return
	}
	global.mu.Lock()
	defer global.mu.Unlock()

	// 新会话的 map 字段在这里统一初始化，热路径自增时无需判空。
	global.app.ConnectionsTotal++
	s := &sessionMetrics{
		SessionID:       sessionID,
		RequestID:       requestID,
		UserID:          userID,
		UserName:        userName,
		DeviceID:        deviceID,
		Model:           model,
		RemoteAddr:      remoteAddr,
		IPLocation:      normalizeIPLocation(location),
		UserAgent:       userAgent,
		StartedAt:       time.Now(),
		Active:          true,
		EventCounts:     make(map[string]uint64),
		Responses:       make(map[string]*responseMetrics),
		RecentResponses: make([]string, 0, maxRecentResponses),
	}
	global.sessions[sessionID] = s
	addSessionEventLocked(s, "session_started", "App WebSocket accepted", "", 0, "")
}

// SessionEnded 将会话标记为已关闭，且同一会话只会生效一次。
// 它会更新断链计数、累加业务在线时长，并裁剪过旧的已结束会话。
func SessionEnded(sessionID, reason string, duration time.Duration) {
	global.mu.Lock()
	defer global.mu.Unlock()

	// 同一会话只收口一次：EndedAt 已写入说明 SessionEnded 已生效，重复调用直接忽略。
	if s := global.sessions[sessionID]; s != nil {
		if !s.EndedAt.IsZero() {
			return
		}
		s.EndedAt = time.Now()
		s.EndReason = reason
		s.Active = false
		s.AppDisconnects++
		addSessionEventLocked(s, "session_ended", reason, "", 0, "")
	}

	// 会话记录不存在时也累计全局断链计数，保证断链信息不因会话过期而丢失。
	global.app.DisconnectsTotal++
	incMap(global.app.DisconnectReasons, reason)
	// 只有白名单原因视为正常关闭，其余（心跳超时、写失败等）归入异常断链。
	if reason == "normal" || reason == "provider_return" || reason == "session_close" {
		global.app.NormalDisconnects++
	} else {
		global.app.AbnormalDisconnects++
	}
	if strings.Contains(reason, "heartbeat_timeout") {
		global.app.HeartbeatTimeouts++
	}
	// 会话在线时长按业务维度累加，供诊断页与 token、限流指标关联分析。
	global.business.SessionDurationMs += uint64(duration.Milliseconds())
	pruneEndedSessionsLocked()
}

// AppDisconnectReason 记录 readPump 在整个会话结束前看到的底层断链原因。
// 这个原因通常比最终 close reason 更细。
func AppDisconnectReason(sessionID, reason string) {
	global.mu.Lock()
	defer global.mu.Unlock()
	incMap(global.app.DisconnectReasons, reason)
	if strings.Contains(reason, "heartbeat_timeout") {
		global.app.HeartbeatTimeouts++
	}
	if s := global.sessions[sessionID]; s != nil {
		addSessionEventLocked(s, "app_disconnect_detected", reason, "", 0, "")
	}
}

// CapacityRejected 在活跃会话达到 capacity.max_active_sessions，网关拒绝新 WebSocket 时自增。
func CapacityRejected() {
	global.counters.goCapacityRejected.Add(1)
	stats.RecordResourceEvent(stats.ResourceEvent{
		Source: stats.SourceSystem,
		Kind:   stats.ResourceKindCapacityRejected,
	})
}

// AppMessage 记录一条 App 上行消息。
// 它在消息转发或转换为 OpenAI 事件前调用；二进制消息会统一归类为 binary_audio。
func AppMessage(sessionID, eventType string, bytes int, binary bool) {
	global.mu.Lock()
	defer global.mu.Unlock()
	// 负数或异常字节数在转 uint64 前钳制为 0，避免溢出成巨大数值。
	global.app.MessagesTotal++
	global.app.BytesIn += uint64(maxInt(bytes, 0))
	// 二进制帧统一归类为 binary_audio，让诊断页无需区分不同二进制协议前缀。
	if binary {
		global.app.BinaryMessages++
		eventType = "binary_audio"
	} else {
		global.app.TextMessages++
	}
	// 空事件类型归一到 unknown，保证计数 map 键稳定可读。
	if eventType == "" {
		eventType = "unknown"
	}
	incMap(global.app.MessageTypes, eventType)
	if s := global.sessions[sessionID]; s != nil {
		s.AppMessages++
		s.AppBytesIn += uint64(maxInt(bytes, 0))
		incMap(s.EventCounts, "app:"+eventType)
		addSessionEventLocked(s, "app_message", eventType, "", bytes, "")
	}
}

// AppJSONParseError 记录 App 或调试页面文本帧中的非法 JSON。
func AppJSONParseError(sessionID string, err error) {
	global.mu.Lock()
	defer global.mu.Unlock()
	global.app.JSONParseErrors++
	recordErrorLocked(sessionID, "app_json_parse_error", errorString(err), "")
}

// AppPingSent 记录协议层 Go 到 App 的 ping，并保存时间戳用于后续估算 pong 延迟。
func AppPingSent(sessionID string) {
	global.mu.Lock()
	defer global.mu.Unlock()
	global.app.PingSent++
	if s := global.sessions[sessionID]; s != nil {
		s.AppPingSent++
		s.LastAppPingAt = time.Now()
		addSessionEventLocked(s, "app_ping_sent", "Go -> App WS Ping", "", 0, "")
	}
}

// AppPongReceived 记录协议层 App 到 Go 的 pong。
// 如果存在对应的 ping 时间戳，就同步更新 pong 延迟的最小值、最大值和平均值。
func AppPongReceived(sessionID string) {
	global.mu.Lock()
	defer global.mu.Unlock()
	global.app.PongReceived++
	if s := global.sessions[sessionID]; s != nil {
		s.AppPongReceived++
		if !s.LastAppPingAt.IsZero() {
			// time.Since 精度到微秒，除以 1000 换算成毫秒后参与延迟统计。
			s.LastPongLatencyMs = float64(time.Since(s.LastAppPingAt).Microseconds()) / 1000
			updateLatency(&global.app.PongLatencyMs, s.LastPongLatencyMs)
		}
		addSessionEventLocked(s, "app_pong_received", "App -> Go WS Pong", "", 0, "")
	}
}

// AppWrite 记录 Go 写回 App、耳机或浏览器的字节数。
func AppWrite(sessionID string, bytes int) {
	// 先走 atomic 累计全局下行字节，会话为空时不再加锁；只有会话维度才需要 mu 保护。
	size := uint64(maxInt(bytes, 0))
	global.counters.appBytesOut.Add(size)
	if sessionID == "" {
		return
	}
	global.mu.Lock()
	defer global.mu.Unlock()
	if s := global.sessions[sessionID]; s != nil {
		s.AppBytesOut += size
	}
}

// SlowConsumerDrop 记录 App 下行队列长时间满载的情况。
// 此时 Go 会丢弃一条下行消息，避免阻塞整个会话。
func SlowConsumerDrop(sessionID string, bytes int) {
	// 丢弃消息同时计一次发送队列超时：两者都反映 App 下行通道能力不足，口径保持一致。
	global.counters.appSlowConsumerDrops.Add(1)
	global.counters.goSendQueueTimeouts.Add(1)
	global.mu.Lock()
	defer global.mu.Unlock()
	if s := global.sessions[sessionID]; s != nil {
		s.SlowConsumerDrops++
		addSessionEventLocked(s, "slow_consumer_drop", "sendChan full", "", bytes, "")
	}
}

// QueueDepth 记录最近一次 App 下行队列和 OpenAI 上行队列的长度。
// 它是“最近观测值”指标，不是累计计数器。
func QueueDepth(sessionID string, sendLen, sendCap, apiLen, apiCap int) {
	// 队列深度是最近观测值而非累计值：先用 atomic 覆盖全局最近值，会话维度再在锁内更新。
	global.counters.lastSendQueueLen.Store(int64(sendLen))
	global.counters.lastSendQueueCap.Store(int64(sendCap))
	global.counters.lastAPISendQueueLen.Store(int64(apiLen))
	global.counters.lastAPISendQueueCap.Store(int64(apiCap))
	if sessionID == "" {
		return
	}
	global.mu.Lock()
	defer global.mu.Unlock()
	if s := global.sessions[sessionID]; s != nil {
		s.LastSendQueueLen = sendLen
		s.LastSendQueueCap = sendCap
		s.LastAPIQueueLen = apiLen
		s.LastAPIQueueCap = apiCap
	}
}

// APISendQueueTimeout 记录 App 消息在 send_queue_timeout_ms 内无法进入 OpenAI 上行队列的情况。
func APISendQueueTimeout(sessionID, eventType, reason string) {
	// 超时同时计入内存计数和统一资源事件流，让调试页与持久化统计口径一致。
	global.counters.goAPISendTimeouts.Add(1)
	stats.RecordResourceEvent(stats.ResourceEvent{
		Source: stats.SourceRealtime,
		Kind:   stats.ResourceKindError,
		Status: "failed",
		Error:  strings.TrimSpace(eventType + " " + reason),
	})
	global.mu.Lock()
	defer global.mu.Unlock()
	recordErrorLocked(sessionID, "api_send_queue_timeout", eventType+" "+reason, "")
}

// CriticalAppEventQueueTimeout 记录必须送达 App 的关键事件在下行队列中超时。
// 关键事件包括错误、重连要求、响应结束和工具执行结果；这些事件丢失会让客户端状态机失真。
func CriticalAppEventQueueTimeout(sessionID, eventType string, bytes int) {
	global.counters.goCriticalTimeouts.Add(1)
	stats.RecordResourceEvent(stats.ResourceEvent{
		Source: stats.SourceRealtime,
		Kind:   stats.ResourceKindError,
		Status: "failed",
		Error:  strings.TrimSpace(eventType),
	})
	global.mu.Lock()
	defer global.mu.Unlock()
	if s := global.sessions[sessionID]; s != nil {
		incMap(s.EventCounts, "app_queue_timeout:"+eventType)
		addSessionEventLocked(s, "critical_app_event_queue_timeout", eventType, "", bytes, "")
	}
	recordErrorLocked(sessionID, "critical_app_event_queue_timeout", eventType, "")
}

// OpenAIConnectAttempt 记录每一次拨号连接 OpenAI Realtime 的尝试。
func OpenAIConnectAttempt(sessionID string) {
	global.mu.Lock()
	defer global.mu.Unlock()
	global.openai.ConnectAttempts++
	if s := global.sessions[sessionID]; s != nil {
		addSessionEventLocked(s, "openai_connect_attempt", "", "", 0, "")
	}
}

// OpenAIConnectResult 记录一次 OpenAI 拨号结果。
// 成功会进入成功计数，失败会把原因写入错误时间线。
func OpenAIConnectResult(sessionID string, err error) {
	global.mu.Lock()
	defer global.mu.Unlock()
	if err != nil {
		global.openai.ConnectFailures++
		recordErrorLocked(sessionID, "openai_connect_failed", errorString(err), "")
		return
	}
	global.openai.ConnectSuccess++
	if s := global.sessions[sessionID]; s != nil {
		addSessionEventLocked(s, "openai_connected", "", "", 0, "")
	}
}

// OpenAIPingSent 记录 Go 主动向 OpenAI 上游发送的 WebSocket Ping。
// 它用于区分“上游没有任何服务端事件”与“本地没有主动探活”两种情况。
func OpenAIPingSent(sessionID string) {
	global.counters.openAIPingSent.Add(1)
	if sessionID == "" {
		return
	}
	global.mu.Lock()
	defer global.mu.Unlock()
	if s := global.sessions[sessionID]; s != nil {
		s.OpenAIPingSent++
		addSessionEventLocked(s, "openai_ping_sent", "Go -> OpenAI WS Ping", "", 0, "")
	}
}

// OpenAIPingFailed 记录主动探活 OpenAI 上游失败。
// 失败会进入全局错误时间线，便于诊断页和日志把它与重连、读超时关联起来。
func OpenAIPingFailed(sessionID string, err error) {
	global.counters.openAIPingFailures.Add(1)
	global.mu.Lock()
	defer global.mu.Unlock()
	if s := global.sessions[sessionID]; s != nil {
		s.OpenAIPingFailed++
		addSessionEventLocked(s, "openai_ping_failed", errorString(err), "", 0, "")
	}
	recordErrorLocked(sessionID, "openai_ping_failed", errorString(err), "")
}

// OpenAIClientEvent 记录一条从 Go 写往 OpenAI 的事件。
func OpenAIClientEvent(sessionID, eventType, reason string, bytes int) {
	global.counters.openAIClientEvents.Add(1)
	global.mu.Lock()
	defer global.mu.Unlock()
	if eventType == "" {
		eventType = "unknown"
	}
	incMap(global.openai.ClientEventTypes, eventType)
	if s := global.sessions[sessionID]; s != nil {
		s.OpenAIEventsUp++
		incMap(s.EventCounts, "openai_up:"+eventType)
		addSessionEventLocked(s, "openai_client_event", eventType+" "+reason, "", bytes, "")
	}
}

// OpenAIWriteError 记录写 OpenAI 上游 WebSocket 失败。
func OpenAIWriteError(sessionID, eventType, reason string, err error) {
	global.mu.Lock()
	defer global.mu.Unlock()
	recordErrorLocked(sessionID, "openai_write_error", eventType+" "+reason+": "+errorString(err), "")
}

// OpenAIServerEvent 记录一条从 OpenAI 读到、并准备转发或转换给 App 的事件。
func OpenAIServerEvent(sessionID, eventType, responseID string, bytes int) {
	global.counters.openAIServerEvents.Add(1)
	global.mu.Lock()
	defer global.mu.Unlock()
	if eventType == "" {
		eventType = "unknown"
	}
	incMap(global.openai.ServerEventTypes, eventType)
	if s := global.sessions[sessionID]; s != nil {
		s.OpenAIEventsDown++
		incMap(s.EventCounts, "openai_down:"+eventType)
		addSessionEventLocked(s, "openai_server_event", eventType, responseID, bytes, "")
	}
}

// OpenAIResponseCreated 开始记录一次响应生命周期。
func OpenAIResponseCreated(sessionID, responseID string) {
	global.counters.openAIResponseCreated.Add(1)
	if sessionID == "" {
		return
	}
	global.mu.Lock()
	defer global.mu.Unlock()
	if s := global.sessions[sessionID]; s != nil {
		r := ensureResponseLocked(s, responseID)
		r.CreatedAt = time.Now()
		r.Status = "in_progress"
		addSessionEventLocked(s, "response_created", "", responseID, 0, "")
	}
}

// OpenAITextDelta 记录流式助手文本，并为最终诊断快照保留有上限的响应文本。
func OpenAITextDelta(sessionID, responseID, delta string) {
	// 按 rune（字符）计数而非字节，多字节文本的字符数与界面感知一致。
	global.counters.openAITextChars.Add(uint64(len([]rune(delta))))
	if sessionID == "" {
		return
	}
	global.mu.Lock()
	defer global.mu.Unlock()
	if s := global.sessions[sessionID]; s != nil {
		r := ensureResponseLocked(s, responseID)
		markFirstDeltaLocked(r)
		appendLimited(&r.Text, delta, maxResponseTextChars)
	}
}

// OpenAITranscriptDelta 记录 OpenAI 返回的流式转写文本。
func OpenAITranscriptDelta(sessionID, responseID, delta string) {
	global.counters.openAITranscriptChars.Add(uint64(len([]rune(delta))))
	if sessionID == "" {
		return
	}
	global.mu.Lock()
	defer global.mu.Unlock()
	if s := global.sessions[sessionID]; s != nil {
		r := ensureResponseLocked(s, responseID)
		markFirstDeltaLocked(r)
		appendLimited(&r.Transcript, delta, maxResponseTextChars)
	}
}

// OpenAIAudioDelta 记录流式 base64 音频 payload。
// 解码字节数和 PCM 时长只是诊断用估算值。
func OpenAIAudioDelta(sessionID, responseID string, encodedBytes int) {
	// 解码字节数与 PCM 时长均为估算值，不解析实际音频数据，避免热路径开销。
	decoded := estimateBase64DecodedBytes(encodedBytes)
	outputMs := uint64(estimatePCM16Ms(decoded))
	global.counters.openAIAudioPackets.Add(1)
	global.counters.openAIAudioBytes.Add(uint64(decoded))
	global.counters.businessOutputAudioMs.Add(outputMs)
	if sessionID == "" {
		return
	}
	global.mu.Lock()
	defer global.mu.Unlock()
	if s := global.sessions[sessionID]; s != nil {
		r := ensureResponseLocked(s, responseID)
		markFirstDeltaLocked(r)
		r.AudioPackets++
		r.AudioBytes += uint64(decoded)
		s.OutputAudioMs += outputMs
	}
}

// InputAudio 记录 App 发送给 OpenAI 的 base64 音频。
func InputAudio(sessionID string, encodedBytes int) {
	// 与 OpenAIAudioDelta 同一套估算口径，保证输入输出音频时长可比。
	ms := uint64(estimatePCM16Ms(estimateBase64DecodedBytes(encodedBytes)))
	global.counters.businessInputAudioMs.Add(ms)
	if sessionID == "" {
		return
	}
	global.mu.Lock()
	defer global.mu.Unlock()
	if s := global.sessions[sessionID]; s != nil {
		s.InputAudioMs += ms
	}
}

// OpenAIResponseDone 收口一次响应生命周期。
// 它会更新响应状态分布、token 用量、首包延迟和完整响应耗时。
func OpenAIResponseDone(sessionID, responseID, status string, inputTokens, outputTokens int) {
	OpenAIResponseDoneUsage(sessionID, responseID, status, ResponseTokenUsage{
		InputTokens:  inputTokens,
		OutputTokens: outputTokens,
		TotalTokens:  inputTokens + outputTokens,
	})
}

// OpenAIResponseDoneUsage 收口一次响应生命周期，并记录 response.done 返回的完整 token 明细。
func OpenAIResponseDoneUsage(sessionID, responseID, status string, usage ResponseTokenUsage) {
	global.counters.openAIResponseDone.Add(1)
	// 按 status 更新响应结果分布；未匹配的状态不计入任何分布，只保留在 done 总数中。
	switch status {
	case "completed":
		global.counters.openAIResponseOK.Add(1)
	case "cancelled":
		global.counters.openAIResponseCancel.Add(1)
	case "failed", "incomplete":
		global.counters.openAIResponseFailed.Add(1)
	}
	// 归一化 token 明细：TotalTokens 未填时回退为输入输出之和；负数计数转 uint64 前钳制为 0。
	if usage.TotalTokens <= 0 {
		usage.TotalTokens = usage.InputTokens + usage.OutputTokens
	}
	input := uint64(maxInt(usage.InputTokens, 0))
	output := uint64(maxInt(usage.OutputTokens, 0))
	totalTokens := uint64(maxInt(usage.TotalTokens, 0))
	cached := uint64(maxInt(usage.CachedTokens, 0))
	reasoning := uint64(maxInt(usage.ReasoningTokens, 0))
	global.counters.businessInputTokens.Add(input)
	global.counters.businessOutputTokens.Add(output)
	global.counters.businessTotalTokens.Add(totalTokens)
	global.counters.businessCachedTokens.Add(cached)
	global.counters.businessReasoning.Add(reasoning)

	// 业务总量走 atomic 热路径；按天分组需要 map 计数，进入锁内更新。
	global.mu.Lock()
	defer global.mu.Unlock()
	incMapBy(global.business.TokensByDay, time.Now().Format("2006-01-02"), totalTokens)

	// 会话维度：回填响应摘要的 token 与耗时，并把用量拆分到用户和模型，便于定位超限来源。
	if s := global.sessions[sessionID]; s != nil {
		r := ensureResponseLocked(s, responseID)
		now := time.Now()
		r.DoneAt = now
		r.Status = status
		r.InputTokens += input
		r.OutputTokens += output
		r.CachedTokens += cached
		r.ReasoningTokens += reasoning
		s.InputTokens += input
		s.OutputTokens += output
		s.CachedTokens += cached
		s.ReasoningTokens += reasoning
		incMapBy(global.business.TokensByUser, s.UserID, totalTokens)
		incMapBy(global.business.TokensByModel, s.Model, totalTokens)
		// 耗时由微秒换算毫秒；created_at 缺失时跳过，无法推断的指标不猜测。
		if !r.CreatedAt.IsZero() {
			r.DurationMs = float64(now.Sub(r.CreatedAt).Microseconds()) / 1000
			updateLatency(&global.openai.ResponseLatencyMs, r.DurationMs)
		}
		if !r.FirstDeltaAt.IsZero() && !r.CreatedAt.IsZero() {
			r.FirstDeltaMs = float64(r.FirstDeltaAt.Sub(r.CreatedAt).Microseconds()) / 1000
			updateLatency(&global.openai.FirstDeltaLatencyMs, r.FirstDeltaMs)
		}
		addSessionEventLocked(s, "response_done", status, responseID, 0, "")
	}
}

// OpenAIError 记录 OpenAI 上游返回的错误事件。
func OpenAIError(sessionID, code, message string) {
	stats.RecordResourceEvent(stats.ResourceEvent{
		Source: stats.SourceRealtime,
		Kind:   stats.ResourceKindError,
		Status: "failed",
		Error:  strings.TrimSpace(code + " " + message),
	})
	global.mu.Lock()
	defer global.mu.Unlock()
	recordErrorLocked(sessionID, "openai_error", message, code)
}

// ReconnectRequested 记录 OpenAI 读协程或写协程发起了重连请求。
func ReconnectRequested(sessionID, reason string) {
	global.mu.Lock()
	defer global.mu.Unlock()
	global.openai.ReconnectRequests++
	incMap(global.openai.ReconnectReasons, reason)
	if s := global.sessions[sessionID]; s != nil {
		s.OpenAIReconnects++
		addSessionEventLocked(s, "openai_reconnect_requested", reason, "", 0, "")
	}
}

// ReconnectResult 记录一次 OpenAI 重连尝试是否成功。
func ReconnectResult(sessionID, reason string, err error) {
	global.mu.Lock()
	defer global.mu.Unlock()
	if err != nil {
		global.openai.ReconnectFailures++
		recordErrorLocked(sessionID, "openai_reconnect_failed", reason+": "+errorString(err), "")
		return
	}
	global.openai.ReconnectSuccess++
	if s := global.sessions[sessionID]; s != nil {
		addSessionEventLocked(s, "openai_reconnect_success", reason, "", 0, "")
	}
}

// SessionRestore 记录重连 OpenAI 后，网关是否成功重放 session.update 和最近对话历史。
func SessionRestore(sessionID string, historyEvents int, err error) {
	global.mu.Lock()
	defer global.mu.Unlock()
	if err != nil {
		global.openai.SessionRestoreFailure++
		recordErrorLocked(sessionID, "openai_restore_failed", errorString(err), "")
		return
	}
	global.openai.SessionRestoreSuccess++
	if s := global.sessions[sessionID]; s != nil {
		addSessionEventLocked(s, "openai_session_restored", "history_events", "", historyEvents, "")
	}
}

// RateLimitRejected 记录被本地或 Redis 限流拦截的请求。
func RateLimitRejected(userID, model, path, reason string) {
	global.counters.businessRateRejected.Add(1)
	stats.RecordResourceEvent(stats.ResourceEvent{
		Source: sourceFromPath(path),
		Kind:   stats.ResourceKindRateLimitRejected,
		Model:  model,
		UserID: userID,
		Status: "rejected",
	})
	global.mu.Lock()
	defer global.mu.Unlock()
	// 限流发生在连接建立前，没有会话可关联，只进全局错误摘要。
	recordErrorLocked("", "rate_limit_rejected", strings.Join([]string{userID, model, path, reason}, " "), "")
}

// BillingError 记录 response.done 返回 usage 后，token 或计费写入失败的情况。
func BillingError(sessionID string, err error) {
	global.counters.businessBillingErrors.Add(1)
	stats.RecordResourceEvent(stats.ResourceEvent{
		Source: stats.SourceRealtime,
		Kind:   stats.ResourceKindError,
		Status: "failed",
		Error:  errorString(err),
	})
	global.mu.Lock()
	defer global.mu.Unlock()
	recordErrorLocked(sessionID, "billing_error", errorString(err), "")
}

// sourceFromPath 按请求路径归类限流来源。
// 使用子串匹配兼容不同版本的路由前缀，未命中的路径统一归为系统来源。
func sourceFromPath(path string) string {
	path = strings.TrimSpace(path)
	if strings.Contains(path, "/ws/realtime/") {
		return stats.SourceRealtime
	}
	if strings.Contains(path, "/responses") {
		return stats.SourceResponses
	}
	return stats.SourceSystem
}

// Snapshot 返回可直接转成 JSON 的安全快照，供 /api/debug/status 使用。
// map 和 slice 字段会被复制，避免调用方修改全局指标收集器的内部状态。
func Snapshot() map[string]any {
	global.mu.Lock()
	defer global.mu.Unlock()

	// 先在锁内复制各指标结构，并把 atomic 热路径计数合入副本；atomic 在锁外仍会继续变化，
	// 快照读到的是调用时刻的近似值，诊断场景可接受。
	app := global.app
	applyAppHotCounters(&app)
	app.MessageTypes = cloneMap(global.app.MessageTypes)
	app.DisconnectReasons = cloneMap(global.app.DisconnectReasons)

	openai := global.openai
	applyOpenAIHotCounters(&openai)
	openai.ClientEventTypes = cloneMap(global.openai.ClientEventTypes)
	openai.ServerEventTypes = cloneMap(global.openai.ServerEventTypes)
	openai.ReconnectReasons = cloneMap(global.openai.ReconnectReasons)

	errors := global.errors
	errors.ByCode = cloneMap(global.errors.ByCode)
	errors.ByReason = cloneMap(global.errors.ByReason)
	errors.Recent = append([]eventRecord(nil), global.errors.Recent...)

	business := global.business
	applyBusinessHotCounters(&business)
	business.TokensByUser = cloneMap(global.business.TokensByUser)
	business.TokensByModel = cloneMap(global.business.TokensByModel)
	business.TokensByDay = cloneMap(global.business.TokensByDay)

	goStats := global.goStats
	applyGoHotCounters(&goStats)

	// 会话按开始时间倒序排列并裁剪到 maxRecentSessions，保证轮询响应体大小稳定。
	sessions := make([]map[string]any, 0, len(global.sessions))
	for _, s := range global.sessions {
		sessions = append(sessions, snapshotSessionLocked(s))
	}
	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i]["started_at"].(string) > sessions[j]["started_at"].(string)
	})
	if len(sessions) > maxRecentSessions {
		sessions = sessions[:maxRecentSessions]
	}

	// uptime_seconds 是进程启动以来的秒数；started_at 用 RFC3339 便于 JSON 直接解析。
	return map[string]any{
		"started_at":       global.started.Format(time.RFC3339),
		"uptime_seconds":   int64(time.Since(global.started).Seconds()),
		"app":              app,
		"go":               goStats,
		"openai":           openai,
		"errors":           errors,
		"business":         business,
		"recent_sessions":  sessions,
		"session_retained": len(global.sessions),
	}
}

// snapshotSessionLocked 将单个会话转换为诊断页面使用的 JSON 结构。
// 调用方必须已经持有 global.mu。
func snapshotSessionLocked(s *sessionMetrics) map[string]any {
	// 按最近到最早展开响应摘要（RecentResponses 是按创建顺序追加的 id 环），
	// 并截断大段文本，防止长对话撑大轮询响应。
	responses := make([]map[string]any, 0, len(s.RecentResponses))
	for i := len(s.RecentResponses) - 1; i >= 0; i-- {
		id := s.RecentResponses[i]
		if r := s.Responses[id]; r != nil {
			responses = append(responses, map[string]any{
				"response_id":      r.ResponseID,
				"status":           r.Status,
				"created_at":       timeString(r.CreatedAt),
				"done_at":          timeString(r.DoneAt),
				"first_delta_ms":   r.FirstDeltaMs,
				"duration_ms":      r.DurationMs,
				"text":             truncateString(r.Text, maxSnapshotResponseChars),
				"transcript":       truncateString(r.Transcript, maxSnapshotResponseChars),
				"audio_bytes":      r.AudioBytes,
				"audio_packets":    r.AudioPackets,
				"input_tokens":     r.InputTokens,
				"output_tokens":    r.OutputTokens,
				"cached_tokens":    r.CachedTokens,
				"reasoning_tokens": r.ReasoningTokens,
			})
		}
	}
	return map[string]any{
		"session_id":           s.SessionID,
		"request_id":           s.RequestID,
		"user_id":              s.UserID,
		"user_name":            s.UserName,
		"device_id":            s.DeviceID,
		"model":                s.Model,
		"remote_addr":          s.RemoteAddr,
		"real_ip":              realIPFromRemoteAddr(s.RemoteAddr),
		"ip_location":          sessionIPLocation(s),
		"user_agent":           s.UserAgent,
		"started_at":           s.StartedAt.Format(time.RFC3339),
		"ended_at":             timeString(s.EndedAt),
		"active":               s.Active,
		"end_reason":           s.EndReason,
		"duration_seconds":     sessionDurationSeconds(s),
		"app_messages":         s.AppMessages,
		"app_bytes_in":         s.AppBytesIn,
		"app_bytes_out":        s.AppBytesOut,
		"app_ping_sent":        s.AppPingSent,
		"app_pong_received":    s.AppPongReceived,
		"openai_ping_sent":     s.OpenAIPingSent,
		"openai_ping_failed":   s.OpenAIPingFailed,
		"last_pong_latency_ms": s.LastPongLatencyMs,
		"openai_events_up":     s.OpenAIEventsUp,
		"openai_events_down":   s.OpenAIEventsDown,
		"openai_reconnects":    s.OpenAIReconnects,
		"slow_consumer_drops":  s.SlowConsumerDrops,
		"pipeline_workers":     4, // 与 OpenAI Provider 的四协程流水线一一对应
		"send_queue_len":       s.LastSendQueueLen,
		"send_queue_cap":       s.LastSendQueueCap,
		"api_queue_len":        s.LastAPIQueueLen,
		"api_queue_cap":        s.LastAPIQueueCap,
		"input_audio_ms":       s.InputAudioMs,
		"output_audio_ms":      s.OutputAudioMs,
		"input_tokens":         s.InputTokens,
		"output_tokens":        s.OutputTokens,
		"cached_tokens":        s.CachedTokens,
		"reasoning_tokens":     s.ReasoningTokens,
		"event_counts":         cloneMap(s.EventCounts),
		"events":               append([]eventRecord(nil), s.Events...),
		"responses":            responses,
	}
}

// applyAppHotCounters 把 App 侧 atomic 热路径计数合入快照副本。
func applyAppHotCounters(app *appMetrics) {
	app.BytesOut = global.counters.appBytesOut.Load()
	app.SlowConsumerDrops = global.counters.appSlowConsumerDrops.Load()
}

// applyGoHotCounters 把网关压力 atomic 计数合入快照副本。
func applyGoHotCounters(goStats *goMetrics) {
	goStats.CapacityRejected = global.counters.goCapacityRejected.Load()
	goStats.APISendQueueTimeouts = global.counters.goAPISendTimeouts.Load()
	goStats.SendQueueTimeouts = global.counters.goSendQueueTimeouts.Load()
	goStats.CriticalAppEventQueueTimeouts = global.counters.goCriticalTimeouts.Load()
	goStats.LastSendQueueLen = int(global.counters.lastSendQueueLen.Load())
	goStats.LastSendQueueCap = int(global.counters.lastSendQueueCap.Load())
	goStats.LastAPISendQueueLen = int(global.counters.lastAPISendQueueLen.Load())
	goStats.LastAPISendQueueCap = int(global.counters.lastAPISendQueueCap.Load())
}

// applyOpenAIHotCounters 把 OpenAI 侧 atomic 计数合入快照副本。
func applyOpenAIHotCounters(openai *openAIMetrics) {
	openai.PingSent = global.counters.openAIPingSent.Load()
	openai.PingFailures = global.counters.openAIPingFailures.Load()
	openai.ClientEventsTotal = global.counters.openAIClientEvents.Load()
	openai.ServerEventsTotal = global.counters.openAIServerEvents.Load()
	openai.ResponseCreated = global.counters.openAIResponseCreated.Load()
	openai.ResponseDone = global.counters.openAIResponseDone.Load()
	openai.ResponseCompleted = global.counters.openAIResponseOK.Load()
	openai.ResponseCancelled = global.counters.openAIResponseCancel.Load()
	openai.ResponseFailed = global.counters.openAIResponseFailed.Load()
	openai.TextDeltaChars = global.counters.openAITextChars.Load()
	openai.TranscriptDeltaChars = global.counters.openAITranscriptChars.Load()
	openai.AudioDeltaBytes = global.counters.openAIAudioBytes.Load()
	openai.AudioDeltaPackets = global.counters.openAIAudioPackets.Load()
}

// applyBusinessHotCounters 把业务用量 atomic 计数合入快照副本。
func applyBusinessHotCounters(business *businessMetrics) {
	business.InputAudioMs = global.counters.businessInputAudioMs.Load()
	business.OutputAudioMs = global.counters.businessOutputAudioMs.Load()
	business.InputTokens = global.counters.businessInputTokens.Load()
	business.OutputTokens = global.counters.businessOutputTokens.Load()
	business.TotalTokens = global.counters.businessTotalTokens.Load()
	business.CachedTokens = global.counters.businessCachedTokens.Load()
	business.ReasoningTokens = global.counters.businessReasoning.Load()
	business.RateLimitRejected = global.counters.businessRateRejected.Load()
	business.BillingErrors = global.counters.businessBillingErrors.Load()
}

// addSessionEventLocked 向会话时间线追加一条事件，并只保留最新 maxSessionEvents 条。
// 调用方必须已经持有 global.mu。
func addSessionEventLocked(s *sessionMetrics, kind, detail, responseID string, bytes int, code string) {
	if s == nil {
		return
	}
	rec := eventRecord{
		At:         time.Now().Format(time.RFC3339Nano),
		Kind:       kind,
		Detail:     strings.TrimSpace(detail),
		ResponseID: responseID,
		Bytes:      bytes,
		Code:       code,
	}
	s.Events = append(s.Events, rec)
	// 超过上限时只保留最新尾部并复制到新切片，旧底层数组可被 GC 回收，事件线不会无限增长。
	if len(s.Events) > maxSessionEvents {
		s.Events = append([]eventRecord(nil), s.Events[len(s.Events)-maxSessionEvents:]...)
	}
}

// recordErrorLocked 写入全局错误摘要。
// 如果能找到对应会话，也会把同一错误同步写入该会话时间线。
// 调用方必须已经持有 global.mu。
func recordErrorLocked(sessionID, reason, detail, code string) {
	global.errors.Total++
	if code == "" {
		code = "unknown"
	}
	incMap(global.errors.ByCode, code)
	incMap(global.errors.ByReason, reason)
	rec := eventRecord{
		At:     time.Now().Format(time.RFC3339Nano),
		Kind:   reason,
		Detail: detail,
		Code:   code,
	}
	global.errors.Recent = append(global.errors.Recent, rec)
	// 全局错误线复用 maxSessionEvents 上限，超过时同样只保留最新尾部。
	if len(global.errors.Recent) > maxSessionEvents {
		global.errors.Recent = append([]eventRecord(nil), global.errors.Recent[len(global.errors.Recent)-maxSessionEvents:]...)
	}
	// 找到会话时把同一错误镜像到会话时间线，诊断页可同时从全局与会话视角看到它。
	if s := global.sessions[sessionID]; s != nil {
		addSessionEventLocked(s, "error:"+reason, detail, "", 0, code)
	}
}

// pruneEndedSessionsLocked 防止长时间压测时无限保留已关闭会话。
// 活跃会话永远不会被裁剪。
func pruneEndedSessionsLocked() {
	if len(global.sessions) <= maxRecentSessions {
		return
	}

	type endedSession struct {
		id string
		at time.Time
	}
	// 只收集已结束会话，活跃会话永远不会被裁剪；EndedAt 缺失时退回开始时间参与排序。
	ended := make([]endedSession, 0, len(global.sessions))
	for id, s := range global.sessions {
		if s == nil || s.Active {
			continue
		}
		at := s.EndedAt
		if at.IsZero() {
			at = s.StartedAt
		}
		ended = append(ended, endedSession{id: id, at: at})
	}
	sort.Slice(ended, func(i, j int) bool {
		return ended[i].at.Before(ended[j].at)
	})

	// 从结束时间最早的开始删除，只回收到内存上限，不区分会话归属。
	deleteCount := len(global.sessions) - maxRecentSessions
	for i := 0; i < deleteCount && i < len(ended); i++ {
		delete(global.sessions, ended[i].id)
	}
}

// ensureResponseLocked 返回已有响应记录，找不到时创建新记录。
// 它还会把单会话响应环裁剪到 maxRecentResponses。
func ensureResponseLocked(s *sessionMetrics, responseID string) *responseMetrics {
	// responseID 缺失（部分 delta 事件不带 id）归一到 unknown，保证快照键稳定。
	if responseID == "" {
		responseID = "unknown"
	}
	if r := s.Responses[responseID]; r != nil {
		return r
	}
	r := &responseMetrics{ResponseID: responseID, CreatedAt: time.Now(), Status: "unknown"}
	s.Responses[responseID] = r
	s.RecentResponses = append(s.RecentResponses, responseID)
	// 单会话保留最近 maxRecentResponses 条响应，超过时淘汰最旧的并删除其记录，避免长对话内存膨胀。
	if len(s.RecentResponses) > maxRecentResponses {
		old := s.RecentResponses[0]
		s.RecentResponses = append([]string(nil), s.RecentResponses[1:]...)
		delete(s.Responses, old)
	}
	return r
}

// markFirstDeltaLocked 保存首个文本、音频或转写 delta 到达时间。
// 后续会用它和 response.created 时间比较，计算首包延迟。
func markFirstDeltaLocked(r *responseMetrics) {
	if r == nil || !r.FirstDeltaAt.IsZero() {
		return
	}
	r.FirstDeltaAt = time.Now()
}

// updateLatency 在不保存全量样本的情况下更新最小值、最大值和平均值。
func updateLatency(s *latencySummary, value float64) {
	// 负延迟视为无效样本（时钟回拨或记录错误），直接忽略以避免污染 min/avg。
	if value < 0 {
		return
	}
	if s.Count == 0 || value < s.Min {
		s.Min = value
	}
	if value > s.Max {
		s.Max = value
	}
	total := s.Avg*float64(s.Count) + value
	s.Count++
	s.Avg = total / float64(s.Count)
}

// incMap 对字符串计数器自增，并把空 key 归一为 unknown。
func incMap(m map[string]uint64, key string) {
	if key == "" {
		key = "unknown"
	}
	m[key]++
}

// incMapBy 给字符串计数器累加指定数值。
func incMapBy(m map[string]uint64, key string, value uint64) {
	if key == "" {
		key = "unknown"
	}
	m[key] += value
}

// cloneMap 在 Snapshot 对外导出前复制 map。
func cloneMap(src map[string]uint64) map[string]uint64 {
	dst := make(map[string]uint64, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// appendLimited 追加流式文本，同时只保留最新的限定长度内容。
func appendLimited(dst *string, delta string, limit int) {
	if delta == "" {
		return
	}
	*dst += delta
	// 超过上限时按字节保留最新尾部；UTF-8 多字节字符可能被截断，快照只做展示，允许该情况。
	if len(*dst) > limit {
		*dst = (*dst)[len(*dst)-limit:]
	}
}

// truncateString 限制 JSON 快照中的大段响应文本长度。
func truncateString(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "\n... (truncated)"
}

// estimateBase64DecodedBytes 根据 base64 长度估算解码后的字节数。
func estimateBase64DecodedBytes(encodedLen int) int {
	if encodedLen <= 0 {
		return 0
	}
	// 按每 4 个 base64 字符对应 3 字节近似，忽略尾部 padding，误差不超过 2 字节，诊断用足够。
	return encodedLen * 3 / 4
}

// estimatePCM16Ms 按 24kHz 单声道 PCM16 估算音频时长。
func estimatePCM16Ms(bytes int) int {
	if bytes <= 0 {
		return 0
	}
	// 24kHz 单声道 16 位采样：每秒 24000 个采样点、每个采样点 2 字节，结果单位毫秒。
	return bytes * 1000 / (24000 * 2)
}

// sessionDurationSeconds 返回会话在线时长。
// 活跃会话使用当前时间计算，已关闭会话使用 EndedAt 计算。
func sessionDurationSeconds(s *sessionMetrics) float64 {
	end := s.EndedAt
	if end.IsZero() {
		end = time.Now()
	}
	return end.Sub(s.StartedAt).Seconds()
}

// timeString 把零时间转为空字符串，让 JSON 输出更干净。
func timeString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

// realIPFromRemoteAddr 从 Gin ClientIP 或 RemoteAddr 中提取纯 IP。
// RemoteAddr 可能是 "1.2.3.4:5678"、IPv6 或已经解析好的 IP 字符串。
func realIPFromRemoteAddr(remoteAddr string) string {
	value := strings.TrimSpace(remoteAddr)
	if value == "" {
		return ""
	}
	// SplitHostPort 成功说明是 host:port 形式（IPv6 带方括号）；失败则输入本身是纯 IP。
	if host, _, err := net.SplitHostPort(value); err == nil {
		return strings.Trim(host, "[]")
	}
	return strings.Trim(value, "[]")
}

// ipLocationSummary 返回不依赖外部服务的 IP 归属摘要。
// 当前先稳定区分本机、内网、公网和非法地址；后续可在这里接入 GeoIP 数据库或第三方解析。
func ipLocationSummary(ipText string) map[string]string {
	ip := net.ParseIP(strings.TrimSpace(ipText))
	if ip == nil {
		return map[string]string{
			"status":  "unknown",
			"display": "未知地址",
			"source":  "local_ip_classifier",
		}
	}
	if ip.IsLoopback() {
		return map[string]string{
			"status":  "loopback",
			"display": "本机地址",
			"source":  "local_ip_classifier",
		}
	}
	if ip.IsPrivate() {
		return map[string]string{
			"status":  "private",
			"display": "内网地址",
			"source":  "local_ip_classifier",
		}
	}
	return map[string]string{
		"status":  "public",
		"display": "公网地址（未配置 IP 地理库）",
		"source":  "local_ip_classifier",
	}
}

// sessionIPLocation 返回会话展示用的 IP 归属。
// 优先使用外部传入的归属信息；未提供时才用本地 IP 分类器兜底，整个过程不依赖外部服务。
func sessionIPLocation(s *sessionMetrics) map[string]string {
	// 返回副本，避免调用方修改会话持有的归属数据。
	if s != nil && len(s.IPLocation) > 0 {
		return cloneStringMap(s.IPLocation)
	}
	if s == nil {
		return ipLocationSummary("")
	}
	return ipLocationSummary(realIPFromRemoteAddr(s.RemoteAddr))
}

// normalizeIPLocation 清洗代理/CDN 传入的 IP 归属字段，只保留非空值并统一 key 为小写。
func normalizeIPLocation(location map[string]string) map[string]string {
	// 空输入返回 nil，快照侧会回退到本地 IP 分类器。
	if len(location) == 0 {
		return nil
	}
	out := make(map[string]string, len(location)+2)
	for key, value := range location {
		key = strings.TrimSpace(strings.ToLower(key))
		value = strings.TrimSpace(value)
		if key != "" && value != "" {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	// source 与 status 缺失时补默认值，保证快照字段完整。
	if out["source"] == "" {
		out["source"] = "request_header"
	}
	if out["status"] == "" {
		out["status"] = "provided"
	}
	// display 由 country/region/city 按顺序拼接，缺失字段自动跳过。
	if out["display"] == "" {
		parts := make([]string, 0, 3)
		for _, key := range []string{"country", "region", "city"} {
			if out[key] != "" {
				parts = append(parts, out[key])
			}
		}
		if len(parts) > 0 {
			out["display"] = strings.Join(parts, " / ")
		}
	}
	return out
}

// cloneStringMap 在导出前复制字符串 map，避免调用方修改收集器内部状态。
func cloneStringMap(src map[string]string) map[string]string {
	out := make(map[string]string, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}

// errorString 安全地把 nil 或非 nil error 转成字符串。
func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// maxInt 在转换为 uint64 前钳制负数计数。
func maxInt(v, min int) int {
	if v < min {
		return min
	}
	return v
}
