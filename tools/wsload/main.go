// tools/wsload/main.go
// 文件功能：TozoAI Realtime WebSocket 的轻量压测工具。解析命令行参数后并发建立 WS 连接
// 并收发消息，统计连接成功率、消息量、延迟百分位（P50/P95/P99）、close code 与错误分布，
// 输出 JSON 报告（可选写文件）。输入是 flags 与可选的 JWT token，输出是 stdout 的 JSON 报告。
// 不负责：容量结论的生成——报告固定声明“未实测，不能声明”，避免工具自身夸大容量。
// 安全边界：token 只用于 WS 连接鉴权头，带 json:"-" 标签，永不进入 JSON 报告。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// loadConfig 压测参数，来自命令行 flags；Token 带 json:"-" 标签，不会进入 JSON 报告。
type loadConfig struct {
	URL        string        `json:"url"`
	Users      int           `json:"users"`
	Ramp       time.Duration `json:"ramp"`
	Duration   time.Duration `json:"duration"`
	Token      string        `json:"-"`
	Message    string        `json:"message"`
	DebugURL   string        `json:"debug_url,omitempty"`
	ReportPath string        `json:"report_path,omitempty"`
}

// reportConfig 报告中的压测配置回显（不含 Token，避免敏感信息落盘）。
type reportConfig struct {
	URL        string `json:"url"`
	Users      int    `json:"users"`
	Ramp       string `json:"ramp"`
	Duration   string `json:"duration"`
	Message    string `json:"message"`
	DebugURL   string `json:"debug_url,omitempty"`
	ReportPath string `json:"report_path,omitempty"`
}

// loadReport JSON 压测报告：配置回显、时间区间、汇总指标、延迟分位、close code 与错误分布。
type loadReport struct {
	Config             reportConfig      `json:"config"`
	StartedAt          string            `json:"started_at"`
	FinishedAt         string            `json:"finished_at"`
	Summary            reportSummary     `json:"summary"`
	Latency            latencyReport     `json:"latency"`
	CloseCodes         map[string]int64  `json:"close_codes"`
	Errors             map[string]int64  `json:"errors"`
	DebugSnapshots     []json.RawMessage `json:"debug_snapshots,omitempty"`
	CapacityConclusion string            `json:"capacity_conclusion"`
}

// reportSummary 报告汇总指标：连接成功/失败数、消息收发量、消息速率（条/秒）与错误率。
type reportSummary struct {
	ConnectSuccess int64   `json:"connect_success"`
	ConnectFailed  int64   `json:"connect_failed"`
	MessagesSent   int64   `json:"messages_sent"`
	MessagesRecv   int64   `json:"messages_recv"`
	MessagesPerSec float64 `json:"messages_per_sec"`
	ErrorRate      float64 `json:"error_rate"`
}

// latencyReport 三类延迟（连接建立、首字节、整条消息完成）的 P50/P95/P99 分位，单位毫秒。
type latencyReport struct {
	ConnectP50Ms   int64 `json:"connect_p50_ms"`
	ConnectP95Ms   int64 `json:"connect_p95_ms"`
	ConnectP99Ms   int64 `json:"connect_p99_ms"`
	FirstByteP50Ms int64 `json:"first_byte_p50_ms"`
	FirstByteP95Ms int64 `json:"first_byte_p95_ms"`
	FirstByteP99Ms int64 `json:"first_byte_p99_ms"`
	CompleteP50Ms  int64 `json:"complete_p50_ms"`
	CompleteP95Ms  int64 `json:"complete_p95_ms"`
	CompleteP99Ms  int64 `json:"complete_p99_ms"`
}

// runStats 压测运行期的聚合统计：计数器用原子变量，延迟切片与计数 map 由互斥锁保护。
type runStats struct {
	startedAt time.Time
	endedAt   time.Time

	connectSuccess atomic.Int64
	connectFailed  atomic.Int64
	messagesSent   atomic.Int64
	messagesRecv   atomic.Int64

	mu                 sync.Mutex
	connectLatencies   []time.Duration
	firstByteLatencies []time.Duration
	completeLatencies  []time.Duration
	closeCodes         map[string]int64
	errors             map[string]int64
	debugSnapshots     []json.RawMessage
}

func main() {
	// 参数解析失败按 flag 包约定退出：-h 请求帮助时正常返回，其余错误退出码 2。
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	// 执行压测并生成报告；序列化或写文件失败时输出错误并退出。
	stats := runStatsForConfig(context.Background(), cfg)
	report := buildReport(cfg, stats)
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "marshal report:", err)
		os.Exit(1)
	}
	if cfg.ReportPath != "" {
		if err := os.WriteFile(cfg.ReportPath, data, 0644); err != nil {
			fmt.Fprintln(os.Stderr, "write report:", err)
			os.Exit(1)
		}
	}
	fmt.Println(string(data))
}

// parseConfig 解析命令行参数并校验压测配置合法性。
// 校验规则：users 与 duration 必须大于 0，ramp 不得为负，URL scheme 必须是 ws/wss；
// 任一不满足即返回错误，不进入压测（失败关闭）。
func parseConfig(args []string) (loadConfig, error) {
	// 默认值面向本地试跑：单用户、10 秒压测。
	cfg := loadConfig{
		URL:      "ws://127.0.0.1:8096/ws/realtime/openai",
		Users:    1,
		Ramp:     time.Second,
		Duration: 10 * time.Second,
		Message:  "ping",
	}
	fs := flag.NewFlagSet("wsload", flag.ContinueOnError)
	fs.StringVar(&cfg.URL, "url", cfg.URL, "WebSocket URL, for example ws://127.0.0.1:8096/ws/realtime/openai")
	fs.IntVar(&cfg.Users, "users", cfg.Users, "并发用户/连接数")
	fs.DurationVar(&cfg.Ramp, "ramp", cfg.Ramp, "连接爬坡时间，例如 30s")
	fs.DurationVar(&cfg.Duration, "duration", cfg.Duration, "压测持续时间，例如 5m")
	fs.StringVar(&cfg.Token, "token", "", "JWT token，非空时写入 Authorization: Bearer")
	fs.StringVar(&cfg.Message, "message", cfg.Message, "连接成功后发送的文本消息")
	fs.StringVar(&cfg.DebugURL, "debug-url", "", "可选 Go 诊断接口 URL，例如 http://127.0.0.1:8096/api/debug/status")
	fs.StringVar(&cfg.ReportPath, "report", "", "可选 JSON 报告输出路径")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	// 非法参数形状在启动压测前拦截，避免产生误导性报告。
	if cfg.Users <= 0 {
		return cfg, fmt.Errorf("users must be > 0")
	}
	if cfg.Duration <= 0 {
		return cfg, fmt.Errorf("duration must be > 0")
	}
	if cfg.Ramp < 0 {
		return cfg, fmt.Errorf("ramp must be >= 0")
	}
	// 只接受 ws/wss，防止把 HTTP 地址误当 WS 端点使用。
	parsed, err := url.Parse(cfg.URL)
	if err != nil {
		return cfg, fmt.Errorf("invalid url: %w", err)
	}
	if parsed.Scheme != "ws" && parsed.Scheme != "wss" {
		return cfg, fmt.Errorf("url scheme must be ws or wss")
	}
	return cfg, nil
}

// runStatsForConfig 执行一次完整压测：按 ramp 间隔逐个启动用户连接，全部结束后返回聚合统计。
// 整体运行时限为 duration+ramp 再加 10 秒兜底，防止个别连接挂死拖住整个压测。
// 配置了 DebugURL 时，在压测前后各抓取一次诊断快照，用于对比峰值前后的服务状态。
func runStatsForConfig(ctx context.Context, cfg loadConfig) *runStats {
	stats := newRunStats()
	ctx, cancel := context.WithTimeout(ctx, cfg.Duration+cfg.Ramp+10*time.Second)
	defer cancel()

	if cfg.DebugURL != "" {
		stats.captureDebugSnapshot(ctx, cfg.DebugURL)
	}

	// 连接爬坡：每个用户按 step 间隔启动，step = ramp / (users-1)；Ramp 为 0 时同时发起。
	var wg sync.WaitGroup
	step := time.Duration(0)
	if cfg.Users > 1 && cfg.Ramp > 0 {
		step = cfg.Ramp / time.Duration(cfg.Users-1)
	}
	for i := 0; i < cfg.Users; i++ {
		wg.Add(1)
		go func(userIndex int) {
			defer wg.Done()
			runOneUser(ctx, cfg, stats, userIndex)
		}(i)
		if step > 0 {
			// 爬坡等待期间 ctx 已取消时不再继续等待，避免超时后仍拖延启动。
			select {
			case <-time.After(step):
			case <-ctx.Done():
				break
			}
		}
	}
	wg.Wait()
	stats.endedAt = time.Now()

	// 压测结束后再抓一次快照；改用独立 context，避免继承已超时的 ctx。
	if cfg.DebugURL != "" {
		stats.captureDebugSnapshot(context.Background(), cfg.DebugURL)
	}
	return stats
}

// runOneUser 模拟单个用户：建立 WS 连接（可选携带 Bearer token），发送 {user} 替换后的
// 消息并等待一次回包，随后保持连接直至 duration 结束再主动 close。
// 任一步失败只记录该用户自身的错误统计，不影响其他用户。
func runOneUser(ctx context.Context, cfg loadConfig, stats *runStats, userIndex int) {
	header := http.Header{}
	if strings.TrimSpace(cfg.Token) != "" {
		header.Set("Authorization", "Bearer "+strings.TrimSpace(cfg.Token))
	}
	start := time.Now()
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, cfg.URL, header)
	if err != nil {
		stats.recordConnect(false, 0, err)
		return
	}
	defer conn.Close()
	stats.recordConnect(true, time.Since(start), nil)

	// 消息中的 {user} 占位符替换为当前用户序号，便于区分各连接的实际请求内容。
	message := strings.ReplaceAll(cfg.Message, "{user}", strconv.Itoa(userIndex))
	if strings.TrimSpace(message) != "" {
		// 发送首条消息并等待一次回包；first-byte 与 complete 延迟都以发送时刻起算。
		writeStart := time.Now()
		if err := conn.WriteMessage(websocket.TextMessage, []byte(message)); err != nil {
			stats.recordError("write: " + err.Error())
			return
		}
		stats.messagesSent.Add(1)
		_ = conn.SetReadDeadline(time.Now().Add(cfg.Duration))
		if _, _, err := conn.ReadMessage(); err != nil {
			stats.recordError("read: " + err.Error())
			// 服务端主动关闭时记录 close code，用于区分正常关闭与异常断连。
			if closeErr, ok := err.(*websocket.CloseError); ok {
				stats.recordCloseCode(closeErr.Code)
			}
			return
		}
		stats.messagesRecv.Add(1)
		stats.recordFirstByte(time.Since(writeStart))
		stats.recordComplete(time.Since(writeStart))
	}

	// 剩余时间保持连接以模拟长连接负载，到点后主动发送 normal close（1000）。
	deadline := time.NewTimer(cfg.Duration)
	defer deadline.Stop()
	<-deadline.C
	_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "wsload done"))
	stats.recordCloseCode(websocket.CloseNormalClosure)
}

// newRunStats 创建空统计对象，预分配延迟样本与计数容器。
func newRunStats() *runStats {
	return &runStats{
		startedAt:          time.Now(),
		closeCodes:         make(map[string]int64),
		errors:             make(map[string]int64),
		connectLatencies:   make([]time.Duration, 0),
		firstByteLatencies: make([]time.Duration, 0),
		completeLatencies:  make([]time.Duration, 0),
		debugSnapshots:     make([]json.RawMessage, 0, 2),
	}
}

// recordConnect 记录一次连接结果：成功时追加连接延迟样本，失败时累加失败数并记错误。
func (s *runStats) recordConnect(success bool, latency time.Duration, err error) {
	if success {
		s.connectSuccess.Add(1)
		s.mu.Lock()
		s.connectLatencies = append(s.connectLatencies, latency)
		s.mu.Unlock()
		return
	}
	s.connectFailed.Add(1)
	if err != nil {
		s.recordError(err.Error())
	}
}

func (s *runStats) recordFirstByte(latency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.firstByteLatencies = append(s.firstByteLatencies, latency)
}

func (s *runStats) recordComplete(latency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completeLatencies = append(s.completeLatencies, latency)
}

func (s *runStats) recordCloseCode(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCodes[strconv.Itoa(code)]++
}

// recordError 记录错误原因计数；空白原因统一记为 unknown，保证统计 key 稳定。
func (s *runStats) recordError(reason string) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "unknown"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errors[reason]++
}

// captureDebugSnapshot 抓取一次诊断接口快照（GET debugURL），解码为原始 JSON 存入报告。
// 请求失败或响应非 JSON 时记录错误并继续，不影响压测主流程。
func (s *runStats) captureDebugSnapshot(ctx context.Context, debugURL string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, debugURL, nil)
	if err != nil {
		s.recordError("debug_url: " + err.Error())
		return
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		s.recordError("debug_snapshot: " + err.Error())
		return
	}
	defer resp.Body.Close()
	var raw json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		s.recordError("debug_snapshot_decode: " + err.Error())
		return
	}
	s.mu.Lock()
	s.debugSnapshots = append(s.debugSnapshots, raw)
	s.mu.Unlock()
}

// buildReport 把 runStats 汇总为报告结构。先在锁内拷贝全部可变数据，之后无锁计算，
// 避免生成报告期间阻塞运行中的统计写入。
// 关键口径：错误率 = 连接失败数 / 总连接尝试数；消息速率 = 发送数 / 实际运行秒数。
func buildReport(cfg loadConfig, stats *runStats) loadReport {
	if stats == nil {
		stats = newRunStats()
	}
	stats.mu.Lock()
	connectLatencies := append([]time.Duration(nil), stats.connectLatencies...)
	firstByteLatencies := append([]time.Duration(nil), stats.firstByteLatencies...)
	completeLatencies := append([]time.Duration(nil), stats.completeLatencies...)
	closeCodes := cloneInt64Map(stats.closeCodes)
	errorCounts := cloneInt64Map(stats.errors)
	debugSnapshots := append([]json.RawMessage(nil), stats.debugSnapshots...)
	stats.mu.Unlock()

	started := stats.startedAt
	if started.IsZero() {
		started = time.Now()
	}
	finished := stats.endedAt
	if finished.IsZero() {
		finished = time.Now()
	}
	elapsed := finished.Sub(started).Seconds()
	if elapsed <= 0 {
		elapsed = cfg.Duration.Seconds()
	}
	if elapsed <= 0 {
		elapsed = 1
	}

	success := stats.connectSuccess.Load()
	failed := stats.connectFailed.Load()
	totalConnect := success + failed
	errorRate := float64(0)
	if totalConnect > 0 {
		errorRate = float64(failed) / float64(totalConnect)
	}

	return loadReport{
		Config: reportConfig{
			URL:        cfg.URL,
			Users:      cfg.Users,
			Ramp:       cfg.Ramp.String(),
			Duration:   cfg.Duration.String(),
			Message:    cfg.Message,
			DebugURL:   cfg.DebugURL,
			ReportPath: cfg.ReportPath,
		},
		StartedAt:  started.Format(time.RFC3339),
		FinishedAt: finished.Format(time.RFC3339),
		Summary: reportSummary{
			ConnectSuccess: success,
			ConnectFailed:  failed,
			MessagesSent:   stats.messagesSent.Load(),
			MessagesRecv:   stats.messagesRecv.Load(),
			MessagesPerSec: float64(stats.messagesSent.Load()) / elapsed,
			ErrorRate:      errorRate,
		},
		Latency: latencyReport{
			ConnectP50Ms:   percentile(connectLatencies, 50).Milliseconds(),
			ConnectP95Ms:   percentile(connectLatencies, 95).Milliseconds(),
			ConnectP99Ms:   percentile(connectLatencies, 99).Milliseconds(),
			FirstByteP50Ms: percentile(firstByteLatencies, 50).Milliseconds(),
			FirstByteP95Ms: percentile(firstByteLatencies, 95).Milliseconds(),
			FirstByteP99Ms: percentile(firstByteLatencies, 99).Milliseconds(),
			CompleteP50Ms:  percentile(completeLatencies, 50).Milliseconds(),
			CompleteP95Ms:  percentile(completeLatencies, 95).Milliseconds(),
			CompleteP99Ms:  percentile(completeLatencies, 99).Milliseconds(),
		},
		CloseCodes:         closeCodes,
		Errors:             errorCounts,
		DebugSnapshots:     debugSnapshots,
		CapacityConclusion: "当前未实测百万并发，不能声明已达到。百万并发和 1 秒响应必须由多实例拓扑、LB 长连接策略、OS FD/socket 参数、Redis/指标容量、OpenAI/第三方中转配额和真实压测报告共同证明。",
	}
}

// percentile 计算样本的 p 百分位延迟（nearest-rank 法）：复制并排序样本后按
// rank = ceil(p*n/100) 取值；空样本返回 0，p <= 0 取最小值，p >= 100 取最大值。
func percentile(samples []time.Duration, p int) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	values := append([]time.Duration(nil), samples...)
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	if p <= 0 {
		return values[0]
	}
	if p >= 100 {
		return values[len(values)-1]
	}
	rank := (p*len(values) + 99) / 100
	if rank < 1 {
		rank = 1
	}
	if rank > len(values) {
		rank = len(values)
	}
	return values[rank-1]
}

// cloneInt64Map 拷贝计数器 map，使报告快照与运行期统计互不影响。
func cloneInt64Map(src map[string]int64) map[string]int64 {
	dst := make(map[string]int64, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}
