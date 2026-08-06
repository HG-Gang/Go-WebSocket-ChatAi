// internal/service/alert/dingtalk.go
// 钉钉机器人告警发送：
// - 输入：handler 在容量达到上限或恢复时构造的 OverloadEvent
// - 按配置的冷却时间抑制重复告警，避免过载风暴刷屏
// - 构造钉钉 text 消息，经 hmac-sha256 签名 webhook 发送
// - 每次发送/抑制/失败都写审计日志，并同步到统一资源统计
//
// 安全边界：
//   - webhook/secret 仅存于内存配置，审计日志只记录 webhook 的 sha256 摘要（logger.RedactField），
//     任何日志不落 token 明文
//   - secret 为空时不签名直接使用 webhook（兼容无加签机器人）；非空时按钉钉官方签名规范加签
//   - 发送失败返回错误（失败关闭），由调用方记录，本包不重试
package alert

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/logger"
	"TozoAI-Chat-Api/internal/service/stats"

	"go.uber.org/zap"
)

var dingTalkState = struct {
	sync.Mutex
	lastSent map[string]time.Time
}{lastSent: make(map[string]time.Time)}

const (
	// OverloadStatusFiring 表示过载信号当前正在触发。
	OverloadStatusFiring = "firing"
	// OverloadStatusRecovered 表示先前触发的过载信号已经恢复。
	OverloadStatusRecovered = "recovered"
)

// OverloadEvent 描述一次实例过载拒绝事件。
// handler 在容量达到上限时构造该事件，alert 包负责按配置发送钉钉机器人通知。
type OverloadEvent struct {
	Provider       string
	UserID         string
	UserName       string
	RemoteAddr     string
	IPLocation     map[string]string
	ActiveSessions int64
	MaxSessions    int64
	Reason         string
	Status         string
}

// NotifyOverload 在实例达到容量上限时发送钉钉机器人告警。
// 未启用告警时直接返回 nil；启用但 webhook 缺失时返回错误，让调用方日志明确暴露配置问题。
func NotifyOverload(ctx context.Context, event OverloadEvent) error {
	// 先读配置并做基本校验：未启用直接返回；启用但 webhook 缺失时返回错误，让配置问题在日志中明确暴露。
	cfg := dingTalkConfig()
	if !cfg.enabled {
		return nil
	}
	if cfg.webhook == "" {
		err := fmt.Errorf("dingtalk webhook is empty")
		writeDingTalkAudit("dingtalk_failed", event, cfg, "failed", err)
		return err
	}
	// 冷却期内抑制发送但不报错，避免过载风暴高频刷屏钉钉。
	if suppressedByCooldown(event, cfg.cooldown) {
		writeDingTalkAudit("dingtalk_suppressed", event, cfg, "cooldown", nil)
		return nil
	}

	// 组装钉钉 text 消息体，@ 名单与全员通知取自配置。
	body, err := json.Marshal(dingTalkTextPayload{
		MsgType: "text",
		Text: dingTalkText{
			Content: formatOverloadMessage(event),
		},
		At: dingTalkAt{
			AtMobiles: cfg.atMobiles,
			IsAtAll:   cfg.isAtAll,
		},
	})
	if err != nil {
		writeDingTalkAudit("dingtalk_failed", event, cfg, "failed", err)
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, signedWebhook(cfg.webhook, cfg.secret), bytes.NewReader(body))
	if err != nil {
		writeDingTalkAudit("dingtalk_failed", event, cfg, "failed", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	// HTTP 发送失败或返回非 2xx 一律视为告警失败，钉钉侧业务错误码不解析，统一记日志返回错误。
	client := &http.Client{Timeout: cfg.timeout}
	resp, err := client.Do(req)
	if err != nil {
		writeDingTalkAudit("dingtalk_failed", event, cfg, "failed", err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := fmt.Errorf("dingtalk webhook status %d", resp.StatusCode)
		writeDingTalkAudit("dingtalk_failed", event, cfg, "failed", err, zap.Int("http_status", resp.StatusCode))
		return err
	}
	// 发送成功后才记统一资源统计与审计日志，保证指标口径与真实发送结果一致。
	eventStatus := overloadStatus(event)
	resourceKind := stats.ResourceKindAlertFiring
	if eventStatus == OverloadStatusRecovered {
		resourceKind = stats.ResourceKindAlertRecovered
	}
	stats.RecordResourceEvent(stats.ResourceEvent{
		Source:   stats.SourceSystem,
		Kind:     resourceKind,
		Provider: event.Provider,
		UserID:   event.UserID,
		UserName: event.UserName,
		Status:   "sent",
	})
	writeDingTalkAudit(resourceKind, event, cfg, eventStatus, nil)
	writeDingTalkAudit("dingtalk_sent", event, cfg, "sent", nil, zap.Int("http_status", resp.StatusCode))
	return nil
}

func writeDingTalkAudit(eventName string, event OverloadEvent, cfg dingTalkConfigValue, status string, err error, extra ...zap.Field) {
	if conf.Global == nil || strings.TrimSpace(conf.Global.Logs.RootDir) == "" {
		return
	}
	reason := strings.TrimSpace(event.Reason)
	if reason == "" {
		reason = "capacity_rejected"
	}
	// 审计字段含事件、状态、调用方元数据与脱敏后的 webhook（sha256 摘要），不落 token 明文。
	fields := []zap.Field{
		zap.String("event", eventName),
		zap.String("provider", strings.TrimSpace(event.Provider)),
		zap.String("reason", reason),
		zap.String("status", status),
		zap.String("alert_status", overloadStatus(event)),
		zap.String("user_id", event.UserID),
		zap.String("user_name", event.UserName),
		zap.String("remote_addr", event.RemoteAddr),
		zap.Any("ip_location", event.IPLocation),
		zap.Int64("active_sessions", event.ActiveSessions),
		zap.Int64("max_sessions", event.MaxSessions),
		zap.String("webhook", logger.RedactField("webhook", cfg.webhook)),
		zap.Int64("cooldown_seconds", int64(cfg.cooldown/time.Second)),
	}
	if err != nil {
		fields = append(fields, zap.String("error", err.Error()))
	}
	fields = append(fields, extra...)
	logger.GetModelLogger("global").Info("dingtalk alert audit", fields...)
}

func formatOverloadMessage(event OverloadEvent) string {
	provider := strings.TrimSpace(event.Provider)
	if provider == "" {
		provider = "unknown"
	}
	reason := strings.TrimSpace(event.Reason)
	if reason == "" {
		reason = "capacity_rejected"
	}
	title := "系统过载预警"
	if overloadStatus(event) == OverloadStatusRecovered {
		title = "系统过载恢复"
	}
	return fmt.Sprintf(
		"%s\nprovider: %s\nreason: %s\nactive/max: %d/%d\nuser_id: %s\nuser_name: %s\nremote_addr: %s\nip_location: %s\nip_location_source: %s\ntime: %s",
		title,
		provider,
		reason,
		event.ActiveSessions,
		event.MaxSessions,
		emptyAsDash(event.UserID),
		emptyAsDash(event.UserName),
		emptyAsDash(event.RemoteAddr),
		emptyAsDash(locationDisplay(event.IPLocation)),
		emptyAsDash(event.IPLocation["source"]),
		time.Now().Format(time.RFC3339),
	)
}

func overloadStatus(event OverloadEvent) string {
	// 只有显式标注 recovered 才视为恢复事件，其余（含空）一律按 firing 处理。
	status := strings.ToLower(strings.TrimSpace(event.Status))
	if status == OverloadStatusRecovered {
		return OverloadStatusRecovered
	}
	return OverloadStatusFiring
}

func locationDisplay(location map[string]string) string {
	if len(location) == 0 {
		return ""
	}
	if display := strings.TrimSpace(location["display"]); display != "" {
		return display
	}
	parts := make([]string, 0, 3)
	for _, key := range []string{"country", "region", "city"} {
		if value := strings.TrimSpace(location[key]); value != "" {
			parts = append(parts, value)
		}
	}
	if len(parts) == 0 {
		return strings.TrimSpace(location["status"])
	}
	return strings.Join(parts, " / ")
}

func suppressedByCooldown(event OverloadEvent, cooldown time.Duration) bool {
	if cooldown <= 0 {
		return false
	}
	// 冷却按"状态+provider+reason"维度隔离，避免不同来源的告警互相抑制。
	key := overloadStatus(event) + ":" + strings.TrimSpace(event.Provider) + ":" + strings.TrimSpace(event.Reason)
	if key == "::" {
		key = overloadStatus(event) + ":unknown:capacity_rejected"
	}

	// 进程内互斥保护冷却状态；状态不持久化，进程重启后冷却归零。
	now := time.Now()
	dingTalkState.Lock()
	defer dingTalkState.Unlock()
	if last := dingTalkState.lastSent[key]; !last.IsZero() && now.Sub(last) < cooldown {
		return true
	}
	dingTalkState.lastSent[key] = now
	return false
}

type dingTalkTextPayload struct {
	MsgType string       `json:"msgtype"`
	Text    dingTalkText `json:"text"`
	At      dingTalkAt   `json:"at,omitempty"`
}

type dingTalkText struct {
	Content string `json:"content"`
}

type dingTalkAt struct {
	AtMobiles []string `json:"atMobiles,omitempty"`
	IsAtAll   bool     `json:"isAtAll,omitempty"`
}

type dingTalkConfigValue struct {
	enabled   bool
	webhook   string
	secret    string
	cooldown  time.Duration
	timeout   time.Duration
	atMobiles []string
	isAtAll   bool
}

func dingTalkConfig() dingTalkConfigValue {
	if conf.Global == nil {
		return dingTalkConfigValue{}
	}
	raw := conf.Global.Alerts.DingTalk
	// 超时配置缺失或非正数时回退 3 秒，避免告警请求无限挂起阻塞调用方。
	timeout := time.Duration(raw.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	// 冷却时间为负值时按 0 处理（即禁用冷却），配置单位秒。
	cooldown := time.Duration(raw.CooldownSeconds) * time.Second
	if raw.CooldownSeconds < 0 {
		cooldown = 0
	}
	return dingTalkConfigValue{
		enabled:   raw.Enabled,
		webhook:   strings.TrimSpace(raw.Webhook),
		secret:    strings.TrimSpace(raw.Secret),
		cooldown:  cooldown,
		timeout:   timeout,
		atMobiles: append([]string(nil), raw.AtMobiles...),
		isAtAll:   raw.IsAtAll,
	}
}

// signedWebhook 按钉钉机器人签名规范给 webhook 追加 timestamp 与 sign 参数。
// secret 为空时不签名（兼容无加签的机器人）；非空时对 timestamp+"\n"+secret 做 hmac-sha256，
// base64 后 URL 转义，timestamp 为毫秒，与钉钉服务端校验口径一致。
func signedWebhook(webhook, secret string) string {
	if strings.TrimSpace(secret) == "" {
		return webhook
	}
	timestamp := fmt.Sprintf("%d", time.Now().UnixMilli())
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp + "\n" + secret))
	sign := url.QueryEscape(base64.StdEncoding.EncodeToString(mac.Sum(nil)))

	// webhook 已带查询参数时改用 & 追加，避免破坏原有参数。
	sep := "?"
	if strings.Contains(webhook, "?") {
		sep = "&"
	}
	return webhook + sep + "timestamp=" + timestamp + "&sign=" + sign
}

func emptyAsDash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "-"
	}
	return value
}

// ResetForTest 清空告警冷却状态，避免单测互相污染。
func ResetForTest() {
	dingTalkState.Lock()
	defer dingTalkState.Unlock()
	dingTalkState.lastSent = make(map[string]time.Time)
}
