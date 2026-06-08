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
	cfg := dingTalkConfig()
	if !cfg.enabled {
		return nil
	}
	if cfg.webhook == "" {
		err := fmt.Errorf("dingtalk webhook is empty")
		writeDingTalkAudit("dingtalk_failed", event, cfg, "failed", err)
		return err
	}
	if suppressedByCooldown(event, cfg.cooldown) {
		writeDingTalkAudit("dingtalk_suppressed", event, cfg, "cooldown", nil)
		return nil
	}

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
	key := overloadStatus(event) + ":" + strings.TrimSpace(event.Provider) + ":" + strings.TrimSpace(event.Reason)
	if key == "::" {
		key = overloadStatus(event) + ":unknown:capacity_rejected"
	}

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
	timeout := time.Duration(raw.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
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

func signedWebhook(webhook, secret string) string {
	if strings.TrimSpace(secret) == "" {
		return webhook
	}
	timestamp := fmt.Sprintf("%d", time.Now().UnixMilli())
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp + "\n" + secret))
	sign := url.QueryEscape(base64.StdEncoding.EncodeToString(mac.Sum(nil)))

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
