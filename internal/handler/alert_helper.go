package handler

import (
	"context"
	"time"

	"go.uber.org/zap"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/service/alert"
)

// notifyCapacityOverloadAlert 在实例容量拒绝新 WS 连接时触发钉钉告警。
// 这里使用短超时独立 context，避免客户端断开导致告警被取消，也避免 webhook 卡住拒绝路径太久。
func notifyCapacityOverloadAlert(log *zap.Logger, providerName, userID, userName, remoteAddr string, ipLocation map[string]string, activeSessions int64) {
	maxSessions := int64(0)
	if conf.Global != nil {
		maxSessions = conf.Global.Capacity.MaxActiveSessions
	}

	ctx, cancel := context.WithTimeout(context.Background(), alertTimeout())
	defer cancel()
	if err := alert.NotifyOverload(ctx, alert.OverloadEvent{
		Provider:       providerName,
		UserID:         userID,
		UserName:       userName,
		RemoteAddr:     remoteAddr,
		IPLocation:     ipLocation,
		ActiveSessions: activeSessions,
		MaxSessions:    maxSessions,
		Reason:         "capacity_rejected",
	}); err != nil && log != nil {
		log.Warn("发送钉钉过载告警失败", zap.String("provider", providerName), zap.Error(err))
	}
}

func alertTimeout() time.Duration {
	if conf.Global == nil || conf.Global.Alerts.DingTalk.TimeoutMs <= 0 {
		return 3 * time.Second
	}
	return time.Duration(conf.Global.Alerts.DingTalk.TimeoutMs) * time.Millisecond
}
