// internal/handler/alert_helper.go
// 告警辅助：实例容量不足拒绝新 WS 连接时发送钉钉告警。
//
// 文件功能：
//   - notifyCapacityOverloadAlert：携带会话数、用户身份和 IP 所在地组装过载事件并发送告警。
//
// 安全边界：
//   - 告警发送失败只记录日志，不影响拒绝连接的主流程（拒绝本身是安全关闭，不能因告警失败放行）。
//   - 告警使用独立短超时 context，避免客户端断开取消告警，也避免 webhook 阻塞容量拒绝路径。
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

// alertTimeout 返回告警请求的超时时间，单位毫秒。
// 配置缺失或非法时回退到 3 秒，保证告警永远不会让主流程等待过久。
func alertTimeout() time.Duration {
	if conf.Global == nil || conf.Global.Alerts.DingTalk.TimeoutMs <= 0 {
		return 3 * time.Second
	}
	return time.Duration(conf.Global.Alerts.DingTalk.TimeoutMs) * time.Millisecond
}
