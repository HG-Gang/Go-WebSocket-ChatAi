package session

import (
	"sync/atomic"

	"TozoAI-Chat-Api/conf"
)

var activeSessions atomic.Int64

// TryAcquireCapacity 预占一个当前进程内的 WebSocket 会话名额。
// 单实例硬上限用于避免过载雪崩：实例达到配置容量后，新客户端会快速失败，
// 由负载均衡转发到其他节点。
func TryAcquireCapacity() bool {
	if conf.Global == nil || conf.Global.Capacity.MaxActiveSessions <= 0 {
		activeSessions.Add(1)
		return true
	}

	max := conf.Global.Capacity.MaxActiveSessions
	for {
		current := activeSessions.Load()
		if current >= max {
			return false
		}
		if activeSessions.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func ReleaseCapacity() {
	for {
		current := activeSessions.Load()
		if current <= 0 {
			return
		}
		if activeSessions.CompareAndSwap(current, current-1) {
			return
		}
	}
}

func ActiveCount() int64 {
	return activeSessions.Load()
}
