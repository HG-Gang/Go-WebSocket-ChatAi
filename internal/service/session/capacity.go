// internal/service/session/capacity.go
// 单实例 WebSocket 会话容量控制：
//   - 以进程内原子计数器跟踪活跃会话数
//   - 达到 conf.Global.Capacity.MaxActiveSessions 时拒绝新连接（快速失败），
//     由负载均衡把被拒客户端转发到其他节点
//   - 活跃数同时供监控指标与过载告警使用
//
// 明确不负责：跨实例分布式计数（各实例独立计容量）。
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
	// 未配置容量上限（<=0）视为不限制，直接占位成功。
	if conf.Global == nil || conf.Global.Capacity.MaxActiveSessions <= 0 {
		activeSessions.Add(1)
		return true
	}

	// CAS 循环避免并发抢占时超卖：当前值已达上限直接失败，否则原子 +1 后返回。
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

// ReleaseCapacity 归还一个会话名额，连接关闭时调用。
// 计数已为零时直接返回，防止并发重复释放把计数减为负数。
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

// ActiveCount 返回当前活跃会话数，供监控指标与过载告警使用。
func ActiveCount() int64 {
	return activeSessions.Load()
}
