# TozoAI WebSocket 压测工具

`tools/wsload` 是 Realtime WebSocket 的轻量压测工具，用来生成可复现的连接数、消息数、延迟、错误分布和 close code 报告。它只能证明本次命令实际覆盖到的规模，不能单独证明“百万并发”或“1 秒稳定响应”。

## 参数

```powershell
$env:GOCACHE = (Join-Path (Get-Location) '.tmp\go-build')
go run ./tools/wsload -h
```

常用参数：

- `-url`：WebSocket 地址，默认 `ws://127.0.0.1:8096/ws/realtime/openai`。
- `-users`：并发连接数，必须大于 0。
- `-ramp`：连接爬坡时间，例如 `30s`。多个用户会在该时间窗口内逐步连接。
- `-duration`：压测持续时间，例如 `5m`。
- `-token`：可选 JWT。非空时写入 `Authorization: Bearer <token>`。
- `-message`：连接成功后发送的文本消息，支持 `{user}` 占位符。
- `-debug-url`：可选 Go 诊断接口，例如 `http://127.0.0.1:8096/api/debug/status`。
- `-report`：可选 JSON 报告输出路径。

## 示例

开发环境小规模验证：

```powershell
$env:GOCACHE = (Join-Path (Get-Location) '.tmp\go-build')
go run ./tools/wsload `
  -url ws://127.0.0.1:8096/ws/realtime/openai `
  -users 10 `
  -ramp 10s `
  -duration 1m `
  -token "<jwt>" `
  -message "ping-{user}" `
  -debug-url http://127.0.0.1:8096/api/debug/status `
  -report .tmp\wsload-report.json
```

如果只是检查工具参数，不会发起连接：

```powershell
$env:GOCACHE = (Join-Path (Get-Location) '.tmp\go-build')
go run ./tools/wsload -h
```

## 报告字段

报告是 JSON，核心字段包括：

- `config`：本次压测参数快照。
- `started_at` / `finished_at`：压测起止时间。
- `summary.connect_success` / `summary.connect_failed`：连接成功和失败数。
- `summary.messages_sent` / `summary.messages_recv`：已发送和已收到消息数。
- `summary.messages_per_sec`：按运行时间计算的发送速率。
- `summary.error_rate`：连接失败数除以总连接尝试数。
- `latency.connect_p50_ms` / `connect_p95_ms` / `connect_p99_ms`：WebSocket 握手延迟。
- `latency.first_byte_*`：发送首条消息到收到首条响应的延迟。
- `latency.complete_*`：当前实现按单条消息响应完成耗时统计。
- `close_codes`：WebSocket close code 聚合。
- `errors`：连接、读写、诊断采样等错误文本聚合。
- `debug_snapshots`：配置 `-debug-url` 时采样到的诊断 JSON。
- `capacity_conclusion`：固定容量边界声明，避免把小规模压测误写成百万并发证明。

## 验证命令

目标测试覆盖参数解析、百分位算法、报告结构、close code 聚合和本地 WebSocket echo 压测路径：

```powershell
$env:GOCACHE = (Join-Path (Get-Location) '.tmp\go-build')
go test ./tools/wsload -run "Config|Latency|Report|CloseCode|Percentile|Echo" -count=1
```

全量回归：

```powershell
$env:GOCACHE = (Join-Path (Get-Location) '.tmp\go-build')
go test ./... -count=1
```

## 生产容量边界

`wsload` 是压测工具，不是容量承诺。要声明生产百万并发，需要额外提供：

- 多实例拓扑、实例规格、区域和负载均衡长连接策略。
- 每实例连接上限、CPU、内存、goroutine、FD/socket 和 GC 曲线。
- Redis Cluster 容量、连接池、QPS、慢查询和故障恢复数据。
- OpenAI 或第三方中转的 Realtime 并发、RPM/TPM、音频 token 和限流配额证明。
- P50/P95/P99 握手、首包、首音频和完整响应延迟。
- 错误率、close code、断链原因、容量拒绝和告警记录。
- 成本估算和峰值流量下的降级策略。

没有这些材料时，只能说“完成了某次指定规模的压测”，不能说“已经达到百万并发”。
