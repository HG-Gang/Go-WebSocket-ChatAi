# TozoAI-Chat-Api 重构与稳定性优化分析

日期：2026-05-14

## 目标

当前项目承担 App/耳机到 OpenAI Realtime 的实时 WebSocket 转发职责。本次优化重点不是把单个 Go 进程改成能独立承载百万连接，而是把服务改造成可水平扩展、可准入控制、可恢复、可观测、可压测的长连接基础形态。

百万并发必须由多实例集群、四层/七层负载均衡、Redis/消息队列/观测系统、OpenAI 账号配额和容量压测共同完成。单进程无限接入会导致文件描述符、内存、网络带宽、Redis 连接池和上游 API 配额同时崩溃。

## 核心链路

优化后的主链路：

1. App/耳机通过 `/ws/realtime/openai` 建立 WebSocket。
2. Handler 做 JWT 鉴权、限流、单实例活跃会话准入控制。
3. Session 记录 Redis 会话元数据，并把 user_id/session_id 注入 Provider。
4. OpenAI Provider 建立服务端到 OpenAI Realtime 的 WebSocket。
5. Client 使用四协程模型：
   - `readPump`: App -> Go，唯一 App 读者，负责旧 `msgType` 协议转换，并把 OpenAI client event 投递到上游写队列。
   - `openAIWritePump`: Go -> OpenAI，唯一 OpenAI 写者，负责 response 状态机、上游写入、写失败重连、重连后 session/history 恢复。
   - `recvPump`: OpenAI -> Go，唯一 OpenAI 读者，负责读超时、流式响应处理、response 状态推进，并把重连请求交给 `openAIWritePump`。
   - `writePump`: Go -> App，唯一 App 写者，负责 App Ping 和音频/text 下行推送。

旧 PHP Gateway Events 迁移细节见 `GATEWAY_EVENTS_MIGRATION_2026-05-14.md`。

## 代码改进明细

### 1. 配置加载修复

修改文件：

- `conf/loader.go`
- `conf/config.go`
- `conf/config.yaml`
- `conf/models/openai.yaml`
- `conf/loader_test.go`

旧问题：

`conf/models/openai.yaml` 是一个单模型根配置，但旧 loader 直接 `MergeInConfig()` 到全局根节点。这样很容易导致 `default_model`、`realtime` 等字段没有覆盖到 `Global.Models["openai"]`。

新逻辑：

- 先加载 `conf/config.yaml` 和环境配置。
- 解码到 `GlobalConfig`。
- 再单独读取 `conf/models/*.yaml`。
- 文件名作为模型名，比如 `openai.yaml` 写入 `Global.Models["openai"]`。
- 通过测试验证模型文件能覆盖 `default_model`，同时保留主配置里的 `endpoint`。

### 2. OpenAI Realtime 协议兼容

修改文件：

- `pkg/protocol/openai/server_events.go`
- `pkg/protocol/openai/server_events_test.go`
- `internal/provider/openai/client_ws.go`
- `internal/provider/openai/events_server.go`

旧问题：

代码只识别旧预览事件名：

- `response.text.delta`
- `response.audio.delta`
- `response.audio_transcript.delta`

新逻辑：

新增当前 Realtime 事件名，并保留旧事件名兼容：

- `response.output_text.delta`
- `response.output_audio.delta`
- `response.output_audio_transcript.delta`
- `response.output_text.done`
- `response.output_audio.done`
- `response.output_audio_transcript.done`

这样新模型返回当前协议事件时，Go 服务能正确包装成 App 侧统一事件；旧客户端或旧模型仍可继续透传/解析。

### 3. OpenAI 上游心跳与半开连接检测

修改文件：

- `internal/provider/openai/client_ws.go`
- `internal/provider/openai/config.go`
- `conf/config.yaml`
- `conf/models/openai.yaml`

旧问题：

只有 `recvPump` 设置读超时。如果 TCP 半开、NAT 映射失效、代理静默丢包，服务可能需要较长时间才发现连接不可用。

新逻辑：

- `recvPump` 独占 OpenAI 读取，按 `api_read_timeout/api_pong_timeout` 设置读 deadline。
- `openAIWritePump` 独占 OpenAI 写入；App 读协程不直接写上游，避免 OpenAI 写阻塞拖住 App 心跳读取。
- OpenAI 任意消息会刷新读 deadline。
- 写 OpenAI 前设置 `api_write_timeout`，避免写操作无限阻塞。
- `recvPump` 发现读异常后通过 `apiReconnectChan` 请求 `openAIWritePump` 执行重连和恢复，保证重连恢复写入不和普通上游写入并发。
- `reconnMu` 串行化上游重连，避免重复重建连接。
- 不再单独启动 `apiPingPump` 长期协程；上游半开连接主要通过读超时与写超时发现。

新增配置：

- `api_ping_interval`
- `api_pong_timeout`
- `api_write_timeout`

### 4. OpenAI 重连后的最小会话恢复

修改文件：

- `internal/provider/openai/client_ws.go`
- `internal/provider/openai/client_ws_test.go`
- `pkg/response/response.go`

旧问题：

上游 OpenAI 断线后，只是重新建立 WebSocket。新的 Realtime 连接没有旧 session、历史 item、上下文状态，长聊容易出现“重连成功但模型失忆”。

新逻辑：

- `replayState` 缓存可安全恢复的事件：
  - `session.update`
  - `conversation.item.create`
  - `conversation.item.truncate`
  - `conversation.item.delete`
- 不缓存这些高风险事件：
  - `input_audio_buffer.append`
  - `input_audio_buffer.commit`
  - `response.create`
  - `response.cancel`
- 重连成功后自动重放 session 和最近上下文。
- 恢复完成后向 App 发送 `session_restored`。

设计理由：

音频 buffer 和 response.create 不能盲目重放，否则可能重复用户语音、重复生成响应，或触发 active response 冲突。

### 5. App 下行背压控制

修改文件：

- `internal/provider/openai/client_ws.go`
- `internal/provider/openai/config.go`

旧问题：

`sendChan` 满时立即丢消息。实时音频/文本增量如果被直接丢弃，会造成音频断裂或文本缺字。

新逻辑：

- `safeSend` 在队列满时短暂等待 `send_queue_timeout_ms`。
- 若 App 仍无法消费，则丢弃并打 warn 日志。

这不是最终的流控闭环，但比立即丢弃更能吸收瞬时抖动。后续应增加按连接统计的慢客户端断开策略。

### 6. 单实例会话准入控制

修改文件：

- `internal/service/session/capacity.go`
- `internal/handler/openai_handler.go`
- `cmd/server/main.go`
- `conf/config.yaml`

旧问题：

服务没有单实例活跃 WS 上限。压测或流量突增时会一直接入，直到资源耗尽。

新逻辑：

- 新增 `capacity.max_active_sessions`。
- Handler 升级 WebSocket 前调用 `TryAcquireCapacity()`。
- 超过上限返回 `503 server overloaded, retry another node`。
- `/health` 返回当前 `active_sessions`。

这使服务可以配合负载均衡做水平扩展和故障迁移。

### 7. Redis 连接池与限流内存回收

修改文件：

- `internal/service/redis/redis.go`
- `internal/middleware/rate.go`
- `conf/config.yaml`

旧问题：

- Redis 连接池固定为 10，不适合高并发。
- 本地限流器按 user/model/path 生成后不回收，高基数用户会导致内存长期增长。

新逻辑：

- Redis 支持 `pool_size` 和 `min_idle_conns` 配置。
- 本地限流器增加 `lastSeen`。
- 每分钟清理 10 分钟未访问的本地 limiter。
- Redis 限流使用请求上下文，客户端断开后 Redis 操作能更快取消。

### 8. Token 用量记录

修改文件：

- `internal/provider/provider.go`
- `internal/service/session/manager.go`
- `internal/provider/openai/client_ws.go`

旧问题：

`billing.GetSessionUsage()` 会在 Session 关闭时读取用量，但 Realtime `response.done` 的 usage 没有实际写入 billing。

新逻辑：

- 新增 `SessionContextProvider` 可选接口。
- Session 启动时向 Provider 注入 user_id/session_id。
- OpenAI `response.done` 带 usage 时写入 billing。

## 当前仍需补齐的生产能力

### 1. 集群层

百万并发必须拆分为多实例：

- L4/L7 负载均衡。
- WebSocket sticky routing 或 session routing。
- 每实例容量上限和自动摘除。
- 跨实例会话状态恢复策略。

### 2. OpenAI 账号与上游配额

每个 App 会话对应一个 OpenAI Realtime 上游连接时，上游并发和成本会成为第一瓶颈。必须确认：

- OpenAI 组织级连接并发限制。
- RPM/TPM/音频 token 限制。
- 峰值成本。
- 是否需要分层模型，例如 `gpt-realtime-mini` 承接低成本场景。

### 3. 慢客户端治理

当前已做下行短等待，但还应增加：

- 每连接丢包计数。
- 连续下行阻塞超过阈值主动断开。
- App 侧 ACK 或低水位通知。
- 音频 delta 优先级队列。

### 4. 压测体系

需要单独建设：

- WebSocket 长连接压测工具。
- 音频二进制包模拟。
- OpenAI mock server。
- 断网、半开连接、上游 429、上游 close、慢客户端场景。
- p50/p95/p99 延迟和 goroutine/FD/内存曲线。

### 5. 观测体系

建议补充：

- Prometheus 指标。
- 活跃连接数、重连次数、恢复成功率。
- sendChan 满次数。
- OpenAI 事件延迟。
- 每用户/设备错误率。
- billing 写入失败率。

## 验证

已执行：

```powershell
$env:GOCACHE='D:\Software\PhpProject\Go\Code\TozoAI-Chat-Api\.tmp\go-build'; go test ./...
```

结果：所有包通过。

新增测试覆盖：

- `conf/loader_test.go`: 模型配置覆盖和环境变量替换。
- `pkg/protocol/openai/server_events_test.go`: 当前 Realtime 事件名和旧预览事件名兼容。
- `internal/provider/openai/client_ws_test.go`: 重连恢复缓存只保存安全可重放事件。
