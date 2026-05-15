# TozoAI 调试指标与页面代码详细注释

## 说明

这份文档是源码旁路注释，目标是解释本次新增/调整的 `metrics`、调试接口和 Web 页面逻辑。没有把每一行都塞成 `//` 注释，是因为那会显著降低 Go/JS 可读性，并增加后续维护成本；源码中已补充关键注释，逐行级别的功能说明集中放在本文档。

## 1. `internal/service/metrics/metrics.go`

### 文件职责

`metrics.go` 是进程内实时指标收集器。它不替代 Redis，也不做跨实例聚合；它只记录当前 Go 进程中 App/WebSocket、Go 队列、OpenAI Realtime、业务用量和错误的最近状态，供 `/api/debug/status` 和 `web/diagnostics.html` 高频轮询。

### 并发模型

一个用户会话使用 4 条主要协程：

- `readPump`：读取 App/耳机消息，分类并写入 OpenAI 发送队列。
- `openAIWritePump`：管理写 OpenAI 的单写者模型和重连请求。
- `recvPump`：读取 OpenAI Realtime 事件，转换为网关标准响应。
- `writePump`：写回 App/耳机，并执行 Go->App WebSocket ping。

这 4 条协程都会调用 `metrics`，所以 `collector.mu` 保护所有指标写入。`Snapshot()` 返回时会克隆 map/slice，避免调试接口调用方修改内部状态。

### 常量

- `maxRecentSessions = 50`：最多保留 50 个最近 session。活跃 session 不会被裁剪，已结束 session 超过上限会删除最旧的。
- `maxSessionEvents = 120`：每个 session 最多保留 120 条事件，错误时间线也最多保留 120 条。
- `maxRecentResponses = 30`：每个 session 最多保留 30 个 response 摘要。
- `maxResponseTextChars = 64 * 1024`：单个 response 内存里最多保留 64KB 文本/转写，避免长对话撑爆内存。
- `maxSnapshotResponseChars = 8 * 1024`：接口返回时再截断为 8KB，避免诊断页轮询传输过大。

### 数据结构

- `collector`：总容器。字段包括全局启动时间、App 指标、Go 指标、OpenAI 指标、错误指标、业务指标、session 明细表。
- `appMetrics`：App/耳机/浏览器侧指标。
  - `ConnectionsTotal`：累计接入 App WebSocket 次数。
  - `DisconnectsTotal`：累计 session 结束次数。
  - `NormalDisconnects`：正常关闭、provider 返回、session close。
  - `AbnormalDisconnects`：除正常原因外的关闭。
  - `HeartbeatTimeouts`：心跳超时断链次数。
  - `MessagesTotal`：App 上行消息总数。
  - `TextMessages` / `BinaryMessages`：文本帧和二进制帧计数。
  - `BytesIn` / `BytesOut`：App 上行/下行字节数。
  - `PingSent` / `PongReceived`：Go->App ping 和 App->Go pong 次数。
  - `PongLatencyMs`：Pong 延迟 min/max/avg。
  - `SlowConsumerDrops`：App 下行队列满导致丢弃消息次数。
  - `JSONParseErrors`：App 文本帧 JSON 解析错误次数。
  - `MessageTypes`：按 App 事件类型统计，如 `session.update`、`response.create`、`binary_audio`。
  - `DisconnectReasons`：断链原因分布。
- `goMetrics`：Go 网关内部压力指标。
  - `CapacityRejected`：达到最大活跃连接后拒绝新连接次数。
  - `APISendQueueTimeouts`：App->OpenAI 队列入队超时次数。
  - `SendQueueTimeouts`：Go->App 队列满导致丢弃次数。
  - `LastSendQueueLen/Cap`：最近一次 App 下行队列长度/容量。
  - `LastAPISendQueueLen/Cap`：最近一次 OpenAI 上行队列长度/容量。
- `openAIMetrics`：OpenAI Realtime 上游指标。
  - `ConnectAttempts/Success/Failures`：OpenAI 建连尝试、成功、失败。
  - `ReconnectRequests/Success/Failures`：重连请求、成功、失败。
  - `SessionRestoreSuccess/Failure`：重连后恢复 session/history 成败。
  - `ClientEventsTotal`：Go 写给 OpenAI 的事件总数。
  - `ServerEventsTotal`：OpenAI 返回给 Go 的事件总数。
  - `ClientEventTypes/ServerEventTypes`：上下行事件类型分布。
  - `ResponseCreated/Done/Completed/Cancelled/Failed`：response 生命周期状态分布。
  - `TextDeltaChars/TranscriptDeltaChars`：文本/转写 delta 字符数。
  - `AudioDeltaBytes/Packets`：OpenAI 音频 delta 估算字节和包数。
  - `FirstDeltaLatencyMs`：从 `response.created` 到首个 text/audio/transcript delta 的延迟。
  - `ResponseLatencyMs`：从 `response.created` 到 `response.done` 的完整响应耗时。
  - `ReconnectReasons`：按原因统计重连，例如读超时、写失败。
- `errorMetrics`：错误总量、按 code、按 reason、最近错误明细。
- `businessMetrics`：业务用量。
  - `InputTokens/OutputTokens/TotalTokens`：OpenAI usage token。
  - `InputAudioMs/OutputAudioMs`：按 PCM16 24kHz 单声道估算的音频输入/输出时长。
  - `SessionDurationMs`：所有 session 在线时长累计。
  - `RateLimitRejected`：限流拒绝次数。
  - `BillingErrors`：计费记录失败次数。
  - `TokensByUser/TokensByModel/TokensByDay`：用户、模型、日期维度 token 统计。
- `sessionMetrics`：单个 session 的保留明细，包括用户、设备、来源、开始/结束、App 收发、OpenAI 上下行、队列、token、事件时间线、response 摘要。
- `eventRecord`：时间线单条记录，字段为时间、事件类型、详情、response_id、字节数、错误 code。
- `responseMetrics`：单个 OpenAI response 的创建时间、首包时间、完成时间、文本、转写、音频、token、延迟。

### 入口函数

- `SessionStarted(...)`：session 启动时调用。创建 `sessionMetrics`，累计连接数，并写入第一条 `session_started` 事件。
- `SessionEnded(...)`：session 结束时调用。防止重复结束，记录关闭原因、正常/异常断开、在线时长，并裁剪已结束 session。
- `AppDisconnectReason(...)`：`readPump` 检测到底层关闭时调用，用于记录更细的断链原因。
- `CapacityRejected()`：超过 `capacity.max_active_sessions` 拒绝连接时调用。
- `AppMessage(...)`：App 发来每个消息时调用。统计消息数、字节、文本/二进制、事件类型。
- `AppJSONParseError(...)`：App 文本帧不是合法 JSON 时调用。
- `AppPingSent(...)`：`writePump` 发送 Go->App ping 时调用，同时保存 ping 时间。
- `AppPongReceived(...)`：App 回 pong 时调用，并计算和上次 ping 的延迟。
- `AppWrite(...)`：Go 写回 App 成功后调用，统计下行字节。
- `SlowConsumerDrop(...)`：App 下行队列满时调用，标记慢消费者。
- `QueueDepth(...)`：队列入队前后调用，记录 sendChan/apiSendChan 最近长度。
- `APISendQueueTimeout(...)`：App->OpenAI 队列入队超时时调用。
- `OpenAIConnectAttempt(...)`：拨 OpenAI Realtime 前调用。
- `OpenAIConnectResult(...)`：拨号完成后调用，区分成功或失败。
- `OpenAIClientEvent(...)`：Go 写给 OpenAI 的原生事件成功入队/转发时调用。
- `OpenAIWriteError(...)`：写 OpenAI WebSocket 失败时调用。
- `OpenAIServerEvent(...)`：OpenAI 返回一个服务端事件时调用。
- `OpenAIResponseCreated(...)`：收到 `response.created` 时调用，开始响应计时。
- `OpenAITextDelta(...)`：收到文本 delta 时调用，累加字符并保存最近文本。
- `OpenAITranscriptDelta(...)`：收到转写 delta 时调用。
- `OpenAIAudioDelta(...)`：收到音频 delta 时调用，估算 base64 解码后字节和播放时长。
- `InputAudio(...)`：App 发送 input audio append 时调用，估算输入音频时长。
- `OpenAIResponseDone(...)`：收到 `response.done` 时调用，收口状态、token、首包延迟、完整响应耗时。
- `OpenAIError(...)`：OpenAI 返回 error 事件时调用。
- `ReconnectRequested(...)`：读/写 OpenAI 失败后请求重连时调用。
- `ReconnectResult(...)`：重连完成后调用，记录成功或失败。
- `SessionRestore(...)`：重连后恢复 `session.update` 和历史上下文时调用。
- `RateLimitRejected(...)`：限流中间件拒绝请求时调用。
- `BillingError(...)`：记录 token usage 到 billing 失败时调用。
- `Snapshot()`：唯一对外读接口，返回 JSON-ready map 给 `/api/debug/status`。

### 内部工具函数

- `snapshotSessionLocked(...)`：把 `sessionMetrics` 转成接口返回字段。
- `addSessionEventLocked(...)`：追加 session 事件，并裁剪到 120 条。
- `recordErrorLocked(...)`：写全局错误，也同步写入指定 session 的事件线。
- `pruneEndedSessionsLocked()`：只裁剪已结束 session，防止压测时无限增长。
- `ensureResponseLocked(...)`：按 response_id 查找或创建 response 记录。
- `markFirstDeltaLocked(...)`：记录首个 delta 的时间。
- `updateLatency(...)`：用在线算法更新 min/max/avg，不保存所有采样。
- `incMap(...)` / `incMapBy(...)`：按字符串 key 自增。
- `cloneMap(...)`：Snapshot 时复制 map，避免暴露内部状态。
- `appendLimited(...)`：流式拼接文本并限制最大长度。
- `truncateString(...)`：接口输出时截断超长文本。
- `estimateBase64DecodedBytes(...)`：按 base64 长度估算解码字节。
- `estimatePCM16Ms(...)`：按 24kHz/16bit/mono 估算音频毫秒。
- `sessionDurationSeconds(...)`：活跃 session 用当前时间，关闭 session 用结束时间。
- `timeString(...)`：零时间输出空字符串。
- `errorString(...)`：安全地把 error 转字符串。
- `maxInt(...)`：把负数钳制为最小值，避免转 uint64 溢出。

## 2. `internal/handler/debug_handler.go`

- `DebugStatusHandler`：HTTP 入口，返回 `/api/debug/status`。
- `buildDebugServerStatus`：输出环境、监听地址、Go 版本、CPU、goroutine、运行时间。
- `buildDebugMemoryStatus`：输出 alloc/heap/sys/stack/GC 等 runtime 内存指标。
- `buildDebugCapacityStatus`：输出当前活跃 session、最大 session、使用率。
- `buildDebugFeatureStatus`：输出 JWT、限流、计费、Fallback、Redis 开关。
- `buildDebugRedisStatus`：Redis 未启用或 client nil 时返回明确状态；可用时输出 ping 和连接池。
- `buildDebugNetworkStatus`：输出当前进程可见代理，并标记 OpenAI wss/https 实际会使用的代理。
- `buildDebugOpenAIStatus`：输出模型、endpoint、voice、WS URL、心跳、超时、重连、恢复策略。
- `metrics.Snapshot()`：把 `metrics.go` 的进程内指标挂进 `data.metrics`。

## 3. `internal/handler/redis_handler.go`

- `RedisKeyInfo`：Redis 页面每一行的数据模型，包含 key、类型、TTL、分类、说明、完整值。
- `RedisKeysHandler`：处理 `/api/redis/keys`。
  - `pattern`：SCAN 匹配模式。
  - `cursor`：Redis SCAN 游标。
  - `count`：单次扫描数量，上限 1000。
  - `full=1`：list/zset 拉完整内容，否则只拉前 100 条。
  - Redis nil：返回 503，避免空指针。
  - string/hash/list/set/zset：分别用 Redis 原生命令取值。
- `explainRedisKey`：按 key 名判断用途：
  - `session`：Go WebSocket 会话元数据。
  - `billing`：token、音频、用量、计费。
  - `rate_limit`：限流窗口计数。
  - `openai`：OpenAI/Reatime 会话或上下文恢复。
  - `auth`：JWT/token 相关。
  - `other`：未知业务 key，需要结合 TTL/value 判断。

## 4. `internal/service/session/manager.go`

- `Session` 新增 `DeviceID`、`RemoteAddr`、`UserAgent`，用于定位单设备、单来源断链。
- `NewSession` 接收这些元数据，并写入 session 对象。
- `Start`：
  - 调用 `metrics.SessionStarted`。
  - 把 session 元数据写入 Redis hash。
  - 把 logger 和 session context 传给 Provider。
  - 阻塞运行 `Provider.HandleWS`。
  - Provider 返回后调用 `metrics.SessionEnded`。
- `Close`：
  - 关闭 App WebSocket。
  - 关闭 Provider。
  - 更新 Redis session 状态。
  - 再次调用 `metrics.SessionEnded`，该函数内部会防重复。

## 5. `internal/handler/openai_handler.go`

- `OpenAIRealtimeHandler`：
  - 从 JWT 中取 `user_id`。
  - 生成 request_id。
  - 读取 `X-Device-ID` 或 `device_id` 查询参数。
  - 读取来源 IP 和 User-Agent。
  - 容量拒绝时调用 `metrics.CapacityRejected`。
  - 升级 WebSocket 后创建 session。
- `OpenAIFallbackHandler`：保留 HTTP 降级能力。

## 6. `internal/provider/openai/client_ws.go`

- `Connect`：统计 OpenAI 拨号尝试、成功、失败。
- `readPump`：读取 App 消息，统计 App 消息类型、JSON 错误、断链原因。
- `writePump`：写回 App，统计下行字节、Go->App ping、App pong。
- `openAIWritePump`：单写者写 OpenAI，并处理重连请求。
- `recvPump`：读取 OpenAI，统计服务端事件、错误、超时和重连。
- `forwardClientEvent`：统计 Go->OpenAI 事件和输入音频。
- `handleOpenAIMessageGateway`：统计 OpenAI->Go 事件、response 生命周期、delta、token、错误。
- `safeSend`：App 下行队列满时记录慢消费者。
- `restoreRealtimeState` / `reconnect`：统计 session 恢复成败。

## 7. `web/index.html`

- 顶部新增颜色模式选择框：深色、浅色、海洋、护眼、强对比。
- 链路统计中的 `Session 事件` 数字支持 hover 摘要，旁边按钮打开明细弹框。
- 消息发送区域新增“用户文本输入”，快捷文本发送不再写死。
- JSON textarea 和实时日志区域增高，便于一次看到更多内容。
- 新增“OpenAI 完整响应”面板：
  - response 下拉列表。
  - 状态/耗时/text/transcript/audio 元信息。
  - 聚合最终内容。
  - 最终 response.done/end 原始 JSON。

## 8. `web/ws-test.js`

- `createEventStats`：保存事件统计、Session 时间线、response 聚合 map。
- `connect`：连接成功后重置统计，并记录 `app_ws_open`。
- `onmessage`：
  - 识别 Go 标准包装和 OpenAI 原始事件。
  - session.created/updated/restored 进入 Session 时间线。
  - response.created 创建完整响应记录。
  - text/transcript/audio delta 持续拼接。
  - response.done/end 收口完整响应。
- `recordSessionEvent`：维护当前页面 Session 事件明细。
- `ensureResponseRecord`：确保 response_id 有聚合记录，缺失时生成 `local-*`。
- `finalizeResponseRecord`：把最终 JSON 内容和流式聚合内容合并。
- `updateCompleteResponseUI`：渲染完整响应面板。
- `initTheme/applyTheme`：保存并应用颜色模式。

## 9. `web/style.css`

- `body.theme-light`、`theme-ocean`、`theme-sepia`、`theme-contrast`：不同颜色变量。
- `.theme-switcher`：顶部颜色模式选择器。
- `#msg-body`：增高 JSON 输入框。
- `.log-container`：增高实时日志区域。
- `.response-output`：完整响应展示区域。
- `.modal` / `.modal-content` / `.session-event-list`：Session 事件弹框。

## 10. `web/redis.html`

- 请求 `/api/redis/keys?pattern=*&count=1000&full=1`。
- 表格新增 `category` 和 `description`。
- `value` 不再截断到 2000 字符，便于完整查看每个 key 内容。
- 分类统计优先使用后端返回的 `category`，比单纯字符串匹配更清晰。

## 11. `web/diagnostics.html`

- `refreshAll`：同时拉 `/api/debug/status`、`/health`、`/api/redis/keys?full=1`。
- `renderDebug`：渲染 Go/Redis/OpenAI/网络代理/配置状态。
- `renderMetrics`：渲染 `metrics.Snapshot()` 的 App、Go、OpenAI、业务、错误、容量指标。
- `renderRecentSessions`：渲染最近 session 的服务端真实事件和 response 摘要。
- `summarizeEvents`：只展示最近 5 个事件，避免表格过长。
- `summarizeResponses`：展示 response 文本/转写/音频包和耗时。
- `renderRedisKeys`：按 Redis 后端分类统计 key。

## 12. `conf/loader.go`

- 保留 `conf/models/*.yaml` 的模型专属配置加载。
- 增加 root-level model override 保护：
  - `conf/config.yaml` 中的 `models.openai.api_key/default_model/endpoint` 是更高优先级覆盖。
  - `conf/models/openai.yaml` 中旧的 `default_model` 不再覆盖根配置期望。
- 这样修复了测试中 `DefaultModel` 应读取 `gpt-realtime-2` 的问题，同时保留模型文件里的 Realtime 心跳/重连配置。

## 13. 运行和验证

已验证：

- `gofmt`
- `go test ./...`
- `node --check web/ws-test.js`
- `web/diagnostics.html`、`web/audio.html`、`web/redis.html` 内联脚本语法检查

本地服务入口：

- `http://127.0.0.1:8096/web/`
- `http://127.0.0.1:8096/web/diagnostics.html`
- `http://127.0.0.1:8096/web/redis.html`
