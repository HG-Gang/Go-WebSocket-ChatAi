# Web 调试页面整理与增强说明

## 目标

`web/` 目录下页面定位为本地/测试环境诊断工具，核心目标是把 App、Go 网关、Redis、OpenAI Realtime 四段链路的信息集中展示出来，减少长连接异常、OpenAI 无响应、反复断链时只能翻日志的问题。

> 注意：这些页面和 `/api/debug/status`、`/api/redis/keys` 会暴露运行状态和 Redis key 摘要，生产环境应加鉴权或仅内网开放。

## 本次新增/调整

### 后端诊断接口

新增 `GET /api/debug/status`，只读输出以下信息，不输出 API Key、JWT Secret、Redis Password：

- `server`：环境、监听地址、运行时间、Go 版本、OS/Arch、CPU 数、goroutine 数。
- `memory`：Alloc、Heap、Sys、Stack、GC 次数、GC 暂停。
- `capacity`：当前活跃 WebSocket 会话、最大会话限制、使用率。
- `features`：JWT、限流、计费、Fallback、Redis 开关。
- `redis`：Redis 是否启用、地址、DB、Ping 耗时、连接池 hits/misses/timeouts/total/idle/stale。
- `network`：当前 Go 进程可见的 `HTTP_PROXY`、`HTTPS_PROXY`、`ALL_PROXY`、`NO_PROXY`，以及 OpenAI `wss/https` 连接实际依赖的代理状态。
- `openai`：OpenAI Realtime 是否启用、默认模型、endpoint、voice、API Key 是否配置、WS URL。
- `openai.realtime`：App 心跳、OpenAI 读写超时、重连次数/延迟、会话恢复、历史恢复条数、发送队列超时。
- `metrics`：App 连接/断链/心跳/消息、Go 队列/容量、OpenAI 连接/事件/流式/重连、业务 token/audio、错误、最近会话事件与响应摘要。
- `routes`：当前调试相关路由清单。

同时增强 `/api/redis/keys`：Redis 未启用时返回 `503`，支持 `full=1` 拉取完整 key 内容，并为每个 key 返回 `category` 和 `description` 解释用途。

## 页面能力清单

### 1. `web/index.html` / `web/ws-test.js`：WS 消息测试

当前可显示：

- WebSocket 连接状态、会话 ID、连接时长。
- 最后 Ping / Pong、收到消息数、发送消息数、重连次数。
- Go `/health` 状态、Go 活跃会话、Go 时间。
- OpenAI 下行事件：`session_created`、`session_updated`、`begin`、`end`、`text_delta`、`audio_delta`、`transcript_text_delta`、`error`、`reconnect_required`。
- 最近事件、最近 `response_id`。
- session 事件数、响应开始/结束次数、错误/重连要求次数。
- 文本字符数、转写字符数、音频 delta 包数、音频 payload 近似大小。
- 最近响应耗时、平均响应耗时。
- 当前浏览器会话经历的 Session 事件明细：WS 打开/关闭/异常、发送消息、OpenAI session/response 事件，可通过按钮弹窗查看，也可在统计数字 hover 查看最近摘要。
- OpenAI 完整响应：展示流式聚合后的最终内容，并保留最终 `response.done/end` 原始 JSON。
- 原始 JSON 日志、可导出日志。
- 多颜色模式：深色、浅色、海洋、护眼、强对比。

当前可操作：

- 自动生成 JWT token。
- 连接/断开/手动重连 `/ws/realtime/openai`。
- 发送 OpenAI 原生事件：`session.update`、`response.create`、`input_audio_buffer.append`、`input_audio_buffer.commit`、`conversation.item.create`。
- 在文本输入框中自定义快捷问题，并发送 `conversation.item.create + response.create`。
- 应用层心跳：已改为当前 Go 网关支持的 `{"type":"ping"}`，服务端回 `{"type":"pong"}`。

适合定位：

- App→Go 是否成功接入。
- Go 是否能转发 OpenAI Realtime 原生事件。
- 长连接是否出现心跳超时。
- OpenAI response 是否重复 create、是否 begin 后无 end。
- OpenAI 上游断开后 Go 是否发出 `reconnect_required`。

### 2. `web/audio.html`：语音对话测试

当前可显示：

- AudioContext 是否就绪、麦克风是否验证、WS 是否连接、session 是否创建、播放是否进行。
- 输入/输出采样率、处理器类型、发送音频帧数、接收音频包数、播放队列长度。
- 麦克风音量、波形。
- 用户转写、AI 文本/转写增量、音频 delta 播放、错误日志。

当前可操作：

- 初始化浏览器音频上下文。
- 测试麦克风输入。
- 连接/断开 WebSocket。
- 自动发送 `session.update`。
- 按住录音，松开提交音频。
- 发送文本消息触发 OpenAI 回复。

本次兼容增强：

- 支持 Go 标准包装中 `content` 直接为字符串的 `text_delta` / `transcript_text_delta` / `audio_delta`。
- `session_created` 支持读取 `content.session.id`。
- 增加跳转诊断看板入口。

### 3. `web/redis.html`：Redis 监控

当前可显示：

- Redis key、类型、TTL、值预览。
- 每个 key 的完整值（`full=1`）、用途分类、用途解释。
- Key 总数、String 数、Hash 数、List/Set/ZSet/其他数。
- 最后刷新时间。
- Session Key、Billing/Usage、Rate Limit、OpenAI/Realtime 分类数量。

当前可操作：

- 按 pattern 扫描 key。
- 手动刷新。
- 3/5/10/30 秒自动刷新。

适合定位：

- session 是否写入 Redis。
- 计费/用量 key 是否增长。
- 限流 key 是否异常堆积。
- OpenAI/Realtime 相关 key 是否存在 TTL 异常。

### 4. `web/diagnostics.html`：统一诊断看板

当前可显示：

- Web 调试页能力清单。
- Go 运行状态：健康状态、运行时间、Go 版本、goroutine、CPU、活跃会话、容量使用率。
- 内存/GC：Alloc、Heap、Sys、GC 次数、GC 暂停。
- 功能开关：JWT、Rate Limit、Billing、Fallback。
- 网络代理：OpenAI 代理是否启用、HTTP_PROXY、HTTPS_PROXY、NO_PROXY。
- Redis：可用性、地址/DB、Ping、错误、连接池总连接/空闲/陈旧、hits/misses、wait/timeouts。
- OpenAI Realtime：模型、WS URL、endpoint、voice、API Key 是否配置、4 协程链路、App 心跳、OpenAI 超时、重连、恢复策略。
- Redis key 分类统计：Session、Billing、Rate Limit、OpenAI/Realtime、永久 key、带过期 key、其他。
- 链路统计：App 连接/消息/字节/心跳，Go 队列/压力，OpenAI 连接/重连/事件/响应/流式/延迟，业务 token/audio，错误，goroutine/连接与内存/连接。
- 最近会话明细：Session、用户/设备、状态、App 收发、OpenAI 上下行、4 协程模型、队列、最近事件、完整响应摘要。
- 原始诊断 JSON。
- 已实现统计说明清单。

当前可操作：

- 手动刷新。
- 关闭/开启 3/5/10 秒自动刷新。

## 已实现的服务端指标

当前版本已增加进程内聚合指标并输出到 `/api/debug/status`，用于本地/测试环境实时诊断。生产环境如需长期留存和跨实例聚合，建议后续再把同一批指标接入 Prometheus/日志平台。

- App 维度：用户 ID、设备 ID、连接来源、在线时长、主动断开/异常断开原因。
- App→Go：消息类型分布、音频帧大小、上行/下行字节、JSON 解析错误、旧 `msgType` 分类。
- Go 会话：每会话 4 协程模型标记、`sendChan` 长度、`apiSendChan` 长度、慢消费者次数。
- Go 心跳：Ping 发送次数、Pong 次数、Pong 延迟、心跳超时次数、断链原因分布。
- OpenAI 事件：`session.created/updated`、`response.create/cancel/done` 状态分布。
- OpenAI 流式：text delta 字符、transcript delta 字符、audio delta 包数/字节、首包延迟、完整响应耗时。
- OpenAI 重连：重连原因、重试次数、恢复 session 成功率、恢复 history 条数。
- 错误：OpenAI `error.code`、网络 read/write timeout、鉴权失败、限流拒绝、Redis/账单错误。
- 容量：实例活跃连接、goroutine/连接比例、内存/连接比例、GC 暂停。
- 业务：token 使用量、语音输入/输出时长、用户/模型/日期维度用量、账单异常。

## 页面入口

- `/web/`：WS 消息测试。
- `/web/audio.html`：语音对话测试。
- `/web/redis.html`：Redis 监控。
- `/web/diagnostics.html`：统一诊断看板。
