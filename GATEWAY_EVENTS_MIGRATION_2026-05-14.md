# 旧 PHP Gateway Events 迁移与 Go 四协程重构说明

日期：2026-05-14

## 背景

旧项目核心文件位于：

- `D:\Software\PhpProject\TOZO\chatgpt-websocket-php74\plugin\webman\gateway\Events.php`
- `D:\Software\PhpProject\TOZO\chatgpt-websocket-php74\plugin\webman\gateway\handler\MinMax\SceneChatHandler.php`
- `D:\Software\PhpProject\TOZO\chatgpt-websocket-php74\plugin\webman\gateway\handler\MinMax\OpenAIResponseHandler.php`

旧 PHP 主要职责是：

1. 接收 App/耳机 WebSocket 消息。
2. 按 `msgType` 处理文本、音频、停止、历史会话、天气/导航等场景。
3. 建立或复用 OpenAI Realtime WebSocket。
4. 把 App 消息转换为 OpenAI client event。
5. 把 OpenAI server event 转换为旧 App 期望的响应结构。
6. 用本地状态避免重复发送 `response.create` 或对空响应发送 `response.cancel`。

本次 Go 重构重点迁移这些核心链路，并修复长聊时容易出现的 active response 冲突、断链后状态丢失、旧 App 协议不兼容等问题。

## 四协程模型

当前 Go 会话层已最终收敛为每个用户连接四个主协程：

1. `readPump`
   - 唯一读取 App WebSocket。
   - 处理 App `ping` 并回 `pong`。
   - 接收旧 `msgType` 协议或 OpenAI 原生 client event。
   - 把消息转换为 OpenAI client event 后投递到 `apiSendChan`。

2. `openAIWritePump`
   - 唯一写入 OpenAI WebSocket。
   - 消费 `apiSendChan`。
   - 串行执行 `response.create/cancel` 状态机。
   - 处理 OpenAI 写失败后的重连和一次重试。
   - 接收 `recvPump` 的重连请求，并在重连后恢复 `session.update` 和最近历史。

3. `recvPump`
   - 唯一读取 OpenAI WebSocket。
   - 维护 OpenAI 读超时。
   - 接收 OpenAI 流式事件。
   - 推进 `response.create/cancel` 状态机。
   - 把 OpenAI 响应转换为 App 音频/文本事件。
   - 发现 OpenAI 读异常时，请求 `openAIWritePump` 执行重连。

4. `writePump`
   - 唯一写入 App WebSocket。
   - 发送 App Ping。
   - 串行推送文本、音频、错误、会话恢复等下行消息。

这样 App 和 OpenAI 两侧都遵守单读/单写约束，比 3 协程职责更清楚，也比 5 协程少一个长期 OpenAI Ping 协程。

## 迁移后的主要文件

- `internal/provider/openai/client_ws.go`
  - 会话生命周期。
  - 四协程启动。
  - App 读、OpenAI 读、App 写。
  - OpenAI 重连和最小上下文恢复。
  - OpenAI server event 到 App response 的转换。

- `internal/provider/openai/gateway_protocol.go`
  - 新增文件。
  - 旧 `Events.php` / `SceneChatHandler.php` 的 Go 侧协议适配层。
  - `msgType` 到 OpenAI client event 的转换。
  - `response.create/cancel` 状态机。

- `internal/provider/openai/gateway_protocol_test.go`
  - 新增测试。
  - 覆盖旧文本协议、App 心跳、response 队列门控、响应 ID 兼容。

- `pkg/response/response.go`
  - App 下行响应同时兼容旧字段 `responseId` 和新字段 `response_id`。

## 旧 msgType 映射

已迁移的核心映射：

| 旧 msgType | Go 处理逻辑 |
|---|---|
| `text` | 生成 `session.update`、`conversation.item.create`、`response.create` |
| `text_command` | 同文本消息，保留后续命令识别响应处理 |
| `audio` | 生成 `session.update`、`input_audio_buffer.append`、`input_audio_buffer.commit` |
| `speaker` | 生成 `session.update`、`input_audio_buffer.append`，用于连续音频流 |
| `stop` | 通过 response 状态机发送 `response.cancel` |
| `HistConv` | 解析 `historyContent`，恢复最近历史上下文，并回 `HistConvCompleted` |
| `session_close_gpt` | 关闭当前 OpenAI 会话 |
| `open_weather_reject_coordinate` | 通知 App 天气错误，并给 OpenAI 注入用户拒绝定位的上下文 |
| `type=ping` | 不转发 OpenAI，直接返回 `type=pong` |

未完整迁移的外部工具执行：

- `weather_service_search`
- 高德/Google/Mapbox 导航工具实际 HTTP 调用
- TOZO 知识库搜索
- 独立 TTS HTTP/流式接口
- 阿里转写/同声传译等非 OpenAI Realtime 旁路连接

这些旧模块依赖外部 SDK、API key、业务数据库和旧 PHP 工具类。当前 Go 层已保留协议出口：未知或未配置工具会返回明确错误，不会把无效 `msgType` 直接发给 OpenAI。

## response 状态机

旧 PHP 中的这些状态已迁移到 `openAIResponseGate`：

- `idle`
- `creating`
- `active`
- `cancelling`
- `pendingCreate`

关键规则：

1. 如果当前已有 active/creating/cancelling response，新的 `response.create` 不立即发送，而是进入 pending。
2. 收到 `response.created` 后记录当前 `response_id`。
3. 收到 `response.done` 或 `response.cancelled` 后回到 idle。
4. 回到 idle 后自动 flush pending 的 `response.create`。
5. 如果 App 发 `stop`，仅在本地确实有活跃响应时发送 `response.cancel`，避免 OpenAI 返回 `response_cancel_not_active`。
6. 如果 OpenAI 返回 `conversation_already_has_active_response`，本地状态同步为 active，等待 done/cancelled 释放。

这对应旧 `Events.php` 中：

- `sendOpenAIResponseCreate`
- `sendOpenAIResponseCancel`
- `flushPendingOpenAIResponseCreate`
- `syncOpenAIResponseStateFromError`

## OpenAI 响应转换

OpenAI server event 现在转换为旧 App 兼容响应：

| OpenAI event | App response |
|---|---|
| `response.created` | `begin` |
| `response.done` completed | `end` |
| `response.done` cancelled | `stop_success` |
| `response.output_text.delta` / `response.text.delta` | `text_delta` |
| `response.output_audio.delta` / `response.audio.delta` | `audio_delta` |
| `response.output_audio_transcript.delta` / legacy transcript delta | `text_delta` |
| `conversation.item.input_audio_transcription.completed` | `audioTransCompleted`，随后触发 `response.create` |
| `response.function_call_arguments.done` | `command_app` |
| `error` | `error` |

## 重连与恢复

已有策略：

- App 断开：当前会话结束，关闭 OpenAI 上游连接。
- OpenAI 断开：`recvPump` 发现读异常，向 `openAIWritePump` 发送重连请求。
- 重连成功：重放安全状态。
- 安全重放内容：
  - 最近 `session.update`
  - 最近 `conversation.item.create/truncate/delete`
- 不重放内容：
  - `input_audio_buffer.append`
  - `input_audio_buffer.commit`
  - `response.create`
  - `response.cancel`

原因：音频 buffer 和 response 控制事件重复发送会导致重复识别、重复回复或 active response 冲突。

## 百万并发说明

本次重构让单连接更稳定、更少 goroutine、更可控，但百万并发不是单进程目标。生产上必须配合：

- 多实例水平扩展。
- 负载均衡和连接准入。
- 每实例 `capacity.max_active_sessions`。
- Redis 集群和连接池调优。
- OpenAI 上游连接/配额容量评估。
- 慢客户端治理。
- Prometheus/日志/链路追踪。
- OpenAI mock server 压测。

## 验证

已执行：

```powershell
$env:GOCACHE='D:\Software\PhpProject\Go\Code\TozoAI-Chat-Api\.tmp\go-build'; go test ./...
```

结果：全部通过。

新增测试：

- 旧 `msgType=text` 转 OpenAI `session.update + conversation.item.create + response.create`。
- App `type=ping` 返回 `pong`，不转发 OpenAI。
- `response.create` 在 active response 期间进入 pending，并在 `response.done` 后 flush。
- App 下行响应同时包含 `responseId` 和 `response_id`。
