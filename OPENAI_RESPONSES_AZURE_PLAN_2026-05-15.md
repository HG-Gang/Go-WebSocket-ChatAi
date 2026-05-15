# OpenAI Responses 与 Azure OpenAI 接入说明

## 1. 本轮优先完成内容

本轮按优先级先完成第二、第三步：

1. Web 目录新增 Azure OpenAI 监控页：`web/azure.html`。
2. 诊断看板新增 Azure OpenAI 配置快照与 OpenAI Responses API 配置快照。
3. 新增 OpenAI Responses API 独立后端模块：`internal/provider/openairesponses`。
4. 新增 OpenAI Responses API 路由：
   - `GET /api/openai/responses/status`
   - `POST /api/openai/responses`
5. 新增 OpenAI Responses API 独立配置文件：`conf/models/openairesponses.yaml`。

## 2. Responses API、Chat Completions、Realtime 的区别

### Responses API

Responses API 是 OpenAI 新一代统一 HTTP 接口，核心地址是 `/v1/responses`。它适合普通文本、多模态输入、工具调用、状态化任务、代理类工作流等场景。当前项目中它被接入为独立 HTTP 模块，不占用 Realtime WebSocket 的四协程链路。

适合场景：

- 普通问答。
- 多模态请求。
- 需要统一输出结构的任务。
- 后续接工具调用、文件、代理工作流。

当前项目目录：

- `internal/provider/openairesponses`
- `internal/handler/openai_responses_handler.go`
- `conf/models/openairesponses.yaml`
- `web/responses.html`

### Chat Completions

Chat Completions 是传统 HTTP 聊天接口，输入核心是 `messages` 数组。它适合稳定的普通聊天补全、兼容旧项目、迁移成本低的场景，但接口能力边界比 Responses API 更窄。

当前项目里原来的 `/v1/chat/completions` 只作为 fallback 降级接口保留，不作为新的主链路扩展方向。

### Realtime API

Realtime API 是低延迟 WebSocket 接口，适合语音对话、音频流式输入输出、耳机/App 长连接场景。当前项目里的 `internal/provider/openai` 是 Realtime 专用目录，里面包含长连接、心跳、重连、四协程、状态机、流式音频/文本转发等逻辑。

当前项目目录：

- `internal/provider/openai`
- `internal/handler/openai_handler.go`
- `web/index.html`
- `web/audio.html`

## 3. 当前新增的 OpenAI Responses API 调用方式

普通请求：

```json
{
  "input": "请用三句话解释 Responses API 和 Chat Completions 的区别。"
}
```

高级请求可以直接使用接近官方 `/v1/responses` 的 JSON 结构。网关只会在调用方没有传入时补齐：

- `model`
- `instructions`
- `store`

默认配置：

- `model`: `conf/models/openairesponses.yaml` 的 `default_model`
- `endpoint`: `https://api.openai.com/v1`
- `store`: `false`
- `timeout_ms`: `60000`

## 4. Azure OpenAI 监控已覆盖内容

当前先做配置与能力监控，业务 provider 下一阶段补齐。`web/azure.html` 和 `web/diagnostics.html` 已展示：

- Azure 是否启用。
- Azure API Key 是否配置。
- Endpoint。
- API Version。
- 默认模型。
- Realtime WS URL。
- Realtime 心跳、超时、重连、恢复配置。
- Chat deployment。
- Realtime deployment。
- Image deployment。
- TTS deployment。
- STT deployment。
- TST deployment。
- HTTPS 代理是否对 Azure 生效。

## 5. 下一阶段 Azure provider 建议拆分

建议新增以下独立目录，避免所有能力塞进一个大文件：

- `internal/provider/azure/realtime`
- `internal/provider/azure/chat`
- `internal/provider/azure/responses`
- `internal/provider/azure/images`
- `internal/provider/azure/audio`

建议新增路由：

- `GET /ws/realtime/azure`
- `POST /api/azure/chat/completions`
- `POST /api/azure/responses`
- `POST /api/azure/images/generations`
- `POST /api/azure/images/edits`
- `POST /api/azure/audio/speech`
- `POST /api/azure/audio/transcriptions`
- `POST /api/azure/tst`

## 6. 为什么 Responses API 目录必须和 Realtime 目录分开

Realtime 是长连接模型，核心问题是连接生命周期、心跳、断线重连、队列、流式收发和四协程调度。

Responses API 是短连接 HTTP 模型，核心问题是请求体校验、HTTP 超时、代理、错误码、响应聚合和业务重试。

两者如果混在一个 provider 里，会导致配置、日志、监控和错误恢复策略边界不清晰，也会增加长聊时的排障成本。
