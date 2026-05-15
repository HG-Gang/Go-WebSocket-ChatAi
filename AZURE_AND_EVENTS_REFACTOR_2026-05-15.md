# Azure 接入与旧 Events.php 复刻优化说明（2026-05-15）

## 1. 本次完成范围

### 1.1 Azure OpenAI Realtime

- 新增 `azureai` Provider 注册：`internal/provider/azureai/init.go`。
- Azure Realtime 复用 `internal/provider/openai/client_ws.go` 的四协程模型：
  - `readPump`：读取 App / 耳机消息，兼容旧 `msgType` 协议。
  - `openAIWritePump`：唯一写上游 WS，负责 `response.create/cancel` 状态机和重连写恢复。
  - `recvPump`：唯一读上游 WS，解析流式响应、错误、函数调用和重连触发。
  - `writePump`：唯一写 App WS，负责下行消息和 App Ping。
- `internal/provider/openai/config.go` 新增 Azure Realtime URL/Header 生成逻辑：
  - OpenAI：`wss://api.openai.com/v1/realtime?model=...` + `Authorization: Bearer ...`。
  - Azure GA：`wss://{resource}.openai.azure.com/openai/v1/realtime?model={deployment}` + `api-key`。
  - Azure Preview：`wss://{resource}.openai.azure.com/openai/realtime?api-version=...&deployment=...` + `api-key`。

### 1.2 Azure HTTP 能力代理

新增 `internal/provider/azureai/client.go`，统一封装 deployment 路径、`api-version`、`api-key` 鉴权。

已接入本地路由：

| 能力 | 本地路由 | Azure 上游路径 |
| --- | --- | --- |
| Chat Completions | `POST /api/azure/chat/completions` | `/openai/deployments/{deployment}/chat/completions` |
| Completions | `POST /api/azure/completions` | `/openai/deployments/{deployment}/completions` |
| 文生图 | `POST /api/azure/images/generations` | `/openai/deployments/{deployment}/images/generations` |
| 图生图/图片编辑 | `POST /api/azure/images/edits` | `/openai/deployments/{deployment}/images/edits` |
| TTS | `POST /api/azure/audio/speech` | `/openai/deployments/{deployment}/audio/speech` |
| STT | `POST /api/azure/audio/transcriptions` | `/openai/deployments/{deployment}/audio/transcriptions` |
| TST/语音翻译 | `POST /api/azure/audio/translations`、`POST /api/azure/tst` | `/openai/deployments/{deployment}/audio/translations` |
| Realtime | `GET /ws/realtime/azure` | Azure Realtime WebSocket |

### 1.3 旧 PHP Events.php 协议复刻

`internal/provider/openai/gateway_protocol.go` 已继续补齐旧项目核心协议：

- 旧 App `msgType` 兼容：
  - `text`
  - `audio`
  - `speaker`
  - `text_command`
  - `stop`
  - `HistConv`
  - `session_close_gpt`
  - `weather_service_search`
  - `open_weather_reject_coordinate`
  - `map_service_search`
  - `tts`
  - `tts_voice`
- Session 差分更新：
  - 对比 `voice`
  - 对比 `instructions`
  - 对比 `mode/msgType`
  - 对比工具列表 hash
- 已补齐旧 Events.php 的基础 tools：
  - `get_open_weather`
  - `search_tozo_knowledge`
  - `map_command_to_code`
  - `get_specify_route_navigation`
  - `get_nearby_route_navigation`
- 函数调用处理：
  - `map_command_to_code`：返回旧 App `command_app`，兼容 `code_exit_chat/code_end_chat -> code_quit`。
  - `get_open_weather`：缺少城市和坐标时返回 `open_weather_missing_coordinates`；Provider 未配置时返回函数输出给模型，避免卡住。
  - `search_tozo_knowledge`：Provider 未配置时返回函数输出给模型，避免模型一直等待 tool output。
  - 地图导航函数：缺少坐标返回 `map_service_missing_coordinates`；地图 Provider 未配置返回 `map_service_fail`。

## 2. 配置变更

### 2.1 Azure 配置

`conf/models/azureai.yaml` 新增/保留以下字段：

- `endpoint`
- `api_key`
- `extra.api_version`
- `extra.deployment_name`
- `extra.chat_deployment`
- `extra.completions_deployment`
- `extra.responses_deployment`
- `extra.realtime_deployment`
- `extra.image_deployment`
- `extra.tts_deployment`
- `extra.stt_deployment`
- `extra.tst_deployment`

建议最小配置示例：

```yaml
enabled: true
api_key: "${AZURE_OPENAI_API_KEY}"
endpoint: "https://你的资源名.openai.azure.com"
extra:
  api_version: "2024-10-21"
  chat_deployment: "你的聊天部署名"
  realtime_deployment: "你的 realtime 部署名"
  image_deployment: "你的图片部署名"
  tts_deployment: "你的 tts 部署名"
  stt_deployment: "你的 stt 部署名"
```

### 2.2 OpenAI Key 安全处理

`conf/models/openai.yaml` 已改为：

```yaml
api_key: "${OPENAI_API_KEY}"
```

原因：项目要推送 GitHub，API Key 不能写死在配置文件里。启动前需要在系统环境变量或代理启动脚本中注入 `OPENAI_API_KEY`。

## 3. Web 监控同步

### 3.1 Azure 监控页

`web/azure.html` 已从“预留配置监控”改成真实模块状态：

- 显示 Azure 是否启用。
- 显示 API Key 是否配置。
- 显示 endpoint、api-version、deployment。
- 显示每个模块的本地路由。
- 显示上游路径和监控重点。
- 复制原始 Azure 诊断 JSON。

### 3.2 诊断看板

`web/diagnostics.html` 已更新 Azure 文案，说明 HTTP 代理路由已接入，实际可用性取决于 Azure 配置。

`/api/debug/status` 已新增：

- `azure.modules`
- `azure.timeout_ms`
- `azure.completions_deployment`
- Azure 路由清单

## 4. 尚未完全落地的旧项目外部服务

旧 PHP 项目里天气、TOZO 知识库、地图 Provider 依赖额外 API 和业务数据源：

- OpenWeather API
- TOZO 知识库 / 向量库
- Google Maps
- Amap
- Mapbox

当前 Go 项目已完成协议识别、工具 schema、函数调用防卡死和旧 App 事件兼容，但还没有真正调用这些外部服务。下一步应新增独立 service 层：

- `internal/service/weather`
- `internal/service/knowledge`
- `internal/service/navigation`

这样不会污染 Realtime 四协程主链路，也便于以后按不同国家、地图供应商、知识库后端扩展。

## 5. 验证结果

已执行：

```powershell
gofmt -w ...
$env:GOCACHE = (Resolve-Path '.').Path + '\.tmp\gocache'; go test ./...
$env:GOCACHE = (Resolve-Path '.').Path + '\.tmp\gocache'; go build ./cmd/server
node --check web\ws-test.js
node -e "检查 web/*.html 内联脚本"
```

结果：全部通过。

## 6. 百万并发现实边界

本次改动继续保持四协程单连接模型，能解决并发写冲突、长连接状态混乱、OpenAI active response 重复创建等旧项目高频问题。但“单机百万并发不卡顿”不现实，需要工程化拆分：

- 多实例水平扩容。
- L4/L7 负载均衡。
- Redis Cluster 或专用状态存储。
- 每实例连接上限和熔断。
- 上游 OpenAI/Azure 连接池与账号级限流。
- Linux 内核参数、文件句柄、端口、网卡和内存调优。
- 指标系统接 Prometheus/Grafana。

当前代码已经具备这些方向的接入点：Provider 工厂、独立配置、容量限制、metrics snapshot、Redis session、四协程 Realtime 主链路。
