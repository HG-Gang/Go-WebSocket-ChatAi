# OpenAI Realtime 打断与 function_call 补全说明

## 本次解决的问题

本次重点处理 App/耳机在 OpenAI 正在流式输出时发起新一轮输入导致的两个错误：

- `conversation_already_has_active_response`：默认会话中已有活跃 Response 时又发送了新的 `response.create`。
- `response_cancel_not_active`：本地在 OpenAI 还没有创建出 Response，或 Response 已结束时发送了 `response.cancel`。

## 打断状态机策略

当前 Go 侧通过 `openAIResponseGate` 串行化 `response.create` 与 `response.cancel`：

1. `idle`：没有活跃响应，可以直接发送 `response.create`。
2. `creating`：已经发送 `response.create`，等待 OpenAI 返回 `response.created`。
3. `active`：OpenAI 已返回 `response.created`，可以继续收流或取消。
4. `cancelling`：已经发送 `response.cancel`，等待 `response.done(cancelled)`。

新增策略：

- App 文本新问题、提交语音、天气补坐标等用户新一轮输入，会先投递一次 `response.cancel`。
- 如果上游仍在 `creating` 且还没有 `response_id`，不立即发取消，先记录 `cancelAfterCreated`，等收到 `response.created` 后再取消。
- 如果 OpenAI 返回 `conversation_already_has_active_response`，本地状态切到 `active`，随后投递一次取消，等取消完成后再释放 pending 的 `response.create`。
- 如果 OpenAI 返回 `response_cancel_not_active`，本地状态回到 `idle`，并释放 pending 的 `response.create`。

这样做的目的不是吞掉错误，而是把错误转化为可恢复的本地状态，保证下一轮用户输入能继续执行。

## function_call 执行层

新增文件：

- `internal/provider/openai/tool_execution.go`
- `internal/provider/openai/tool_execution_test.go`

已接入的工具：

- `get_open_weather`
  - 支持城市优先查询。
  - 没有城市且没有坐标时，推送 `open_weather_missing_coordinates` 给 App。
  - 已配置 OpenWeather Key 时真实请求 OpenWeather。
  - 失败时仍会回填 function_call_output，让 OpenAI 给用户解释。

- `search_tozo_knowledge`
  - 支持自建知识库接口：`extra.tozo_knowledge_endpoint`。
  - 支持 OpenAI Vector Store Search：`extra.tozo_vector_store_id`。
  - 会把 `product_name + query` 组合成更精准的检索词。

- `get_specify_route_navigation`
  - 当前实现 Mapbox 查询。
  - 成功后推送 `map_service_places` 给 App，并把地点结果回填给 OpenAI。

- `get_nearby_route_navigation`
  - 当前实现 Mapbox category 查询。
  - 缺少坐标时推送 `map_service_missing_coordinates` 给 App。

## 配置项

`conf/models/openai.yaml` 新增：

- `extra.tool_timeout_ms`
- `extra.open_weather_api_key`
- `extra.open_weather_endpoint`
- `extra.tozo_knowledge_endpoint`
- `extra.tozo_knowledge_api_key`
- `extra.tozo_vector_store_id`
- `extra.tozo_knowledge_max_results`
- `extra.default_map_sdk`
- `extra.mapbox_api_key`
- `extra.mapbox_endpoint`

敏感密钥仍建议通过环境变量注入，不要写死到仓库。

## 验证结果

已执行：

```powershell
$env:GOCACHE=(Resolve-Path '.').Path + '\.gocache'
go test ./...
```

结果：全项目测试通过。

## 后续未完成项

- Amap/Google 导航执行器还没有完全实现，目前只保留配置位和扩展入口。
- 天气和知识库执行器已经支持真实 HTTP 调用，但线上可用性取决于外部 Key、代理和服务地址配置。
- 生产百万并发还需要压测、网关层限流、Redis/日志异步化、连接实例水平扩容和系统参数调优共同完成，不能只靠单进程代码保证。
