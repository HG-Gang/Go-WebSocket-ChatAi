# TozoAI-Chat-Api 重构报告

## 一、第一步：参考 go-xiaozhi 三协程模式，改进 OpenAI 实时 WebSocket 逻辑

### 修改文件清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `pkg/protocol/openai/types.go` | **重写** | 完整的 OpenAI Realtime API 类型定义，参考 go-xiaozhi |
| `pkg/protocol/openai/client_events.go` | **重写** | 9 种客户端事件 + 类型化反序列化（替代原有简化版） |
| `pkg/protocol/openai/server_events.go` | **重写** | 27 种服务端事件 + 类型化反序列化（替代原有 3 种） |
| `internal/provider/openai/client_ws.go` | **重写** | 四协程架构（替代原有问题代码） |
| `internal/provider/openai/config.go` | **重写** | OpenAI 专属配置封装 |
| `internal/provider/openai/events_client.go` | **重写** | 客户端事件处理器 |
| `internal/provider/openai/events_server.go` | **重写** | 服务端事件处理器 |
| `internal/provider/openai/init.go` | **新增** | Provider 工厂注册 |

### 架构改进

**原有问题（client_ws.go）：**

1. **锁泄漏 BUG**：在 `for` 循环内使用 `defer c.connMu.Unlock()`，导致锁只在函数退出时释放，实际上每次迭代都会新增一个 defer 但不执行，最终锁永不释放
2. **类型重复定义**：`client_ws.go` 中重新定义了 `ModelConfig`、`RealtimeConfig`、`Provider` 接口、`ZapLogger` 等，与 `conf` 包和 `provider` 包冲突
3. **并发写冲突**：上行和下行协程都直接写 `appConn`，gorilla/websocket 不支持并发写
4. **裸 JSON 处理**：使用 `map[string]interface{}` 处理事件，无类型安全

**新架构（参考 go-xiaozhi 三协程模型 + 扩展为四协程）：**

```
App WS ←→ readPump  → msgChan → processPump → OpenAI API
         writePump ← sendChan ← recvPump    ← OpenAI API
```

- **readPump**：只读 App 消息，写入 `msgChan`
- **writePump**：只写 App 消息（从 `sendChan` 消费）+ 心跳
- **processPump**：从 `msgChan` 消费，解析事件，转发到 OpenAI
- **recvPump**：从 OpenAI 读取响应，写入 `sendChan`

关键改进：
- `appConn` 的唯一写入者是 `writePump`，消除并发写
- 协程通过 `context.CancelFunc` 协调退出，任一出错即取消全部
- 使用 `sync.WaitGroup` 确保所有协程退出后才返回

### protocol/openai 事件系统改进

**原有实现（3 种事件）：**
- `ClientEventBase`/`ServerEventBase` 仅含 `Type string`
- `MarshalClientEvent`/`UnmarshalClientEvent` 用 `interface{}` 参数

**新实现（参考 go-xiaozhi，完整覆盖）：**
- 定义 `ClientEvent`/`ServerEvent` 接口，实现多态分发
- 9 种客户端事件类型 + 27 种服务端事件类型
- 泛型反序列化函数 `unmarshalClientEvent[T]` / `unmarshalServerEvent[T]`
- 按 `type` 字段自动路由到正确的结构体

---

## 二、第二步：工厂模式分析与修复

### 发现的问题

| # | 问题 | 严重度 | 文件 |
|---|------|--------|------|
| 1 | **Register() 从未被调用** | 🔴致命 | `provider.go` |
| 2 | **openai.Client 未实现 provider.Provider** | 🔴致命 | `client_ws.go` |
| 3 | **main.go 未导入 openai 包** | 🔴致命 | `main.go` |
| 4 | **provider.AIProvider 不存在** | 🔴致命 | `ws/client.go`, `ws/handler.go` |
| 5 | **client_ws.go 重复定义 Provider 接口** | 🟡冲突 | `client_ws.go` |
| 6 | **鉴权中间件全局应用** | 🟡设计 | `main.go` |

### 修复方案

#### 问题1-3：Provider 工厂注册链断裂

**根因**：Go 的工厂模式依赖 `init()` + 空导入触发注册，三个环节全部缺失。

**修复**：
1. 新增 `internal/provider/openai/init.go`，在 `init()` 中调用 `provider.Register("openai", ...)`
2. 重写 `openai.Client`，完整实现 `provider.Provider` 接口的 5 个方法
3. 在 `main.go` 中添加空导入：`_ "TozoAI-Chat-Api/internal/provider/openai"`

**数据流**：
```
main.go 空导入 openai 包
    → 触发 openai/init.go 的 init()
    → 调用 provider.Register("openai", factory)
    → factories["openai"] = factory

handler 调用 provider.Create("openai")
    → 查找 factories["openai"]
    → 调用 factory(cfg) 创建 openai.Client
    → 返回 provider.Provider 接口
```

#### 问题4：provider.AIProvider 不存在

**根因**：`internal/ws/` 包引用了 `provider.AIProvider` 类型，但 `provider.go` 中只定义了 `Provider` 接口。

**修复**：重写 `ws/client.go` 和 `ws/handler.go`，使用正确的 `provider.Provider` 类型。标记为预留代码，当前主流程不使用。

#### 问题5：client_ws.go 重复类型定义

**根因**：`client_ws.go` 中定义了自己的 `ModelConfig`、`RealtimeConfig`、`Provider` 接口等，与 `conf` 包和 `provider` 包冲突。

**修复**：删除所有重复定义，改用 `conf.ModelConfig` 和 `provider.Provider`。

#### 问题6：鉴权中间件全局应用

**根因**：原代码将 `Auth()` 和 `RateLimit()` 注册为全局中间件，导致 `/test/generate-token` 也需要鉴权。

**修复**：拆分路由组，公开路由不需要鉴权，业务路由才加中间件。

### 修改文件清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `internal/provider/provider.go` | **重写** | 精简接口（移除 HandleClientEvent/HandleServerEvent），完善注释 |
| `internal/provider/openai/init.go` | **新增** | 工厂注册（init() + Register） |
| `cmd/server/main.go` | **重写** | 添加空导入、拆分路由组 |
| `internal/handler/openai_handler.go` | **重写** | 安全类型断言、完善注释 |
| `internal/ws/client.go` | **重写** | 修复 provider.AIProvider → provider.Provider |
| `internal/ws/handler.go` | **重写** | 修复类型引用 |
