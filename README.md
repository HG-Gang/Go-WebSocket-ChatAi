# Go-WebSocket-ChatAi

Go WebSocket 网关项目，用 Go 重写旧 PHP Webman/Gateway WebSocket 项目的核心链路，面向 App / 耳机实时语音聊天、OpenAI Realtime、OpenAI Responses、Azure OpenAI Realtime 与 Azure 普通 HTTP 能力代理。

项目重点是解决长时间 WebSocket 聊天中常见的无响应、反复断链、上游 active response 冲突、心跳不清晰、日志和监控不足等问题。

## 核心能力

- App / 耳机通过 WebSocket 接入 Go 服务。
- Go 服务连接 OpenAI Realtime，转发文本、音频、旧 App `msgType` 协议和 OpenAI 原生 Realtime 事件。
- Realtime 连接使用四协程模型：
  - `readPump`：读取 App 消息，解析旧协议或 OpenAI 原生事件。
  - `openAIWritePump`：唯一写 OpenAI / Azure Realtime 上游，负责响应状态机、重连写恢复。
  - `recvPump`：唯一读 OpenAI / Azure Realtime 上游，处理流式文本、音频、转写、错误、函数调用。
  - `writePump`：唯一写 App，负责下行消息和 App 心跳 Ping。
- 支持 OpenAI Responses API 独立 HTTP 模块。
- 支持 Azure OpenAI：
  - Realtime WebSocket
  - Chat Completions
  - Completions
  - 文生图
  - 图生图 / 图片编辑
  - TTS
  - STT
  - TST / 语音翻译代理入口
- 兼容旧 PHP `Events.php` 的核心事件和消息类型：
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
- 提供 Web 调试页面：
  - WebSocket 测试面板
  - 语音对话测试
  - Redis 监控
  - 诊断看板
  - OpenAI Responses 测试
  - Azure OpenAI 监控
- 提供 Redis 会话、账单、限流、链路统计、最近会话明细和日志记录。

## 技术栈

- Go
- Gin
- Gorilla WebSocket
- Zap 日志
- Redis
- JWT 鉴权
- Viper 配置加载
- OpenAI Realtime API
- OpenAI Responses API
- Azure OpenAI API

## 目录结构

```text
.
├── cmd/server                 # 服务启动入口
├── conf                       # 全局配置、环境配置、模型配置
│   └── models                 # openai / azureai / openairesponses 模型配置
├── internal
│   ├── handler                # HTTP / WebSocket 路由处理器
│   ├── logger                 # Zap 日志初始化与模型日志
│   ├── middleware             # JWT、限流中间件
│   ├── provider               # AI Provider 工厂与具体实现
│   │   ├── openai             # OpenAI Realtime 四协程主链路
│   │   ├── azureai            # Azure OpenAI HTTP 代理与 Realtime 注册
│   │   └── openairesponses    # OpenAI Responses API 客户端
│   ├── rate                   # 限流工厂
│   └── service                # Redis、会话、指标、账单服务
├── pkg                        # 通用错误、协议、响应结构
├── web                        # 本地调试页面
├── logs                       # 日志目录，仅提交 .gitkeep
└── *.md                       # 架构、迁移、优化说明文档
```

## 环境要求

- Go 1.25 或兼容版本
- Redis 6+，默认地址：`127.0.0.1:6379`
- Windows / Linux / macOS 均可运行
- 访问 OpenAI / Azure OpenAI 需要可用网络或代理

## 配置说明

主配置文件：

```text
conf/config.yaml
```

环境覆盖配置：

```text
conf/config_dev.yaml
conf/config_prod.yaml
```

模型配置：

```text
conf/models/openai.yaml
conf/models/openairesponses.yaml
conf/models/azureai.yaml
```

敏感密钥不要写死到代码或 YAML 中，建议使用环境变量：

```powershell
# OpenAI Realtime / Responses 使用
$env:OPENAI_API_KEY="你的 OpenAI API Key"

# Azure OpenAI 使用
$env:AZURE_OPENAI_API_KEY="你的 Azure OpenAI API Key"
```

如果当前 Windows 机器不能直连 OpenAI，可以配置代理：

```powershell
$env:HTTP_PROXY="http://192.168.0.74:6478"
$env:HTTPS_PROXY="http://192.168.0.74:6478"
$env:NO_PROXY="localhost,127.0.0.1,::1"
```

项目中也保留了本地代理脚本：

```text
proxy-toggle.bat
```

## 启动服务

### 1. 安装依赖

```powershell
go mod download
```

### 2. 启动 Redis

确认 Redis 已监听：

```powershell
redis-cli ping
```

返回：

```text
PONG
```

### 3. 启动 Go 服务

```powershell
go run ./cmd/server
```

默认监听：

```text
:8096
```

健康检查：

```powershell
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8096/health
```

## 构建

```powershell
go build ./cmd/server
```

生成：

```text
server.exe
```

`server.exe`、日志、`.idea`、`.tmp` 等本地文件已通过 `.gitignore` 排除，不会提交到 GitHub。

## 测试

```powershell
$env:GOCACHE = (Resolve-Path ".").Path + "\.tmp\gocache"
go test ./...
```

如果本机 Go telemetry 写入目录被 Windows 权限拦截，可能会看到 telemetry 提示，但只要 `go test` 退出码为 0，项目测试就是通过的。

## Web 调试页面

服务启动后访问：

```text
http://127.0.0.1:8096/web/
```

页面列表：

| 页面 | 地址 | 作用 |
| --- | --- | --- |
| WebSocket 测试面板 | `/web/` | 连接 OpenAI Realtime，发送文本、旧协议 JSON、查看链路统计 |
| 语音对话测试 | `/web/audio.html` | 浏览器麦克风、音频上行、音频下行播放测试 |
| Redis 监控 | `/web/redis.html` | 查看 Redis key、类型、TTL、值、业务解释 |
| 诊断看板 | `/web/diagnostics.html` | Go / Redis / OpenAI / Azure / 指标聚合 |
| Responses 测试 | `/web/responses.html` | OpenAI Responses API 调试 |
| Azure 监控 | `/web/azure.html` | Azure OpenAI endpoint、deployment、路由、代理状态 |

## 主要接口

### 公开接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/health` | 健康检查 |
| `GET` | `/test/generate-token?userId=1001` | 本地生成 JWT token |
| `GET` | `/api/debug/status` | 诊断快照 |
| `GET` | `/api/redis/keys` | Redis key 明细 |
| `GET` | `/api/openai/responses/status` | OpenAI Responses 配置状态 |
| `GET` | `/api/azure/status` | Azure OpenAI 配置状态 |
| `GET` | `/web/*` | 调试页面 |

### 受保护接口

当 `conf/config.yaml` 中 `jwt.enabled: true` 时，以下接口需要请求头：

```text
Authorization: Bearer <token>
```

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/ws/realtime/openai` | OpenAI Realtime WebSocket |
| `GET` | `/ws/realtime/azure` | Azure OpenAI Realtime WebSocket |
| `POST` | `/api/openai/responses` | OpenAI Responses API |
| `POST` | `/api/azure/chat/completions` | Azure Chat Completions |
| `POST` | `/api/azure/completions` | Azure Completions |
| `POST` | `/api/azure/images/generations` | Azure 文生图 |
| `POST` | `/api/azure/images/edits` | Azure 图生图 / 图片编辑 |
| `POST` | `/api/azure/audio/speech` | Azure TTS |
| `POST` | `/api/azure/audio/transcriptions` | Azure STT |
| `POST` | `/api/azure/audio/translations` | Azure 语音翻译 |
| `POST` | `/api/azure/tst` | Azure TST 预留统一入口 |

## 本地 WebSocket 测试流程

1. 打开：

```text
http://127.0.0.1:8096/web/
```

2. 点击“自动获取 Token”。

3. 点击“连接”。

4. 在“用户问题”输入文本。

5. 点击“发送用户问题给 GPT”。

6. 查看：

- 实时日志
- 链路统计
- Session 事件明细
- 完整响应内容
- Redis key
- 诊断看板

## 旧 PHP 项目迁移说明

旧项目路径：

```text
D:\Software\PhpProject\TOZO\chatgpt-websocket-php74\plugin\webman\gateway
```

当前 Go 项目已迁移和优化的核心点：

- `Events.php` 的连接生命周期思想
- App ping/pong 处理
- 旧 `msgType` 分发
- OpenAI Realtime 连接复用
- `session.update` 差分同步
- `response.create` / `response.cancel` 状态机
- 文本、音频、speaker、历史会话、stop、天气坐标拒绝等旧协议
- TOZO 指令、天气、知识库、地图工具 schema
- OpenAI 函数调用完成后的 App 兼容响应

仍需后续接入真实外部服务的模块：

- OpenWeather 查询
- TOZO 知识库 / 向量库检索
- Google / Amap / Mapbox 地图服务
- 更完整的会议、同传、翻译业务闭环

当前实现已经避免函数调用后长时间等待，未接外部 Provider 时会返回明确错误或降级说明。

## 日志

日志目录：

```text
logs/
```

日志文件不会提交到 GitHub。

日志格式已优化为：

- 不输出 ANSI 颜色控制字符。
- 时间使用年月日时分秒。
- 按模型写入不同日志目录和文件。
- Realtime 会话日志携带 `request_id`、`session_id`、`user_id`。

## 并发与部署说明

当前代码支持单实例容量限制：

```yaml
capacity:
  max_active_sessions: 100000
```

百万并发不能只靠单台机器完成，需要：

- 多实例水平扩容
- 负载均衡
- Redis Cluster
- 上游 OpenAI / Azure 配额规划
- Linux 文件句柄、端口、内核网络参数调优
- Prometheus / Grafana 指标系统
- 独立日志采集系统
- 慢消费者保护和限流熔断

当前项目已经预留 Provider 工厂、Metrics、Redis Session、容量限制、模型配置隔离等扩展点。

## GitHub 推送

远程仓库：

```text
https://github.com/HG-Gang/Go-WebSocket-ChatAi.git
```

常用命令：

```powershell
# 查看状态
git status

# 暂存修改
git add .

# 提交修改
git commit -m "更新说明"

# 推送 main 分支
git push
```

如果远程分支比本地新，先同步：

```powershell
git pull --rebase origin main
git push
```

## 安全注意事项

- 不要提交真实 API Key。
- 不要提交 `.env`。
- 不要提交日志文件。
- 不要提交 `server.exe` 等编译产物。
- 生产环境必须修改 JWT Secret。
- 生产环境建议关闭公开的调试页面，或加上鉴权和 IP 白名单。

## 相关文档

- `AZURE_AND_EVENTS_REFACTOR_2026-05-15.md`
- `GATEWAY_EVENTS_MIGRATION_2026-05-14.md`
- `OPENAI_RESPONSES_AZURE_PLAN_2026-05-15.md`
- `REFACTOR_ANALYSIS_2026-05-14.md`
- `WEB_DEBUG_PAGES_ANALYSIS_2026-05-14.md`
