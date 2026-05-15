# 当前项目符合性检查清单

日期：2026-05-15

## 1. 日志格式

- 文件：`internal/logger/logger.go`
- 已使用 `zapcore.CapitalLevelEncoder`，文件日志不再写入 ANSI 颜色控制字符，避免出现 `[34mINFO[0m`。
- 已使用 `localSecondTimeEncoder` 输出 `2006-01-02 15:04:05`，不再输出毫秒和 `+0800`。
- `GetModelLogger("global")` 会按当前启用模型归一到 `openai`，日志文件形如 `logs/openai/openai-2026-05-15.log`。

## 2. OpenAI 配置加载

- 文件：`conf/loader.go`
- `conf/config.yaml` 中的 `models.openai.default_model`、`endpoint`、非空 `api_key` 具有更高优先级。
- `${OPENAI_API_KEY}` 为空时不会覆盖模型文件中已有的非空 key。
- 当前 `conf/models/openai.yaml` 不提交真实密钥，只保留 `${OPENAI_API_KEY}`。

## 3. WebSocket 测试面板

- 文件：`web/index.html`、`web/ws-test.js`、`web/style.css`
- 已支持深色、浅色、海蓝、暖色、高对比多种颜色模式。
- 链路统计已包含当前会话的 Session 事件数量，并支持鼠标悬停摘要和弹窗明细。
- 消息发送区包含用户文本输入框、JSON 消息输入框，日志区域已加高。
- OpenAI 流式输出会按 `response_id` 聚合，`response.done/end` 到达后显示最终完整响应和原始 JSON。
- Session 事件弹窗有 `.modal-backdrop[hidden] { display: none !important; }`，刷新页面不会强制弹出且可以关闭。

## 4. Redis 监控面板

- 文件：`web/redis.html`、`internal/handler/redis_handler.go`
- `/api/redis/keys` 支持 `full=1` 返回完整 key 内容。
- 每个 key 返回 `category` 和 `description`，解释当前 key 的分类和作用。
- 页面显示 key 总数、类型统计、Session、Billing/Usage、Rate Limit、OpenAI/Realtime 分类统计。

## 5. 统一诊断看板与统计

- 文件：`web/diagnostics.html`、`internal/handler/debug_handler.go`、`internal/service/metrics/metrics.go`
- `/api/debug/status` 输出 Go 运行时、内存、容量、功能开关、Redis、网络代理、OpenAI 配置和进程内指标。
- `metrics.Snapshot()` 已覆盖 App、Go 队列、OpenAI 连接/事件/重连/流式、业务 token/audio、错误和最近 session 明细。
- 页面可展示最近 session 的服务端真实事件和响应摘要。

## 6. 注释语言

- 当前新增和关键逻辑注释均使用中文。
- 保留的 `OpenAI`、`Redis`、`WebSocket`、`response_id`、`session.update` 等属于协议名或技术名，不视为英文逻辑注释。

## 7. 推送 GitHub 前注意

- `.gitignore` 已排除 `.idea/`、`.tmp/`、`.env*`、`*.log`、`*.exe`。
- 当前目录没有 `.git`，如需推送 GitHub，需要重新初始化仓库并绑定远程地址。
- 启动前需要设置 `OPENAI_API_KEY`；Windows 代理可使用 `proxy-toggle.bat` 设置后重新打开 GoLand 或 PowerShell。
