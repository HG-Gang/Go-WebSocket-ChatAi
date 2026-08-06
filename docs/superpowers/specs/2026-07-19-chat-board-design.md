# 聊天面板 + 请求数据看板 设计规格

**日期**：2026-07-19  
**状态**：已确认（待实现计划）  
**范围**：内部调试/运维 + App 联调演示台  
**路线**：单体增强（Go API 扩展 + 原生 Web 页 + ECharts CDN）

---

## 1. 背景与目标

### 1.1 现状

- 已有模型配置：`openai`（Realtime）、`openairesponses`（HTTP Responses）、`azureai`（HTTP 多能力，默认关闭）。
- 已有 `/api/web/models`、`/api/web/metrics`（内存最近 500 条 `WebRequestRecord`）、Responses/Azure 代理接口。
- 已有 `web/chat.html`（项目助手 + Realtime，非通用多模型 GPT UI）。
- `conf.DB` 为预留配置，**尚无**实际 DB 读写代码。
- **无**用户附件上传 API；**无**统一多 Provider 聊天入口；**无**持久化请求明细表 UI。

### 1.2 目标（一期）

1. **多模型聊天面板**：可选择已配置且启用的全部模型；ChatGPT 风格对话；支持图片 + PDF/文本附件解析后提问。
2. **请求数据看板**：每次请求落库；列表展示 Token/费用/状态/端点等；列勾选；筛选；多种图表。
3. **请求详情**：聊天侧抽屉 + 列表行展开，展示输入/输出/缓存/思考 Token 明细与耗时。
4. **统一 Provider 抽象**：按 `model_config` 路由到 Responses / Azure Chat 等，计量字段一致。

### 1.3 非目标（一期不做）

- 多租户账号体系与权限隔离（仍用现有 JWT/测试 Token）。
- 服务端持久化多轮会话历史（会话仅浏览器内存）。
- Realtime 语音作为默认聊天通道（统一入口优先 HTTP；Realtime 仍走现有调试页）。
- 独立 SPA 构建工具链（Vue/React 工程）。
- 任意二进制格式的完整解析（仅图片 + PDF/文本）。

---

## 2. 用户与使用场景

| 角色 | 场景 |
|------|------|
| 内部调试/运维 | 切换模型测效果；看 Token/费用；筛失败请求 |
| App 联调 | 模拟 App 发问与附件；核对请求明细字段与联调日志 |

成功标准：

- 能在一页完成：选模型 → 发文本/图/PDF → 看回复 → 看本条请求详情 → 在看板筛历史。
- 服务重启后，请求明细仍可从 DB 查询（SQLite 默认）。
- 列表列可勾选；筛选可用；至少 5 类图表可切换。

---

## 3. 总体架构

```
Browser  /web/chat-board.html
    │  JWT Bearer
    ├─ GET  /api/web/models
    ├─ POST /api/web/upload
    ├─ POST /api/web/chat          (SSE 流式)
    ├─ GET  /api/web/requests
    └─ GET  /api/web/requests/stats
              │
              ▼
        Gin handlers
              │
    ┌─────────┼──────────────┐
    ▼         ▼              ▼
 upload    chat router    requestlog store
  local     (provider)     (SQLite/MySQL)
  files        │
               ├─ openairesponses
               └─ azure chat/completions
```

**原则**：

- 复用现有 JWT、模型配置、Responses/Azure 客户端逻辑。
- 扩展现有 `WebRequestRecord` 字段语义，写入 DB 表 `web_request_logs`；可选仍写入内存 metrics 便于兼容旧诊断页。
- 前端无构建：`chat-board.html` + `chat-board.js` + 现有 `style.css`/`theme.js` + ECharts CDN。

---

## 4. 后端设计

### 4.1 统一聊天 API

`POST /api/web/chat`（需 JWT）

**请求 JSON（示意）**：

```json
{
  "model_config": "openairesponses",
  "model": "gpt-4.1",
  "reasoning_effort": "medium",
  "messages": [
    { "role": "user", "content": "解释这张图", "attachment_ids": ["uuid1"] }
  ],
  "stream": true
}
```

**行为**：

1. 校验 `model_config` 存在且 `enabled`。
2. 解析 `attachment_ids` → 本地文件 → 组装上游 input（图片多模态 / 文本注入）。
3. 按 Provider 路由：
   - `openairesponses` → 现有 Responses 客户端。
   - `azureai` → Azure chat completions（启用时）。
   - 其他未实现类型返回明确 501 + 错误文案。
4. `stream=true`：SSE 推送 `delta` / `done` / `error` 事件。
5. 结束（成功或失败）写入 `web_request_logs` 一条完整记录；计算费用（读模型 `extra` 单价，缺省为 0）。

**SSE 事件**：

| event | 含义 |
|-------|------|
| `delta` | 文本增量 |
| `meta` | 可选：response_id、model |
| `done` | 最终 usage + 完整 record 摘要 |
| `error` | 错误信息 |

### 4.2 上传 API

`POST /api/web/upload`（multipart，需 JWT）

- 字段：`file`
- 允许：`image/*`、`application/pdf`、`text/plain`、`text/markdown`、`text/csv` 等文本类
- 默认单文件 ≤ 10MB（配置项 `web_chat.max_upload_bytes`）
- 存储：`./data/uploads/{yyyy}/{mm}/{uuid}{ext}`
- 返回：`{ id, name, mime, size, kind: image|pdf|text }`
- PDF：服务端抽取文本（截断上限，如 100k 字符），抽取失败则返回错误提示
- 图片：保存原文件；聊天时转 base64 data URL 或受控本地读取路径（不对外裸暴露无鉴权静态目录）

### 4.3 请求列表与统计 API

`GET /api/web/requests`

Query：`page`、`size`、`from`、`to`、`model`、`model_config`、`status`、`provider`、`q`（匹配 endpoint/error/request_id）

响应：`{ total, page, size, items: [...] }`

`GET /api/web/requests/stats`

Query：`period=day|week|month` + 同筛选子集

响应聚合：

- 时间桶请求量
- 按模型 Token 堆叠（input/output/cached/reasoning）
- 费用汇总
- 状态分布
- first_token_ms 分桶

### 4.4 数据表 `web_request_logs`

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增 |
| request_id | TEXT | 业务请求 ID |
| created_at | DATETIME/INTEGER | 时间 |
| model_config | TEXT | 配置名 |
| model | TEXT | 模型名 |
| provider | TEXT | openai/azure 等 |
| input_tokens | INTEGER | |
| output_tokens | INTEGER | |
| cached_input_tokens | INTEGER | |
| reasoning_tokens | INTEGER | |
| total_tokens | INTEGER | |
| total_cost | REAL | 估算费用 |
| fee | REAL | 与 total_cost 对齐或预留 |
| status | TEXT | success/error/... |
| api_key_masked | TEXT | 脱敏 |
| reasoning_effort | TEXT | |
| endpoint | TEXT | |
| type | TEXT | chat/responses/... |
| billing_mode | TEXT | |
| first_token_ms | INTEGER | |
| latency_ms | INTEGER | |
| user_agent | TEXT | |
| user_id | TEXT | JWT sub |
| error_message | TEXT | |
| extra_json | TEXT | 可选扩展 |

索引：`created_at`、`model`、`status`、`model_config`、`request_id`。

### 4.5 DB 接入

- 启用 `conf.DB`：`enabled`、`driver`（`sqlite` 默认 / `mysql`）、`dsn`。
- 默认开发：`driver: sqlite`，`dsn: ./data/tozoai.db`。
- 启动时 AutoMigrate / 建表 SQL。
- 包建议：`internal/service/requestlog`（Insert / List / Stats）。
- 实现库：优先 `database/sql` + 驱动（`modernc.org/sqlite` 或 `go-sql-driver/mysql`），避免强绑 ORM；若项目后续统一 GORM 可再换。

### 4.6 费用计算

从 `ModelConfig.Extra` 读取：

- `input_price_per_1m`
- `output_price_per_1m`
- `cached_input_price_per_1m`
- `reasoning_price_per_1m`

缺失则费用记 0；文档/配置示例中补全 openairesponses 单价字段（可选，不阻塞功能）。

### 4.7 安全

- 上传目录不可无鉴权公开；读取仅通过鉴权接口或服务端内嵌。
- 日志与 DB **禁止**写完整 API Key、完整上游 Authorization。
- 生产环境继续受 JWT / Origin / public_debug 开关约束。
- 上传 MIME 白名单 + 扩展名校验；拒绝可执行类型。

---

## 5. 前端设计

### 5.1 页面

路径：`/web/chat-board.html`  
脚本：`/web/chat-board.js`  
样式：复用 `style.css` + 页面局部样式；主题复用 `theme.js`。  
图表：ECharts CDN。

布局：

```
┌─────────────────────────────────────────────────────────┐
│ 顶栏：标题 | 主题 | Token 获取 | 导航到其他调试页          │
├──────────────────────────┬──────────────────────────────┤
│ 左：聊天                 │ 右：看板（Tab）               │
│ · 模型选择 / 推理强度    │ · 列表 | 图表                 │
│ · 消息流                 │ · 筛选条                      │
│ · 输入 + 附件 + 发送     │ · 列勾选                      │
│ · 抽屉：请求详情         │ · 表格 / ECharts              │
└──────────────────────────┴──────────────────────────────┘
```

窄屏：上下堆叠或 Tab 切换「聊天 / 看板」。

### 5.2 聊天交互

- 加载模型列表 → 过滤 `enabled`。
- 会话数组仅在内存；清空会话不删 DB 请求。
- 粘贴/选择图片 → 先 upload 再挂到待发送消息。
- 发送后 SSE 渲染助手气泡；`done` 时打开/刷新请求详情，并刷新看板列表。
- 错误：气泡下红条 + 仍写失败请求记录。

### 5.3 看板列表

**默认列**：时间、模型、输入、输出、缓存输入、思考 Token、总计、费用、状态  

**可选列**：API 密钥、推理强度、端点、类型、计费模式、首 Token 耗时、总耗时、User-Agent、Provider、RequestId  

列勾选偏好：`localStorage` key `tozo-chat-board-columns`。

**筛选**：时间范围、模型、状态、model_config/provider、关键字。

**行展开**：Token 明细（输入/输出/缓存/思考）+ 与聊天抽屉一致的字段。

### 5.4 图表

1. 请求量时间线（折线）
2. 按模型 Token 堆叠柱/面积
3. 费用饼图或柱状
4. 状态分布饼图
5. 首 Token 耗时直方图

`period` 与列表筛选联动。

---

## 6. 与现有代码关系

| 现有 | 关系 |
|------|------|
| `WebRequestRecord` | 字段对齐；DB 行可映射为同一 JSON 形状 |
| `/api/web/metrics` | 保留；新看板主读 DB；可选双写内存 |
| `openairesponses` | 统一聊天首选实现路径 |
| `azureai` chat | 启用时接入同一入口 |
| `web/chat.html` | 保留项目助手；新页独立不替换 |
| `diagnostics.html` | 导航互链 |

---

## 7. 配置增量（示意）

```yaml
db:
  enabled: true
  driver: sqlite
  dsn: ./data/tozoai.db

web_chat:
  max_upload_bytes: 10485760
  max_pdf_chars: 100000
  upload_dir: ./data/uploads
```

模型 `extra` 可增加单价字段以便费用非零。

---

## 8. 测试要点

- 单元：费用计算；MIME 白名单；List/Stats SQL 筛选；SSE 错误路径写失败记录。
- 集成：mock 上游成功/失败；上传图片+PDF 后聊天；重启后列表仍在。
- 前端手工：列勾选持久化；筛选；图表切换；窄屏布局。

---

## 9. 实现分期建议

| 阶段 | 内容 |
|------|------|
| P0 | DB 接入 + `web_request_logs` + 写/读 API |
| P1 | 统一 `POST /api/web/chat`（非流式可先）+ 模型选择聊天 UI |
| P2 | SSE 流式 + 请求详情抽屉 |
| P3 | 上传 + 图片/PDF/文本 |
| P4 | 看板表格列勾选/筛选 + ECharts |

可在实现计划中把 P0–P4 拆为可合并 PR 的任务。

---

## 10. 已确认决策摘要

| 项 | 决策 |
|----|------|
| 交付范围 | 聊天 + 看板一起 |
| 通道 | 统一多 Provider 抽象（HTTP 优先） |
| 持久化 | 数据库（默认 SQLite） |
| 附件 | 图片 + PDF/文本 |
| 用户 | 内部运维 + App 联调 |
| 架构 | 单体增强 + ECharts CDN |
| 会话 | 仅浏览器内存 |
| 流式 | SSE 优先 |

---

## 11. 开放风险

1. PDF 解析依赖选择（轻量库 vs 外部工具）——实现时选纯 Go 或简单文本提取，避免重依赖。
2. Azure 默认 `enabled: false`——UI 仅展示已启用模型。
3. 上游 usage 字段名不一致——沿用 `web_metrics_handler` 的归一化逻辑。
4. 费用依赖配置单价——未配置时显示 0，不阻塞。
