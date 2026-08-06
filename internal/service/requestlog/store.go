// internal/service/requestlog/store.go
// Web 聊天/看板请求明细持久化存储（SQLite，MySQL 预留）：
// - 负责 web_request_logs 表的建表迁移、明细写入、分页查询与图表聚合
// - 输入：调用方组装好的 Record / ListFilter
// - 输出：带 id 的 Record、分页结果（items+total）、StatsResult 聚合
//
// 明确不负责：
// - 明细的采集与组装（由 handler/provider 调用方完成）
// - APIKey 脱敏：api_key_masked 列只存调用方传入的掩码值，本层不保存明文
// - 跨进程共享：连接句柄为进程内单例，多实例各自落库
package requestlog

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Record 与看板 WebRequestRecord 字段对齐，便于前后端共用 JSON 形状。
type Record struct {
	ID              int64   `json:"id"`
	RequestID       string  `json:"request_id"`
	Time            string  `json:"time"`
	Timestamp       int64   `json:"timestamp"`
	ModelConfig     string  `json:"model_config"`
	Model           string  `json:"model"`
	Provider        string  `json:"provider"`
	InputTokens     int64   `json:"input_tokens"`
	OutputTokens    int64   `json:"output_tokens"`
	CachedTokens    int64   `json:"cached_input_tokens"`
	ReasoningTokens int64   `json:"reasoning_tokens"`
	TotalTokens     int64   `json:"total_tokens"`
	TotalCost       float64 `json:"total_cost"`
	Fee             float64 `json:"fee"`
	Status          string  `json:"status"`
	APIKey          string  `json:"api_key"`
	ReasoningEffort string  `json:"reasoning_effort"`
	Endpoint        string  `json:"endpoint"`
	Type            string  `json:"type"`
	BillingMode     string  `json:"billing_mode"`
	FirstTokenMs    int64   `json:"first_token_ms"`
	LatencyMs       int64   `json:"latency_ms"`
	UserAgent       string  `json:"user_agent"`
	UserID          string  `json:"user_id"`
	Error           string  `json:"error,omitempty"`
}

// ListFilter 列表筛选条件。
type ListFilter struct {
	From        int64
	To          int64
	Model       string
	ModelConfig string
	Status      string
	Provider    string
	Q           string
	Page        int
	Size        int
}

// StatsBucket 时间桶聚合。
type StatsBucket struct {
	Key             string  `json:"key"`
	Requests        int64   `json:"requests"`
	InputTokens     int64   `json:"input_tokens"`
	OutputTokens    int64   `json:"output_tokens"`
	CachedTokens    int64   `json:"cached_input_tokens"`
	ReasoningTokens int64   `json:"reasoning_tokens"`
	TotalTokens     int64   `json:"total_tokens"`
	TotalCost       float64 `json:"total_cost"`
}

// StatsResult 看板图表数据。
type StatsResult struct {
	Timeline    []StatsBucket    `json:"timeline"`
	ByModel     []map[string]any `json:"by_model"`
	ByStatus    []map[string]any `json:"by_status"`
	CostByModel []map[string]any `json:"cost_by_model"`
	FirstToken  []map[string]any `json:"first_token"`
	Summary     map[string]any   `json:"summary"`
}

var (
	dbMu   sync.RWMutex
	global *sql.DB
)

// Init 初始化数据库。driver 支持 sqlite；mysql 预留（需 dsn）。
func Init(enabled bool, driver, dsn string) error {
	// 未启用时直接返回 nil，调用方无需区分"未启用"与"已初始化"。
	if !enabled {
		return nil
	}
	driver = strings.TrimSpace(strings.ToLower(driver))
	// driver 留空默认 sqlite；非 sqlite 驱动缺少 dsn 时失败关闭，避免误用错误驱动。
	if driver == "" {
		driver = "sqlite"
	}
	if dsn == "" {
		if driver == "sqlite" {
			dsn = "./data/tozoai.db"
		} else {
			return fmt.Errorf("db.dsn is required for driver %s", driver)
		}
	}
	if driver == "sqlite" {
		// modernc.org/sqlite 直接用文件路径作为 DSN，需提前确保父目录存在；
		// 目录创建失败不阻断后续 Open，最终以 Open 报错为准。
		if err := os.MkdirAll(filepath.Dir(dsn), 0o755); err != nil && filepath.Dir(dsn) != "." {
			// dsn 可能是相对文件名无目录，此时落到默认 data 目录。
			_ = os.MkdirAll("data", 0o755)
		}
		// 含 mode= 的 DSN 视为内存库等特殊用法，无需创建父目录。
		if !strings.Contains(dsn, "mode=") {
			dir := filepath.Dir(dsn)
			if dir != "" && dir != "." {
				_ = os.MkdirAll(dir, 0o755)
			}
		}
	}

	sqlDriver := driver
	if driver == "sqlite" {
		sqlDriver = "sqlite"
	}

	conn, err := sql.Open(sqlDriver, dsn)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	// SQLite 单写者模型下限制连接池规模，避免并发写放大。
	conn.SetMaxOpenConns(8)
	conn.SetMaxIdleConns(4)
	conn.SetConnMaxLifetime(time.Hour)

	// sql.Open 不实际建连，Ping 失败时关闭连接并失败关闭，防止带病启动。
	if err := conn.Ping(); err != nil {
		_ = conn.Close()
		return fmt.Errorf("ping db: %w", err)
	}
	if err := migrate(conn, driver); err != nil {
		_ = conn.Close()
		return err
	}

	// 新连接全部就绪后才替换全局句柄，替换期间旧连接仍可用。
	dbMu.Lock()
	if global != nil {
		_ = global.Close()
	}
	global = conn
	dbMu.Unlock()
	return nil
}

// Close 关闭数据库连接。
func Close() {
	dbMu.Lock()
	defer dbMu.Unlock()
	if global != nil {
		_ = global.Close()
		global = nil
	}
}

// Enabled 是否已初始化可用。
func Enabled() bool {
	dbMu.RLock()
	defer dbMu.RUnlock()
	return global != nil
}

func migrate(conn *sql.DB, driver string) error {
	// DDL 兼容 SQLite 与 MySQL：SQLite 用 AUTOINCREMENT/REAL，MySQL 在下方替换为 BIGINT/DOUBLE。
	ddl := `
CREATE TABLE IF NOT EXISTS web_request_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL,
  time_text TEXT NOT NULL DEFAULT '',
  model_config TEXT NOT NULL DEFAULT '',
  model TEXT NOT NULL DEFAULT '',
  provider TEXT NOT NULL DEFAULT '',
  input_tokens INTEGER NOT NULL DEFAULT 0,
  output_tokens INTEGER NOT NULL DEFAULT 0,
  cached_input_tokens INTEGER NOT NULL DEFAULT 0,
  reasoning_tokens INTEGER NOT NULL DEFAULT 0,
  total_tokens INTEGER NOT NULL DEFAULT 0,
  total_cost REAL NOT NULL DEFAULT 0,
  fee REAL NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT '',
  api_key_masked TEXT NOT NULL DEFAULT '',
  reasoning_effort TEXT NOT NULL DEFAULT '',
  endpoint TEXT NOT NULL DEFAULT '',
  type TEXT NOT NULL DEFAULT '',
  billing_mode TEXT NOT NULL DEFAULT '',
  first_token_ms INTEGER NOT NULL DEFAULT 0,
  latency_ms INTEGER NOT NULL DEFAULT 0,
  user_agent TEXT NOT NULL DEFAULT '',
  user_id TEXT NOT NULL DEFAULT '',
  error_message TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_web_req_created ON web_request_logs(created_at);
CREATE INDEX IF NOT EXISTS idx_web_req_model ON web_request_logs(model);
CREATE INDEX IF NOT EXISTS idx_web_req_status ON web_request_logs(status);
CREATE INDEX IF NOT EXISTS idx_web_req_config ON web_request_logs(model_config);
CREATE INDEX IF NOT EXISTS idx_web_req_request_id ON web_request_logs(request_id);
`
	if driver == "mysql" {
		// MySQL 主键自增与浮点类型与 SQLite 不同，做最小差异替换，其余列保持兼容。
		ddl = strings.ReplaceAll(ddl, "INTEGER PRIMARY KEY AUTOINCREMENT", "BIGINT PRIMARY KEY AUTO_INCREMENT")
		ddl = strings.ReplaceAll(ddl, "REAL", "DOUBLE")
	}
	_, err := conn.Exec(ddl)
	return err
}

// Insert 写入一条请求明细，返回带 id 的记录。
// APIKey 字段按调用方约定传入掩码值（落 api_key_masked 列），本层不负责脱敏。
func Insert(ctx context.Context, rec Record) (Record, error) {
	dbMu.RLock()
	conn := global
	dbMu.RUnlock()
	if conn == nil {
		return rec, fmt.Errorf("requestlog db not initialized")
	}
	// 调用方未提供时间戳时用当前时间兜底，保证 created_at 恒有值。
	now := time.Now()
	if rec.Timestamp == 0 {
		rec.Timestamp = now.UnixMilli()
	}
	if rec.Time == "" {
		rec.Time = now.Format("2006-01-02 15:04:05")
	}
	res, err := conn.ExecContext(ctx, `
INSERT INTO web_request_logs (
  request_id, created_at, time_text, model_config, model, provider,
  input_tokens, output_tokens, cached_input_tokens, reasoning_tokens, total_tokens,
  total_cost, fee, status, api_key_masked, reasoning_effort, endpoint, type, billing_mode,
  first_token_ms, latency_ms, user_agent, user_id, error_message
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		rec.RequestID, rec.Timestamp, rec.Time, rec.ModelConfig, rec.Model, rec.Provider,
		rec.InputTokens, rec.OutputTokens, rec.CachedTokens, rec.ReasoningTokens, rec.TotalTokens,
		rec.TotalCost, rec.Fee, rec.Status, rec.APIKey, rec.ReasoningEffort, rec.Endpoint, rec.Type, rec.BillingMode,
		rec.FirstTokenMs, rec.LatencyMs, rec.UserAgent, rec.UserID, rec.Error,
	)
	if err != nil {
		return rec, err
	}
	id, _ := res.LastInsertId()
	rec.ID = id
	return rec, nil
}

// List 分页查询。
func List(ctx context.Context, f ListFilter) (items []Record, total int64, err error) {
	dbMu.RLock()
	conn := global
	dbMu.RUnlock()
	if conn == nil {
		return nil, 0, fmt.Errorf("requestlog db not initialized")
	}
	// 分页参数兜底：默认第 1 页每页 20 条，单页上限 200，防止超大请求拖垮 SQLite。
	if f.Page < 1 {
		f.Page = 1
	}
	if f.Size < 1 {
		f.Size = 20
	}
	if f.Size > 200 {
		f.Size = 200
	}

	where, args := buildWhere(f)
	// 先查总数再查明细，供前端分页组件使用；两处查询共用同一过滤条件。
	countSQL := "SELECT COUNT(1) FROM web_request_logs" + where
	if err = conn.QueryRowContext(ctx, countSQL, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	offset := (f.Page - 1) * f.Size
	querySQL := `
SELECT id, request_id, created_at, time_text, model_config, model, provider,
  input_tokens, output_tokens, cached_input_tokens, reasoning_tokens, total_tokens,
  total_cost, fee, status, api_key_masked, reasoning_effort, endpoint, type, billing_mode,
  first_token_ms, latency_ms, user_agent, user_id, error_message
FROM web_request_logs` + where + ` ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?`
	args2 := append(append([]any{}, args...), f.Size, offset)
	rows, err := conn.QueryContext(ctx, querySQL, args2...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	items = make([]Record, 0, f.Size)
	for rows.Next() {
		var r Record
		if err = rows.Scan(
			&r.ID, &r.RequestID, &r.Timestamp, &r.Time, &r.ModelConfig, &r.Model, &r.Provider,
			&r.InputTokens, &r.OutputTokens, &r.CachedTokens, &r.ReasoningTokens, &r.TotalTokens,
			&r.TotalCost, &r.Fee, &r.Status, &r.APIKey, &r.ReasoningEffort, &r.Endpoint, &r.Type, &r.BillingMode,
			&r.FirstTokenMs, &r.LatencyMs, &r.UserAgent, &r.UserID, &r.Error,
		); err != nil {
			return nil, 0, err
		}
		items = append(items, r)
	}
	return items, total, rows.Err()
}

// Stats 聚合图表数据。period: day|week|month
func Stats(ctx context.Context, period string, f ListFilter) (*StatsResult, error) {
	dbMu.RLock()
	conn := global
	dbMu.RUnlock()
	if conn == nil {
		return nil, fmt.Errorf("requestlog db not initialized")
	}
	now := time.Now()
	var from time.Time
	var bucketLayout string
	var bucketMs int64
	// day 为当天零点起按小时分桶，week/month 为滚动 7/30 天按日期分桶；未知 period 回退到 day。
	switch strings.ToLower(period) {
	case "week":
		from = now.AddDate(0, 0, -7)
		bucketLayout = "2006-01-02"
		bucketMs = 24 * 60 * 60 * 1000
	case "month":
		from = now.AddDate(0, 0, -30)
		bucketLayout = "2006-01-02"
		bucketMs = 24 * 60 * 60 * 1000
	default:
		period = "day"
		from = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
		bucketLayout = "15:04"
		bucketMs = 60 * 60 * 1000
	}
	if f.From == 0 {
		f.From = from.UnixMilli()
	}
	if f.To == 0 {
		f.To = now.UnixMilli()
	}

	where, args := buildWhere(f)
	querySQL := `
SELECT id, request_id, created_at, time_text, model_config, model, provider,
  input_tokens, output_tokens, cached_input_tokens, reasoning_tokens, total_tokens,
  total_cost, fee, status, api_key_masked, reasoning_effort, endpoint, type, billing_mode,
  first_token_ms, latency_ms, user_agent, user_id, error_message
FROM web_request_logs` + where + ` ORDER BY created_at ASC`
	rows, err := conn.QueryContext(ctx, querySQL, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	timelineMap := map[string]*StatsBucket{}
	modelMap := map[string]*StatsBucket{}
	statusMap := map[string]int64{}
	costMap := map[string]float64{}
	// 首 token 时延分桶（单位毫秒）：0-200 / 200-500 / 500-1000 / 1000-2000 / 2000+，
	// 最后桶用 1<<62 表示无上限。
	ftBuckets := []struct {
		name string
		max  int64
	}{
		{"0-200ms", 200},
		{"200-500ms", 500},
		{"500-1000ms", 1000},
		{"1000-2000ms", 2000},
		{"2000ms+", 1 << 62},
	}
	ftCounts := make([]int64, len(ftBuckets))

	var sumReq, sumIn, sumOut, sumCached, sumReason, sumTotal, sumLat int64
	var sumCost float64

	for rows.Next() {
		var r Record
		if err = rows.Scan(
			&r.ID, &r.RequestID, &r.Timestamp, &r.Time, &r.ModelConfig, &r.Model, &r.Provider,
			&r.InputTokens, &r.OutputTokens, &r.CachedTokens, &r.ReasoningTokens, &r.TotalTokens,
			&r.TotalCost, &r.Fee, &r.Status, &r.APIKey, &r.ReasoningEffort, &r.Endpoint, &r.Type, &r.BillingMode,
			&r.FirstTokenMs, &r.LatencyMs, &r.UserAgent, &r.UserID, &r.Error,
		); err != nil {
			return nil, err
		}
		t := time.UnixMilli(r.Timestamp)
		// 桶内 key 用固定格式字符串（小时对齐到 "15:00"），保证后续可按字典序排序。
		var key string
		if bucketMs >= 24*60*60*1000 {
			key = t.Format(bucketLayout)
		} else {
			// day 视图按小时整点对齐。
			key = t.Format("15:00")
		}
		tb := timelineMap[key]
		if tb == nil {
			tb = &StatsBucket{Key: key}
			timelineMap[key] = tb
		}
		tb.Requests++
		tb.InputTokens += r.InputTokens
		tb.OutputTokens += r.OutputTokens
		tb.CachedTokens += r.CachedTokens
		tb.ReasoningTokens += r.ReasoningTokens
		tb.TotalTokens += r.TotalTokens
		tb.TotalCost += r.TotalCost

		mk := r.Model
		if mk == "" {
			mk = "unknown"
		}
		mb := modelMap[mk]
		if mb == nil {
			mb = &StatsBucket{Key: mk}
			modelMap[mk] = mb
		}
		mb.Requests++
		mb.InputTokens += r.InputTokens
		mb.OutputTokens += r.OutputTokens
		mb.CachedTokens += r.CachedTokens
		mb.ReasoningTokens += r.ReasoningTokens
		mb.TotalTokens += r.TotalTokens
		mb.TotalCost += r.TotalCost
		costMap[mk] += r.TotalCost

		st := r.Status
		if st == "" {
			st = "unknown"
		}
		statusMap[st]++

		ft := r.FirstTokenMs
		for i, b := range ftBuckets {
			if ft <= b.max {
				ftCounts[i]++
				break
			}
		}

		sumReq++
		sumIn += r.InputTokens
		sumOut += r.OutputTokens
		sumCached += r.CachedTokens
		sumReason += r.ReasoningTokens
		sumTotal += r.TotalTokens
		sumCost += r.TotalCost
		sumLat += r.LatencyMs
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	// 时间线 key 固定为日期或整点字符串，字典序即时间序，用插入排序即可满足小规模排序。
	timeline := make([]StatsBucket, 0, len(timelineMap))
	keys := make([]string, 0, len(timelineMap))
	for k := range timelineMap {
		keys = append(keys, k)
	}
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	for _, k := range keys {
		timeline = append(timeline, *timelineMap[k])
	}

	byModel := make([]map[string]any, 0, len(modelMap))
	for _, m := range modelMap {
		byModel = append(byModel, map[string]any{
			"name":                m.Key,
			"requests":            m.Requests,
			"input_tokens":        m.InputTokens,
			"output_tokens":       m.OutputTokens,
			"cached_input_tokens": m.CachedTokens,
			"reasoning_tokens":    m.ReasoningTokens,
			"total_tokens":        m.TotalTokens,
			"total_cost":          m.TotalCost,
		})
	}
	byStatus := make([]map[string]any, 0, len(statusMap))
	for k, v := range statusMap {
		byStatus = append(byStatus, map[string]any{"name": k, "value": v})
	}
	costByModel := make([]map[string]any, 0, len(costMap))
	for k, v := range costMap {
		costByModel = append(costByModel, map[string]any{"name": k, "value": v})
	}
	firstToken := make([]map[string]any, 0, len(ftBuckets))
	for i, b := range ftBuckets {
		firstToken = append(firstToken, map[string]any{"name": b.name, "value": ftCounts[i]})
	}
	avgLat := int64(0)
	if sumReq > 0 {
		avgLat = sumLat / sumReq
	}
	return &StatsResult{
		Timeline:    timeline,
		ByModel:     byModel,
		ByStatus:    byStatus,
		CostByModel: costByModel,
		FirstToken:  firstToken,
		Summary: map[string]any{
			"period":              period,
			"requests":            sumReq,
			"input_tokens":        sumIn,
			"output_tokens":       sumOut,
			"cached_input_tokens": sumCached,
			"reasoning_tokens":    sumReason,
			"total_tokens":        sumTotal,
			"total_cost":          sumCost,
			"avg_latency_ms":      avgLat,
		},
	}, nil
}

// buildWhere 根据过滤条件拼装 WHERE 子句。
// 全部使用参数化占位符，过滤值只进参数不进 SQL 文本，防止注入。
func buildWhere(f ListFilter) (string, []any) {
	parts := make([]string, 0, 8)
	args := make([]any, 0, 8)
	if f.From > 0 {
		parts = append(parts, "created_at >= ?")
		args = append(args, f.From)
	}
	if f.To > 0 {
		parts = append(parts, "created_at <= ?")
		args = append(args, f.To)
	}
	if s := strings.TrimSpace(f.Model); s != "" {
		parts = append(parts, "model = ?")
		args = append(args, s)
	}
	if s := strings.TrimSpace(f.ModelConfig); s != "" {
		parts = append(parts, "model_config = ?")
		args = append(args, s)
	}
	if s := strings.TrimSpace(f.Status); s != "" {
		parts = append(parts, "status = ?")
		args = append(args, s)
	}
	if s := strings.TrimSpace(f.Provider); s != "" {
		parts = append(parts, "provider = ?")
		args = append(args, s)
	}
	if s := strings.TrimSpace(f.Q); s != "" {
		like := "%" + s + "%"
		parts = append(parts, "(endpoint LIKE ? OR error_message LIKE ? OR request_id LIKE ? OR model LIKE ?)")
		args = append(args, like, like, like, like)
	}
	if len(parts) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(parts, " AND "), args
}
