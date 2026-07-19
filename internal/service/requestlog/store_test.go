package requestlog

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestInsertListStats(t *testing.T) {
	dir := t.TempDir()
	dsn := filepath.Join(dir, "test.db")
	if err := Init(true, "sqlite", dsn); err != nil {
		t.Fatalf("init: %v", err)
	}
	t.Cleanup(Close)

	ctx := context.Background()
	now := time.Now()
	rec := Record{
		RequestID:       "r1",
		Timestamp:       now.UnixMilli(),
		Time:            now.Format("2006-01-02 15:04:05"),
		ModelConfig:     "openairesponses",
		Model:           "gpt-4.1",
		Provider:        "openairesponses",
		InputTokens:     10,
		OutputTokens:    20,
		CachedTokens:    2,
		ReasoningTokens: 1,
		TotalTokens:     31,
		TotalCost:       0.01,
		Fee:             0.01,
		Status:          "completed",
		APIKey:          "sk-***",
		Endpoint:        "https://example.com/v1",
		Type:            "openai",
		BillingMode:     "token",
		FirstTokenMs:    120,
		LatencyMs:       300,
		UserAgent:       "test",
		UserID:          "u1",
	}
	got, err := Insert(ctx, rec)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got.ID == 0 {
		t.Fatal("expected id")
	}

	items, total, err := List(ctx, ListFilter{Page: 1, Size: 10, Model: "gpt-4.1"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("list total=%d len=%d", total, len(items))
	}
	if items[0].OutputTokens != 20 {
		t.Fatalf("output tokens=%d", items[0].OutputTokens)
	}

	stats, err := Stats(ctx, "day", ListFilter{})
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Summary["requests"].(int64) != 1 {
		t.Fatalf("stats requests=%v", stats.Summary["requests"])
	}

	// ensure file exists
	if _, err := os.Stat(dsn); err != nil {
		t.Fatalf("db file: %v", err)
	}
}
