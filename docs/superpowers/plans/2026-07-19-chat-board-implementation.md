# Chat Board Implementation Plan

> **For agentic workers:** Execute task-by-task. User requested full closed-loop implementation in-session.

**Goal:** Multi-model chat panel + persistent request dashboard at `/web/chat-board.html`.

**Architecture:** Go handlers + SQLite `web_request_logs` + native HTML/JS + ECharts CDN.

**Tech Stack:** Gin, database/sql + modernc.org/sqlite, openairesponses, JWT, ECharts.

## Tasks
1. DB store + config
2. Upload + chat + requests handlers
3. Route wiring
4. Frontend page
5. Verify build/tests

---
