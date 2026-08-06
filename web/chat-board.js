/**
 * web/chat-board.js — 多模型聊天 + 请求看板
 *
 * 页面功能：驱动 chat-board.html 的统一 HTTP（Responses）多模型聊天、附件上传解析、请求记录持久化看板与 ECharts 统计图表；负责 Token 获取、模型列表加载、SSE 流式/JSON 聊天与请求筛选分页。
 * 依赖接口：/test/generate-token、/api/web/models、/api/web/upload、/api/web/chat（支持 SSE）、/api/web/requests、/api/web/requests/stats。
 * 调试用途：验证统一聊天链路、附件上传与请求明细/费用统计，仅本地开发调试。
 */
(function () {
    const COL_KEY = 'tozo-chat-board-columns';
    const ALL_COLS = [
        { key: 'time', label: '时间', def: true },
        { key: 'model', label: '模型', def: true },
        { key: 'input_tokens', label: '输入', def: true },
        { key: 'output_tokens', label: '输出', def: true },
        { key: 'cached_input_tokens', label: '缓存输入', def: true },
        { key: 'reasoning_tokens', label: '思考Token', def: true },
        { key: 'total_tokens', label: '总计', def: true },
        { key: 'total_cost', label: '费用', def: true },
        { key: 'status', label: '状态', def: true },
        { key: 'api_key', label: 'API密钥', def: false },
        { key: 'reasoning_effort', label: '推理强度', def: false },
        { key: 'endpoint', label: '端点', def: false },
        { key: 'type', label: '类型', def: false },
        { key: 'billing_mode', label: '计费模式', def: false },
        { key: 'first_token_ms', label: '首Token耗时', def: false },
        { key: 'latency_ms', label: '耗时', def: false },
        { key: 'user_agent', label: 'User-Agent', def: false },
        { key: 'provider', label: 'Provider', def: false },
        { key: 'request_id', label: 'RequestId', def: false },
        { key: 'model_config', label: '配置名', def: false },
    ];

    const state = {
        token: '',
        models: [],
        messages: [], // {role, content, attachment_ids, error?}
        pendingAttachments: [], // {id,name,mime,kind,size}
        page: 1,
        size: 20,
        total: 0,
        items: [],
        cols: loadCols(),
        charts: {},
    };

    const $ = (id) => document.getElementById(id);

    document.addEventListener('DOMContentLoaded', () => {
        $('token-api').value = `${location.origin}/test/generate-token`;
        initMobileView();
        bindUI();
        renderColPicker();
        renderTableHeader();
        appendMsg('system', '聊天看板已加载。请获取 Token 后选择模型发送消息。');
        generateToken().then(() => {
            loadModels();
            refreshList();
            refreshStats();
        });
    });

    function isMobileLayout() {
        return window.matchMedia('(max-width: 768px)').matches;
    }

    function setMainView(view) {
        const v = view === 'board' ? 'board' : 'chat';
        document.body.setAttribute('data-view', v);
        try { localStorage.setItem('tozo-chat-board-view', v); } catch (_) {}
        document.querySelectorAll('.view-btn').forEach((btn) => {
            btn.classList.toggle('active', btn.dataset.view === v);
        });
        if (v === 'board') {
            refreshList();
            refreshStats();
            setTimeout(resizeCharts, 80);
        }
    }

    function initMobileView() {
        let saved = 'chat';
        try { saved = localStorage.getItem('tozo-chat-board-view') || 'chat'; } catch (_) {}
        document.body.setAttribute('data-view', saved === 'board' ? 'board' : 'chat');
        document.querySelectorAll('.view-btn').forEach((btn) => {
            btn.classList.toggle('active', btn.dataset.view === document.body.getAttribute('data-view'));
        });
    }

    function resizeCharts() {
        Object.values(state.charts).forEach((c) => {
            try { c && c.resize(); } catch (_) {}
        });
    }

    function bindUI() {
        $('btn-token').onclick = () => generateToken();
        $('btn-reload-models').onclick = () => loadModels();
        $('btn-send').onclick = () => sendChat();
        $('btn-clear-chat').onclick = () => {
            state.messages = [];
            $('messages').innerHTML = '';
            appendMsg('system', '会话已清空（不影响看板历史）');
        };
        $('btn-upload').onclick = () => $('file-input').click();
        $('file-input').onchange = (e) => uploadFiles(e.target.files);
        $('chat-input').addEventListener('paste', onPaste);
        $('chat-input').addEventListener('keydown', (e) => {
            // 手机端 Enter 换行，桌面 Ctrl/Cmd+Enter 或非 shift Enter 发送
            if (e.key === 'Enter' && !e.shiftKey) {
                if (isMobileLayout() && !e.ctrlKey && !e.metaKey) return;
                e.preventDefault();
                sendChat();
            }
        });

        document.querySelectorAll('.view-btn').forEach((btn) => {
            btn.onclick = () => setMainView(btn.dataset.view);
        });

        document.querySelectorAll('.tab-btn').forEach((btn) => {
            btn.onclick = () => {
                document.querySelectorAll('.tab-btn').forEach((b) => b.classList.remove('active'));
                btn.classList.add('active');
                const tab = btn.dataset.tab;
                $('tab-list').hidden = tab !== 'list';
                $('tab-charts').hidden = tab !== 'charts';
                if (tab === 'charts') {
                    refreshStats();
                    setTimeout(resizeCharts, 50);
                }
            };
        });

        $('btn-filter').onclick = () => { state.page = 1; refreshList(); refreshStats(); };
        $('btn-reset-filter').onclick = () => {
            ['f-from', 'f-to', 'f-model', 'f-config', 'f-q'].forEach((id) => $(id).value = '');
            $('f-status').value = '';
            state.page = 1;
            refreshList();
            refreshStats();
        };
        $('btn-refresh-list').onclick = () => refreshList();
        $('btn-prev').onclick = () => { if (state.page > 1) { state.page--; refreshList(); } };
        $('btn-next').onclick = () => {
            const max = Math.max(1, Math.ceil(state.total / state.size));
            if (state.page < max) { state.page++; refreshList(); }
        };
        $('stats-period').onchange = () => refreshStats();

        let resizeTimer = null;
        window.addEventListener('resize', () => {
            clearTimeout(resizeTimer);
            resizeTimer = setTimeout(resizeCharts, 120);
        });
        window.addEventListener('orientationchange', () => setTimeout(resizeCharts, 200));

        if (typeof window.initThemeFromSelect === 'function') {
            // theme.js may auto-bind
        } else if ($('theme-select')) {
            const saved = localStorage.getItem('tozo-ws-theme') || 'dark';
            $('theme-select').value = saved;
            document.body.classList.remove('theme-dark', 'theme-light', 'theme-ocean', 'theme-sepia', 'theme-contrast');
            if (saved !== 'dark') document.body.classList.add(`theme-${saved}`);
            $('theme-select').onchange = () => {
                const t = $('theme-select').value;
                document.body.classList.remove('theme-dark', 'theme-light', 'theme-ocean', 'theme-sepia', 'theme-contrast');
                if (t !== 'dark') document.body.classList.add(`theme-${t}`);
                localStorage.setItem('tozo-ws-theme', t);
                setTimeout(resizeCharts, 50);
            };
        }
    }

    function loadCols() {
        try {
            const raw = localStorage.getItem(COL_KEY);
            if (raw) return JSON.parse(raw);
        } catch (_) {}
        const o = {};
        ALL_COLS.forEach((c) => { o[c.key] = c.def; });
        return o;
    }

    function saveCols() {
        localStorage.setItem(COL_KEY, JSON.stringify(state.cols));
    }

    function renderColPicker() {
        const box = $('col-picker');
        box.innerHTML = '<span style="width:100%;font-weight:600;">显示列：</span>';
        ALL_COLS.forEach((c) => {
            const id = `col-${c.key}`;
            const lab = document.createElement('label');
            lab.innerHTML = `<input type="checkbox" id="${id}" ${state.cols[c.key] ? 'checked' : ''}> ${c.label}`;
            box.appendChild(lab);
            lab.querySelector('input').onchange = (e) => {
                state.cols[c.key] = e.target.checked;
                saveCols();
                renderTableHeader();
                renderTableBody();
            };
        });
    }

    function visibleCols() {
        return ALL_COLS.filter((c) => state.cols[c.key]);
    }

    function renderTableHeader() {
        const tr = document.createElement('tr');
        visibleCols().forEach((c) => {
            const th = document.createElement('th');
            th.textContent = c.label;
            tr.appendChild(th);
        });
        $('req-thead').innerHTML = '';
        $('req-thead').appendChild(tr);
    }

    function cellValue(item, key) {
        const v = item[key];
        if (key === 'total_cost' || key === 'fee') return Number(v || 0).toFixed(6);
        if (key === 'status') return v || '-';
        if (v === undefined || v === null || v === '') return '-';
        return String(v);
    }

    function renderTableBody() {
        const tbody = $('req-tbody');
        tbody.innerHTML = '';
        const cols = visibleCols();
        state.items.forEach((item, idx) => {
            const tr = document.createElement('tr');
            cols.forEach((c) => {
                const td = document.createElement('td');
                const text = cellValue(item, c.key);
                td.textContent = text;
                if (c.key === 'status') {
                    td.className = String(item.status).includes('fail') || item.status === 'failed' ? 'status-fail' : 'status-ok';
                }
                tr.appendChild(td);
            });
            tr.onclick = () => {
                tbody.querySelectorAll('tr').forEach((r) => r.classList.remove('selected'));
                tr.classList.add('selected');
                showRecordDetail($('row-detail'), item);
            };
            tbody.appendChild(tr);
            if (idx === 0) {
                // auto select first
            }
        });
        const max = Math.max(1, Math.ceil(state.total / state.size) || 1);
        $('page-info').textContent = `第 ${state.page}/${max} 页 · 共 ${state.total} 条`;
    }

    function showRecordDetail(el, rec) {
        if (!rec) {
            el.textContent = '暂无';
            return;
        }
        const lines = [
            `时间: ${rec.time || '-'}`,
            `RequestId: ${rec.request_id || '-'}`,
            `状态: ${rec.status || '-'}`,
            `模型: ${rec.model || '-'}  配置: ${rec.model_config || '-'}  Provider: ${rec.provider || '-'}`,
            `端点: ${rec.endpoint || '-'}`,
            `类型: ${rec.type || '-'}  计费: ${rec.billing_mode || '-'}  推理: ${rec.reasoning_effort || '-'}`,
            `API Key: ${rec.api_key || '-'}`,
            `—— Token 明细 ——`,
            `输入: ${rec.input_tokens ?? 0}`,
            `输出: ${rec.output_tokens ?? 0}`,
            `缓存输入: ${rec.cached_input_tokens ?? 0}`,
            `思考: ${rec.reasoning_tokens ?? 0}`,
            `总计: ${rec.total_tokens ?? 0}`,
            `费用: ${Number(rec.total_cost || 0).toFixed(6)}`,
            `首 Token: ${rec.first_token_ms ?? 0} ms  总耗时: ${rec.latency_ms ?? 0} ms`,
            `User-Agent: ${rec.user_agent || '-'}`,
            rec.error ? `错误: ${rec.error}` : '',
        ].filter(Boolean);
        el.textContent = lines.join('\n');
    }

    async function generateToken() {
        const userId = $('user-id').value || '1001';
        let base = $('token-api').value || `${location.origin}/test/generate-token`;
        const sep = base.includes('?') ? '&' : '?';
        const url = `${base}${sep}userId=${encodeURIComponent(userId)}`;
        try {
            const resp = await fetch(url);
            const data = await resp.json();
            if (data.token) {
                state.token = data.token;
                $('token').value = data.token;
                appendMsg('system', 'Token 获取成功');
                return true;
            }
            appendMsg('system', 'Token 获取失败: ' + JSON.stringify(data));
        } catch (e) {
            appendMsg('system', 'Token 请求失败: ' + e.message);
        }
        return false;
    }

    function authHeaders(json) {
        const h = {};
        if (json) h['Content-Type'] = 'application/json';
        const token = $('token').value || state.token;
        if (token) h['Authorization'] = `Bearer ${token}`;
        return h;
    }

    async function ensureToken() {
        if ($('token').value || state.token) return true;
        return generateToken();
    }

    async function loadModels() {
        await ensureToken();
        try {
            const resp = await fetch('/api/web/models', { headers: authHeaders() });
            const data = await resp.json();
            // 仅展示适合统一聊天（Responses HTTP）的配置：启用、有 endpoint、有 Key、非 Azure
            const list = (data.data || []).filter((m) => {
                if (!m.enabled) return false;
                if (!m.api_key_configured) return false;
                if (!m.endpoint) return false;
                const t = String(m.type || '').toLowerCase();
                const n = String(m.name || '').toLowerCase();
                if (t === 'azure' || n === 'azureai') return false;
                return true;
            });
            state.models = list;
            const sel = $('model-config');
            sel.innerHTML = '';
            list.forEach((m) => {
                const opt = document.createElement('option');
                opt.value = m.name;
                opt.textContent = `${m.name} · ${m.default_model || '-'} · ${m.type || ''}`;
                sel.appendChild(opt);
            });
            if ([...sel.options].some((o) => o.value === 'openairesponses')) {
                sel.value = 'openairesponses';
            }
            if (!list.length) appendMsg('system', '没有可用的聊天模型（需 enabled + API Key + HTTP endpoint，且非 Azure）');
        } catch (e) {
            appendMsg('system', '加载模型失败: ' + e.message);
        }
    }

    function appendMsg(role, content, extra) {
        const box = $('messages');
        const div = document.createElement('div');
        div.className = `msg ${role}`;
        const bubble = document.createElement('div');
        bubble.className = 'bubble';
        bubble.textContent = content;
        div.appendChild(bubble);
        if (extra && extra.meta) {
            const meta = document.createElement('div');
            meta.className = 'meta';
            meta.textContent = extra.meta;
            div.appendChild(meta);
        }
        if (extra && extra.error) {
            const err = document.createElement('div');
            err.className = 'err';
            err.textContent = extra.error;
            div.appendChild(err);
        }
        box.appendChild(div);
        box.scrollTop = box.scrollHeight;
        return bubble;
    }

    function renderPendingAttachments() {
        const box = $('attach-list');
        box.innerHTML = '';
        state.pendingAttachments.forEach((a, idx) => {
            const chip = document.createElement('span');
            chip.className = 'attach-chip';
            chip.innerHTML = `${escapeHtml(a.kind)}: ${escapeHtml(a.name)} <button type="button" title="移除">×</button>`;
            chip.querySelector('button').onclick = () => {
                state.pendingAttachments.splice(idx, 1);
                renderPendingAttachments();
            };
            box.appendChild(chip);
        });
    }

    function escapeHtml(s) {
        const d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    }

    async function uploadFiles(fileList) {
        if (!fileList || !fileList.length) return;
        await ensureToken();
        for (const file of fileList) {
            const fd = new FormData();
            fd.append('file', file);
            try {
                const resp = await fetch('/api/web/upload', {
                    method: 'POST',
                    headers: authHeaders(false),
                    body: fd,
                });
                const data = await resp.json();
                if (data.code !== 200) {
                    appendMsg('system', `上传失败 ${file.name}: ${data.error || resp.status}`);
                    continue;
                }
                state.pendingAttachments.push(data.data);
                appendMsg('system', `已上传: ${data.data.name} (${data.data.kind})`);
            } catch (e) {
                appendMsg('system', `上传异常 ${file.name}: ${e.message}`);
            }
        }
        $('file-input').value = '';
        renderPendingAttachments();
    }

    async function onPaste(e) {
        const items = e.clipboardData && e.clipboardData.items;
        if (!items) return;
        const files = [];
        for (const it of items) {
            if (it.type && it.type.startsWith('image/')) {
                const f = it.getAsFile();
                if (f) files.push(f);
            }
        }
        if (files.length) {
            e.preventDefault();
            await uploadFiles(files);
        }
    }

    async function sendChat() {
        const text = ($('chat-input').value || '').trim();
        const attachIds = state.pendingAttachments.map((a) => a.id);
        if (!text && !attachIds.length) return;
        await ensureToken();

        const userMsg = {
            role: 'user',
            content: text,
            attachment_ids: attachIds.slice(),
        };
        state.messages.push(userMsg);
        appendMsg('user', text || '(仅附件)');
        $('chat-input').value = '';
        state.pendingAttachments = [];
        renderPendingAttachments();

        const bubble = appendMsg('assistant', '…');
        const body = {
            model_config: $('model-config').value,
            model: ($('model-name').value || '').trim(),
            reasoning_effort: $('reasoning').value,
            // 只上传 user/assistant，避免 system 气泡进上游；附件仅挂在 user 消息上
            messages: state.messages
                .filter((m) => m.role === 'user' || m.role === 'assistant')
                .map((m) => ({
                    role: m.role,
                    content: m.content || '',
                    attachment_ids: m.role === 'user' ? (m.attachment_ids || []) : [],
                })),
            stream: $('use-stream').checked,
        };

        try {
            if (body.stream) {
                await sendChatStream(body, bubble);
            } else {
                await sendChatJSON(body, bubble);
            }
        } catch (e) {
            bubble.textContent = '';
            appendMsg('system', '发送失败: ' + e.message);
        }
        refreshList();
        refreshStats();
        // 手机端发完消息后提示可切到看板看明细（不强制跳转）
        if (isMobileLayout() && $('request-detail').textContent && $('request-detail').textContent !== '暂无') {
            // keep user in chat; board data already refreshed
        }
    }

    async function sendChatJSON(body, bubble) {
        const resp = await fetch('/api/web/chat', {
            method: 'POST',
            headers: authHeaders(true),
            body: JSON.stringify(body),
        });
        const data = await resp.json();
        if (data.code !== 200) {
            bubble.textContent = data.error_summary || data.error || '请求失败';
            state.messages.push({ role: 'assistant', content: bubble.textContent });
            if (data.record) showRecordDetail($('request-detail'), data.record);
            return;
        }
        const text = (data.data && data.data.output_text) || '';
        bubble.textContent = text || '(空响应)';
        state.messages.push({ role: 'assistant', content: text });
        if (data.record) showRecordDetail($('request-detail'), data.record);
    }

    async function sendChatStream(body, bubble) {
        const resp = await fetch('/api/web/chat', {
            method: 'POST',
            headers: authHeaders(true),
            body: JSON.stringify(body),
        });
        if (!resp.ok) {
            const t = await resp.text();
            throw new Error(t || resp.statusText);
        }
        const reader = resp.body.getReader();
        const decoder = new TextDecoder();
        let buf = '';
        let full = '';
        bubble.textContent = '';

        let finished = false;
        const handleBlock = (block) => {
            const lines = block.split('\n');
            let event = 'message';
            let dataLine = '';
            for (const line of lines) {
                if (line.startsWith('event:')) event = line.slice(6).trim();
                if (line.startsWith('data:')) dataLine += line.slice(5).trim();
            }
            if (!dataLine) return;
            let payload;
            try { payload = JSON.parse(dataLine); } catch { return; }
            if (event === 'delta' && payload.text) {
                full += payload.text;
                bubble.textContent = full;
                $('messages').scrollTop = $('messages').scrollHeight;
            } else if (event === 'done') {
                full = payload.output_text || full;
                bubble.textContent = full || '(空响应)';
                state.messages.push({ role: 'assistant', content: full });
                if (payload.record) showRecordDetail($('request-detail'), payload.record);
                finished = true;
            } else if (event === 'error') {
                bubble.textContent = payload.error || '错误';
                state.messages.push({ role: 'assistant', content: bubble.textContent });
                if (payload.record) showRecordDetail($('request-detail'), payload.record);
                finished = true;
            }
        };

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            buf += decoder.decode(value, { stream: true });
            const parts = buf.split('\n\n');
            buf = parts.pop() || '';
            parts.forEach(handleBlock);
        }
        // 刷尾包，避免最后一帧未以 \n\n 结尾时丢失 done
        if (buf.trim()) handleBlock(buf);
        if (!finished && full) {
            bubble.textContent = full;
            state.messages.push({ role: 'assistant', content: full });
        } else if (!finished) {
            bubble.textContent = bubble.textContent || '未收到完整响应';
            state.messages.push({ role: 'assistant', content: bubble.textContent });
        }
    }

    function filterQuery() {
        const p = new URLSearchParams();
        p.set('page', String(state.page));
        p.set('size', String(state.size));
        const from = $('f-from').value;
        const to = $('f-to').value;
        if (from) p.set('from', String(new Date(from).getTime()));
        if (to) p.set('to', String(new Date(to).getTime()));
        if ($('f-model').value.trim()) p.set('model', $('f-model').value.trim());
        if ($('f-status').value) p.set('status', $('f-status').value);
        if ($('f-config').value.trim()) p.set('model_config', $('f-config').value.trim());
        if ($('f-q').value.trim()) p.set('q', $('f-q').value.trim());
        return p;
    }

    async function refreshList() {
        await ensureToken();
        try {
            const resp = await fetch('/api/web/requests?' + filterQuery().toString(), { headers: authHeaders() });
            const data = await resp.json();
            if (data.code !== 200) {
                $('row-detail').textContent = data.error || '加载失败';
                return;
            }
            state.items = data.items || [];
            state.total = data.total || 0;
            renderTableHeader();
            renderTableBody();
        } catch (e) {
            $('row-detail').textContent = '加载列表失败: ' + e.message;
        }
    }

    async function refreshStats() {
        await ensureToken();
        if (typeof echarts === 'undefined') return;
        try {
            const p = filterQuery();
            p.set('period', $('stats-period').value || 'day');
            p.delete('page');
            p.delete('size');
            const resp = await fetch('/api/web/requests/stats?' + p.toString(), { headers: authHeaders() });
            const data = await resp.json();
            if (data.code !== 200) return;
            const s = data.data || {};
            renderCharts(s);
        } catch (_) {}
    }

    function ensureChart(id) {
        if (!state.charts[id]) {
            state.charts[id] = echarts.init($(id));
        }
        return state.charts[id];
    }

    function renderCharts(s) {
        const tl = s.timeline || [];
        ensureChart('chart-timeline').setOption({
            title: { text: '请求量', left: 'center', textStyle: { fontSize: 13, color: '#8b949e' } },
            tooltip: { trigger: 'axis' },
            xAxis: { type: 'category', data: tl.map((x) => x.key) },
            yAxis: { type: 'value' },
            series: [{ type: 'line', data: tl.map((x) => x.requests), smooth: true, areaStyle: {} }],
            grid: { left: 40, right: 16, top: 40, bottom: 28 },
        });

        const models = s.by_model || [];
        ensureChart('chart-tokens').setOption({
            title: { text: 'Token 堆叠（按模型）', left: 'center', textStyle: { fontSize: 13, color: '#8b949e' } },
            tooltip: { trigger: 'axis' },
            legend: { bottom: 0, textStyle: { color: '#8b949e' } },
            xAxis: { type: 'category', data: models.map((m) => m.name) },
            yAxis: { type: 'value' },
            series: [
                { name: '输入', type: 'bar', stack: 't', data: models.map((m) => m.input_tokens) },
                { name: '输出', type: 'bar', stack: 't', data: models.map((m) => m.output_tokens) },
                { name: '缓存', type: 'bar', stack: 't', data: models.map((m) => m.cached_input_tokens) },
                { name: '思考', type: 'bar', stack: 't', data: models.map((m) => m.reasoning_tokens) },
            ],
            grid: { left: 48, right: 16, top: 40, bottom: 48 },
        });

        const cost = s.cost_by_model || [];
        ensureChart('chart-cost').setOption({
            title: { text: '费用分布', left: 'center', textStyle: { fontSize: 13, color: '#8b949e' } },
            tooltip: { trigger: 'item' },
            series: [{ type: 'pie', radius: ['35%', '65%'], data: cost.map((x) => ({ name: x.name, value: x.value })) }],
        });

        const st = s.by_status || [];
        ensureChart('chart-status').setOption({
            title: { text: '状态分布', left: 'center', textStyle: { fontSize: 13, color: '#8b949e' } },
            tooltip: { trigger: 'item' },
            series: [{ type: 'pie', radius: '60%', data: st.map((x) => ({ name: x.name, value: x.value })) }],
        });

        const ft = s.first_token || [];
        ensureChart('chart-ft').setOption({
            title: { text: '首 Token 耗时分布', left: 'center', textStyle: { fontSize: 13, color: '#8b949e' } },
            tooltip: {},
            xAxis: { type: 'category', data: ft.map((x) => x.name) },
            yAxis: { type: 'value' },
            series: [{ type: 'bar', data: ft.map((x) => x.value) }],
            grid: { left: 40, right: 16, top: 40, bottom: 28 },
        });
    }
})();
