(function () {
    // KEY 是所有 Web 调试页共享的颜色模式存储键。
    // index.html 修改主题后，audio/chat/redis/diagnostics/responses/azure 等页面都会读取同一个值。
    const KEY = 'tozo-ws-theme';
    // THEMES 是允许写入 body/html class 的白名单，避免外部输入拼出任意 class。
    const THEMES = ['dark', 'light', 'ocean', 'sepia', 'contrast'];
    const CLASSES = THEMES.map(name => `theme-${name}`);

    // styleText 是跨页面的兜底主题变量。
    // WebStaticHandler 会给 HTML 自动注入本脚本，因此新增页面只要使用这些 CSS 变量就能继承统一主题。
    const styleText = `
body.theme-dark {
    --bg-primary:#0d1117; --bg-secondary:#161b22; --bg-tertiary:#21262d; --border:#30363d;
    --text-primary:#e6edf3; --text-secondary:#8b949e; --text-muted:#6e7681;
    --accent-blue:#58a6ff; --accent-green:#3fb950; --accent-red:#f85149; --accent-yellow:#d29922; --accent-purple:#bc8cff; --accent-orange:#f0883e;
    --bg:#0d1117; --bg2:#161b22; --bg3:#21262d; --text:#e6edf3; --text2:#8b949e; --blue:#58a6ff; --green:#3fb950; --red:#f85149; --yellow:#d29922; --purple:#bc8cff;
    --surface:#161b22; --surface-2:#21262d; --line:#30363d; --muted:#8b949e; --accent:#2ea043; --accent-2:#3fb950; --danger:#f85149; --warn:#d29922;
    color-scheme:dark;
}
body.theme-light {
    --bg-primary:#f6f8fa; --bg-secondary:#ffffff; --bg-tertiary:#f0f3f6; --border:#d0d7de;
    --text-primary:#1f2328; --text-secondary:#57606a; --text-muted:#6e7781;
    --accent-blue:#0969da; --accent-green:#1a7f37; --accent-red:#cf222e; --accent-yellow:#9a6700; --accent-purple:#8250df; --accent-orange:#bc4c00;
    --bg:#f6f8fa; --bg2:#ffffff; --bg3:#f0f3f6; --text:#1f2328; --text2:#57606a; --blue:#0969da; --green:#1a7f37; --red:#cf222e; --yellow:#9a6700; --purple:#8250df;
    --surface:#ffffff; --surface-2:#f0f3f6; --line:#d0d7de; --muted:#57606a; --accent:#1a7f37; --accent-2:#116329; --danger:#cf222e; --warn:#9a6700;
    color-scheme:light;
}
body.theme-ocean {
    --bg-primary:#07151f; --bg-secondary:#0d2230; --bg-tertiary:#123144; --border:#24506b;
    --text-primary:#e8f7ff; --text-secondary:#9bc4d8; --text-muted:#75a6bb;
    --accent-blue:#58c7ff; --accent-green:#6ee7b7; --accent-red:#fb7185; --accent-yellow:#facc15; --accent-purple:#c084fc; --accent-orange:#fb923c;
    --bg:#07151f; --bg2:#0d2230; --bg3:#123144; --text:#e8f7ff; --text2:#9bc4d8; --blue:#58c7ff; --green:#6ee7b7; --red:#fb7185; --yellow:#facc15; --purple:#c084fc;
    --surface:#0d2230; --surface-2:#123144; --line:#24506b; --muted:#9bc4d8; --accent:#2dd4bf; --accent-2:#14b8a6; --danger:#fb7185; --warn:#facc15;
    color-scheme:dark;
}
body.theme-sepia {
    --bg-primary:#f7f1e6; --bg-secondary:#fffaf0; --bg-tertiary:#efe3cf; --border:#d7c3a5;
    --text-primary:#332817; --text-secondary:#6d5a3a; --text-muted:#8a7656;
    --accent-blue:#2f6f9f; --accent-green:#3f7d3f; --accent-red:#a23b32; --accent-yellow:#946200; --accent-purple:#7c4d8f; --accent-orange:#a85f22;
    --bg:#f7f1e6; --bg2:#fffaf0; --bg3:#efe3cf; --text:#332817; --text2:#6d5a3a; --blue:#2f6f9f; --green:#3f7d3f; --red:#a23b32; --yellow:#946200; --purple:#7c4d8f;
    --surface:#fffaf0; --surface-2:#efe3cf; --line:#d7c3a5; --muted:#6d5a3a; --accent:#3f7d3f; --accent-2:#2f6630; --danger:#a23b32; --warn:#946200;
    color-scheme:light;
}
body.theme-contrast {
    --bg-primary:#000000; --bg-secondary:#111111; --bg-tertiary:#1e1e1e; --border:#ffffff;
    --text-primary:#ffffff; --text-secondary:#f5f5f5; --text-muted:#d0d0d0;
    --accent-blue:#00b7ff; --accent-green:#00ff66; --accent-red:#ff3b30; --accent-yellow:#ffdd00; --accent-purple:#d783ff; --accent-orange:#ff9f0a;
    --bg:#000000; --bg2:#111111; --bg3:#1e1e1e; --text:#ffffff; --text2:#f5f5f5; --blue:#00b7ff; --green:#00ff66; --red:#ff3b30; --yellow:#ffdd00; --purple:#d783ff;
    --surface:#111111; --surface-2:#1e1e1e; --line:#ffffff; --muted:#f5f5f5; --accent:#00ff66; --accent-2:#00cc52; --danger:#ff3b30; --warn:#ffdd00;
    color-scheme:dark;
}
body[class*="theme-"] {
    background:var(--bg-primary, var(--bg));
    color:var(--text-primary, var(--text));
}
body[class*="theme-"] .header-bar,
body[class*="theme-"] .stat-card,
body[class*="theme-"] .table-wrap,
body[class*="theme-"] .billing-panel,
body[class*="theme-"] .billing-card {
    background:var(--bg-secondary, var(--bg2));
    color:var(--text-primary, var(--text));
    border-color:var(--border);
}
body[class*="theme-"] .header-bar h2,
body[class*="theme-"] .billing-panel h3,
body[class*="theme-"] .stat-card .num,
body[class*="theme-"] .billing-card .value {
    color:var(--accent-blue, var(--blue));
}
body[class*="theme-"] .header-bar .subtitle,
body[class*="theme-"] .stat-card .label,
body[class*="theme-"] .billing-card .name,
body[class*="theme-"] .key-desc {
    color:var(--text-secondary, var(--text2));
}
body[class*="theme-"] .json-value,
body[class*="theme-"] .mini-table th,
body[class*="theme-"] .mini-table td {
    background:var(--bg-tertiary, var(--bg3));
    color:var(--text-primary, var(--text));
    border-color:var(--border);
}
body[class*="theme-"] input,
body[class*="theme-"] select,
body[class*="theme-"] textarea,
body[class*="theme-"] .layui-input,
body[class*="theme-"] .layui-select,
body[class*="theme-"] .layui-textarea {
    background:var(--bg-tertiary, var(--bg3));
    color:var(--text-primary, var(--text));
    border-color:var(--border);
}
body[class*="theme-"] .layui-table-view,
body[class*="theme-"] .layui-table,
body[class*="theme-"] .layui-table thead tr,
body[class*="theme-"] .layui-table-header,
body[class*="theme-"] .layui-table-body,
body[class*="theme-"] .layui-table tbody tr {
    background:var(--bg-secondary, var(--bg2));
    color:var(--text-primary, var(--text));
}
body[class*="theme-"] .layui-table th,
body[class*="theme-"] .layui-table td {
    border-color:var(--border);
    color:var(--text-primary, var(--text));
}
body[class*="theme-"] .layui-table-hover,
body[class*="theme-"] .layui-table tbody tr:hover {
    background:var(--bg-tertiary, var(--bg3)) !important;
}
body[class*="theme-"] a {
    color:var(--accent-blue, var(--blue));
}
body[class*="theme-"] .topbar,
body[class*="theme-"] .pane,
body[class*="theme-"] .composer,
body[class*="theme-"] .modal-card {
    background:var(--surface, var(--bg-secondary, var(--bg2)));
    color:var(--text-primary, var(--text));
    border-color:var(--line, var(--border));
}
body[class*="theme-"] .workspace {
    background:var(--line, var(--border));
}
body[class*="theme-"] .pane-header,
body[class*="theme-"] .editor-status {
    color:var(--text-secondary, var(--muted));
    border-color:var(--line, var(--border));
}
body[class*="theme-"] .brand span,
body[class*="theme-"] label,
body[class*="theme-"] .project-meta,
body[class*="theme-"] .file-path,
body[class*="theme-"] .file-type,
body[class*="theme-"] .msg .role,
body[class*="theme-"] .small {
    color:var(--text-secondary, var(--muted));
}
body[class*="theme-"] button {
    background:var(--surface, var(--bg-secondary, var(--bg2)));
    color:var(--text-primary, var(--text));
    border-color:var(--line, var(--border));
}
body[class*="theme-"] button.primary {
    background:var(--accent, var(--accent-green, var(--green)));
    border-color:var(--accent, var(--accent-green, var(--green)));
    color:#fff;
}
body[class*="theme-"] button.danger {
    color:var(--danger, var(--accent-red, var(--red)));
}
body[class*="theme-"] .status,
body[class*="theme-"] .msg .content {
    background:var(--surface-2, var(--bg-tertiary, var(--bg3)));
    color:var(--text-primary, var(--text));
    border-color:var(--line, var(--border));
}
body[class*="theme-"] .chat-log {
    background:var(--bg-primary, var(--bg));
}
body[class*="theme-"] .msg.user .content,
body[class*="theme-"] .file-item.active {
    background:var(--bg-tertiary, var(--bg3));
    color:var(--text-primary, var(--text));
    border-color:var(--line, var(--border));
}
body[class*="theme-"] .file-item {
    color:var(--text-primary, var(--text));
}
body[class*="theme-"] .file-item:hover {
    background:var(--surface-2, var(--bg-tertiary, var(--bg3)));
    color:var(--text-primary, var(--text));
}
body[class*="theme-"] .connection-panel,
body[class*="theme-"] #editor.chat-output,
body[class*="theme-"] .inspector-body,
body[class*="theme-"] .token-card,
body[class*="theme-"] .chart-box,
body[class*="theme-"] .raw-event {
    color:var(--text-primary, var(--text));
    border-color:var(--line, var(--border));
}
body[class*="theme-"] .connection-panel,
body[class*="theme-"] .token-card,
body[class*="theme-"] .chart-box,
body[class*="theme-"] .raw-event {
    background:var(--surface, var(--bg-secondary, var(--bg2)));
}
body[class*="theme-"] #editor.chat-output,
body[class*="theme-"] .inspector-body,
body[class*="theme-"] .chat-log {
    background:var(--bg-primary, var(--bg));
}
body[class*="theme-"] .raw-event header {
    color:var(--text-secondary, var(--muted));
    border-color:var(--line, var(--border));
}
body[class*="theme-"] .raw-event pre,
body[class*="theme-"] .pending-write pre {
    background:#0b1220;
    color:#dbeafe;
}
`;

    // normalize 只允许已登记的主题名，localStorage 被手工改坏时回退到 dark。
    function normalize(theme) {
        return THEMES.includes(theme) ? theme : 'dark';
    }

    // getStoredTheme 从浏览器本地存储读取当前主题。
    // localStorage 在隐私模式或嵌入环境可能不可用，此时不阻断页面渲染。
    function getStoredTheme() {
        try {
            return normalize(localStorage.getItem(KEY) || 'dark');
        } catch {
            return 'dark';
        }
    }

    // setStoredTheme 持久化主题选择。
    // 存储失败只影响跨页面记忆，不影响当前页面立即应用主题。
    function setStoredTheme(theme) {
        try {
            localStorage.setItem(KEY, theme);
        } catch {
            // 存储失败时保持当前页主题即可，不能让 UI 初始化失败。
        }
    }

    // ensureStyle 保证共享主题样式只注入一次。
    // 部分页面已经有自己的样式，本样式只提供变量和常见组件覆盖，不替换页面布局。
    function ensureStyle() {
        if (document.getElementById('tozo-shared-theme-style')) return;
        const style = document.createElement('style');
        style.id = 'tozo-shared-theme-style';
        style.textContent = styleText;
        (document.head || document.documentElement).appendChild(style);
    }

    // setClass 同时作用在 html 和 body，兼容不同页面把变量挂在不同根节点的写法。
    function setClass(target, theme) {
        if (!target) return;
        target.classList.remove(...CLASSES);
        target.classList.add(`theme-${theme}`);
    }

    // syncSelector 让页面上的 #theme-select 与真实主题保持一致。
    // 新增页面只要提供同名 select，就自动获得双向同步能力。
    function syncSelector(theme) {
        const select = document.getElementById('theme-select');
        if (select && select.value !== theme) {
            select.value = theme;
        }
    }

    // applyTheme 是主题系统唯一写入口。
    // persist=false 用于启动和跨标签同步，避免 storage 事件来回写入造成重复广播。
    function applyTheme(theme, options) {
        const normalized = normalize(theme);
        ensureStyle();
        setClass(document.documentElement, normalized);
        setClass(document.body, normalized);
        syncSelector(normalized);
        if (!options || options.persist !== false) {
            setStoredTheme(normalized);
        }
        document.dispatchEvent(new CustomEvent('tozo-theme-change', { detail: { theme: normalized } }));
    }

    // bindSelector 绑定当前页面的颜色模式下拉框。
    // data-tozo-theme-bound 防止页面局部重渲染后重复注册 change 监听器。
    function bindSelector() {
        const select = document.getElementById('theme-select');
        if (!select || select.dataset.tozoThemeBound === '1') return;
        select.dataset.tozoThemeBound = '1';
        select.value = getStoredTheme();
        select.addEventListener('change', () => applyTheme(select.value));
    }

    // boot 在 DOM 可用后应用主题并绑定控件。
    // 先 apply 再 bind，确保页面首屏就拿到正确 class。
    function boot() {
        applyTheme(getStoredTheme(), { persist: false });
        bindSelector();
    }

    // TozoTheme 暴露给页面脚本和浏览器控制台，用于主动读取或切换主题。
    window.TozoTheme = {
        key: KEY,
        themes: THEMES.slice(),
        getTheme: getStoredTheme,
        applyTheme
    };

    ensureStyle();
    if (document.body) {
        boot();
    } else {
        setClass(document.documentElement, getStoredTheme());
        document.addEventListener('DOMContentLoaded', boot, { once: true });
    }

    // storage 事件让一个页面切换主题后，其他已打开页面同步更新。
    // 这里不再持久化，避免当前事件处理再次触发跨标签写入。
    window.addEventListener('storage', event => {
        if (event.key === KEY) {
            applyTheme(event.newValue || 'dark', { persist: false });
        }
    });
})();
