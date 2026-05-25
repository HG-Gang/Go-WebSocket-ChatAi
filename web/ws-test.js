/**
 * web/ws-test.js
 * TozoAI WebSocket 测试客户端
 *
 * 功能：
 *   1. 连接 Go 服务的 WS 端口（自动获取 JWT Token）
 *   2. 显示 WS Ping/Pong 心跳状态（浏览器自动响应 Ping）
 *   3. 发送 OpenAI Realtime 事件（session.update / response.create 等）
 *   4. 接收并解析 Go 服务转发的 OpenAI 响应
 *   5. 实时日志记录（分级别：info/warn/error/send/recv/heartbeat）
 *   6. 模拟心跳超时（验证 Go 服务的超时断开逻辑）
 *
 * 心跳说明：
 *   Go 服务通过 WS 协议层 Ping/Pong 检测客户端存活。
 *   浏览器 WebSocket API 自动响应 Ping（无需手动发送 Pong）。
 *   本测试面板通过 onclose 事件检测到被服务端断开来验证心跳超时逻辑。
 */

// ======================== 全局状态 ========================
function createEventStats() {
    return {
        sessionEvents: 0,          // session.created / session.updated / session_restored 次数
        beginCount: 0,             // OpenAI response.created / begin 次数
        endCount: 0,               // OpenAI response.done / end 次数
        errorCount: 0,             // OpenAI 或 Go 包装后的错误次数
        reconnectRequired: 0,      // Go 认为 OpenAI 上游需要重连且通知 App 的次数
        textChars: 0,              // AI 文本增量字符数
        transcriptChars: 0,        // 音频转写文本字符数
        audioEvents: 0,            // OpenAI 音频 delta 包数量
        audioBytes: 0,             // base64 音频 payload 近似字节数，用于观察流量级别
        lastEvent: '-',
        lastResponseId: '-',
        sessionTimeline: [],       // 当前会话经历过的 session.created/session.updated/session_restored 明细
        recentEvents: [],          // 最近收到或发送的链路事件明细，给“最近事件”弹窗展示
        responses: new Map(),      // response_id -> 聚合后的完整响应内容
        currentResponseId: '',
        responseStartAt: new Map(),// response_id -> performance.now()
        latencies: [],             // 每次 begin→end 耗时，最多保留最近 50 条
        lastLatencyMs: null
    };
}

const state = {
    ws: null,                    // WebSocket 实例
    connected: false,            // 当前连接状态
    connStartTime: null,         // 连接建立时间
    durationTimer: null,         // 连接时长刷新定时器
    heartbeatTimer: null,        // 客户端心跳定时器
    simulateTimeoutTimer: null,  // 模拟超时定时器
    msgCount: 0,                 // 收到的消息计数
    sentCount: 0,                // 发送的消息计数
    reconnectCount: 0,           // 重连计数
    sessionId: null,             // 当前会话 ID
    token: '',                   // JWT Token
    suppressPong: false,         // 是否抑制 Pong 响应（模拟超时）
    healthTimer: null,           // Go /health 轮询定时器
    eventStats: createEventStats(), // 当前连接内的链路事件统计
};

// ======================== DOM 元素引用 ========================
const dom = {
    userId:            () => document.getElementById('user-id'),
    wsUrl:             () => document.getElementById('ws-url'),
    token:             () => document.getElementById('token'),
    tokenApi:          () => document.getElementById('token-api'),
    btnConnect:        () => document.getElementById('btn-connect'),
    btnDisconnect:     () => document.getElementById('btn-disconnect'),
    btnReconnect:      () => document.getElementById('btn-reconnect'),
    btnGenToken:       () => document.getElementById('btn-gen-token'),
    connStatus:        () => document.getElementById('conn-status'),
    sessionId:         () => document.getElementById('session-id'),
    connDuration:      () => document.getElementById('conn-duration'),
    lastPing:          () => document.getElementById('last-ping'),
    lastPong:          () => document.getElementById('last-pong'),
    msgCount:          () => document.getElementById('msg-count'),
    sentCount:         () => document.getElementById('sent-count'),
    reconnectCount:    () => document.getElementById('reconnect-count'),
    goHealthStatus:    () => document.getElementById('go-health-status'),
    goHealthActive:    () => document.getElementById('go-health-active'),
    goHealthTime:      () => document.getElementById('go-health-time'),
    statLastEvent:     () => document.getElementById('stat-last-event'),
    btnRecentEvents:   () => document.getElementById('btn-recent-events'),
    statLastResponseId:() => document.getElementById('stat-last-response-id'),
    statSessionEvents: () => document.getElementById('stat-session-events'),
    statBeginCount:    () => document.getElementById('stat-begin-count'),
    statEndCount:      () => document.getElementById('stat-end-count'),
    statErrorCount:    () => document.getElementById('stat-error-count'),
    statReconnectReq:  () => document.getElementById('stat-reconnect-required'),
    statTextChars:     () => document.getElementById('stat-text-chars'),
    statTranscriptChars:() => document.getElementById('stat-transcript-chars'),
    statAudioEvents:   () => document.getElementById('stat-audio-events'),
    statAudioBytes:    () => document.getElementById('stat-audio-bytes'),
    statLastLatency:   () => document.getElementById('stat-last-latency'),
    statAvgLatency:    () => document.getElementById('stat-avg-latency'),
    heartbeatInterval: () => document.getElementById('heartbeat-interval'),
    btnStartHeartbeat: () => document.getElementById('btn-start-heartbeat'),
    btnStopHeartbeat:  () => document.getElementById('btn-stop-heartbeat'),
    btnSimTimeout:     () => document.getElementById('btn-simulate-timeout'),
    heartbeatTimeline: () => document.getElementById('heartbeat-timeline'),
    msgType:           () => document.getElementById('msg-type'),
    msgBody:           () => document.getElementById('msg-body'),
    btnSend:           () => document.getElementById('btn-send'),
    btnFillTemplate:   () => document.getElementById('btn-fill-template'),
    btnSendTextMsg:    () => document.getElementById('btn-send-text-msg'),
    logContainer:      () => document.getElementById('log-container'),
    logAutoScroll:     () => document.getElementById('log-auto-scroll'),
    logLevelFilter:    () => document.getElementById('log-level-filter'),
    logRawFilter:      () => document.getElementById('log-raw-filter'),
    logTextFilter:     () => document.getElementById('log-text-filter'),
    btnClearLog:       () => document.getElementById('btn-clear-log'),
    btnExportLog:      () => document.getElementById('btn-export-log'),
    themeSelect:       () => document.getElementById('theme-select'),
    quickTextInput:    () => document.getElementById('quick-text-input'),
    btnSessionEvents:  () => document.getElementById('btn-session-events'),
    recentEventsModal: () => document.getElementById('recent-events-modal'),
    btnCloseRecentEvents: () => document.getElementById('btn-close-recent-events'),
    recentEventList:   () => document.getElementById('recent-event-list'),
    sessionEventsModal:() => document.getElementById('session-events-modal'),
    btnCloseSessionEvents: () => document.getElementById('btn-close-session-events'),
    sessionEventList:  () => document.getElementById('session-event-list'),
    completeResponseFilter: () => document.getElementById('complete-response-filter'),
    completeResponseList: () => document.getElementById('complete-response-list'),
    completeResponseMeta: () => document.getElementById('complete-response-meta'),
    completeResponseContent: () => document.getElementById('complete-response-content'),
    btnClearResponses: () => document.getElementById('btn-clear-responses'),
    // 服务配置面板
    svcCfgProvider:       () => document.getElementById('service-config-provider'),
    svcCfgUpdated:        () => document.getElementById('service-config-updated'),
    btnRefreshSvcCfg:     () => document.getElementById('btn-refresh-service-config'),
    svcCfgModelKey:       () => document.getElementById('svc-cfg-model-key'),
    svcCfgEnabled:        () => document.getElementById('svc-cfg-enabled'),
    svcCfgDefaultModel:   () => document.getElementById('svc-cfg-default-model'),
    svcCfgVoice:          () => document.getElementById('svc-cfg-voice'),
    svcCfgApiKey:         () => document.getElementById('svc-cfg-api-key'),
    svcCfgOrg:            () => document.getElementById('svc-cfg-org'),
    svcCfgWsUrl:          () => document.getElementById('svc-cfg-ws-url'),
    svcCfgEndpoint:       () => document.getElementById('svc-cfg-endpoint'),
    svcCfgProxySource:    () => document.getElementById('svc-cfg-proxy-source'),
    svcCfgProxyEffective: () => document.getElementById('svc-cfg-proxy-effective'),
    svcCfgRateRps:        () => document.getElementById('svc-cfg-rate-rps'),
    svcCfgRateBurst:      () => document.getElementById('svc-cfg-rate-burst'),
    svcCfgMaxTtl:         () => document.getElementById('svc-cfg-max-ttl'),
    svcCfgAppPing:        () => document.getElementById('svc-cfg-app-ping'),
    svcCfgAppPong:        () => document.getElementById('svc-cfg-app-pong'),
    svcCfgApiPing:        () => document.getElementById('svc-cfg-api-ping'),
    svcCfgApiPong:        () => document.getElementById('svc-cfg-api-pong'),
    svcCfgApiRead:        () => document.getElementById('svc-cfg-api-read'),
    svcCfgApiWrite:       () => document.getElementById('svc-cfg-api-write'),
    svcCfgRestore:        () => document.getElementById('svc-cfg-restore'),
    svcCfgRestoreLimit:   () => document.getElementById('svc-cfg-restore-limit'),
    svcCfgInstructions:   () => document.getElementById('svc-cfg-instructions'),
    svcCfgExtraCard1:     () => document.getElementById('svc-cfg-extra-card-1'),
    svcCfgExtraLabel1:    () => document.getElementById('svc-cfg-extra-label-1'),
    svcCfgExtraValue1:    () => document.getElementById('svc-cfg-extra-value-1'),
    svcCfgExtraCard2:     () => document.getElementById('svc-cfg-extra-card-2'),
    svcCfgExtraLabel2:    () => document.getElementById('svc-cfg-extra-label-2'),
    svcCfgExtraValue2:    () => document.getElementById('svc-cfg-extra-value-2'),
};

// ======================== 消息模板 ========================
const templates = {
    // 2025 年 OpenAI Realtime GA 模板：
    // session 必须显式带 type=realtime，并把音频格式 / VAD / 语音都挪到 audio.input / audio.output 下，
    // 顶层 modalities 改名为 output_modalities，单值数组。详见 OpenAI Realtime GA 迁移指南。
    'session.update': {
        type: 'session.update',
        session: {
            type: 'realtime',
            model: 'gpt-realtime',
            output_modalities: ['audio'],
            instructions: '你是一个智能助手，请用中文回答用户的问题。',
            audio: {
                input: {
                    format: { type: 'audio/pcm', rate: 24000 },
                    turn_detection: {
                        type: 'server_vad',
                        threshold: 0.5,
                        prefix_padding_ms: 300,
                        silence_duration_ms: 500
                    },
                    transcription: { model: 'whisper-1' }
                },
                output: {
                    format: { type: 'audio/pcm' },
                    voice: 'alloy'
                }
            }
        }
    },
    'response.create': {
        type: 'response.create',
        response: {
            output_modalities: ['audio']
        }
    },
    'response.cancel': {
        type: 'response.cancel'
    },
    'input_audio_buffer.append': {
        type: 'input_audio_buffer.append',
        audio: '<base64_pcm16_audio_data>'
    },
    'input_audio_buffer.commit': {
        type: 'input_audio_buffer.commit'
    },
    'input_audio_buffer.clear': {
        type: 'input_audio_buffer.clear'
    },
    'conversation.item.create': {
        type: 'conversation.item.create',
        item: {
            type: 'message',
            role: 'user',
            content: [
                {
                    type: 'input_text',
                    text: '你好，请用一句话介绍一下你自己。'
                }
            ]
        }
    },
    'conversation.item.delete': {
        type: 'conversation.item.delete',
        item_id: '<要删除的 item_id>'
    },
    'conversation.item.truncate': {
        type: 'conversation.item.truncate',
        item_id: '<要截断的 assistant item_id>',
        content_index: 0,
        audio_end_ms: 1000
    },
    'legacy.text': {
        msgType: 'text',
        content: '你好，请用一句话介绍一下你自己。'
    },
    'legacy.stop': {
        msgType: 'stop'
    },
    'legacy.session_close': {
        msgType: 'session_close_gpt'
    },
    'ping': {
        type: 'ping',
        timestamp: Date.now()
    }
};

// ======================== 初始化 ========================
document.addEventListener('DOMContentLoaded', () => {
    dom.recentEventsModal().hidden = true;
    dom.sessionEventsModal().hidden = true;

    // 绑定按钮事件
    dom.btnConnect().addEventListener('click', connect);
    dom.btnDisconnect().addEventListener('click', disconnect);
    dom.btnReconnect().addEventListener('click', reconnect);
    dom.btnGenToken().addEventListener('click', generateToken);
    dom.btnStartHeartbeat().addEventListener('click', startClientHeartbeat);
    dom.btnStopHeartbeat().addEventListener('click', stopClientHeartbeat);
    dom.btnSimTimeout().addEventListener('click', simulateTimeout);
    dom.btnSend().addEventListener('click', sendMessage);
    dom.btnFillTemplate().addEventListener('click', fillTemplate);
    dom.btnSendTextMsg().addEventListener('click', sendTextMessage);
    dom.btnClearLog().addEventListener('click', clearLog);
    dom.btnExportLog().addEventListener('click', exportLog);
    dom.logLevelFilter().addEventListener('change', applyLogFilters);
    dom.logRawFilter().addEventListener('change', applyLogFilters);
    dom.logTextFilter().addEventListener('input', applyLogFilters);
    dom.themeSelect().addEventListener('change', () => applyTheme(dom.themeSelect().value));
    dom.btnRecentEvents().addEventListener('click', openRecentEventsModal);
    dom.btnCloseRecentEvents().addEventListener('click', (event) => {
        event.preventDefault();
        event.stopPropagation();
        closeRecentEventsModal();
    });
    dom.recentEventsModal().addEventListener('click', (event) => {
        if (event.target === dom.recentEventsModal()) closeRecentEventsModal();
    });
    dom.btnSessionEvents().addEventListener('click', openSessionEventsModal);
    dom.btnCloseSessionEvents().addEventListener('click', (event) => {
        event.preventDefault();
        event.stopPropagation();
        closeSessionEventsModal();
    });
    dom.sessionEventsModal().addEventListener('click', (event) => {
        if (event.target === dom.sessionEventsModal()) closeSessionEventsModal();
    });
    dom.completeResponseFilter().addEventListener('change', () => updateCompleteResponseUI(''));
    dom.completeResponseList().addEventListener('change', () => updateCompleteResponseUI(dom.completeResponseList().value));
    dom.btnClearResponses().addEventListener('click', clearCompleteResponses);

    // 消息类型切换时自动填充模板
    dom.msgType().addEventListener('change', fillTemplate);

    // 用户问题输入框变化时，同步到高级调试 JSON 的 text 字段
    // 仅在 msg-type 为 conversation.item.create / legacy.text 时同步，避免覆盖其他消息体
    dom.quickTextInput().addEventListener('input', syncQuickTextToMsgBody);

    // 服务配置面板：切换模型 / 刷新 / WS 地址变更时重绘
    dom.svcCfgProvider().addEventListener('change', renderServiceConfig);
    dom.btnRefreshSvcCfg().addEventListener('click', fetchServiceConfig);
    dom.wsUrl().addEventListener('change', renderServiceConfig);
    dom.wsUrl().addEventListener('input', renderServiceConfig);

    // 初始填充模板
    fillTemplate();
    initTheme();
    updateEventStatsUI();
    updateCompleteResponseUI();
    startHealthPolling();
    startServiceConfigPolling();

    // 从 URL 参数读取 userId 并自动填充（如 ?userId=1001）
    const urlParams = new URLSearchParams(window.location.search);
    const userIdParam = urlParams.get('userId');
    if (userIdParam) {
        dom.userId().value = userIdParam;
        log('info', '系统', `从 URL 参数读取 userId=${userIdParam}`);
    }

    log('info', '系统', '测试面板已就绪');
    log('info', '系统', '请确保 Go 服务已启动：go run cmd/server/main.go');
    log('info', '系统', '确保 Redis 已启动：redis-server');
});

// ======================== Token 管理 ========================

/**
 * 从 Go 服务的 /test/generate-token 接口自动获取 JWT Token
 */
async function generateToken() {
    const userId = dom.userId().value || '1001';
    // 将 userId 附加到 Token 接口 URL
    const baseUrl = dom.tokenApi().value;
    const separator = baseUrl.includes('?') ? '&' : '?';
    const apiUrl = `${baseUrl}${separator}userId=${encodeURIComponent(userId)}`;
    log('info', 'Token', `正在为 userId=${userId} 从 ${apiUrl} 获取 Token...`);

    try {
        const resp = await fetch(apiUrl);
        const data = await resp.json();

        if (data.code === 200 && data.token) {
            state.token = data.token;
            dom.token().value = data.token;
            log('info', 'Token', `获取成功，有效期 24 小时`);
            log('info', 'Token', `Token: ${data.token.substring(0, 50)}...`);
        } else {
            log('error', 'Token', `获取失败: ${JSON.stringify(data)}`);
        }
    } catch (err) {
        log('error', 'Token', `请求失败: ${err.message}`);
        log('warn', 'Token', '请确保 Go 服务已启动并监听正确端口');
    }
}

// ======================== WebSocket 连接管理 ========================

/**
 * 建立 WebSocket 连接
 * 流程：
 *   1. 获取或使用已有 Token
 *   2. 拼接 WS URL（Token 通过 URL 参数传递，因为浏览器 WS 不支持自定义 Header）
 *   3. 创建 WebSocket 实例
 *   4. 注册 onopen/onmessage/onclose/onerror 回调
 */
async function connect() {
    // 如果没有 Token，先自动获取
    if (!dom.token().value) {
        await generateToken();
    }
    state.token = dom.token().value;

    if (!state.token) {
        log('error', '连接', 'Token 为空，无法连接');
        return;
    }

    const wsUrl = dom.wsUrl().value;
    // 将 Token 附加到 URL 参数（因为浏览器 WebSocket API 不支持自定义 Header）
    // Go 服务端的 Auth 中间件需要从 URL 参数中提取 Token
    const separator = wsUrl.includes('?') ? '&' : '?';
    const fullUrl = `${wsUrl}${separator}token=${encodeURIComponent(state.token)}`;

    log('info', '连接', `正在连接 ${wsUrl}...`);
    updateConnStatus('connecting', '连接中...');

    try {
        state.ws = new WebSocket(fullUrl);
    } catch (err) {
        log('error', '连接', `创建 WebSocket 失败: ${err.message}`);
        updateConnStatus('disconnected', '连接失败');
        return;
    }

    // ---- onopen: 连接建立成功 ----
    state.ws.onopen = () => {
        state.connected = true;
        state.connStartTime = Date.now();
        state.msgCount = 0;
        state.sentCount = 0;
        state.eventStats = createEventStats();
        updateEventStatsUI();
        updateCompleteResponseUI();
        updateConnStatus('connected', '已连接');
        updateButtons(true);
        startDurationTimer();
        fetchHealthStatus();

        recordSessionEvent('app_ws_open', { url: wsUrl });
        recordEvent('app_ws_open', '', { url: wsUrl });
        log('info', '连接', 'WebSocket 连接已建立');
        log('info', '心跳', '浏览器将自动响应服务端 Ping（WS 协议层）');
        log('info', '心跳', '服务端 Ping 间隔和 Pong 超时由 config.yaml 的 realtime 配置控制');
    };

    // ---- onmessage: 收到服务端消息 ----
    state.ws.onmessage = (event) => {
        state.msgCount++;
        dom.msgCount().textContent = state.msgCount;

        const data = event.data;
        let parsed = null;

        try {
            parsed = JSON.parse(data);
        } catch (e) {
            // 非 JSON 消息（如二进制音频）
            log('recv', '收到', `[二进制/非JSON] 长度: ${data.length}`);
            return;
        }

        // 解析响应事件类型：
        // - Go 标准包装：{ code, response, content, responseId, response_id }
        // - OpenAI 原始事件：{ type, response, delta, ... }
        const responseEvent = typeof parsed.response === 'string' ? parsed.response : (parsed.type || 'unknown');
        const responseId = getResponseId(parsed);
        recordEvent(responseEvent, responseId, parsed);

        // 特殊事件处理
        switch (responseEvent) {
            case 'session_created':
            case 'session.created':
                // 提取会话信息
                if (parsed.content && parsed.content.session) {
                    state.sessionId = parsed.content.session.id;
                    dom.sessionId().textContent = state.sessionId || '-';
                } else if (parsed.session?.id) {
                    state.sessionId = parsed.session.id;
                    dom.sessionId().textContent = state.sessionId || '-';
                }
                state.eventStats.sessionEvents++;
                recordSessionEvent(responseEvent, parsed);
                updateEventStatsUI();
                log('recv', 'OpenAI', `会话已创建: ${state.sessionId}`);
                break;

            case 'session_updated':
            case 'session.updated':
            case 'session_restored':
                state.eventStats.sessionEvents++;
                recordSessionEvent(responseEvent, parsed);
                updateEventStatsUI();
                log('recv', 'OpenAI', '会话配置已更新');
                break;

            case 'begin':
            case 'response.created':
                state.eventStats.beginCount++;
                const beginId = responseId || createSyntheticResponseId();
                state.eventStats.currentResponseId = beginId;
                if (beginId) {
                    state.eventStats.responseStartAt.set(beginId, performance.now());
                    ensureResponseRecord(beginId).startedAt = Date.now();
                }
                updateEventStatsUI();
                recordSessionEvent(responseEvent, parsed);
                updateCompleteResponseUI(beginId);
                log('recv', 'OpenAI', `响应开始 [response_id: ${beginId || '-'}]`);
                break;

            case 'end':
            case 'response.done':
            case 'stop_success':
                state.eventStats.endCount++;
                const endId = responseId || state.eventStats.currentResponseId;
                recordResponseLatency(endId);
                finalizeResponseRecord(endId, parsed);
                updateEventStatsUI();
                recordSessionEvent(responseEvent, parsed);
                updateCompleteResponseUI(endId);
                log('recv', 'OpenAI', `响应结束 [response_id: ${endId || '-'}]`);
                break;

            case 'text_delta':
            case 'response.text.delta':
                const textDelta = extractDelta(parsed);
                state.eventStats.textChars += textDelta.length;
                appendResponseText(responseId || state.eventStats.currentResponseId, textDelta, 'text');
                updateEventStatsUI();
                updateCompleteResponseUI(responseId || state.eventStats.currentResponseId);
                log('recv', '文本', textDelta);
                break;

            case 'audio_delta':
            case 'response.audio.delta':
                const audioDelta = extractAudioDelta(parsed);
                state.eventStats.audioEvents++;
                state.eventStats.audioBytes += audioDelta.length;
                appendResponseAudio(responseId || state.eventStats.currentResponseId, audioDelta.length);
                updateEventStatsUI();
                updateCompleteResponseUI(responseId || state.eventStats.currentResponseId);
                // 音频增量（高频，默认不显示详情）
                if (shouldLogLevel('heartbeat')) {
                    log('heartbeat', '音频', `收到音频增量 [payload=${audioDelta.length} chars, frame=${data.length} bytes]`);
                }
                break;

            case 'transcript_text_delta':
            case 'response.audio_transcript.delta':
                const transcript = extractDelta(parsed);
                state.eventStats.transcriptChars += transcript.length;
                appendResponseText(responseId || state.eventStats.currentResponseId, transcript, 'transcript');
                updateEventStatsUI();
                updateCompleteResponseUI(responseId || state.eventStats.currentResponseId);
                log('recv', '转写', transcript);
                break;

            case 'error':
                state.eventStats.errorCount++;
                updateEventStatsUI();
                log('error', 'OpenAI', `错误: ${JSON.stringify(parsed.content)}`);
                break;

            case 'reconnect_required':
                state.eventStats.reconnectRequired++;
                updateEventStatsUI();
                log('warn', '重连', `服务端要求重连: ${parsed.content}`);
                state.reconnectCount++;
                dom.reconnectCount().textContent = state.reconnectCount;
                break;

            case 'heartbeat':
            case 'pong':
                // 应用层心跳（如果服务端发送）
                dom.lastPong().textContent = formatTime(Date.now());
                addHeartbeatEvent('pong', `服务端心跳 ts=${parsed.ts || parsed.timestamp || parsed.time || parsed.content?.ts || '-'}`);
                if (shouldLogLevel('heartbeat')) {
                    log('heartbeat', '心跳', `收到服务端心跳`);
                }
                return; // 不在主日志中显示

            default:
                // 其他事件（直接透传的 OpenAI 原始事件）
                log('recv', responseEvent, `收到事件`);
        }

        // 显示原始数据（可折叠）
        if (shouldShowRawData() && responseEvent !== 'audio_delta') {
            try {
                const pretty = JSON.stringify(parsed, null, 2);
                if (pretty.length < 2000) {
                    logRawData(pretty);
                } else {
                    logRawData(pretty.substring(0, 2000) + '\n... (截断)');
                }
            } catch (e) {
                // 忽略
            }
        }
    };

    // ---- onclose: 连接关闭 ----
    state.ws.onclose = (event) => {
        state.connected = false;
        stopDurationTimer();
        stopClientHeartbeat();
        updateButtons(false);

        const reason = describeCloseReason(event);
        const wasClean = event.wasClean;

        if (wasClean) {
            updateConnStatus('disconnected', '已断开');
            recordSessionEvent('app_ws_closed', { code: event.code, reason, raw_reason: event.reason || '', clean: true });
            recordEvent('app_ws_closed', '', { code: event.code, reason, raw_reason: event.reason || '', clean: true });
            log('info', '连接', `连接已正常关闭 [code: ${event.code}, reason: ${reason}]`);
        } else {
            updateConnStatus('error', '异常断开');
            recordSessionEvent('app_ws_abnormal_close', { code: event.code, reason, raw_reason: event.reason || '', clean: false });
            recordEvent('app_ws_abnormal_close', '', { code: event.code, reason, raw_reason: event.reason || '', clean: false });
            log('warn', '连接', `连接异常关闭 [code: ${event.code}, reason: ${reason}]`);

            // 判断是否是心跳超时导致的断开
            if (event.code === 1006) {
                log('warn', '心跳', '可能是心跳超时导致服务端主动断开（code 1006）');
                log('info', '心跳', '验证方式: 查看 Go 服务日志中是否有 "App 心跳超时，会话结束" 记录');
            }
        }
    };

    // ---- onerror: 连接错误 ----
    state.ws.onerror = (event) => {
        recordSessionEvent('app_ws_error', { type: event.type || 'error' });
        recordEvent('app_ws_error', '', { type: event.type || 'error' });
        log('error', '连接', 'WebSocket 错误（通常紧跟 onclose）');
        updateConnStatus('error', '连接错误');
    };
}

/**
 * 断开 WebSocket 连接
 */
function disconnect(reason = '用户在测试面板点击断开连接') {
    if (state.ws) {
        log('info', '连接', '正在断开连接...');
        state.ws.close(1000, reason);
        state.ws = null;
    }
}

/**
 * 手动重连
 */
function reconnect() {
    state.reconnectCount++;
    dom.reconnectCount().textContent = state.reconnectCount;
    log('info', '重连', `手动重连 (第 ${state.reconnectCount} 次)`);
    disconnect('用户在测试面板点击手动重连');
    setTimeout(connect, 500);
}

function describeCloseReason(event) {
    const codeText = getCloseCodeMeaning(event.code, event.wasClean);
    if (event.reason && event.reason.trim()) {
        return `${event.reason.trim()}；${codeText}`;
    }
    return codeText;
}

function getCloseCodeMeaning(code, wasClean) {
    const closeReasons = {
        1000: '正常关闭：用户主动断开、页面刷新、服务端正常结束会话或重启前主动关闭',
        1001: '端点离开：浏览器页面关闭、网络切换、服务端进程退出或上游网关下线',
        1002: '协议错误：收到不符合 WebSocket 协议的数据',
        1003: '数据类型不支持：服务端或浏览器拒绝当前消息格式',
        1006: '异常关闭：网络中断、代理断开、服务进程退出或心跳超时，浏览器没有收到关闭帧',
        1007: '消息内容格式错误：收到无法解析的文本或二进制内容',
        1008: '策略拒绝：鉴权失败、限流、权限不足或服务端主动拒绝',
        1009: '消息过大：单条消息超过服务端或浏览器限制',
        1010: '扩展协商失败：浏览器要求的 WebSocket 扩展未被服务端接受',
        1011: '服务端内部错误：Go 网关处理连接时发生异常',
        1012: '服务端重启：网关或上游服务正在重启',
        1013: '服务端过载：网关、Redis、OpenAI 或代理链路暂时不可用',
        1014: '上游网关错误：代理或上游服务返回异常',
        1015: 'TLS 握手失败：证书、代理或安全连接协商失败'
    };
    if (closeReasons[code]) {
        return closeReasons[code];
    }
    return wasClean
        ? `正常关闭：未识别的关闭码 ${code}`
        : `异常关闭：未识别的关闭码 ${code}，需要结合 Go 服务日志和代理日志排查`;
}

// ======================== 心跳测试 ========================

/**
 * 启动客户端应用层心跳（模拟 App 的心跳行为）
 * 注意：WS 协议层 Ping/Pong 由浏览器自动处理，无需手动发送。
 * 此功能发送应用层 JSON 心跳消息，用于测试服务端是否正确响应。
 */
function startClientHeartbeat() {
    const interval = parseInt(dom.heartbeatInterval().value) * 1000;
    if (state.heartbeatTimer) clearInterval(state.heartbeatTimer);

    log('info', '心跳', `启动客户端应用层心跳，间隔 ${interval / 1000}s`);
    dom.btnStartHeartbeat().disabled = true;
    dom.btnStopHeartbeat().disabled = false;

    state.heartbeatTimer = setInterval(() => {
        if (!state.connected) return;

        const now = Date.now();
        const pingMsg = JSON.stringify({
            type: 'ping',   // 当前 Go 网关应用层心跳：App 发 ping，Go 回 pong
            client_id: state.sessionId || dom.userId().value || 'debug-web',
            timestamp: now
        });

        try {
            state.ws.send(pingMsg);
            dom.lastPing().textContent = formatTime(now);
            addHeartbeatEvent('ping', `客户端心跳 ts=${now}`);

            if (shouldLogLevel('heartbeat')) {
                log('heartbeat', '心跳', `已发送客户端心跳`);
            }
        } catch (err) {
            log('error', '心跳', `发送心跳失败: ${err.message}`);
        }
    }, interval);
}

/**
 * 停止客户端心跳
 */
function stopClientHeartbeat() {
    if (state.heartbeatTimer) {
        clearInterval(state.heartbeatTimer);
        state.heartbeatTimer = null;
    }
    dom.btnStartHeartbeat().disabled = !state.connected;
    dom.btnStopHeartbeat().disabled = true;
    log('info', '心跳', '客户端心跳已停止');
}

/**
 * 模拟心跳超时（让连接空闲一段时间，触发服务端超时断开）
 *
 * 原理：
 *   浏览器 WebSocket API 无法阻止自动响应 Ping。
 *   因此，我们的模拟方式是：断开当前连接，等待 N 秒后重新连接。
 *   如果服务端配置了心跳超时（如 60s），在此期间服务端会检测到 Pong 超时并关闭连接。
 *
 *   更准确的测试方式：使用 Go 测试代码（如 ws_test.go）直接控制 Pong 响应。
 */
function simulateTimeout() {
    log('warn', '超时模拟', '正在模拟心跳超时...');
    log('info', '超时模拟', '由于浏览器会自动响应 WS Ping，无法完全模拟超时');
    log('info', '超时模拟', '将断开连接并等待，观察服务端日志中的心跳超时处理');
    log('info', '超时模拟', '查看 Go 服务日志: "App 心跳超时，会话结束（Go 将主动断开 OpenAI 连接）"');

    addHeartbeatEvent('timeout', '开始模拟超时（断开连接）');

    // 断开连接但不触发重连
    if (state.ws) {
        // 直接关闭底层连接（不发送 Close 帧，模拟 App 崩溃）
        state.ws.close();
        state.ws = null;
        state.connected = false;
        updateConnStatus('error', '模拟超时');
        updateButtons(false);
        stopDurationTimer();
        stopClientHeartbeat();
    }
}

// ======================== 消息发送 ========================

/**
 * 发送消息到 Go 服务
 */
function sendMessage() {
    if (!state.connected || !state.ws) {
        log('error', '发送', '未连接，无法发送');
        return;
    }

    const body = dom.msgBody().value.trim();
    if (!body) {
        log('warn', '发送', '消息内容为空');
        return;
    }

    // 验证 JSON 格式
    try {
        JSON.parse(body);
    } catch (e) {
        log('error', '发送', `JSON 格式错误: ${e.message}`);
        return;
    }

    try {
        state.ws.send(body);
        state.sentCount++;
        dom.sentCount().textContent = state.sentCount;

        const parsed = JSON.parse(body);
        const eventName = parsed.type || parsed.msgType || 'unknown';
        recordSessionEvent('app_send_message', parsed);
        recordEvent(`send:${eventName}`, getResponseId(parsed), parsed);
        log('send', '发送', `高级 JSON 事件已发送：${eventName}，长度 ${body.length} 字符`);
    } catch (err) {
        log('error', '发送', `发送失败: ${err.message}`);
    }
}

/**
 * 快捷发送文本问题
 * 发送 conversation.item.create + response.create 组合
 */
function sendTextMessage() {
    if (!state.connected || !state.ws) {
        log('error', '发送', '未连接');
        return;
    }

    // 1. 先发送 conversation.item.create（添加用户消息）
    const userText = dom.quickTextInput().value.trim() || '你好，请用一句话介绍一下你自己。';
    const createMsg = {
        type: 'conversation.item.create',
        item: {
            type: 'message',
            role: 'user',
            content: [{
                type: 'input_text',
                text: userText
            }]
        }
    };

    // 2. 再发送 response.create（触发生成响应）
    const responseMsg = { type: 'response.create' };

    try {
        state.ws.send(JSON.stringify(createMsg));
        state.sentCount++;
        dom.sentCount().textContent = state.sentCount;
        recordSessionEvent('app_send_message', createMsg);
        recordEvent('send:conversation.item.create', '', createMsg);
        log('send', '发送用户问题', `已发送给 GPT：${userText}`);

        // 稍作延迟再发送 response.create
        setTimeout(() => {
            if (!state.connected || !state.ws) {
                log('warn', '发送用户问题', '连接已断开，未继续发送 response.create');
                return;
            }
            state.ws.send(JSON.stringify(responseMsg));
            state.sentCount++;
            dom.sentCount().textContent = state.sentCount;
            recordSessionEvent('app_send_message', responseMsg);
            recordEvent('send:response.create', '', responseMsg);
            log('send', '发送用户问题', '已发送 response.create，开始等待 OpenAI 回复');
        }, 100);
    } catch (err) {
        log('error', '发送', `发送失败: ${err.message}`);
    }
}

/**
 * 填充消息模板
 */
function fillTemplate() {
    const type = dom.msgType().value;
    if (type === 'custom') {
        dom.msgBody().value = '{\n  "type": "your_event_type",\n  "data": {}\n}';
        return;
    }
    const tpl = templates[type];
    if (tpl) {
        const body = JSON.parse(JSON.stringify(tpl));
        const userText = dom.quickTextInput()?.value?.trim();
        if (type === 'ping') {
            body.timestamp = Date.now();
        }
        if (userText && type === 'conversation.item.create') {
            body.item.content[0].text = userText;
        }
        if (userText && type === 'legacy.text') {
            body.content = userText;
        }
        dom.msgBody().value = JSON.stringify(body, null, 2);
    }
}

/**
 * 把“用户问题”输入框的内容实时同步到高级调试 JSON 的 text 字段。
 * 仅处理 conversation.item.create / legacy.text 两种消息体（它们包含 text 字段），
 * 优先在当前 JSON 上原地更新，保留用户对其他字段的手动编辑；
 * 如果当前 JSON 已经不合法或结构和模板对不上，再回退到 fillTemplate 重新填充。
 */
function syncQuickTextToMsgBody() {
    const type = dom.msgType().value;
    if (type !== 'conversation.item.create' && type !== 'legacy.text') {
        return;
    }

    const userText = dom.quickTextInput().value;
    const body = dom.msgBody().value.trim();
    if (!body) {
        fillTemplate();
        return;
    }

    let parsed;
    try {
        parsed = JSON.parse(body);
    } catch {
        return;
    }

    if (type === 'conversation.item.create') {
        if (parsed && parsed.item && Array.isArray(parsed.item.content)
            && parsed.item.content[0] && typeof parsed.item.content[0].text === 'string') {
            parsed.item.content[0].text = userText;
            dom.msgBody().value = JSON.stringify(parsed, null, 2);
        } else {
            fillTemplate();
        }
        return;
    }

    if (type === 'legacy.text') {
        if (parsed && typeof parsed.content === 'string') {
            parsed.content = userText;
            dom.msgBody().value = JSON.stringify(parsed, null, 2);
        } else {
            fillTemplate();
        }
    }
}

// ======================== 链路统计 ========================

function getResponseId(data) {
    if (!data || typeof data !== 'object') return '';
    if (typeof data.response_id === 'string') return data.response_id;
    if (typeof data.responseId === 'string') return data.responseId;
    if (data.response && typeof data.response === 'object' && typeof data.response.id === 'string') {
        return data.response.id;
    }
    if (data.content && typeof data.content === 'object') {
        if (typeof data.content.response_id === 'string') return data.content.response_id;
        if (typeof data.content.responseId === 'string') return data.content.responseId;
    }
    return '';
}

function extractDelta(data) {
    if (!data || typeof data !== 'object') return '';
    if (typeof data.content === 'string') return data.content;
    if (data.content && typeof data.content.delta === 'string') return data.content.delta;
    if (data.content && typeof data.content.text === 'string') return data.content.text;
    if (typeof data.delta === 'string') return data.delta;
    if (typeof data.transcript === 'string') return data.transcript;
    return '';
}

function extractAudioDelta(data) {
    if (!data || typeof data !== 'object') return '';
    if (typeof data.content === 'string') return data.content;
    if (data.content && typeof data.content.delta === 'string') return data.content.delta;
    if (typeof data.delta === 'string') return data.delta;
    if (typeof data.audio === 'string') return data.audio;
    return '';
}

function recordEvent(eventName, responseId, payload) {
    const name = eventName || 'unknown';
    state.eventStats.lastEvent = name;
    if (responseId) {
        state.eventStats.lastResponseId = responseId;
    }
    const item = {
        time: new Date().toLocaleString(),
        event: name,
        responseId: responseId || '-',
        summary: summarizeObject(payload?.content || payload?.response || payload || {})
    };
    state.eventStats.recentEvents.push(item);
    if (state.eventStats.recentEvents.length > 200) {
        state.eventStats.recentEvents.shift();
    }
    const tooltip = state.eventStats.recentEvents
        .slice(-8)
        .map(v => `${v.time} ${v.event} ${v.responseId}`)
        .join('\n');
    const el = dom.statLastEvent();
    if (el) el.title = tooltip || '暂无最近事件';
    renderRecentEvents();
    updateEventStatsUI();
}

function renderRecentEvents() {
    const list = dom.recentEventList();
    if (!list) return;
    if (!state.eventStats.recentEvents.length) {
        list.textContent = '暂无最近事件';
        return;
    }
    list.innerHTML = state.eventStats.recentEvents
        .slice()
        .reverse()
        .map(item => `<div class="session-event-item"><b>${escapeHtml(item.event)}</b> <span>${escapeHtml(item.time)}</span><br>response_id=${escapeHtml(item.responseId)}<br>${escapeHtml(item.summary)}</div>`)
        .join('');
}

function openRecentEventsModal(event) {
    if (event) {
        event.preventDefault();
        event.stopPropagation();
    }
    renderRecentEvents();
    dom.recentEventsModal().hidden = false;
}

function closeRecentEventsModal(event) {
    if (event) {
        event.preventDefault();
        event.stopPropagation();
    }
    dom.recentEventsModal().hidden = true;
}

function recordResponseLatency(responseId) {
    let id = responseId;
    let startedAt = id ? state.eventStats.responseStartAt.get(id) : undefined;

    if (typeof startedAt !== 'number' && state.eventStats.responseStartAt.size === 1) {
        const first = state.eventStats.responseStartAt.entries().next().value;
        id = first[0];
        startedAt = first[1];
    }
    if (typeof startedAt !== 'number') return;

    const latency = Math.max(0, performance.now() - startedAt);
    state.eventStats.lastLatencyMs = latency;
    state.eventStats.latencies.push(latency);
    if (state.eventStats.latencies.length > 50) {
        state.eventStats.latencies.shift();
    }
    if (id) {
        state.eventStats.responseStartAt.delete(id);
    }
}

function createSyntheticResponseId() {
    return `local-${Date.now()}-${state.eventStats.beginCount}`;
}

/**
 * 记录当前浏览器会话中发生的关键链路事件。
 * 这里的 Session 事件是“调试页面视角”的事件流，和 Go 内存指标中的
 * session.events 互补：前者便于观察页面操作，后者便于观察服务端真实链路。
 */
function recordSessionEvent(eventName, payload) {
    const content = payload?.content || payload?.session || payload;
    const item = {
        time: new Date().toLocaleString(),
        event: eventName,
        responseId: getResponseId(payload) || '-',
        summary: summarizeObject(content)
    };
    state.eventStats.sessionTimeline.push(item);
    if (state.eventStats.sessionTimeline.length > 100) {
        state.eventStats.sessionTimeline.shift();
    }
    state.eventStats.sessionEvents = state.eventStats.sessionTimeline.length;
    const tooltip = state.eventStats.sessionTimeline
        .slice(-5)
        .map(v => `${v.time} ${v.event} ${v.responseId}`)
        .join('\n');
    const el = dom.statSessionEvents();
    if (el) el.title = tooltip || '暂无 Session 事件';
    renderSessionEvents();
    updateEventStatsUI();
}

function renderSessionEvents() {
    const list = dom.sessionEventList();
    if (!list) return;
    if (!state.eventStats.sessionTimeline.length) {
        list.textContent = '暂无 Session 事件';
        return;
    }
    list.innerHTML = state.eventStats.sessionTimeline
        .map(item => `<div class="session-event-item"><b>${escapeHtml(item.event)}</b> <span>${escapeHtml(item.time)}</span><br>response_id=${escapeHtml(item.responseId)}<br>${escapeHtml(item.summary)}</div>`)
        .join('');
}

function openSessionEventsModal(event) {
    if (event) {
        event.preventDefault();
        event.stopPropagation();
    }
    renderSessionEvents();
    dom.sessionEventsModal().hidden = false;
}

function closeSessionEventsModal(event) {
    if (event) {
        event.preventDefault();
        event.stopPropagation();
    }
    dom.sessionEventsModal().hidden = true;
}

/**
 * 确保某个 OpenAI response_id 有本地聚合记录。
 * begin/text_delta/audio_delta/response.done 可能乱序或缺少 response_id，
 * 因此这里会在必要时生成一个 local-* 临时 ID，避免调试页面丢内容。
 */
function ensureResponseRecord(responseId) {
    const id = responseId || state.eventStats.currentResponseId || createSyntheticResponseId();
    if (!state.eventStats.responses.has(id)) {
        state.eventStats.responses.set(id, {
            id,
            startedAt: Date.now(),
            endedAt: null,
            text: '',
            transcript: '',
            audioChars: 0,
            finalContent: '',
            finalPayload: null,
            status: 'streaming'
        });
        refreshResponseOptions(id);
    }
    return state.eventStats.responses.get(id);
}

function appendResponseText(responseId, delta, field) {
    if (!delta) return;
    const rec = ensureResponseRecord(responseId);
    rec[field] = (rec[field] || '') + delta;
    rec.finalContent = rec.text || rec.transcript || '';
}

function appendResponseAudio(responseId, payloadChars) {
    const rec = ensureResponseRecord(responseId);
    rec.audioChars += payloadChars || 0;
}

/**
 * 在 response.done / end 到达时收口完整响应。
 * finalContent 优先使用 OpenAI 最终 JSON 中的文本；如果没有最终文本，
 * 就回退到前面流式拼接得到的 text/transcript。
 */
function finalizeResponseRecord(responseId, payload) {
    const rec = ensureResponseRecord(responseId);
    rec.endedAt = Date.now();
    rec.status = payload?.code && payload.code !== 0 ? 'error' : 'done';
    rec.finalPayload = payload || null;
    const finalText = extractFinalContent(payload);
    if (finalText) {
        rec.finalContent = finalText;
    } else {
        rec.finalContent = rec.text || rec.transcript || '(本次响应没有文本内容，可能只有音频输出)';
    }
    refreshResponseOptions(rec.id);
}

function extractFinalContent(payload) {
    if (!payload || typeof payload !== 'object') return '';
    if (typeof payload.content === 'string') return payload.content;
    if (payload.content?.text) return payload.content.text;
    if (payload.content?.transcript) return payload.content.transcript;
    if (payload.response?.output && Array.isArray(payload.response.output)) {
        return payload.response.output
            .flatMap(item => item.content || [])
            .map(part => part.text || part.transcript || '')
            .filter(Boolean)
            .join('');
    }
    return '';
}

/**
 * 重建完整响应下拉框。
 * 每次新增 response 或状态变化时调用，保证用户能切换查看历史响应。
 */
function refreshResponseOptions(selectedId) {
    const select = dom.completeResponseList();
    if (!select) return;
    const records = filteredResponseRecords();
    const previous = selectedId || select.value;
    select.innerHTML = '';
    for (const rec of records) {
        const option = document.createElement('option');
        option.value = rec.id;
        option.textContent = `${rec.id} | ${responseStatusText(rec.status)} | text=${rec.text.length} transcript=${rec.transcript.length} audio=${formatBytes(rec.audioChars)}`;
        select.appendChild(option);
    }
    if (previous && records.some(rec => rec.id === previous)) {
        select.value = previous;
    } else if (records.length > 0) {
        select.value = records[records.length - 1].id;
    }
}

/**
 * 渲染“OpenAI 完整响应”面板。
 * 面板同时显示聚合后的最终内容和 response.done/end 原始 JSON，
 * 用于对比“流式内容”和“最终完整结果”是否一致。
 */
function updateCompleteResponseUI(selectedId) {
    const select = dom.completeResponseList();
    const content = dom.completeResponseContent();
    const meta = dom.completeResponseMeta();
    if (!select || !content || !meta) return;

    refreshResponseOptions(selectedId);
    const id = selectedId || select.value;
    const rec = state.eventStats.responses.get(id);
    if (!rec) {
        const filter = dom.completeResponseFilter()?.value || 'all';
        meta.textContent = `筛选=${responseFilterText(filter)} | 命中 0 条`;
        content.textContent = '暂无符合当前筛选条件的完整响应';
        return;
    }
    const elapsed = rec.endedAt
        ? `${((rec.endedAt - rec.startedAt) / 1000).toFixed(2)}s`
        : '生成中';
    const records = filteredResponseRecords();
    meta.textContent = `筛选=${responseFilterText(dom.completeResponseFilter()?.value || 'all')} | 当前 ${records.findIndex(item => item.id === rec.id) + 1}/${records.length} | 状态=${responseStatusText(rec.status)} | 耗时=${elapsed} | text=${rec.text.length} chars | transcript=${rec.transcript.length} chars | audio=${formatBytes(rec.audioChars)}`;
    const completeParts = [];
    if (rec.finalContent || rec.text || rec.transcript) {
        completeParts.push(`【最终聚合内容】\n${rec.finalContent || rec.text || rec.transcript}`);
    }
    if (rec.finalPayload) {
        completeParts.push(`【response.done / end 原始 JSON】\n${JSON.stringify(rec.finalPayload, null, 2)}`);
    }
    content.textContent = completeParts.join('\n\n') || '(等待 OpenAI 流式内容)';
}

function clearCompleteResponses() {
    state.eventStats.responses.clear();
    state.eventStats.currentResponseId = '';
    updateCompleteResponseUI();
}

function filteredResponseRecords() {
    const filter = dom.completeResponseFilter()?.value || 'all';
    return Array.from(state.eventStats.responses.values()).filter(rec => {
        switch (filter) {
            case 'streaming': return rec.status === 'streaming';
            case 'done': return rec.status === 'done';
            case 'error': return rec.status === 'error';
            case 'text': return (rec.text || rec.finalContent || '').length > 0;
            case 'transcript': return (rec.transcript || '').length > 0;
            case 'audio': return (rec.audioChars || 0) > 0;
            default: return true;
        }
    });
}

function responseStatusText(status) {
    switch (status) {
        case 'streaming': return '生成中（response.created）';
        case 'done': return '已完成（response.done）';
        case 'error': return '错误（error）';
        default: return status || '-';
    }
}

function responseFilterText(filter) {
    const names = {
        all: '全部响应',
        streaming: '生成中（response.created）',
        done: '已完成（response.done）',
        error: '错误响应（error）',
        text: '包含文本（response.text.delta）',
        transcript: '包含转写（response.audio_transcript.delta）',
        audio: '包含音频（response.audio.delta）'
    };
    return names[filter] || filter;
}

/**
 * 把复杂 payload 压缩成一行摘要，避免 Session 事件弹窗被大 JSON 撑爆。
 */
function summarizeObject(value) {
    try {
        const text = typeof value === 'string' ? value : JSON.stringify(value, null, 2);
        return text.length > 1000 ? text.substring(0, 1000) + '\n... (截断)' : text;
    } catch {
        return String(value);
    }
}

function updateEventStatsUI() {
    const s = state.eventStats;
    setText(dom.statLastEvent(), s.lastEvent);
    setText(dom.statLastResponseId(), s.lastResponseId);
    setText(dom.statSessionEvents(), s.sessionEvents);
    setText(dom.statBeginCount(), s.beginCount);
    setText(dom.statEndCount(), s.endCount);
    setText(dom.statErrorCount(), s.errorCount);
    setText(dom.statReconnectReq(), s.reconnectRequired);
    setText(dom.statTextChars(), s.textChars);
    setText(dom.statTranscriptChars(), s.transcriptChars);
    setText(dom.statAudioEvents(), s.audioEvents);
    setText(dom.statAudioBytes(), formatBytes(s.audioBytes));
    setText(dom.statLastLatency(), s.lastLatencyMs == null ? '-' : formatMs(s.lastLatencyMs));

    const avg = s.latencies.length
        ? s.latencies.reduce((sum, v) => sum + v, 0) / s.latencies.length
        : null;
    setText(dom.statAvgLatency(), avg == null ? '-' : formatMs(avg));
}

function startHealthPolling() {
    if (state.healthTimer) clearInterval(state.healthTimer);
    fetchHealthStatus();
    state.healthTimer = setInterval(fetchHealthStatus, 5000);
}

async function fetchHealthStatus() {
    try {
        const resp = await fetch(`${getHttpBaseURL()}/health`, { cache: 'no-store' });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        setText(dom.goHealthStatus(), data.status || 'ok');
        setText(dom.goHealthActive(), data.active_sessions ?? '-');
        setText(dom.goHealthTime(), data.time ? new Date(data.time).toLocaleTimeString() : '-');
    } catch (err) {
        setText(dom.goHealthStatus(), '不可用');
        setText(dom.goHealthActive(), '-');
        setText(dom.goHealthTime(), '-');
    }
}

function getHttpBaseURL() {
    try {
        const wsURL = new URL(dom.wsUrl().value || window.location.href);
        const protocol = wsURL.protocol === 'wss:' ? 'https:' : 'http:';
        return `${protocol}//${wsURL.host}`;
    } catch {
        return window.location.origin;
    }
}

function setText(el, value) {
    if (el) el.textContent = value;
}

function initTheme() {
    const saved = localStorage.getItem('tozo-ws-theme') || 'dark';
    dom.themeSelect().value = saved;
    applyTheme(saved);
}

function applyTheme(theme) {
    document.body.classList.remove('theme-dark', 'theme-light', 'theme-ocean', 'theme-sepia', 'theme-contrast');
    if (theme && theme !== 'dark') {
        document.body.classList.add(`theme-${theme}`);
    }
    localStorage.setItem('tozo-ws-theme', theme || 'dark');
}

function formatBytes(value) {
    if (value < 1024) return `${value}B`;
    if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)}KB`;
    return `${(value / 1024 / 1024).toFixed(2)}MB`;
}

function formatMs(value) {
    if (value < 1000) return `${Math.round(value)}ms`;
    return `${(value / 1000).toFixed(2)}s`;
}

// ======================== UI 更新 ========================

/**
 * 更新连接状态显示
 */
function updateConnStatus(status, text) {
    const el = dom.connStatus();
    el.innerHTML = `<span class="status-dot ${status}"></span> ${text}`;
}

/**
 * 更新按钮可用状态
 */
function updateButtons(connected) {
    dom.btnConnect().disabled = connected;
    dom.btnDisconnect().disabled = !connected;
    dom.btnReconnect().disabled = !connected;
    dom.btnSend().disabled = !connected;
    dom.btnSendTextMsg().disabled = !connected;
    dom.btnStartHeartbeat().disabled = !connected;
    dom.btnStopHeartbeat().disabled = true;
    dom.btnSimTimeout().disabled = !connected;
}

/**
 * 启动连接时长计时器
 */
function startDurationTimer() {
    stopDurationTimer();
    state.durationTimer = setInterval(() => {
        if (!state.connStartTime) return;
        const elapsed = Math.floor((Date.now() - state.connStartTime) / 1000);
        const min = Math.floor(elapsed / 60);
        const sec = elapsed % 60;
        dom.connDuration().textContent = `${min}分${sec.toString().padStart(2, '0')}秒`;
    }, 1000);
}

function stopDurationTimer() {
    if (state.durationTimer) {
        clearInterval(state.durationTimer);
        state.durationTimer = null;
    }
}

/**
 * 添加心跳时间线事件
 */
function addHeartbeatEvent(type, text) {
    const timeline = dom.heartbeatTimeline();
    const entry = document.createElement('div');
    entry.className = `heartbeat-event ${type}`;
    entry.textContent = `[${formatTime(Date.now())}] ${text}`;
    timeline.appendChild(entry);

    // 最多保留 50 条
    while (timeline.children.length > 50) {
        timeline.removeChild(timeline.firstChild);
    }
    timeline.scrollTop = timeline.scrollHeight;
}

// ======================== 日志系统 ========================

/**
 * 添加日志条目
 * @param {string} level - 日志级别: info/warn/error/send/recv/heartbeat
 * @param {string} tag   - 标签: 连接/发送/心跳/OpenAI 等
 * @param {string} msg   - 日志内容
 */
function log(level, tag, msg) {
    const container = dom.logContainer();
    const entry = document.createElement('div');
    entry.className = `log-entry ${level}`;
    entry.dataset.level = level;
    entry.dataset.search = `${level} ${tag} ${msg}`.toLowerCase();

    const time = formatTime(Date.now());
    entry.innerHTML = `<span class="log-time">[${time}]</span><span class="log-level">[${level}]</span><span class="log-tag">[${tag}]</span><span class="log-msg">${escapeHtml(msg)}</span>`;

    container.appendChild(entry);
    applyLogFilterToEntry(entry);

    // 最多保留 500 条
    while (container.children.length > 500) {
        container.removeChild(container.firstChild);
    }

    // 自动滚动
    if (dom.logAutoScroll().checked) {
        container.scrollTop = container.scrollHeight;
    }
}

/**
 * 添加原始数据日志（可折叠）
 */
function logRawData(data) {
    const container = dom.logContainer();
    const entry = document.createElement('div');
    entry.className = 'log-entry raw';
    entry.dataset.level = 'raw';
    entry.dataset.search = data.toLowerCase();
    entry.innerHTML = `<span class="log-level">[raw]</span><span class="log-data">${escapeHtml(data)}</span>`;
    container.appendChild(entry);
    applyLogFilterToEntry(entry);

    if (dom.logAutoScroll().checked) {
        container.scrollTop = container.scrollHeight;
    }
}

function shouldLogLevel(level) {
    const filter = dom.logLevelFilter()?.value || 'normal';
    return filter === 'all' || filter === level;
}

function shouldShowRawData() {
    return (dom.logRawFilter()?.value || 'show') === 'show';
}

function applyLogFilters() {
    dom.logContainer().querySelectorAll('.log-entry').forEach(applyLogFilterToEntry);
}

function applyLogFilterToEntry(entry) {
    const level = entry.dataset.level || '';
    const levelFilter = dom.logLevelFilter()?.value || 'normal';
    const rawFilter = dom.logRawFilter()?.value || 'show';
    const keyword = (dom.logTextFilter()?.value || '').trim().toLowerCase();

    let visible = true;
    if (level === 'raw' && rawFilter === 'hide') {
        visible = false;
    }
    if (visible && levelFilter === 'normal') {
        visible = level !== 'heartbeat';
    } else if (visible && levelFilter !== 'all') {
        visible = level === levelFilter;
    }
    if (visible && keyword) {
        visible = (entry.dataset.search || entry.textContent.toLowerCase()).includes(keyword);
    }
    entry.classList.toggle('filtered-out', !visible);
}

/**
 * 清空日志
 */
function clearLog() {
    dom.logContainer().innerHTML = '';
    log('info', '系统', '日志已清空');
}

/**
 * 导出日志为文本文件
 */
function exportLog() {
    const container = dom.logContainer();
    const entries = container.querySelectorAll('.log-entry:not(.filtered-out)');
    let text = `TozoAI WebSocket 测试日志\n导出时间: ${new Date().toLocaleString()}\n${'='.repeat(60)}\n\n`;

    entries.forEach(entry => {
        text += entry.textContent + '\n';
    });

    const blob = new Blob([text], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `ws-test-log-${new Date().toISOString().slice(0, 19).replace(/:/g, '-')}.txt`;
    a.click();
    URL.revokeObjectURL(url);

    log('info', '系统', '日志已导出');
}

// ======================== 工具函数 ========================

/**
 * 格式化时间戳为 HH:MM:SS.mmm
 */
function formatTime(ts) {
    const d = new Date(ts);
    return `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}:${d.getSeconds().toString().padStart(2, '0')}.${d.getMilliseconds().toString().padStart(3, '0')}`;
}

/**
 * 页面内容转义（防 XSS）
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// ======================== 服务配置面板 ========================

/**
 * 根据 WS 地址自动判断当前所选模型。
 * ws://host/ws/realtime/azure → "azureai"
 * 其他 → "openai"
 */
function detectProviderFromWsUrl() {
    const url = (dom.wsUrl().value || '').toLowerCase();
    if (url.includes('/azure') || url.includes('/azureai')) return 'azureai';
    return 'openai';
}

/**
 * 获取当前应显示哪个模型的配置（用户手动选 > 跟随 WS 地址）。
 */
function getActiveProvider() {
    const sel = dom.svcCfgProvider().value;
    if (sel === 'auto') return detectProviderFromWsUrl();
    return sel;
}

let _svcCfgCache = null; // 缓存最近一次 /api/debug/status 响应
let _svcCfgTimer = null;

/**
 * 拉取 /api/debug/status 并刷新面板。
 */
async function fetchServiceConfig() {
    try {
        const resp = await fetch(`${getHttpBaseURL()}/api/debug/status`, { cache: 'no-store' });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const json = await resp.json();
        _svcCfgCache = json.data || json;
        renderServiceConfig();
    } catch (err) {
        setText(dom.svcCfgModelKey(), '获取失败');
        setText(dom.svcCfgApiKey(), err.message);
    }
}

/**
 * 渲染服务配置面板。
 */
function renderServiceConfig() {
    if (!_svcCfgCache) return;
    const provider = getActiveProvider();
    const modelData = (provider === 'azureai') ? _svcCfgCache.azure : _svcCfgCache.openai;
    const networkData = _svcCfgCache.network || {};

    if (!modelData) {
        setText(dom.svcCfgModelKey(), `${provider}（后端未返回此模型数据）`);
        return;
    }

    const rt = modelData.realtime || {};

    setText(dom.svcCfgModelKey(), modelData.model_key || provider);
    setText(dom.svcCfgEnabled(), modelData.enabled ? 'ON' : 'OFF');
    setText(dom.svcCfgDefaultModel(), modelData.default_model || '-');
    setText(dom.svcCfgVoice(), modelData.voice || '-');
    setText(dom.svcCfgApiKey(), modelData.api_key_masked || (modelData.api_key_configured ? '已配置（无脱敏字段）' : '未配置'));
    setText(dom.svcCfgOrg(), modelData.organization || '-');
    setText(dom.svcCfgWsUrl(), (provider === 'azureai' ? rt.ws_url : modelData.ws_url) || '-');
    setText(dom.svcCfgEndpoint(), modelData.endpoint || '-');

    // 代理
    const proxyKey = (provider === 'azureai') ? 'azure' : 'openai';
    const proxySource = networkData[`${proxyKey}_proxy_source`] || (rt.proxy_configured ? 'config' : 'none');
    const proxyEffective = networkData[`${proxyKey}_proxy_effective`] || rt.proxy_url || '-';
    setText(dom.svcCfgProxySource(), proxySource);
    setText(dom.svcCfgProxyEffective(), proxyEffective);

    // 限流与 TTL
    setText(dom.svcCfgRateRps(), modelData.rate_rps ?? '-');
    setText(dom.svcCfgRateBurst(), modelData.rate_burst ?? '-');
    setText(dom.svcCfgMaxTtl(), modelData.max_session_ttl || '-');

    // Realtime 心跳参数
    setText(dom.svcCfgAppPing(), rt.app_ping_interval || '-');
    setText(dom.svcCfgAppPong(), rt.app_pong_timeout || '-');
    setText(dom.svcCfgApiPing(), rt.api_ping_interval || '-');
    setText(dom.svcCfgApiPong(), rt.api_pong_timeout || '-');
    setText(dom.svcCfgApiRead(), rt.api_read_timeout || '-');
    setText(dom.svcCfgApiWrite(), rt.api_write_timeout || '-');
    setText(dom.svcCfgRestore(), rt.restore_session ? 'ON' : 'OFF');
    setText(dom.svcCfgRestoreLimit(), rt.restore_history_limit ?? '-');

    // instructions
    const instr = (modelData.instructions || '').trim();
    dom.svcCfgInstructions().textContent = instr ? (instr.length > 200 ? instr.slice(0, 200) + '...' : instr) : '（未配置）';

    // Azure extra fields
    const extra1 = dom.svcCfgExtraCard1();
    const extra2 = dom.svcCfgExtraCard2();
    if (provider === 'azureai') {
        if (modelData.deployment_name || modelData.realtime_deployment) {
            extra1.hidden = false;
            setText(dom.svcCfgExtraLabel1(), 'Deployment');
            setText(dom.svcCfgExtraValue1(), modelData.realtime_deployment || modelData.deployment_name || '-');
        }
        if (modelData.api_version) {
            extra2.hidden = false;
            setText(dom.svcCfgExtraLabel2(), 'API Version');
            setText(dom.svcCfgExtraValue2(), modelData.api_version || '-');
        }
    } else {
        extra1.hidden = true;
        extra2.hidden = true;
    }

    setText(dom.svcCfgUpdated(), new Date().toLocaleTimeString());
}

function startServiceConfigPolling() {
    fetchServiceConfig();
    if (_svcCfgTimer) clearInterval(_svcCfgTimer);
    _svcCfgTimer = setInterval(fetchServiceConfig, 10000);
}
