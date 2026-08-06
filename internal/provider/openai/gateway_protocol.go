// internal/provider/openai/gateway_protocol.go
// 文件功能：旧版 TOZO App 协议（msgType、content、historyContent 字段）与 OpenAI
// Realtime 原生事件之间的协议适配核心。输入为 App 经 WebSocket 发来的原始 JSON 字节流，
// 输出为 gatewayClientPlan：待发往 OpenAI 的事件列表、待回给 App 的响应消息、是否需要
// 打断当前上游响应等。它只做协议规划，不负责网络读写与鉴权。
// 安全边界：App 明文携带的客户端上下文（appUserId、GPS 坐标、语言）只用于组装会话配置，
// 不写入日志；本文件不处理任何密钥或 token。
package openai

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	protocol "TozoAI-Chat-Api/pkg/protocol/openai"
	"TozoAI-Chat-Api/pkg/response"
)

const (
	// 旧版 TOZO App 协议使用的 msgType 取值；这些字符串是 App 侧固定口径，与 OpenAI 事件无关。
	gatewayMsgText                = "text"
	gatewayMsgAudio               = "audio"
	gatewayMsgSpeaker             = "speaker"
	gatewayMsgTextCommand         = "text_command"
	gatewayMsgTTS                 = "tts"
	gatewayMsgTTSVoice            = "tts_voice"
	gatewayMsgStop                = "stop"
	gatewayMsgHistoryConversation = "HistConv"
	gatewayMsgSessionClose        = "session_close_gpt"
	gatewayMsgMapServiceSearch    = "map_service_search"
	gatewayMsgWeatherSearch       = "weather_service_search"
	gatewayMsgWeatherReject       = "open_weather_reject_coordinate"

	// 网关自定义的 App 侧响应事件类型，仅用于旧协议兼容消息，不属于 OpenAI 事件。
	gatewayResponseStopSuccess                  response.ResponseEvent = "stop_success"
	gatewayResponseHistoryConversationCompleted response.ResponseEvent = "HistConvCompleted"
	gatewayResponseAudioTranslateCompleted      response.ResponseEvent = "audioTransCompleted"
	gatewayResponseCommandApp                   response.ResponseEvent = "command_app"
	gatewayResponseMapServicePlaces             response.ResponseEvent = "map_service_places"
	gatewayResponseMapServiceFail               response.ResponseEvent = "map_service_fail"
	gatewayResponseMapServiceMissingCoordinates response.ResponseEvent = "map_service_missing_coordinates"
	gatewayResponseOpenWeatherError             response.ResponseEvent = "open_weather_error"
	gatewayResponseOpenWeatherMissingCoords     response.ResponseEvent = "open_weather_missing_coordinates"
	gatewayResponseOpenWeatherMissingCity       response.ResponseEvent = "open_weather_missing_city"
)

// errGatewaySessionClose 表示 App 请求结束当前会话；调用方捕获后应关闭并清理连接。
var errGatewaySessionClose = errors.New("gateway session close requested")

// gatewayClientPlan 一次 App 消息的完整处理结果：openAIEvents 是待发往 OpenAI 的事件序列，
// appMessages 是待回给 App 的响应消息，closeSession 表示请求结束会话，
// interruptActive 表示用户新一轮输入需要先打断当前上游响应。
type gatewayClientPlan struct {
	openAIEvents    [][]byte
	appMessages     [][]byte
	reason          string
	closeSession    bool
	interruptActive bool // 是否表示用户新一轮输入，需要先打断当前上游响应
}

// gatewaySessionSnapshot 上次已发送 session.update 的配置快照，用于判断配置是否变化。
type gatewaySessionSnapshot struct {
	voice        string
	instructions string
	mode         string
	toolsHash    string
}

// gatewayClientContext 从 App 消息中记忆的客户端上下文，各字段已按新旧别名归一化。
type gatewayClientContext struct {
	appUserID string
	lat       string
	lon       string
	mapSDK    string
	transLang string
}

// gatewayAdapter 是旧 Events.php 与 SceneChatHandler 输入侧逻辑的 Go 替代实现。
// 它同时接收 OpenAI 原生客户端事件，以及使用 msgType、content、historyContent 字段的 TOZO 旧 App 协议。
type gatewayAdapter struct {
	mu          sync.Mutex
	snapshot    gatewaySessionSnapshot
	context     gatewayClientContext
	lastMsgType string
	// 当前会话是否已经向上游发送过任何 session.update（无论来自旧 msgType 流还是原生事件透传）。
	// 一旦为 true，原生 OpenAI 事件分支不再自动 prepend session.update，避免覆盖用户手动配置。
	sessionUpdateEmitted bool
}

func newGatewayAdapter() *gatewayAdapter {
	return &gatewayAdapter{}
}

func (g *gatewayAdapter) lastMessageType() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.lastMsgType
}

func (g *gatewayAdapter) setLastMessageType(msgType string) {
	g.mu.Lock()
	g.lastMsgType = msgType
	g.mu.Unlock()
}

// rememberClientContext 从 App 消息中提取并记忆客户端上下文。
// 新旧 App 对同一字段使用不同别名（如 appUserId/userId），归一化后保存；
// 只有非空值才覆盖，避免后续消息缺字段时清空已记录的信息。
func (g *gatewayAdapter) rememberClientContext(raw map[string]json.RawMessage) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if value := firstNonEmpty(rawString(raw, "appUserId"), rawString(raw, "app_user_id"), rawString(raw, "userId")); value != "" {
		g.context.appUserID = value
	}
	if value := firstNonEmpty(rawString(raw, "lat"), rawString(raw, "latitude")); value != "" {
		g.context.lat = value
	}
	if value := firstNonEmpty(rawString(raw, "lon"), rawString(raw, "lng"), rawString(raw, "longitude")); value != "" {
		g.context.lon = value
	}
	if value := firstNonEmpty(rawString(raw, "map_sdk"), rawString(raw, "mapSdk")); value != "" {
		g.context.mapSDK = value
	}
	if value := firstNonEmpty(rawString(raw, "trans_lang"), rawString(raw, "lang"), rawString(raw, "language")); value != "" {
		g.context.transLang = value
	}
}

func (g *gatewayAdapter) clientContextSnapshot() gatewayClientContext {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.context
}

// buildClientPlan 是协议转换入口：解析 App 原始 JSON，按消息形态分派规划。
// 成功时返回由调用方发送的事件与响应；消息缺必需字段或 JSON 非法时返回错误，
// 不产出任何事件，保证畸形输入不会进入上游连接。
func (g *gatewayAdapter) buildClientPlan(data []byte, cfg *OpenAIConfig, sessionID string) (gatewayClientPlan, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return gatewayClientPlan{}, fmt.Errorf("decode app json: %w", err)
	}
	g.rememberClientContext(raw)

	// 带 type 字段的消息属于 OpenAI 原生事件形态（含 App 侧 ping 心跳），走原生分支。
	if typ := rawString(raw, "type"); typ != "" {
		// ping 是 App 侧心跳而非 OpenAI 事件：就地回 pong，不进入上游连接。
		if typ == "ping" {
			msg, err := json.Marshal(map[string]any{
				"type":      "pong",
				"client_id": firstNonEmpty(rawString(raw, "client_id"), rawString(raw, "clientId"), sessionID),
				"time":      time.Now().Format("2006-01-02 15:04:05"),
			})
			return gatewayClientPlan{appMessages: [][]byte{msg}, reason: "app_ping"}, err
		}
		return g.planRawOpenAIEvent(raw, data, cfg, typ)
	}

	// 其余消息按旧协议 msgType 分派；msgType 缺失视为非法消息直接拒绝。
	msgType := rawString(raw, "msgType")
	if msgType == "" {
		return gatewayClientPlan{}, fmt.Errorf("missing type/msgType")
	}
	g.setLastMessageType(msgType)

	switch msgType {
	case gatewayMsgSessionClose:
		return gatewayClientPlan{closeSession: true, reason: msgType}, nil
	case gatewayMsgText, gatewayMsgTextCommand:
		return g.planText(raw, cfg, msgType)
	case gatewayMsgAudio:
		return g.planAudio(raw, cfg, msgType, true)
	case gatewayMsgSpeaker:
		return g.planAudio(raw, cfg, msgType, false)
	case gatewayMsgTTS, gatewayMsgTTSVoice:
		return g.planUnsupportedTool(msgType, sessionID, "TTS 已迁移到 HTTP 接口：/api/azure/audio/speech 或后续 OpenAI TTS Provider")
	case gatewayMsgHistoryConversation:
		return g.planHistory(raw, sessionID)
	case gatewayMsgStop:
		return g.planStop(raw)
	case gatewayMsgWeatherReject:
		return g.planWeatherReject(raw, cfg, sessionID)
	case gatewayMsgMapServiceSearch:
		return g.planMapServiceSearch(raw, sessionID)
	case gatewayMsgWeatherSearch:
		return g.planWeatherCoordinate(raw, cfg, sessionID)
	default:
		if content := rawString(raw, "content"); content != "" {
			return g.planText(raw, cfg, msgType)
		}
		return g.planUnsupportedTool(msgType, sessionID, "unsupported legacy msgType")
	}
}

// planText 将旧协议文本消息转换为 conversation.item.create（user 角色）+ response.create。
// 文本为空时返回错误拒绝生成事件，避免把空内容送入上游。
func (g *gatewayAdapter) planText(raw map[string]json.RawMessage, cfg *OpenAIConfig, msgType string) (gatewayClientPlan, error) {
	content := rawString(raw, "content")
	if content == "" {
		content = rawNestedString(raw, "send_content", "text")
	}
	if strings.TrimSpace(content) == "" {
		return gatewayClientPlan{}, fmt.Errorf("%s content is empty", msgType)
	}

	events, err := g.sessionUpdateIfNeeded(raw, cfg, msgType)
	if err != nil {
		return gatewayClientPlan{}, err
	}
	item, err := marshalJSON(map[string]any{
		"type": "conversation.item.create",
		"item": map[string]any{
			"type": "message",
			"role": "user",
			"content": []map[string]any{
				{"type": "input_text", "text": content},
			},
		},
	})
	if err != nil {
		return gatewayClientPlan{}, err
	}
	create, err := responseCreateAudio()
	if err != nil {
		return gatewayClientPlan{}, err
	}
	events = append(events, item, create)
	return gatewayClientPlan{openAIEvents: events, reason: msgType, interruptActive: true}, nil
}

// planAudio 将旧协议音频消息转换为 input_audio_buffer.append 事件；commit 为 true
// （audio 消息）时追加 commit 事件触发识别，speaker 消息只追加不提交。
// 只有 commit 的完整一轮输入才标记 interruptActive，打断当前上游响应。
func (g *gatewayAdapter) planAudio(raw map[string]json.RawMessage, cfg *OpenAIConfig, msgType string, commit bool) (gatewayClientPlan, error) {
	audio := rawString(raw, "content")
	if audio == "" {
		audio = rawNestedString(raw, "send_content", "audio")
	}
	if audio == "" {
		return gatewayClientPlan{}, fmt.Errorf("%s audio content is empty", msgType)
	}

	events, err := g.sessionUpdateIfNeeded(raw, cfg, msgType)
	if err != nil {
		return gatewayClientPlan{}, err
	}
	appendEvent, err := marshalJSON(map[string]any{
		"type":  "input_audio_buffer.append",
		"audio": audio,
	})
	if err != nil {
		return gatewayClientPlan{}, err
	}
	events = append(events, appendEvent)
	if commit {
		commitEvent, err := marshalJSON(map[string]any{"type": "input_audio_buffer.commit"})
		if err != nil {
			return gatewayClientPlan{}, err
		}
		events = append(events, commitEvent)
	}
	return gatewayClientPlan{openAIEvents: events, reason: msgType, interruptActive: commit}, nil
}

// planHistory 将 historyContent 中最新一轮的非空文本作为 system 消息注入 OpenAI，
// 并立即向 App 回 HistConvCompleted ack；历史为空时只回 ack，不产生上游事件。
func (g *gatewayAdapter) planHistory(raw map[string]json.RawMessage, sessionID string) (gatewayClientPlan, error) {
	text := latestHistoryContent(raw["historyContent"])
	plan := gatewayClientPlan{reason: gatewayMsgHistoryConversation}
	if text != "" {
		event, err := marshalJSON(map[string]any{
			"type": "conversation.item.create",
			"item": map[string]any{
				"type": "message",
				"role": "system",
				"content": []map[string]any{
					{"type": "input_text", "text": text},
				},
			},
		})
		if err != nil {
			return gatewayClientPlan{}, err
		}
		plan.openAIEvents = append(plan.openAIEvents, event)
	}
	ack := response.NewResponseWithID(0, gatewayResponseHistoryConversationCompleted, "", "", time.Now().UnixMilli())
	msg, err := ack.ToJSON()
	if err != nil {
		return gatewayClientPlan{}, err
	}
	_ = sessionID
	plan.appMessages = append(plan.appMessages, msg)
	return plan, nil
}

// planStop 将旧协议 stop 消息转换为 response.cancel；App 携带 responseId 时指定取消目标，
// 缺省时由 OpenAI 取消当前活跃响应。
func (g *gatewayAdapter) planStop(raw map[string]json.RawMessage) (gatewayClientPlan, error) {
	payload := map[string]any{"type": "response.cancel"}
	if responseID := firstNonEmpty(rawString(raw, "responseId"), rawString(raw, "response_id")); responseID != "" {
		payload["response_id"] = responseID
	}
	data, err := marshalJSON(payload)
	if err != nil {
		return gatewayClientPlan{}, err
	}
	return gatewayClientPlan{openAIEvents: [][]byte{data}, reason: "client_stop"}, nil
}

// planWeatherReject 处理用户拒绝共享 GPS 定位：先向 App 回当前定位天气无法查询的错误提示，
// 再把“用户已拒绝定位”作为 user 消息注入，让模型引导用户开启权限或提供城市名。
func (g *gatewayAdapter) planWeatherReject(raw map[string]json.RawMessage, cfg *OpenAIConfig, sessionID string) (gatewayClientPlan, error) {
	plan := gatewayClientPlan{reason: gatewayMsgWeatherReject}
	notify := response.NewResponseWithID(0, gatewayResponseOpenWeatherError, "", map[string]string{
		"msg": "The user refused to share GPS coordinates, so current-location weather cannot be queried.",
	}, time.Now().UnixMilli())
	msg, err := notify.ToJSON()
	if err != nil {
		return gatewayClientPlan{}, err
	}
	plan.appMessages = append(plan.appMessages, msg)

	events, err := g.sessionUpdateIfNeeded(raw, cfg, gatewayMsgWeatherReject)
	if err != nil {
		return gatewayClientPlan{}, err
	}
	item, err := marshalJSON(map[string]any{
		"type": "conversation.item.create",
		"item": map[string]any{
			"type": "message",
			"role": "user",
			"content": []map[string]any{
				{"type": "input_text", "text": "The user just clicked Deny on the device GPS location permission popup."},
			},
		},
	})
	if err != nil {
		return gatewayClientPlan{}, err
	}
	create, err := responseCreateAudioWithInstructions("Tell the user that location permission was denied and ask them to enable it or provide a city name.")
	if err != nil {
		return gatewayClientPlan{}, err
	}
	_ = sessionID
	plan.openAIEvents = append(events, item, create)
	plan.interruptActive = true
	return plan, nil
}

// planWeatherCoordinate 处理 App 上报的天气查询 GPS 坐标：把坐标内容作为 user 消息注入，
// 并指示模型继续原天气请求；坐标文本缺失时使用固定占位文案，保证请求仍以一条有效消息发出。
func (g *gatewayAdapter) planWeatherCoordinate(raw map[string]json.RawMessage, cfg *OpenAIConfig, sessionID string) (gatewayClientPlan, error) {
	events, err := g.sessionUpdateIfNeeded(raw, cfg, gatewayMsgWeatherSearch)
	if err != nil {
		return gatewayClientPlan{}, err
	}
	content := firstNonEmpty(rawString(raw, "content"), rawNestedString(raw, "send_content", "text"))
	if strings.TrimSpace(content) == "" {
		content = "The user has provided GPS coordinates for the pending weather query."
	}
	item, err := marshalJSON(map[string]any{
		"type": "conversation.item.create",
		"item": map[string]any{
			"type": "message",
			"role": "user",
			"content": []map[string]any{
				{"type": "input_text", "text": content},
			},
		},
	})
	if err != nil {
		return gatewayClientPlan{}, err
	}
	create, err := responseCreateAudioWithInstructions("Use the new user GPS coordinates to continue the weather request. If weather service data is unavailable, explain the limitation briefly.")
	if err != nil {
		return gatewayClientPlan{}, err
	}
	_ = sessionID
	return gatewayClientPlan{openAIEvents: append(events, item, create), reason: gatewayMsgWeatherSearch, interruptActive: true}, nil
}

// planMapServiceSearch 识别旧协议地图服务请求：缺少 lat/lon 时向 App 索要当前位置；
// 坐标齐全时返回 501（Go 版尚未接入外部地图 Provider），两种情况都不生成上游事件。
func (g *gatewayAdapter) planMapServiceSearch(raw map[string]json.RawMessage, sessionID string) (gatewayClientPlan, error) {
	ctx := g.clientContextSnapshot()
	content := rawString(raw, "content")
	var request map[string]any
	if content != "" {
		_ = json.Unmarshal([]byte(content), &request)
	}
	if ctx.lat == "" || ctx.lon == "" {
		resp := response.NewResponseWithID(0, gatewayResponseMapServiceMissingCoordinates, "", map[string]any{
			"content": firstNonEmpty(content, "{}"),
			"message": "缺少 lat/lon，旧项目会向 App 请求当前位置后再继续地图服务。",
		}, time.Now().UnixMilli())
		data, err := resp.ToJSON()
		if err != nil {
			return gatewayClientPlan{}, err
		}
		_ = sessionID
		return gatewayClientPlan{appMessages: [][]byte{data}, reason: gatewayMsgMapServiceSearch}, nil
	}

	resp := response.NewResponseWithID(501, gatewayResponseMapServiceFail, "", map[string]any{
		"message": "Go 版本已识别地图服务请求，但外部地图 Provider（Google/Amap/Mapbox）尚未配置为可调用模块。",
		"request": request,
		"lat":     ctx.lat,
		"lon":     ctx.lon,
		"map_sdk": firstNonEmpty(ctx.mapSDK, "mapbox"),
	}, time.Now().UnixMilli())
	data, err := resp.ToJSON()
	if err != nil {
		return gatewayClientPlan{}, err
	}
	_ = sessionID
	return gatewayClientPlan{appMessages: [][]byte{data}, reason: gatewayMsgMapServiceSearch}, nil
}

// planUnsupportedTool 对未支持或已迁移的旧 msgType 返回 501 错误响应给 App，不进入 OpenAI。
func (g *gatewayAdapter) planUnsupportedTool(msgType, sessionID, message string) (gatewayClientPlan, error) {
	resp := response.NewResponseWithID(501, response.EventError, "", map[string]string{
		"msgType": msgType,
		"message": message,
	}, time.Now().UnixMilli())
	data, err := resp.ToJSON()
	if err != nil {
		return gatewayClientPlan{}, err
	}
	_ = sessionID
	return gatewayClientPlan{appMessages: [][]byte{data}, reason: msgType}, nil
}

// sessionUpdateIfNeeded 在会话配置（voice、instructions、tools、mode）变化时生成
// session.update；配置未变时返回 nil，避免每条消息都重复发送该事件。
// 一旦发出过 session.update（无论此处还是原生分支），sessionUpdateEmitted 即为 true，
// 原生事件分支据此不再自动注入，防止覆盖用户手动配置。
func (g *gatewayAdapter) sessionUpdateIfNeeded(raw map[string]json.RawMessage, cfg *OpenAIConfig, mode string) ([][]byte, error) {
	voice := firstNonEmpty(rawString(raw, "voice"), cfg.Voice, "alloy")
	instructions := firstNonEmpty(cfg.Instructions, defaultTOZOInstructions())
	tools := legacyToolsForMode(raw, mode)
	toolsHash := stableJSONHash(tools)

	g.mu.Lock()
	same := g.snapshot.voice == voice && g.snapshot.instructions == instructions && g.snapshot.mode == mode && g.snapshot.toolsHash == toolsHash
	if same {
		g.mu.Unlock()
		return nil, nil
	}
	g.snapshot = gatewaySessionSnapshot{voice: voice, instructions: instructions, mode: mode, toolsHash: toolsHash}
	g.sessionUpdateEmitted = true
	g.mu.Unlock()

	payload := map[string]any{
		"type":    "session.update",
		"session": buildSessionPayload(cfg, instructions, voice, tools),
	}
	data, err := marshalJSON(payload)
	if err != nil {
		return nil, err
	}
	return [][]byte{data}, nil
}

// buildSessionPayload 组装 session.update 的 session 子对象。
// 该结构遵循 OpenAI Realtime GA 文档要求：必须显式 type=realtime、声明 model、
// 以及把音频格式 / 语音 / VAD 嵌套到 audio.input、audio.output 下。
func buildSessionPayload(cfg *OpenAIConfig, instructions, voice string, tools []any) map[string]any {
	session := map[string]any{
		"type":              "realtime",
		"model":             cfg.GetDefaultModel(),
		"instructions":      instructions,
		"output_modalities": []string{"audio"},
		"audio": map[string]any{
			"input": map[string]any{
				"format":         map[string]any{"type": "audio/pcm", "rate": 24000},
				"turn_detection": nil,
				"transcription":  map[string]any{"model": "whisper-1"},
			},
			"output": map[string]any{
				"format": map[string]any{"type": "audio/pcm"},
				"voice":  voice,
			},
		},
		"tools":       tools,
		"tool_choice": "auto",
	}
	return session
}

// planRawOpenAIEvent 处理已经是 OpenAI Realtime 原生事件（带 type 字段）的 App 数据。
//
// 兼容性：保留原本的「原样透传」语义，让已支持官方协议的 App 客户端无需经过 msgType 转换。
// 升级点：在本会话第一次出现非 session.update 的原生事件时，先自动 prepend 一条
// GA 风格的 session.update，确保 instructions / tools / voice 等基础配置生效；
// 用户自己发过 session.update 后，后续不再注入，避免覆盖用户手动配置。
func (g *gatewayAdapter) planRawOpenAIEvent(raw map[string]json.RawMessage, data []byte, cfg *OpenAIConfig, typ string) (gatewayClientPlan, error) {
	passthrough := append([]byte(nil), data...)

	if typ == string(protocol.ClientEventTypeSessionUpdate) {
		g.mu.Lock()
		g.sessionUpdateEmitted = true
		g.mu.Unlock()
		return gatewayClientPlan{openAIEvents: [][]byte{passthrough}, reason: typ}, nil
	}

	g.mu.Lock()
	alreadyEmitted := g.sessionUpdateEmitted
	g.mu.Unlock()
	if alreadyEmitted {
		return gatewayClientPlan{openAIEvents: [][]byte{passthrough}, reason: typ}, nil
	}

	injected, err := g.sessionUpdateIfNeeded(raw, cfg, "")
	if err != nil {
		return gatewayClientPlan{}, err
	}
	events := append(injected, passthrough)
	return gatewayClientPlan{openAIEvents: events, reason: typ}, nil
}

// defaultTOZOInstructions 默认系统指令：限定 TOZO 助手应答语言与各工具的使用边界，
// 明确天气/导航/工作区工具各自适用的问题范围。
func defaultTOZOInstructions() string {
	return "You are TOZO AI Assistant. Answer in the user's language. Keep replies concise and natural for earbuds. " +
		"For TOZO product questions, use the TOZO knowledge tool. For weather or forecast questions, use the weather tool only. " +
		"For navigation, route, directions, or going somewhere, use navigation tools and do not call the weather tool. " +
		"When the user asks about the current project or asks you to change project files, use the workspace tools. " +
		"Read files before editing them, keep changes focused, and report changed paths clearly."
}

// legacyToolsForMode 组装旧协议模式的工具集：基础工具（天气、TOZO 知识库、工作区文件）
// 对所有模式生效；text_command 额外注入命令映射与导航工具。
func legacyToolsForMode(raw map[string]json.RawMessage, mode string) []any {
	tools := []any{
		openWeatherToolSchema(),
		searchTOZOKnowledgeToolSchema(),
		workspaceListFilesToolSchema(),
		workspaceReadFileToolSchema(),
		workspaceWriteFileToolSchema(),
	}
	if mode == gatewayMsgTextCommand {
		tools = append(tools, mapCommandToolSchema())
		tools = append(tools, navigationToolSchemas(raw)...)
	}
	return tools
}

// openWeatherToolSchema 天气查询工具定义；描述中注入当前服务器时间，
// 使模型对“现在/今天”的理解与实际时间一致，并明确禁止用于导航。
func openWeatherToolSchema() map[string]any {
	now := time.Now()
	return map[string]any{
		"type":        "function",
		"name":        "get_open_weather",
		"description": "Strictly retrieve current weather or forecast only. Current server time is " + now.Format("2006-01-02 15:04:05") + ". Do not use this tool for navigation, routes, directions, or finding places.",
		"parameters": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"city": map[string]any{
					"type":        "string",
					"description": "City/province/region name. Leave empty when the user wants weather at current GPS location.",
				},
				"state": map[string]any{
					"type":        "string",
					"description": "State or province code, useful for US cities.",
				},
				"country": map[string]any{
					"type":        "string",
					"description": "ISO 3166 country code, such as US or CN.",
				},
				"query_type": map[string]any{
					"type":        "string",
					"enum":        []string{"current", "forecast"},
					"description": "Use current for now/today; use forecast for future dates.",
				},
				"target_date": map[string]any{
					"type":        "string",
					"description": "Target date in YYYY-MM-DD. Required for forecast.",
				},
				"cnt": map[string]any{
					"type":        "integer",
					"description": "Forecast days, 1-5.",
				},
			},
			"required":             []string{"query_type"},
			"additionalProperties": false,
		},
	}
}

// searchTOZOKnowledgeToolSchema TOZO 官方知识库检索工具定义，用于产品 FAQ 与参数对比类问题。
func searchTOZOKnowledgeToolSchema() map[string]any {
	return map[string]any{
		"type":        "function",
		"name":        "search_tozo_knowledge",
		"description": "Search the TOZO official knowledge base for product FAQ, troubleshooting, pairing, reset, charging, firmware, warranty, product specifications, and model comparison questions.",
		"parameters": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"query": map[string]any{
					"type":        "string",
					"description": "The user's original product question.",
				},
				"product_name": map[string]any{
					"type":        "string",
					"description": "Specific TOZO model name if mentioned, such as T6, T9, NC9, OpenBuds.",
				},
			},
			"required":             []string{"query"},
			"additionalProperties": false,
		},
	}
}

// workspaceListFilesToolSchema 列出本地项目文件的工作区工具定义，路径限定为项目相对路径。
func workspaceListFilesToolSchema() map[string]any {
	return map[string]any{
		"type":        "function",
		"name":        "workspace_list_files",
		"description": "List files and directories inside the selected local project. Paths must be project-relative.",
		"parameters": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"project_id": map[string]any{
					"type":        "string",
					"description": "Project id from the web workspace selector. Use current when omitted.",
				},
				"path": map[string]any{
					"type":        "string",
					"description": "Project-relative directory path. Use empty string for the project root.",
				},
			},
			"required":             []string{},
			"additionalProperties": false,
		},
	}
}

// workspaceReadFileToolSchema 读取项目内 UTF-8 文本文件的工作区工具定义，路径为项目相对路径。
func workspaceReadFileToolSchema() map[string]any {
	return map[string]any{
		"type":        "function",
		"name":        "workspace_read_file",
		"description": "Read a UTF-8 text file inside the selected local project. Paths must be project-relative.",
		"parameters": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"project_id": map[string]any{
					"type":        "string",
					"description": "Project id from the web workspace selector. Use current when omitted.",
				},
				"path": map[string]any{
					"type":        "string",
					"description": "Project-relative file path to read.",
				},
			},
			"required":             []string{"path"},
			"additionalProperties": false,
		},
	}
}

// workspaceWriteFileToolSchema 写入项目文件的工作区工具定义，要求先读后改、改动聚焦。
func workspaceWriteFileToolSchema() map[string]any {
	return map[string]any{
		"type":        "function",
		"name":        "workspace_write_file",
		"description": "Write a UTF-8 text file inside the selected local project. Read existing files before editing and keep changes focused.",
		"parameters": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"project_id": map[string]any{
					"type":        "string",
					"description": "Project id from the web workspace selector. Use current when omitted.",
				},
				"path": map[string]any{
					"type":        "string",
					"description": "Project-relative file path to write.",
				},
				"content": map[string]any{
					"type":        "string",
					"description": "Complete new file content.",
				},
			},
			"required":             []string{"path", "content"},
			"additionalProperties": false,
		},
	}
}

// mapCommandToolSchema 将耳机/App 控制指令映射为预定义 command_code 的工具定义。
func mapCommandToolSchema() map[string]any {
	return map[string]any{
		"type":        "function",
		"name":        "map_command_to_code",
		"description": "Map an earbud/app control command spoken by the user to one predefined command_code.",
		"parameters": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"command_code": map[string]any{
					"type":        "string",
					"enum":        legacyCommandCodes(),
					"description": "Predefined App command code.",
				},
			},
			"required":             []string{"command_code"},
			"additionalProperties": false,
		},
	}
}

// navigationToolSchemas 导航工具定义（指定目的地与附近地点两类）；描述中注入当前地图 SDK，
// 并固定起点口径为用户当前 GPS 位置，防止模型自行假设起点。
func navigationToolSchemas(raw map[string]json.RawMessage) []any {
	mapSDK := firstNonEmpty(rawString(raw, "map_sdk"), rawString(raw, "mapSdk"), "mapbox")
	descriptionSuffix := " Current map SDK is " + mapSDK + ". The origin is always the user's current GPS location."
	return []any{
		map[string]any{
			"type":        "function",
			"name":        "get_specify_route_navigation",
			"description": "Retrieve navigation candidates for a specified destination. Supported travel modes: Drive, Walk, Bicycle." + descriptionSuffix,
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"destination": map[string]any{
						"type":        "string",
						"description": "Destination only. Do not include origin.",
					},
					"travelMode": map[string]any{
						"type":        "string",
						"description": "Drive, Walk, or Bicycle.",
					},
				},
				"required":             []string{"destination", "travelMode"},
				"additionalProperties": false,
			},
		},
		map[string]any{
			"type":        "function",
			"name":        "get_nearby_route_navigation",
			"description": "Retrieve nearby places by place type and return candidates for navigation." + descriptionSuffix,
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"placeType": map[string]any{
						"type":        "string",
						"description": "Place type, such as hotel, park, restaurant, gas_station, hospital.",
					},
					"travelMode": map[string]any{
						"type":        "string",
						"description": "Drive, Walk, or Bicycle.",
					},
				},
				"required":             []string{"placeType"},
				"additionalProperties": false,
			},
		},
	}
}

// legacyCommandCodes 预定义 App 命令码列表：基础命令码为固定集合，
// 音量百分比（0-100）与自定义降噪等级（1-10）动态生成。
func legacyCommandCodes() []string {
	codes := []string{
		"code_unknown", "code_music_play", "code_music_pause", "code_volume_up", "code_volume_down",
		"code_volume_maximum", "code_volume_minimum", "code_next_song", "code_previous_song",
		"code_quit", "code_exit_chat", "code_end_chat", "code_fast_forward", "code_fast_backward",
		"code_open_ear_tune", "code_close_ear_tune", "code_open_sound_enhancement", "code_close_sound_enhancement",
		"code_switch_noise_cancellation_mode", "code_switch_reduce_wind_noise_mode", "code_switch_windproof_mode",
		"code_switch_normal_mode", "code_switch_transparency_mode", "code_switch_leisure_mode", "code_switch_adaptive_noise_reduction_mode",
		"code_switch_sound_effect_bass_plus", "code_switch_sound_effect_bass_minus", "code_switch_sound_effect_classical",
		"code_switch_sound_effect_dance", "code_switch_sound_effect_deep", "code_switch_sound_effect_hip_pop",
		"code_switch_sound_effect_jazz", "code_switch_sound_effect_original", "code_switch_sound_effect_piano",
		"code_switch_sound_effect_pop", "code_switch_sound_effect_r&b", "code_switch_sound_effect_rock",
		"code_switch_sound_effect_standard", "code_switch_sound_effect_treble_plus", "code_switch_sound_effect_treble_minus",
		"code_switch_sound_effect_vocal", "code_switch_sound_effect_latin", "code_switch_sound_effect_symphony",
		"code_switch_sound_effect_electronic", "code_switch_sound_effect_blues", "code_switch_sound_effect_keyboard",
		"code_switch_sound_effect_trumpet", "code_switch_sound_effect_heavymetal", "code_switch_sound_effect_anime",
		"code_switch_sound_effect_slowrock", "code_switch_sound_effect_podcast", "code_switch_sound_effect_relaxing",
		"code_switch_sound_effect_quiet", "code_switch_sound_effect_awake", "code_switch_sound_effect_crazy",
		"code_switch_sound_effect_folk", "code_switch_sound_effect_country",
	}
	for i := 0; i <= 100; i++ {
		codes = append(codes, fmt.Sprintf("code_volume_percent_%d", i))
	}
	for i := 1; i <= 10; i++ {
		codes = append(codes, fmt.Sprintf("custom_noise_reduction_level_%d", i))
	}
	return codes
}

// stableJSONHash 以 JSON 字节串作为工具集比较指纹；序列化失败返回空串，
// 导致快照必然不匹配并重发 session.update——宁可多发送一次配置也不漏发。
func stableJSONHash(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(data)
}

// responseGateState 上游响应生命周期的状态机取值。
type responseGateState string

const (
	responseGateIdle       responseGateState = "idle"
	responseGateCreating   responseGateState = "creating"
	responseGateActive     responseGateState = "active"
	responseGateCancelling responseGateState = "cancelling"
)

// openAIResponseGate 是 Events::$openaiResponseStates 和 Events::$pendingOpenAIResponseCreates 的 Go 版本。
// 它串行化 response.create 与 response.cancel，避免长时间语音聊天中默认 OpenAI 对话收到重复活跃响应。
type openAIResponseGate struct {
	mu                       sync.Mutex
	state                    responseGateState
	responseID               string
	reason                   string
	pendingCreate            []byte
	pendingCreateCause       string
	cancelAfterCreated       bool
	cancelAfterCreatedReason string
	cancelUnknownActive      bool
	interrupted              map[string]struct{}
}

func newOpenAIResponseGate() *openAIResponseGate {
	return &openAIResponseGate{state: responseGateIdle, interrupted: make(map[string]struct{})}
}

// sendClientEvent 网关的事件发送入口：仅 response.create 与 response.cancel 走串行化
// 状态机，其余事件类型不做干预、原样交给 send 发送。
func (g *openAIResponseGate) sendClientEvent(eventType string, payload []byte, reason string, send func([]byte) error) error {
	switch eventType {
	case string(protocol.ClientEventTypeResponseCreate):
		return g.sendCreate(payload, reason, send)
	case string(protocol.ClientEventTypeResponseCancel):
		return g.sendCancel(extractClientResponseID(payload), reason, send)
	default:
		return send(payload)
	}
}

// sendCreate 发送 response.create：已有活跃响应时把事件暂存为 pendingCreate，
// 待当前响应结束后由 flushPending 补发，保证同一会话同时只有一个活跃响应。
// 实际发送失败时复位为 idle 并返回错误，避免状态卡死在 creating。
func (g *openAIResponseGate) sendCreate(payload []byte, reason string, send func([]byte) error) error {
	g.mu.Lock()
	if g.isBusyLocked() {
		g.pendingCreate = append(g.pendingCreate[:0], payload...)
		g.pendingCreateCause = reason
		g.mu.Unlock()
		return nil
	}
	g.state = responseGateCreating
	g.responseID = ""
	g.reason = reason
	g.mu.Unlock()

	if err := send(payload); err != nil {
		g.setIdle("", "send_create_failed")
		return err
	}
	return nil
}

// sendCancel 发送 response.cancel，按当前状态分三种情况：无活跃响应时直接忽略；
// 已在取消中时仅合并原因去重；正在创建且未拿到 responseId 时先记录取消意图，
// 等上游 response.created 到达后再取消，避免提前发送命中 response_cancel_not_active。
func (g *openAIResponseGate) sendCancel(responseID, reason string, send func([]byte) error) error {
	g.mu.Lock()
	if !g.isBusyLocked() {
		g.mu.Unlock()
		return nil
	}
	if g.state == responseGateCancelling {
		g.reason = "dedupe_cancel:" + reason
		g.mu.Unlock()
		return nil
	}
	if g.state == responseGateCreating && responseID == "" {
		// 上游还没有返回 response.created，此时直接 response.cancel 很容易得到
		// response_cancel_not_active。先记录意图，等 response.created 到达后立刻取消。
		g.cancelAfterCreated = true
		g.cancelAfterCreatedReason = reason
		g.reason = "cancel_after_created:" + reason
		g.mu.Unlock()
		return nil
	}
	targetID := firstNonEmpty(responseID, g.responseID)
	g.state = responseGateCancelling
	g.responseID = targetID
	g.reason = reason
	if targetID != "" {
		g.interrupted[targetID] = struct{}{}
	}
	g.mu.Unlock()

	payload := map[string]any{"type": "response.cancel"}
	if targetID != "" {
		payload["response_id"] = targetID
	}
	data, err := marshalJSON(payload)
	if err != nil {
		return err
	}
	if err := send(data); err != nil {
		g.setIdle(targetID, "send_cancel_failed")
		return err
	}
	return nil
}

// onServerEvent 由上游服务端事件驱动状态迁移；返回 true 表示存在待补发的
// pendingCreate，调用方应在处理完事件后调用 flushPending。
func (g *openAIResponseGate) onServerEvent(evt protocol.ServerEvent) (flushPending bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	switch v := evt.(type) {
	case *protocol.ResponseCreatedEvent:
		g.state = responseGateActive
		g.responseID = v.Response.ID
		g.reason = "response.created"
	case *protocol.ResponseDoneEvent:
		g.state = responseGateIdle
		g.responseID = v.Response.ID
		g.reason = "response.done:" + string(v.Response.Status)
		if v.Response.ID != "" {
			delete(g.interrupted, v.Response.ID)
		}
		return g.pendingCreate != nil
	case *protocol.ResponseCancelledEvent:
		g.state = responseGateIdle
		g.reason = "response.cancelled"
		return g.pendingCreate != nil
	case *protocol.ErrorEvent:
		return g.syncFromErrorLocked(v.Error)
	}
	return false
}

// flushPending 在响应结束后补发暂存的 response.create；无暂存事件时直接返回 nil，
// 发送失败时复位为 idle 并返回错误。
func (g *openAIResponseGate) flushPending(send func([]byte) error) error {
	payload, ok := g.takePendingCreate("test_flush")
	if !ok {
		return nil
	}

	if err := send(payload); err != nil {
		g.setIdle("", "flush_pending_failed")
		return err
	}
	return nil
}

// takePendingCreate 取出暂存的 response.create 并置为 creating 状态；
// 无暂存事件或仍处忙碌状态时返回 false，调用方不发送任何事件。
func (g *openAIResponseGate) takePendingCreate(trigger string) ([]byte, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.pendingCreate == nil || g.isBusyLocked() {
		return nil, false
	}
	payload := append([]byte(nil), g.pendingCreate...)
	reason := g.pendingCreateCause
	g.pendingCreate = nil
	g.pendingCreateCause = ""
	g.state = responseGateCreating
	g.responseID = ""
	g.reason = "flush:" + reason + ":" + trigger
	return payload, true
}

func (g *openAIResponseGate) setIdle(responseID, reason string) {
	g.mu.Lock()
	g.state = responseGateIdle
	g.responseID = responseID
	g.reason = reason
	g.mu.Unlock()
}

func (g *openAIResponseGate) isBusyLocked() bool {
	return g.state == responseGateCreating || g.state == responseGateActive || g.state == responseGateCancelling
}

// syncFromErrorLocked 根据上游错误码修复状态机：response_cancel_not_active 说明
// 没有可取消的响应，复位为 idle；conversation_already_has_active_response 说明上游
// 存在未知活跃响应，记下待取消标记，等响应结束后由调用方补发取消。
func (g *openAIResponseGate) syncFromErrorLocked(err protocol.Error) bool {
	normalized := normalizeErrorCode(err.Code)
	switch normalized {
	case "responsecancelnotactive":
		g.state = responseGateIdle
		g.responseID = ""
		g.cancelAfterCreated = false
		g.cancelAfterCreatedReason = ""
		g.reason = "openai_error:response_cancel_not_active"
		return g.pendingCreate != nil
	case "conversationalreadyhasactiveresponse":
		g.state = responseGateActive
		g.responseID = extractResponseIDFromText(err.Message)
		g.cancelUnknownActive = g.pendingCreate != nil
		g.reason = "openai_error:conversation_already_has_active_response"
	}
	return false
}

func (g *openAIResponseGate) isBusy() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.isBusyLocked()
}

// takeCancelAfterCreated 取出“创建期间请求取消”的意图并返回触发原因，
// 调用方应在收到上游 response.created 后立即补发取消事件。
func (g *openAIResponseGate) takeCancelAfterCreated(responseID string) (string, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.cancelAfterCreated {
		return "", false
	}
	reason := g.cancelAfterCreatedReason
	g.cancelAfterCreated = false
	g.cancelAfterCreatedReason = ""
	if responseID != "" {
		g.responseID = responseID
	}
	return reason, true
}

// takeCancelUnknownActive 取出“上游存在未知活跃响应”的标记，调用方据此补发取消。
func (g *openAIResponseGate) takeCancelUnknownActive() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.cancelUnknownActive {
		return false
	}
	g.cancelUnknownActive = false
	return true
}

// responseCreateAudio 生成仅含音频输出的 response.create 事件。
func responseCreateAudio() ([]byte, error) {
	return marshalJSON(map[string]any{
		"type": "response.create",
		"response": map[string]any{
			"output_modalities": []string{"audio"},
		},
	})
}

// responseCreateAudioWithInstructions 生成带自定义指令的音频输出 response.create 事件。
func responseCreateAudioWithInstructions(instructions string) ([]byte, error) {
	return marshalJSON(map[string]any{
		"type": "response.create",
		"response": map[string]any{
			"output_modalities": []string{"audio"},
			"instructions":      instructions,
		},
	})
}

// responseCreateTextWithInstructions 生成仅含文本输出的 response.create 事件（工作区工具分支使用）。
func responseCreateTextWithInstructions(instructions string) ([]byte, error) {
	return marshalJSON(map[string]any{
		"type": "response.create",
		"response": map[string]any{
			"output_modalities": []string{"text"},
			"instructions":      instructions,
		},
	})
}

func marshalJSON(v any) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// rawString 容错读取 App 消息字段：字符串原样返回、数字转为字符串，
// 字段缺失、null 或类型不匹配时返回空串，避免旧 App 字段类型漂移导致转换失败。
func rawString(raw map[string]json.RawMessage, key string) string {
	value, ok := raw[key]
	if !ok || len(value) == 0 || string(value) == "null" {
		return ""
	}
	var s string
	if err := json.Unmarshal(value, &s); err == nil {
		return s
	}
	var n json.Number
	if err := json.Unmarshal(value, &n); err == nil {
		return n.String()
	}
	return ""
}

// rawNestedString 读取嵌套对象内的字符串字段（如 send_content.text）；外层不是对象时返回空串。
func rawNestedString(raw map[string]json.RawMessage, key, nested string) string {
	value, ok := raw[key]
	if !ok {
		return ""
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(value, &obj); err != nil {
		return ""
	}
	return rawString(obj, nested)
}

// latestHistoryContent 解析 historyContent 的两种形态（JSON 数组或 JSON 字符串包裹的数组），
// 从后往前返回最后一个非空 content，即最新一轮对话历史。
func latestHistoryContent(value json.RawMessage) string {
	if len(value) == 0 || string(value) == "null" {
		return ""
	}
	var rawItems []map[string]json.RawMessage
	if err := json.Unmarshal(value, &rawItems); err != nil {
		var encoded string
		if err := json.Unmarshal(value, &encoded); err != nil || encoded == "" {
			return ""
		}
		if err := json.Unmarshal([]byte(encoded), &rawItems); err != nil {
			return ""
		}
	}
	for i := len(rawItems) - 1; i >= 0; i-- {
		if content := strings.TrimSpace(rawString(rawItems[i], "content")); content != "" {
			return content
		}
	}
	return ""
}

// extractClientResponseID 从 response.cancel payload 中提取响应 ID（兼容两种拼写），
// 提取不到时返回空串，由状态机回退到当前记录的 responseID。
func extractClientResponseID(payload []byte) string {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		return ""
	}
	return firstNonEmpty(rawString(raw, "response_id"), rawString(raw, "responseId"))
}

// normalizeErrorCode 归一化 OpenAI 错误码：转小写并去除非字母数字字符，
// 使同一错误的不同书写格式能稳定匹配。
func normalizeErrorCode(code string) string {
	code = strings.ToLower(code)
	var b strings.Builder
	for _, r := range code {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// responseIDPattern 匹配 OpenAI 响应 ID（resp_ 前缀，仅字母/数字/下划线/连字符）。
var responseIDPattern = regexp.MustCompile(`\bresp_[A-Za-z0-9_-]+\b`)

// extractResponseIDFromText 从错误消息文本中提取响应 ID，用于定位上游活跃响应。
func extractResponseIDFromText(text string) string {
	return responseIDPattern.FindString(text)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// stringFromAny 把工具参数中的任意 JSON 值统一转为去除首尾空白的字符串。
func stringFromAny(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	case nil:
		return ""
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}
