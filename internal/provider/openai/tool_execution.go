package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"TozoAI-Chat-Api/conf"
	"TozoAI-Chat-Api/internal/logger"
	"TozoAI-Chat-Api/internal/service/workspace"
	protocol "TozoAI-Chat-Api/pkg/protocol/openai"
	"TozoAI-Chat-Api/pkg/response"
)

// realtimeToolResult 是 Realtime function_call 执行后的统一结果。
// 它把“回填给 OpenAI 的工具结果”和“立即推给 App 的业务事件”分开，避免工具执行逻辑污染 WebSocket 收发主链路。
type realtimeToolResult struct {
	output           map[string]any
	appResponse      *response.StandardResponse
	continueResponse bool
	textResponse     bool
	cancelActive     bool
	cancelReason     string
	reason           string
}

// executeWeatherFunctionTool 执行 get_open_weather。
// 复刻旧项目逻辑：城市优先于设备坐标；没有城市和坐标时先通知 App 补定位；成功或失败都把结构化结果回填给 OpenAI。
func (c *Client) executeWeatherFunctionTool(ctx context.Context, evt *protocol.ResponseFunctionCallArgumentsDoneEvent, args map[string]any) realtimeToolResult {
	clientCtx := c.gateway.clientContextSnapshot()
	city := stringFromAny(args["city"])
	state := stringFromAny(args["state"])
	country := stringFromAny(args["country"])
	lat := firstNonEmpty(stringFromAny(args["lat"]), clientCtx.lat)
	lon := firstNonEmpty(stringFromAny(args["lon"]), stringFromAny(args["lng"]), clientCtx.lon)
	lang := weatherLang(firstNonEmpty(stringFromAny(args["lang"]), clientCtx.transLang, "en-US"))

	if city == "" && lat == "" && lon == "" {
		content := map[string]any{
			"service_name": evt.Name,
			"args":         args,
			"message":      "天气查询缺少城市和 GPS 坐标，App 需要弹出定位授权或补传坐标。",
		}
		return realtimeToolResult{
			appResponse:  response.NewResponseWithID(0, gatewayResponseOpenWeatherMissingCoords, evt.ResponseID, content, time.Now().UnixMilli()),
			cancelActive: true,
			cancelReason: "weather_missing_coordinates",
			reason:       "weather_missing_coordinates",
		}
	}
	if city == "" && (lat == "" || lon == "") {
		content := map[string]any{
			"service_name": evt.Name,
			"args":         args,
			"message":      "天气查询坐标不完整，需要同时提供 lat 和 lon。",
		}
		return realtimeToolResult{
			appResponse:  response.NewResponseWithID(0, gatewayResponseOpenWeatherMissingCoords, evt.ResponseID, content, time.Now().UnixMilli()),
			cancelActive: true,
			cancelReason: "weather_invalid_coordinates",
			reason:       "weather_invalid_coordinates",
		}
	}

	weatherRequest, limitErr := buildWeatherRequest(args)
	if limitErr != "" {
		output := map[string]any{
			"ok":      false,
			"code":    "weather_date_limit",
			"message": limitErr,
			"args":    args,
		}
		return realtimeToolResult{
			output:           output,
			appResponse:      response.NewResponseWithID(0, gatewayResponseOpenWeatherError, evt.ResponseID, output, time.Now().UnixMilli()),
			continueResponse: true,
			reason:           "weather_date_limit",
		}
	}

	apiKey := firstNonEmpty(c.cfg.ExtraString("open_weather_api_key"), os.Getenv("OPEN_WEATHERMAP_API_KEY"), os.Getenv("OPEN_WEATHER_API_KEY"))
	if apiKey == "" {
		output := map[string]any{
			"ok":      false,
			"code":    "weather_provider_not_configured",
			"message": "OpenWeather API Key 未配置，请在 conf/models/openai.yaml 的 extra.open_weather_api_key 或环境变量 OPEN_WEATHERMAP_API_KEY 中配置。",
			"args":    args,
		}
		return realtimeToolResult{
			output:           output,
			appResponse:      response.NewResponseWithID(501, gatewayResponseOpenWeatherError, evt.ResponseID, output, time.Now().UnixMilli()),
			continueResponse: true,
			reason:           "weather_provider_not_configured",
		}
	}

	endpoint := firstNonEmpty(c.cfg.ExtraString("open_weather_endpoint"), os.Getenv("OPEN_WEATHER_ENDPOINT"), "https://api.openweathermap.org/data/2.5")
	query := url.Values{}
	query.Set("appid", apiKey)
	query.Set("units", "metric")
	query.Set("lang", lang)
	if city != "" {
		query.Set("q", strings.Join(nonEmptyStrings(city, state, country), ","))
	} else {
		query.Set("lat", lat)
		query.Set("lon", lon)
	}
	if weatherRequest.cnt > 0 {
		query.Set("cnt", strconv.Itoa(weatherRequest.cnt))
	}

	rawURL, err := appendToolQuery(joinToolEndpoint(endpoint, weatherRequest.path), query)
	if err != nil {
		return toolExecutionHTTPError(evt, gatewayResponseOpenWeatherError, "weather_invalid_endpoint", err, args)
	}
	statusCode, data, rawBody, err := doToolJSONRequest(ctx, c.toolHTTPClient(), http.MethodGet, rawURL, nil, nil)
	if err != nil {
		return toolExecutionHTTPError(evt, gatewayResponseOpenWeatherError, "weather_request_failed", err, args)
	}
	if statusCode < 200 || statusCode >= 300 {
		output := map[string]any{
			"ok":          false,
			"code":        "weather_http_error",
			"status_code": statusCode,
			"body":        redactedToolBody(firstNonEmpty(rawBody, fmt.Sprint(data))),
			"args":        args,
		}
		return realtimeToolResult{
			output:           output,
			appResponse:      response.NewResponseWithID(502, gatewayResponseOpenWeatherError, evt.ResponseID, output, time.Now().UnixMilli()),
			continueResponse: true,
			reason:           "weather_http_error",
		}
	}

	output := map[string]any{
		"ok":         true,
		"provider":   "openweather",
		"query_type": weatherRequest.queryType,
		"location": map[string]any{
			"city":    city,
			"state":   state,
			"country": country,
			"lat":     lat,
			"lon":     lon,
			"lang":    lang,
		},
		"data": data,
	}
	return realtimeToolResult{output: output, continueResponse: true, reason: "weather_ok"}
}

// executeKnowledgeFunctionTool 执行 search_tozo_knowledge。
// 支持两种后端：自建知识库 HTTP 服务，或 OpenAI Vector Store Search API。
func (c *Client) executeKnowledgeFunctionTool(ctx context.Context, evt *protocol.ResponseFunctionCallArgumentsDoneEvent, args map[string]any) realtimeToolResult {
	query := strings.TrimSpace(stringFromAny(args["query"]))
	productName := strings.TrimSpace(stringFromAny(args["product_name"]))
	if query == "" {
		output := map[string]any{
			"found":   false,
			"code":    "knowledge_query_missing",
			"message": "缺少 query 参数，请让模型基于通用 TOZO 产品知识简短回答。",
			"args":    args,
		}
		return realtimeToolResult{output: output, continueResponse: true, reason: "knowledge_query_missing"}
	}

	searchQuery := query
	if productName != "" {
		searchQuery = productName + " " + query
	}

	endpoint := firstNonEmpty(c.cfg.ExtraString("tozo_knowledge_endpoint"), os.Getenv("TOZO_KNOWLEDGE_ENDPOINT"))
	vectorStoreID := firstNonEmpty(c.cfg.ExtraString("tozo_vector_store_id"), os.Getenv("TOZO_VECTOR_STORE_ID"), os.Getenv("OPENAI_VECTOR_STORE_ID"))
	if endpoint == "" && vectorStoreID != "" {
		endpoint = "https://api.openai.com/v1/vector_stores/" + url.PathEscape(vectorStoreID) + "/search"
	}
	apiKey := firstNonEmpty(c.cfg.ExtraString("tozo_knowledge_api_key"), os.Getenv("TOZO_KNOWLEDGE_API_KEY"), c.cfg.APIKey)
	if endpoint == "" {
		output := map[string]any{
			"found":   false,
			"code":    "tozo_knowledge_provider_not_configured",
			"message": "TOZO 知识库未配置，请配置 extra.tozo_knowledge_endpoint 或 extra.tozo_vector_store_id。",
			"args":    args,
		}
		return realtimeToolResult{
			output:           output,
			appResponse:      response.NewResponseWithID(501, response.EventError, evt.ResponseID, output, time.Now().UnixMilli()),
			continueResponse: true,
			reason:           "tozo_knowledge_provider_not_configured",
		}
	}

	body := map[string]any{}
	headers := map[string]string{"Content-Type": "application/json"}
	if isOpenAIVectorSearchEndpoint(endpoint) {
		body["query"] = searchQuery
		body["max_num_results"] = maxPositive(c.cfg.ExtraInt("tozo_knowledge_max_results"), 3)
		if apiKey != "" {
			headers["Authorization"] = "Bearer " + apiKey
		}
		headers["OpenAI-Beta"] = "assistants=v2"
	} else {
		body["query"] = query
		body["search_query"] = searchQuery
		body["product_name"] = productName
		body["user_id"] = firstNonEmpty(c.userID, c.gateway.clientContextSnapshot().appUserID)
		body["session_id"] = c.sessionID
		if apiKey != "" {
			headers["Authorization"] = "Bearer " + apiKey
		}
	}

	statusCode, data, rawBody, err := doToolJSONRequest(ctx, c.toolHTTPClient(), http.MethodPost, endpoint, body, headers)
	if err != nil {
		return toolExecutionHTTPError(evt, response.EventError, "knowledge_request_failed", err, args)
	}
	if statusCode < 200 || statusCode >= 300 {
		output := map[string]any{
			"found":       false,
			"code":        "knowledge_http_error",
			"status_code": statusCode,
			"message":     "知识库服务返回非 2xx 状态，模型应提示用户稍后再试。",
			"body":        redactedToolBody(firstNonEmpty(rawBody, fmt.Sprint(data))),
		}
		return realtimeToolResult{
			output:           output,
			appResponse:      response.NewResponseWithID(502, response.EventError, evt.ResponseID, output, time.Now().UnixMilli()),
			continueResponse: true,
			reason:           "knowledge_http_error",
		}
	}

	output := normalizeKnowledgeOutput(data, searchQuery)
	return realtimeToolResult{output: output, continueResponse: true, reason: "knowledge_ok"}
}

// executeNavigationFunctionTool 执行地图导航工具。
// 成功时立即向 App 推送候选地点，同时把候选地点作为 function_call_output 回填给 OpenAI 继续生成简短语音提示。
func (c *Client) executeNavigationFunctionTool(ctx context.Context, evt *protocol.ResponseFunctionCallArgumentsDoneEvent, args map[string]any) realtimeToolResult {
	clientCtx := c.gateway.clientContextSnapshot()
	lat := firstNonEmpty(stringFromAny(args["lat"]), clientCtx.lat)
	lon := firstNonEmpty(stringFromAny(args["lon"]), stringFromAny(args["lng"]), clientCtx.lon)
	content := map[string]any{"service_name": evt.Name, "args": args}
	if lat == "" || lon == "" {
		return realtimeToolResult{
			appResponse:  response.NewResponseWithID(0, gatewayResponseMapServiceMissingCoordinates, evt.ResponseID, content, time.Now().UnixMilli()),
			cancelActive: true,
			cancelReason: "map_missing_coordinates",
			reason:       "map_missing_coordinates",
		}
	}

	mapSDK := strings.ToLower(firstNonEmpty(stringFromAny(args["map_sdk"]), clientCtx.mapSDK, c.cfg.ExtraString("default_map_sdk"), "mapbox"))
	if mapSDK != "mapbox" {
		output := map[string]any{
			"ok":      false,
			"code":    "map_provider_not_configured",
			"message": "当前 Go 版本已实现 Mapbox 查询；Google/Amap 可按同一接口继续扩展。",
			"map_sdk": mapSDK,
			"args":    args,
		}
		return realtimeToolResult{
			output:           output,
			appResponse:      response.NewResponseWithID(501, gatewayResponseMapServiceFail, evt.ResponseID, output, time.Now().UnixMilli()),
			continueResponse: true,
			reason:           "map_provider_not_configured",
		}
	}

	apiKey := firstNonEmpty(c.cfg.ExtraString("mapbox_api_key"), os.Getenv("MAPBOX_MAP_API_KEY"), os.Getenv("MAPBOX_API_KEY"))
	if apiKey == "" {
		output := map[string]any{
			"ok":      false,
			"code":    "map_provider_not_configured",
			"message": "Mapbox API Key 未配置，请配置 extra.mapbox_api_key 或环境变量 MAPBOX_MAP_API_KEY。",
			"map_sdk": mapSDK,
			"args":    args,
		}
		return realtimeToolResult{
			output:           output,
			appResponse:      response.NewResponseWithID(501, gatewayResponseMapServiceFail, evt.ResponseID, output, time.Now().UnixMilli()),
			continueResponse: true,
			reason:           "map_provider_not_configured",
		}
	}

	requestURL, requestErr := c.buildMapboxNavigationURL(evt.Name, args, lat, lon, apiKey, clientCtx.transLang)
	if requestErr != nil {
		output := map[string]any{"ok": false, "code": "map_invalid_arguments", "message": requestErr.Error(), "args": args}
		return realtimeToolResult{
			output:           output,
			appResponse:      response.NewResponseWithID(400, gatewayResponseMapServiceFail, evt.ResponseID, output, time.Now().UnixMilli()),
			continueResponse: true,
			reason:           "map_invalid_arguments",
		}
	}

	statusCode, data, rawBody, err := doToolJSONRequest(ctx, c.toolHTTPClient(), http.MethodGet, requestURL, nil, nil)
	if err != nil {
		return toolExecutionHTTPError(evt, gatewayResponseMapServiceFail, "map_request_failed", err, args)
	}
	if statusCode < 200 || statusCode >= 300 {
		output := map[string]any{
			"ok":          false,
			"code":        "map_http_error",
			"status_code": statusCode,
			"body":        redactedToolBody(firstNonEmpty(rawBody, fmt.Sprint(data))),
			"args":        args,
		}
		return realtimeToolResult{
			output:           output,
			appResponse:      response.NewResponseWithID(502, gatewayResponseMapServiceFail, evt.ResponseID, output, time.Now().UnixMilli()),
			continueResponse: true,
			reason:           "map_http_error",
		}
	}

	places := parseMapboxPlaces(data, firstNonEmpty(stringFromAny(args["travelMode"]), "Drive"))
	content = map[string]any{"places": places, "map_sdk": "mapbox"}
	output := map[string]any{
		"ok":           true,
		"service_name": evt.Name,
		"map_sdk":      "mapbox",
		"origin":       map[string]any{"lat": lat, "lon": lon},
		"places":       places,
	}
	return realtimeToolResult{
		output:           output,
		appResponse:      response.NewResponseWithID(0, gatewayResponseMapServicePlaces, evt.ResponseID, content, time.Now().UnixMilli()),
		continueResponse: true,
		reason:           "map_ok",
	}
}

func (c *Client) executeWorkspaceFunctionTool(ctx context.Context, evt *protocol.ResponseFunctionCallArgumentsDoneEvent, args map[string]any) realtimeToolResult {
	projectID := firstNonEmpty(stringFromAny(args["project_id"]), "current")
	relPath := stringFromAny(args["path"])
	output := map[string]any{
		"ok":         false,
		"tool":       evt.Name,
		"project_id": projectID,
		"path":       relPath,
	}

	if err := ctx.Err(); err != nil {
		return workspaceToolResult(evt, output, "workspace_context_cancelled", err)
	}

	switch evt.Name {
	case "workspace_list_files":
		entries, err := workspace.List(projectID, relPath)
		if err != nil {
			return workspaceToolResult(evt, output, "workspace_list_failed", err)
		}
		output["ok"] = true
		output["entries"] = entries
		output["count"] = len(entries)
		return workspaceToolResult(evt, output, "workspace_list_ok", nil)

	case "workspace_read_file":
		file, err := workspace.Read(projectID, relPath)
		if err != nil {
			return workspaceToolResult(evt, output, "workspace_read_failed", err)
		}
		output["ok"] = true
		output["file"] = workspaceFileOutput(file, true)
		return workspaceToolResult(evt, output, "workspace_read_ok", nil)

	case "workspace_write_file":
		if workspaceWriteConfirmEnabled() {
			pending, err := workspace.PreviewWrite(projectID, relPath, stringFromAny(args["content"]), workspace.WriteActor{
				UserID:    c.userID,
				Source:    "model_tool",
				RequestID: c.sessionID,
			})
			if err != nil {
				return workspaceToolResult(evt, output, "workspace_write_failed", err)
			}
			output["ok"] = true
			output["pending_write_id"] = pending.ID
			output["diff"] = pending.Diff
			output["diff_hash"] = pending.DiffHash
			output["status"] = pending.Status
			return workspaceToolResult(evt, output, "workspace_write_pending", nil)
		}
		file, err := workspace.Write(projectID, relPath, stringFromAny(args["content"]))
		if err != nil {
			return workspaceToolResult(evt, output, "workspace_write_failed", err)
		}
		output["ok"] = true
		output["file"] = workspaceFileOutput(file, false)
		return workspaceToolResult(evt, output, "workspace_write_ok", nil)

	default:
		return workspaceToolResult(evt, output, "workspace_unknown_tool", fmt.Errorf("unknown workspace tool: %s", evt.Name))
	}
}

func workspaceWriteConfirmEnabled() bool {
	return conf.Global != nil && conf.Global.Security.WorkspaceWriteConfirm
}

func workspaceToolResult(evt *protocol.ResponseFunctionCallArgumentsDoneEvent, output map[string]any, reason string, err error) realtimeToolResult {
	code := 0
	if err != nil {
		code = 400
		output["ok"] = false
		output["code"] = reason
		output["error"] = err.Error()
	}
	return realtimeToolResult{
		output:           output,
		appResponse:      response.NewResponseWithID(code, response.ResponseEvent("workspace_tool"), evt.ResponseID, output, time.Now().UnixMilli()),
		continueResponse: true,
		textResponse:     true,
		reason:           reason,
	}
}

func workspaceFileOutput(file workspace.FileContent, includeContent bool) map[string]any {
	out := map[string]any{
		"path": file.Path,
		"size": file.Size,
	}
	if includeContent {
		out["content"] = file.Content
	}
	return out
}

func (c *Client) toolHTTPClient() *http.Client {
	return &http.Client{Timeout: c.cfg.GetToolTimeout()}
}

func (c *Client) buildMapboxNavigationURL(functionName string, args map[string]any, lat, lon, apiKey, transLang string) (string, error) {
	base := strings.TrimRight(firstNonEmpty(c.cfg.ExtraString("mapbox_endpoint"), "https://api.mapbox.com/search/searchbox/v1"), "/")
	query := url.Values{}
	query.Set("access_token", apiKey)
	query.Set("language", mapboxLang(firstNonEmpty(stringFromAny(args["lang"]), transLang, "en-US")))
	query.Set("proximity", lon+","+lat)

	switch functionName {
	case "get_specify_route_navigation":
		destination := strings.TrimSpace(stringFromAny(args["destination"]))
		if destination == "" {
			return "", fmt.Errorf("缺少 destination 参数")
		}
		query.Set("q", destination)
		return appendToolQuery(base+"/forward", query)
	case "get_nearby_route_navigation":
		placeType := strings.TrimSpace(stringFromAny(args["placeType"]))
		if placeType == "" {
			return "", fmt.Errorf("缺少 placeType 参数")
		}
		return appendToolQuery(base+"/category/"+url.PathEscape(placeType), query)
	default:
		return "", fmt.Errorf("未知地图函数: %s", functionName)
	}
}

type weatherRequestPlan struct {
	path      string
	cnt       int
	queryType string
}

func buildWeatherRequest(args map[string]any) (weatherRequestPlan, string) {
	queryType := strings.ToLower(firstNonEmpty(stringFromAny(args["query_type"]), "current"))
	targetDate := stringFromAny(args["target_date"])
	cnt := positiveIntFromAny(args["cnt"])
	if targetDate != "" {
		target, err := time.Parse("2006-01-02", targetDate)
		if err == nil {
			now := time.Now().UTC().Truncate(24 * time.Hour)
			days := int(target.UTC().Sub(now).Hours()/24) + 1
			if days > 5 {
				return weatherRequestPlan{}, "当前天气服务最多支持未来 5 天内的天气预报。"
			}
			if days > 0 {
				queryType = "forecast"
				cnt = days * 8
			}
		}
	}
	if queryType == "forecast" {
		if cnt <= 0 {
			cnt = 8
		}
		if cnt <= 5 {
			cnt = cnt * 8
		}
		return weatherRequestPlan{path: "/forecast", cnt: minInt(cnt, 40), queryType: "forecast"}, ""
	}
	return weatherRequestPlan{path: "/weather", queryType: "current"}, ""
}

func doToolJSONRequest(ctx context.Context, client *http.Client, method, rawURL string, body any, headers map[string]string) (int, map[string]any, string, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyData, err := json.Marshal(body)
		if err != nil {
			return 0, nil, "", err
		}
		bodyReader = bytes.NewReader(bodyData)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, bodyReader)
	if err != nil {
		return 0, nil, "", err
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, "", err
	}
	defer resp.Body.Close()

	bodyData, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return resp.StatusCode, nil, "", err
	}
	rawBody := string(bodyData)
	if strings.TrimSpace(rawBody) == "" {
		return resp.StatusCode, map[string]any{}, rawBody, nil
	}
	var decoded map[string]any
	if err := json.Unmarshal(bodyData, &decoded); err != nil {
		return resp.StatusCode, map[string]any{"raw": rawBody}, rawBody, nil
	}
	return resp.StatusCode, decoded, rawBody, nil
}

func toolExecutionHTTPError(evt *protocol.ResponseFunctionCallArgumentsDoneEvent, event response.ResponseEvent, code string, err error, args map[string]any) realtimeToolResult {
	output := map[string]any{
		"ok":      false,
		"code":    code,
		"message": err.Error(),
		"args":    args,
	}
	return realtimeToolResult{
		output:           output,
		appResponse:      response.NewResponseWithID(502, event, evt.ResponseID, output, time.Now().UnixMilli()),
		continueResponse: true,
		reason:           code,
	}
}

func redactedToolBody(body string) string {
	body = strings.TrimSpace(body)
	if body == "" {
		return ""
	}
	return logger.RedactField("content", body)
}

func normalizeKnowledgeOutput(data map[string]any, searchQuery string) map[string]any {
	results, _ := data["data"].([]any)
	if len(results) == 0 {
		if _, hasFound := data["found"]; hasFound {
			return data
		}
		data["found"] = false
		data["search_query"] = searchQuery
		return data
	}
	contents := make([]string, 0, len(results))
	for _, item := range results {
		itemMap, _ := item.(map[string]any)
		for _, block := range asAnySlice(itemMap["content"]) {
			blockMap, _ := block.(map[string]any)
			if blockMap["type"] == "text" {
				if text := strings.TrimSpace(stringFromAny(blockMap["text"])); text != "" {
					contents = append(contents, text)
				}
			}
		}
	}
	if len(contents) == 0 {
		return map[string]any{
			"found":        false,
			"search_query": searchQuery,
			"message":      "知识库返回了结果但没有可用文本内容。",
		}
	}
	return map[string]any{
		"found":            true,
		"search_query":     searchQuery,
		"relevant_content": strings.Join(contents, "\n\n---\n\n"),
		"instruction":      "请基于 relevant_content 自然回答用户，不要逐字照抄；如果知识内容与用户语言不同，请自然翻译并保持简洁。",
	}
}

func parseMapboxPlaces(data map[string]any, travelMode string) []map[string]any {
	features := asAnySlice(data["features"])
	places := make([]map[string]any, 0, len(features))
	for _, feature := range features {
		featureMap, _ := feature.(map[string]any)
		props, _ := featureMap["properties"].(map[string]any)
		coords, _ := props["coordinates"].(map[string]any)
		place := map[string]any{
			"mapbox_id":    stringFromAny(props["mapbox_id"]),
			"name":         stringFromAny(props["name"]),
			"feature_type": stringFromAny(props["feature_type"]),
			"full_address": stringFromAny(props["full_address"]),
			"coordinates": map[string]any{
				"latitude":  stringFromAny(coords["latitude"]),
				"longitude": stringFromAny(coords["longitude"]),
			},
			"distance":   props["distance"],
			"travelMode": travelMode,
		}
		places = append(places, place)
	}
	return places
}

func appendToolQuery(rawURL string, query url.Values) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	existing := u.Query()
	for key, values := range query {
		for _, value := range values {
			existing.Add(key, value)
		}
	}
	u.RawQuery = existing.Encode()
	return u.String(), nil
}

func joinToolEndpoint(endpoint, path string) string {
	endpoint = strings.TrimRight(endpoint, "/")
	if strings.HasSuffix(endpoint, "/weather") || strings.HasSuffix(endpoint, "/forecast") {
		return endpoint
	}
	return endpoint + path
}

func isOpenAIVectorSearchEndpoint(endpoint string) bool {
	return strings.Contains(endpoint, "api.openai.com/v1/vector_stores/") && strings.HasSuffix(strings.TrimRight(endpoint, "/"), "/search")
}

func weatherLang(lang string) string {
	lang = strings.ToLower(strings.ReplaceAll(lang, "-", "_"))
	if strings.HasPrefix(lang, "zh") {
		return "zh_cn"
	}
	if len(lang) >= 2 {
		return lang[:2]
	}
	return "en"
}

func mapboxLang(lang string) string {
	lang = strings.ToLower(lang)
	if idx := strings.IndexAny(lang, "-_"); idx > 0 {
		return lang[:idx]
	}
	if lang == "" {
		return "en"
	}
	return lang
}

func nonEmptyStrings(values ...string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			out = append(out, strings.TrimSpace(value))
		}
	}
	return out
}

func asAnySlice(value any) []any {
	items, _ := value.([]any)
	return items
}

func positiveIntFromAny(value any) int {
	switch v := value.(type) {
	case int:
		if v > 0 {
			return v
		}
	case int64:
		if v > 0 {
			return int(v)
		}
	case float64:
		if v > 0 {
			return int(v)
		}
	case string:
		n, _ := strconv.Atoi(strings.TrimSpace(v))
		if n > 0 {
			return n
		}
	}
	return 0
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxPositive(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}
