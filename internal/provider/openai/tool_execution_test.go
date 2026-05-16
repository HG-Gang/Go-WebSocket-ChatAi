package openai

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"TozoAI-Chat-Api/conf"
	protocol "TozoAI-Chat-Api/pkg/protocol/openai"
)

func TestExecuteWeatherFunctionToolCallsConfiguredEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/weather" {
			t.Fatalf("weather path = %s, want /weather", r.URL.Path)
		}
		if got := r.URL.Query().Get("q"); got != "Shenzhen" {
			t.Fatalf("q = %q, want Shenzhen", got)
		}
		if got := r.URL.Query().Get("appid"); got != "weather-key" {
			t.Fatalf("appid = %q, want weather-key", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"cod":200,"name":"Shenzhen","main":{"temp":28}}`))
	}))
	defer server.Close()

	client := &Client{
		cfg: NewOpenAIConfig(&conf.ModelConfig{Extra: map[string]interface{}{
			"open_weather_endpoint": server.URL,
			"open_weather_api_key":  "weather-key",
			"tool_timeout_ms":       1000,
		}}),
		gateway: newGatewayAdapter(),
	}
	evt := &protocol.ResponseFunctionCallArgumentsDoneEvent{ResponseID: "resp_1", Name: "get_open_weather"}

	result := client.executeWeatherFunctionTool(context.Background(), evt, map[string]any{"city": "Shenzhen"})
	if result.appResponse != nil {
		t.Fatalf("appResponse = %+v, want nil on success", result.appResponse)
	}
	if !result.continueResponse {
		t.Fatalf("continueResponse = false, want true")
	}
	if result.output["ok"] != true {
		t.Fatalf("output ok = %v, want true", result.output["ok"])
	}
	location, _ := result.output["location"].(map[string]any)
	if location["city"] != "Shenzhen" {
		t.Fatalf("location city = %v, want Shenzhen", location["city"])
	}
}

func TestExecuteWeatherFunctionToolMissingLocationCancelsActiveResponse(t *testing.T) {
	client := &Client{
		cfg:     NewOpenAIConfig(&conf.ModelConfig{}),
		gateway: newGatewayAdapter(),
	}
	evt := &protocol.ResponseFunctionCallArgumentsDoneEvent{ResponseID: "resp_1", Name: "get_open_weather"}

	result := client.executeWeatherFunctionTool(context.Background(), evt, map[string]any{})
	if !result.cancelActive {
		t.Fatalf("cancelActive = false, want true")
	}
	if result.continueResponse {
		t.Fatalf("continueResponse = true, want false")
	}
	if result.appResponse == nil || result.appResponse.Response != gatewayResponseOpenWeatherMissingCoords {
		t.Fatalf("appResponse = %+v, want open_weather_missing_coordinates", result.appResponse)
	}
}

func TestExecuteNavigationFunctionToolReturnsMapboxPlaces(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/forward" {
			t.Fatalf("mapbox path = %s, want /forward", r.URL.Path)
		}
		if got := r.URL.Query().Get("q"); got != "Airport" {
			t.Fatalf("q = %q, want Airport", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"features": []map[string]any{
				{
					"properties": map[string]any{
						"mapbox_id":    "poi.1",
						"name":         "Airport",
						"feature_type": "poi",
						"full_address": "Airport Road",
						"distance":     1200,
						"coordinates": map[string]any{
							"latitude":  22.1,
							"longitude": 114.1,
						},
					},
				},
			},
		})
	}))
	defer server.Close()

	client := &Client{
		cfg: NewOpenAIConfig(&conf.ModelConfig{Extra: map[string]interface{}{
			"mapbox_endpoint": server.URL,
			"mapbox_api_key":  "map-key",
			"tool_timeout_ms": 1000,
		}}),
		gateway: newGatewayAdapter(),
	}
	evt := &protocol.ResponseFunctionCallArgumentsDoneEvent{ResponseID: "resp_1", Name: "get_specify_route_navigation"}
	args := map[string]any{"destination": "Airport", "travelMode": "Drive", "lat": "22.0", "lon": "114.0"}

	result := client.executeNavigationFunctionTool(context.Background(), evt, args)
	if !result.continueResponse {
		t.Fatalf("continueResponse = false, want true")
	}
	if result.appResponse == nil || result.appResponse.Response != gatewayResponseMapServicePlaces {
		t.Fatalf("appResponse = %+v, want map_service_places", result.appResponse)
	}
	places, _ := result.output["places"].([]map[string]any)
	if len(places) != 1 || places[0]["name"] != "Airport" {
		t.Fatalf("places = %+v, want one Airport", places)
	}
}
