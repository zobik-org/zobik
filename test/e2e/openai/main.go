// Command openai serves the fake OpenAI endpoint of the test harness as a
// container on a network's Docker network, where the Integration Sidecar of the
// assistant blueprint reaches it. It answers POST /v1/responses with the text of
// the last input message after "echo: " and a fixed usage, so the end-to-end
// test knows each call's cost from the rate table.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
)

// The usage of every response.
const (
	inputTokens  = 100
	cachedTokens = 20
	outputTokens = 50
)

type request struct {
	Model string `json:"model"`
	Input []struct {
		Content string `json:"content"`
	} `json:"input"`
	Store *bool `json:"store"`
}

func main() {
	addr := flag.String("addr", ":8080", "the address to listen on")
	key := flag.String("key", "", "the API key the requests carry as Authorization: Bearer, if any")
	flag.Parse()

	http.HandleFunc("POST /v1/responses", func(w http.ResponseWriter, r *http.Request) {
		if *key != "" && r.Header.Get("Authorization") != "Bearer "+*key {
			fail(w, http.StatusUnauthorized, "invalid_api_key")
			return
		}
		var req request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Model == "" || len(req.Input) == 0 {
			fail(w, http.StatusBadRequest, "invalid_request")
			return
		}
		// The bundle's members send store = false (implementation §7.2).
		if req.Store == nil || *req.Store {
			fail(w, http.StatusBadRequest, "store_not_false")
			return
		}
		text := "echo: " + req.Input[len(req.Input)-1].Content
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"object": "response",
			"status": "completed",
			"model":  req.Model,
			"output": []any{map[string]any{
				"type":    "message",
				"role":    "assistant",
				"content": []any{map[string]any{"type": "output_text", "text": text}},
			}},
			"usage": map[string]any{
				"input_tokens":         inputTokens,
				"input_tokens_details": map[string]int{"cached_tokens": cachedTokens},
				"output_tokens":        outputTokens,
				"total_tokens":         inputTokens + outputTokens,
			},
		})
	})
	log.Fatal(http.ListenAndServe(*addr, nil))
}

func fail(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code}})
}
