package compat

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	agentv1 "github.com/sahara-ai/sahara/gen/sahara/agent/v1"
	eventv1 "github.com/sahara-ai/sahara/gen/sahara/event/v1"
	"github.com/redis/go-redis/v9"
	"github.com/sahara-ai/sahara/gateway/internal/auth"
	"github.com/sahara-ai/sahara/gateway/internal/dispatch"
)

const (
	requestTimeout = 5 * time.Minute
	defaultModel   = "sahara-agent-v1"
)

// Handler serves the OpenAI-compatible HTTP endpoints.
type Handler struct {
	disp          *dispatch.Dispatcher
	rdb           *redis.Client
	authenticator *auth.Authenticator
}

// NewHandler creates an OpenAI compatibility handler.
func NewHandler(disp *dispatch.Dispatcher, rdb *redis.Client, authenticator *auth.Authenticator) *Handler {
	return &Handler{disp: disp, rdb: rdb, authenticator: authenticator}
}

// RegisterRoutes registers all OpenAI-compatible endpoints on the given mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/chat/completions", h.handleChatCompletions)
	mux.HandleFunc("GET /v1/models", h.handleListModels)
}

// ── Authentication ──────────────────────────────────

func (h *Handler) authenticate(w http.ResponseWriter, r *http.Request) (*auth.Claims, bool) {
	claims, err := h.authenticator.Authenticate(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid_api_key", err.Error())
		return nil, false
	}
	return claims, true
}

// ── POST /v1/chat/completions ───────────────────────

func (h *Handler) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	claims, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	_ = claims // available for per-user quota enforcement in future

	var req ChatCompletionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "Invalid JSON: "+err.Error())
		return
	}

	if len(req.Messages) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "messages array is required")
		return
	}

	lastMsg := req.Messages[len(req.Messages)-1]
	if lastMsg.Role != "user" || lastMsg.Content == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "last message must be a non-empty user message")
		return
	}

	model := req.Model
	if model == "" {
		model = defaultModel
	}

	completionID := newCompletionID()
	sessionKey := fmt.Sprintf("compat-%s", completionID)
	taskID := fmt.Sprintf("task_%d", time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()

	submitReq := &agentv1.SubmitTaskRequest{
		TaskId:     taskID,
		SessionKey: sessionKey,
		AgentId:    "default",
		UserMessage: &agentv1.UserMessage{
			Text: lastMsg.Content,
		},
		Options: &agentv1.TaskOptions{
			ModelOverride: model,
		},
	}

	result, err := h.disp.Submit(ctx, submitReq)
	if err != nil {
		slog.Error("compat submit failed", "task_id", taskID, "err", err)
		writeError(w, http.StatusServiceUnavailable, "server_error", "Failed to submit task: "+err.Error())
		return
	}

	slog.Info("compat task submitted",
		"completion_id", completionID,
		"task_id", taskID,
		"run_id", result.RunID,
		"stream", req.Stream,
	)

	reader := NewEventReader(h.rdb, sessionKey)
	go reader.Run(ctx)
	defer reader.Stop()

	if req.Stream {
		h.handleStream(ctx, w, reader, completionID, model)
	} else {
		h.handleNonStream(ctx, w, reader, completionID, model)
	}
}

// ── Streaming (SSE) ─────────────────────────────────

func (h *Handler) handleStream(ctx context.Context, w http.ResponseWriter, reader *EventReader, completionID, model string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "server_error", "Streaming not supported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	// First chunk: role announcement
	sendSSEChunk(w, flusher, completionID, model, ChatChunkDelta{Role: "assistant"}, nil)

	var usage *Usage

	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-reader.Events():
			if !ok {
				sendSSEDone(w, flusher)
				return
			}

			switch evt.Type {
			case eventv1.EventType_EVENT_TYPE_DELTA:
				if delta := evt.Proto.GetDelta(); delta != nil {
					sendSSEChunk(w, flusher, completionID, model,
						ChatChunkDelta{Content: delta.GetText()}, nil)
				}

			case eventv1.EventType_EVENT_TYPE_USAGE:
				if u := evt.Proto.GetUsage(); u != nil {
					usage = &Usage{
						PromptTokens:     int(u.GetInputTokens()),
						CompletionTokens: int(u.GetOutputTokens()),
						TotalTokens:      int(u.GetInputTokens() + u.GetOutputTokens()),
					}
				}

			case eventv1.EventType_EVENT_TYPE_RUN_COMPLETE:
				stop := "stop"
				sendSSEChunk(w, flusher, completionID, model,
					ChatChunkDelta{}, &stop)
				if usage != nil {
					sendSSEUsage(w, flusher, completionID, model, usage)
				}
				sendSSEDone(w, flusher)
				return

			case eventv1.EventType_EVENT_TYPE_RUN_ERROR:
				stop := "stop"
				errPayload := evt.Proto.GetRunError()
				errMsg := "Runtime error"
				if errPayload != nil {
					errMsg = errPayload.GetErrorMessage()
				}
				sendSSEChunk(w, flusher, completionID, model,
					ChatChunkDelta{Content: "\n\n[Error: " + errMsg + "]"}, &stop)
				sendSSEDone(w, flusher)
				return

			case eventv1.EventType_EVENT_TYPE_RUN_ABORT:
				stop := "stop"
				sendSSEChunk(w, flusher, completionID, model,
					ChatChunkDelta{}, &stop)
				sendSSEDone(w, flusher)
				return
			}
		}
	}
}

func sendSSEChunk(w http.ResponseWriter, flusher http.Flusher, id, model string, delta ChatChunkDelta, finishReason *string) {
	chunk := ChatCompletionChunk{
		ID:      id,
		Object:  "chat.completion.chunk",
		Created: time.Now().Unix(),
		Model:   model,
		Choices: []ChatChunkChoice{{
			Index:        0,
			Delta:        delta,
			FinishReason: finishReason,
		}},
	}
	data, _ := json.Marshal(chunk)
	fmt.Fprintf(w, "data: %s\n\n", data)
	flusher.Flush()
}

func sendSSEUsage(w http.ResponseWriter, flusher http.Flusher, id, model string, usage *Usage) {
	chunk := ChatCompletionChunk{
		ID:      id,
		Object:  "chat.completion.chunk",
		Created: time.Now().Unix(),
		Model:   model,
		Choices: []ChatChunkChoice{},
		Usage:   usage,
	}
	data, _ := json.Marshal(chunk)
	fmt.Fprintf(w, "data: %s\n\n", data)
	flusher.Flush()
}

func sendSSEDone(w http.ResponseWriter, flusher http.Flusher) {
	fmt.Fprintf(w, "data: [DONE]\n\n")
	flusher.Flush()
}

// ── Non-streaming ───────────────────────────────────

func (h *Handler) handleNonStream(ctx context.Context, w http.ResponseWriter, reader *EventReader, completionID, model string) {
	var fullText strings.Builder
	var usage *Usage

	for {
		select {
		case <-ctx.Done():
			writeError(w, http.StatusGatewayTimeout, "timeout", "Request timed out waiting for completion")
			return
		case evt, ok := <-reader.Events():
			if !ok {
				writeError(w, http.StatusInternalServerError, "server_error", "Event stream closed unexpectedly")
				return
			}

			switch evt.Type {
			case eventv1.EventType_EVENT_TYPE_DELTA:
				if delta := evt.Proto.GetDelta(); delta != nil {
					fullText.WriteString(delta.GetText())
				}

			case eventv1.EventType_EVENT_TYPE_USAGE:
				if u := evt.Proto.GetUsage(); u != nil {
					usage = &Usage{
						PromptTokens:     int(u.GetInputTokens()),
						CompletionTokens: int(u.GetOutputTokens()),
						TotalTokens:      int(u.GetInputTokens() + u.GetOutputTokens()),
					}
				}

			case eventv1.EventType_EVENT_TYPE_RUN_COMPLETE:
				finalText := fullText.String()
				if rc := evt.Proto.GetRunComplete(); rc != nil && rc.GetFinalText() != "" {
					finalText = rc.GetFinalText()
				}

				resp := ChatCompletionResponse{
					ID:      completionID,
					Object:  "chat.completion",
					Created: time.Now().Unix(),
					Model:   model,
					Choices: []ChatCompletionChoice{{
						Index:        0,
						Message:      &ChatMessage{Role: "assistant", Content: finalText},
						FinishReason: "stop",
					}},
					Usage: usage,
				}

				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(resp)
				return

			case eventv1.EventType_EVENT_TYPE_RUN_ERROR:
				errMsg := "Runtime error"
				if errPayload := evt.Proto.GetRunError(); errPayload != nil {
					errMsg = errPayload.GetErrorMessage()
				}
				writeError(w, http.StatusInternalServerError, "server_error", errMsg)
				return

			case eventv1.EventType_EVENT_TYPE_RUN_ABORT:
				writeError(w, http.StatusInternalServerError, "server_error", "Task was aborted")
				return
			}
		}
	}
}

// ── GET /v1/models ──────────────────────────────────

func (h *Handler) handleListModels(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.authenticate(w, r); !ok {
		return
	}

	resp := ModelList{
		Object: "list",
		Data: []ModelInfo{
			{ID: "sahara-agent-v1", Object: "model", Created: 1700000000, OwnedBy: "sahara"},
			{ID: "claude-sonnet-4-20250514", Object: "model", Created: 1700000000, OwnedBy: "anthropic"},
			{ID: "gpt-4o", Object: "model", Created: 1700000000, OwnedBy: "openai"},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ── Helpers ─────────────────────────────────────────

func writeError(w http.ResponseWriter, status int, errType, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(ErrorResponse{
		Error: ErrorDetail{
			Message: message,
			Type:    errType,
		},
	})
}
