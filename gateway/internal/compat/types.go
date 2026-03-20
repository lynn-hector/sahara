// Package compat implements an OpenAI-compatible HTTP API layer.
//
// This package translates between the OpenAI Chat Completions API format
// and Sahara's internal gRPC + Redis Streams event architecture.
//
// Supported endpoints:
//   - POST /v1/chat/completions  (streaming SSE + non-streaming JSON)
//   - GET  /v1/models            (list available models)
package compat

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// ── Request types ───────────────────────────────────

// ChatCompletionRequest mirrors the OpenAI chat completions request body.
type ChatCompletionRequest struct {
	Model       string          `json:"model"`
	Messages    []ChatMessage   `json:"messages"`
	Stream      bool            `json:"stream,omitempty"`
	MaxTokens   int             `json:"max_tokens,omitempty"`
	Temperature *float64        `json:"temperature,omitempty"`
	User        string          `json:"user,omitempty"`
	Metadata    map[string]any  `json:"metadata,omitempty"`
}

// ChatMessage is a single message in the conversation.
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ── Response types (non-streaming) ──────────────────

// ChatCompletionResponse mirrors the OpenAI chat completions response.
type ChatCompletionResponse struct {
	ID      string                 `json:"id"`
	Object  string                 `json:"object"`
	Created int64                  `json:"created"`
	Model   string                 `json:"model"`
	Choices []ChatCompletionChoice `json:"choices"`
	Usage   *Usage                 `json:"usage,omitempty"`
}

// ChatCompletionChoice is a single choice in the response.
type ChatCompletionChoice struct {
	Index        int          `json:"index"`
	Message      *ChatMessage `json:"message,omitempty"`
	FinishReason string       `json:"finish_reason"`
}

// Usage tracks token consumption.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// ── Response types (streaming SSE) ──────────────────

// ChatCompletionChunk is a single SSE chunk in streaming mode.
type ChatCompletionChunk struct {
	ID      string              `json:"id"`
	Object  string              `json:"object"`
	Created int64               `json:"created"`
	Model   string              `json:"model"`
	Choices []ChatChunkChoice   `json:"choices"`
	Usage   *Usage              `json:"usage,omitempty"`
}

// ChatChunkChoice is a single choice in a streaming chunk.
type ChatChunkChoice struct {
	Index        int           `json:"index"`
	Delta        ChatChunkDelta `json:"delta"`
	FinishReason *string       `json:"finish_reason"`
}

// ChatChunkDelta is the incremental content in a streaming chunk.
type ChatChunkDelta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

// ── Models endpoint ─────────────────────────────────

// ModelList mirrors the OpenAI models list response.
type ModelList struct {
	Object string      `json:"object"`
	Data   []ModelInfo `json:"data"`
}

// ModelInfo describes a single available model.
type ModelInfo struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

// ── Error response ──────────────────────────────────

// ErrorResponse mirrors the OpenAI error response format.
type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail contains the error information.
type ErrorDetail struct {
	Message string  `json:"message"`
	Type    string  `json:"type"`
	Code    *string `json:"code"`
}

// newCompletionID generates a unique completion ID.
func newCompletionID() string {
	return "chatcmpl-" + time.Now().Format("20060102150405") + randomSuffix()
}

func randomSuffix() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
