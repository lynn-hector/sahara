// Package broadcast consumes events from Redis Streams and pushes them to WebSocket connections.
package broadcast

import (
	"context"
	"log/slog"
	"sync"
	"time"

	eventv1 "github.com/sahara-ai/sahara/gen/sahara/event/v1"
	"github.com/sahara-ai/sahara/gateway/internal/ws"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

// Consumer reads AgentEvents from Redis Streams and forwards them to WebSocket clients.
type Consumer struct {
	rdb        *redis.Client
	hub        *ws.Hub
	aggregator *Aggregator

	mu       sync.RWMutex
	streams  map[string]string // stream key → last read ID
	pollWait time.Duration
}

// NewConsumer creates a broadcast consumer with optional delta aggregation.
func NewConsumer(rdb *redis.Client, hub *ws.Hub) *Consumer {
	return &Consumer{
		rdb:        rdb,
		hub:        hub,
		aggregator: NewAggregator(hub),
		streams:    make(map[string]string),
		pollWait:   1 * time.Second,
	}
}

// Subscribe begins tracking events for a session.
// Uses "0" to read from the beginning so no early events are missed.
func (c *Consumer) Subscribe(sessionKey string) {
	streamKey := "events:" + sessionKey
	c.mu.Lock()
	if _, ok := c.streams[streamKey]; !ok {
		c.streams[streamKey] = "0"
	}
	c.mu.Unlock()
	slog.Debug("subscribed to stream", "stream", streamKey)
}

// Unsubscribe stops tracking a session's events.
func (c *Consumer) Unsubscribe(sessionKey string) {
	streamKey := "events:" + sessionKey
	c.mu.Lock()
	delete(c.streams, streamKey)
	c.mu.Unlock()
}

// Run starts the polling loop. Blocks until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) {
	slog.Info("broadcast consumer started")
	for {
		select {
		case <-ctx.Done():
			slog.Info("broadcast consumer stopped")
			return
		default:
		}

		c.mu.RLock()
		if len(c.streams) == 0 {
			c.mu.RUnlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		keys := make([]string, 0, len(c.streams)*2)
		ids := make([]string, 0, len(c.streams))
		for k, id := range c.streams {
			keys = append(keys, k)
			ids = append(ids, id)
		}
		c.mu.RUnlock()

		args := &redis.XReadArgs{
			Streams: append(keys, ids...),
			Count:   100,
			Block:   c.pollWait,
		}

		results, err := c.rdb.XRead(ctx, args).Result()
		if err != nil {
			if err == redis.Nil || ctx.Err() != nil {
				continue
			}
			slog.Error("xread error", "err", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		for _, stream := range results {
			for _, msg := range stream.Messages {
				c.handleMessage(stream.Stream, msg)
			}
		}
	}
}

func (c *Consumer) handleMessage(streamKey string, msg redis.XMessage) {
	dataStr, ok := msg.Values["data"]
	if !ok {
		return
	}

	var raw []byte
	switch v := dataStr.(type) {
	case string:
		raw = []byte(v)
	case []byte:
		raw = v
	default:
		return
	}

	var event eventv1.AgentEvent
	if err := proto.Unmarshal(raw, &event); err != nil {
		slog.Error("failed to unmarshal event", "stream", streamKey, "err", err)
		return
	}

	c.mu.Lock()
	c.streams[streamKey] = msg.ID
	c.mu.Unlock()

	frame := eventToFrame(&event)
	c.aggregator.Push(event.GetSessionKey(), frame)
}

func eventToFrame(e *eventv1.AgentEvent) *ws.EventFrame {
	eventName := eventTypeName(e.GetType())

	var payload any
	switch p := e.GetPayload().(type) {
	case *eventv1.AgentEvent_Delta:
		payload = map[string]any{"text": p.Delta.GetText(), "stream": p.Delta.GetStream()}
	case *eventv1.AgentEvent_ToolStart:
		payload = map[string]any{
			"toolCallId": p.ToolStart.GetToolCallId(),
			"toolName":   p.ToolStart.GetToolName(),
			"inputJson":  p.ToolStart.GetInputJson(),
		}
	case *eventv1.AgentEvent_ToolResult:
		payload = map[string]any{
			"toolCallId": p.ToolResult.GetToolCallId(),
			"toolName":   p.ToolResult.GetToolName(),
			"success":    p.ToolResult.GetSuccess(),
			"output":     p.ToolResult.GetOutput(),
			"durationMs": p.ToolResult.GetDurationMs(),
		}
	case *eventv1.AgentEvent_RunStart:
		payload = map[string]any{
			"agentId":     p.RunStart.GetAgentId(),
			"model":       p.RunStart.GetModel(),
			"startedAtMs": p.RunStart.GetStartedAtMs(),
		}
	case *eventv1.AgentEvent_RunComplete:
		payload = map[string]any{
			"finalText":  p.RunComplete.GetFinalText(),
			"iterations": p.RunComplete.GetIterations(),
			"durationMs": p.RunComplete.GetDurationMs(),
		}
	case *eventv1.AgentEvent_RunError:
		payload = map[string]any{
			"errorCode":    p.RunError.GetErrorCode(),
			"errorMessage": p.RunError.GetErrorMessage(),
			"retryable":    p.RunError.GetRetryable(),
		}
	case *eventv1.AgentEvent_RunAbort:
		payload = map[string]any{
			"reason":    p.RunAbort.GetReason(),
			"abortedBy": p.RunAbort.GetAbortedBy(),
		}
	case *eventv1.AgentEvent_Thinking:
		payload = map[string]any{"text": p.Thinking.GetText()}
	case *eventv1.AgentEvent_Usage:
		payload = map[string]any{
			"model":        p.Usage.GetModel(),
			"inputTokens":  p.Usage.GetInputTokens(),
			"outputTokens": p.Usage.GetOutputTokens(),
			"iteration":    p.Usage.GetIteration(),
		}
	default:
		payload = map[string]any{}
	}

	return ws.NewEventFrame(
		eventName,
		e.GetSessionKey(),
		e.GetRunId(),
		e.GetSeq(),
		payload,
	)
}

func eventTypeName(t eventv1.EventType) string {
	switch t {
	case eventv1.EventType_EVENT_TYPE_DELTA:
		return "agent.delta"
	case eventv1.EventType_EVENT_TYPE_TOOL_START:
		return "agent.tool_start"
	case eventv1.EventType_EVENT_TYPE_TOOL_RESULT:
		return "agent.tool_result"
	case eventv1.EventType_EVENT_TYPE_RUN_START:
		return "agent.run_start"
	case eventv1.EventType_EVENT_TYPE_RUN_COMPLETE:
		return "agent.run_complete"
	case eventv1.EventType_EVENT_TYPE_RUN_ERROR:
		return "agent.run_error"
	case eventv1.EventType_EVENT_TYPE_RUN_ABORT:
		return "agent.run_abort"
	case eventv1.EventType_EVENT_TYPE_THINKING:
		return "agent.thinking"
	case eventv1.EventType_EVENT_TYPE_USAGE:
		return "agent.usage"
	case eventv1.EventType_EVENT_TYPE_INPUT_REQUIRED:
		return "agent.input_required"
	case eventv1.EventType_EVENT_TYPE_TOOL_CONFIRM_REQUIRED:
		return "agent.tool_confirm_required"
	case eventv1.EventType_EVENT_TYPE_MODEL_FALLBACK:
		return "agent.model_fallback"
	default:
		return "agent.unknown"
	}
}
