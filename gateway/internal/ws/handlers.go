package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	agentv1 "github.com/sahara-ai/sahara/gen/sahara/agent/v1"
	"github.com/sahara-ai/sahara/gateway/internal/dispatch"
)

// SessionSubscriber allows the handler to subscribe the broadcast consumer to a session.
type SessionSubscriber interface {
	Subscribe(sessionKey string)
}

// SubmitParams mirrors the client-side agent.submit params.
type SubmitParams struct {
	SessionKey string `json:"sessionKey"`
	AgentID    string `json:"agentId"`
	Text       string `json:"text"`
	ModelOverride string `json:"modelOverride,omitempty"`
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
}

// AbortParams mirrors the client-side agent.abort params.
type AbortParams struct {
	TaskID string `json:"taskId"`
	RunID  string `json:"runId"`
	Reason string `json:"reason,omitempty"`
}

// RegisterHandlers wires up all RPC method handlers.
func RegisterHandlers(router *Router, hub *Hub, disp *dispatch.Dispatcher, sub ...SessionSubscriber) {
	var subscriber SessionSubscriber
	if len(sub) > 0 {
		subscriber = sub[0]
	}
	router.Handle("agent.submit", makeSubmitHandler(hub, disp, subscriber))
	router.Handle("agent.abort", makeAbortHandler(disp))
}

func makeSubmitHandler(hub *Hub, disp *dispatch.Dispatcher, subscriber SessionSubscriber) HandlerFunc {
	return func(conn *Conn, params json.RawMessage) (*ResFrame, error) {
		var p SubmitParams
		if err := json.Unmarshal(params, &p); err != nil {
			return NewErrorResFrame("", 400, "INVALID_PARAMS", err.Error(), false), nil
		}
		if p.SessionKey == "" || p.Text == "" {
			return NewErrorResFrame("", 400, "INVALID_PARAMS", "sessionKey and text are required", false), nil
		}

		taskID := generateTaskID()

		hub.BindSession(conn.ID, p.SessionKey)
		if subscriber != nil {
			subscriber.Subscribe(p.SessionKey)
		}

		req := &agentv1.SubmitTaskRequest{
			TaskId:     taskID,
			SessionKey: p.SessionKey,
			AgentId:    p.AgentID,
			UserMessage: &agentv1.UserMessage{
				Text: p.Text,
			},
			IdempotencyKey: p.IdempotencyKey,
			Options: &agentv1.TaskOptions{
				ModelOverride: p.ModelOverride,
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		result, err := disp.Submit(ctx, req)
		if err != nil {
			slog.Error("submit failed", "task_id", taskID, "err", err)
			return NewErrorResFrame("", 503, "SUBMIT_FAILED", err.Error(), true), nil
		}

		slog.Info("task submitted",
			"task_id", taskID,
			"run_id", result.RunID,
			"worker", result.WorkerAddr,
			"conn_id", conn.ID,
		)

		return &ResFrame{
			Type:   FrameTypeRes,
			Code:   200,
			Status: "accepted",
			Payload: map[string]any{
				"taskId":       taskID,
				"runId":        result.RunID,
				"workerId":     result.WorkerID,
				"acceptedAtMs": result.AcceptedAtMs,
			},
		}, nil
	}
}

func makeAbortHandler(disp *dispatch.Dispatcher) HandlerFunc {
	return func(conn *Conn, params json.RawMessage) (*ResFrame, error) {
		var p AbortParams
		if err := json.Unmarshal(params, &p); err != nil {
			return NewErrorResFrame("", 400, "INVALID_PARAMS", err.Error(), false), nil
		}
		if p.TaskID == "" {
			return NewErrorResFrame("", 400, "INVALID_PARAMS", "taskId is required", false), nil
		}

		// Phase 1 simplified: we don't track which worker has the task yet,
		// so abort is broadcast to the first worker. Full affinity tracking comes later.
		slog.Info("abort requested", "task_id", p.TaskID, "conn_id", conn.ID)

		return &ResFrame{
			Type:   FrameTypeRes,
			Code:   200,
			Status: "ok",
			Payload: map[string]any{
				"taskId": p.TaskID,
				"status": "abort_sent",
			},
		}, nil
	}
}

func generateTaskID() string {
	return fmt.Sprintf("task_%d", time.Now().UnixNano())
}
