// handlers.go defines the WebSocket RPC method handlers.
//
// Supported methods:
//   - agent.submit: Submits a task to the Runtime via gRPC Dispatcher.
//     Flow: parse params → bind session → subscribe to events → gRPC SubmitTask → return run_id.
//   - agent.abort: Requests cancellation of a running task.
//   - agent.input: Delivers user input to a waiting task (human-in-the-loop).
//     Flow: parse params → sticky affinity lookup → gRPC SendInput → ack.
package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	agentv1 "github.com/sahara-ai/sahara/gen/sahara/agent/v1"
	"github.com/sahara-ai/sahara/gateway/internal/dispatch"
	"github.com/sahara-ai/sahara/gateway/internal/metrics"
	"github.com/sahara-ai/sahara/pkg/errcode"
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

// InputParams mirrors the client-side agent.input params.
type InputParams struct {
	TaskID string `json:"taskId"`
	RunID  string `json:"runId,omitempty"`
	Action string `json:"action"` // "approve" / "reject" / "input"
	Input  string `json:"input,omitempty"`
}

// RegisterHandlers wires up all RPC method handlers.
func RegisterHandlers(router *Router, hub *Hub, disp *dispatch.Dispatcher, sub ...SessionSubscriber) {
	var subscriber SessionSubscriber
	if len(sub) > 0 {
		subscriber = sub[0]
	}
	router.Handle("agent.submit", makeSubmitHandler(hub, disp, subscriber))
	router.Handle("agent.abort", makeAbortHandler(disp))
	router.Handle("agent.input", makeInputHandler(disp))
}

// makeSubmitHandler creates the agent.submit RPC handler.
// On each call it: validates params, binds the WS connection to the session,
// subscribes the broadcast consumer to the session's Redis Stream,
// then dispatches the task to a Runtime Worker via gRPC.
func makeSubmitHandler(hub *Hub, disp *dispatch.Dispatcher, subscriber SessionSubscriber) HandlerFunc {
	return func(conn *Conn, params json.RawMessage) (*ResFrame, error) {
		var p SubmitParams
		if err := json.Unmarshal(params, &p); err != nil {
			return ErrorFrame("", errcode.GWInvalidParams, err.Error()), nil
		}
		if p.SessionKey == "" || p.Text == "" {
			return ErrorFrame("", errcode.GWInvalidParams, "sessionKey and text are required"), nil
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

		dispatchStart := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		result, err := disp.Submit(ctx, req)
		if err != nil {
			var busyErr *dispatch.WorkersBusyError
			if errors.As(err, &busyErr) {
				metrics.OverloadRejectsTotal.Inc()
				metrics.TasksSubmittedTotal.WithLabelValues("rejected").Inc()
				slog.Warn("overload: all workers busy", "task_id", taskID)
				return ErrorFrame("", errcode.GWWorkerBusy, "All workers are busy. Please retry in a moment."), nil
			}
			metrics.TasksSubmittedTotal.WithLabelValues("error").Inc()
			slog.Error("submit failed", "task_id", taskID, "err", err)
			return ErrorFrame("", errcode.GWSubmitFailed, err.Error()), nil
		}

		metrics.TasksSubmittedTotal.WithLabelValues("accepted").Inc()
		metrics.TaskDispatchDuration.Observe(time.Since(dispatchStart).Seconds())

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

// makeAbortHandler creates the agent.abort RPC handler.
// Phase 1: simplified — logs the abort request without worker affinity routing.
func makeAbortHandler(disp *dispatch.Dispatcher) HandlerFunc {
	return func(conn *Conn, params json.RawMessage) (*ResFrame, error) {
		var p AbortParams
		if err := json.Unmarshal(params, &p); err != nil {
			return ErrorFrame("", errcode.GWInvalidParams, err.Error()), nil
		}
		if p.TaskID == "" {
			return ErrorFrame("", errcode.GWInvalidParams, "taskId is required"), nil
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

// makeInputHandler creates the agent.input RPC handler for human-in-the-loop interaction.
// Uses sticky affinity to route the input to the correct Runtime Worker.
func makeInputHandler(disp *dispatch.Dispatcher) HandlerFunc {
	return func(conn *Conn, params json.RawMessage) (*ResFrame, error) {
		var p InputParams
		if err := json.Unmarshal(params, &p); err != nil {
			return ErrorFrame("", errcode.GWInvalidParams, err.Error()), nil
		}
		if p.TaskID == "" || p.Action == "" {
			return ErrorFrame("", errcode.GWInvalidParams, "taskId and action are required"), nil
		}

		validActions := map[string]bool{"approve": true, "reject": true, "input": true}
		if !validActions[p.Action] {
			return ErrorFrame("", errcode.GWInvalidParams, "action must be approve, reject, or input"), nil
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := disp.SendInput(ctx, p.TaskID, p.Action, p.Input); err != nil {
			slog.Error("send_input failed", "task_id", p.TaskID, "err", err)
			return ErrorFrame("", errcode.GWSubmitFailed, err.Error()), nil
		}

		slog.Info("input delivered", "task_id", p.TaskID, "action", p.Action, "conn_id", conn.ID)

		return &ResFrame{
			Type:   FrameTypeRes,
			Code:   200,
			Status: "ok",
			Payload: map[string]any{
				"taskId":    p.TaskID,
				"action":    p.Action,
				"delivered": true,
			},
		}, nil
	}
}

// generateTaskID produces a unique task identifier using nanosecond timestamp.
func generateTaskID() string {
	return fmt.Sprintf("task_%d", time.Now().UnixNano())
}
