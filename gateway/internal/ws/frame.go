// frame.go defines the three WebSocket frame types for the Sahara protocol:
//
//   - ReqFrame  (client → server): JSON-RPC style request with method + params
//   - ResFrame  (server → client): response with status code + payload or error
//   - EventFrame(server → client): push event from Runtime via broadcast consumer
//
// Wire format example (req):
//
//	{"type":"req","id":"r1","method":"agent.submit","params":{...}}
package ws

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/sahara-ai/sahara/pkg/errcode"
)

// FrameType defines the type of a WebSocket frame.
type FrameType string

const (
	FrameTypeReq   FrameType = "req"
	FrameTypeRes   FrameType = "res"
	FrameTypeEvent FrameType = "event"
)

// ReqFrame is a client → server RPC request.
type ReqFrame struct {
	Type   FrameType       `json:"type"`
	ID     string          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

// ResFrame is a server → client RPC response.
type ResFrame struct {
	Type    FrameType  `json:"type"`
	ID      string     `json:"id"`
	Code    int        `json:"code"`
	Status  string     `json:"status"`
	Payload any        `json:"payload,omitempty"`
	Error   *ResError  `json:"error,omitempty"`
}

// ResError carries error details in a response frame.
type ResError struct {
	Reason    string `json:"reason"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

// EventFrame is a server → client push event.
type EventFrame struct {
	Type       FrameType `json:"type"`
	Event      string    `json:"event"`
	SessionKey string    `json:"sessionKey"`
	RunID      string    `json:"runId"`
	Seq        int32     `json:"seq"`
	Ts         int64     `json:"ts"`
	Payload    any       `json:"payload,omitempty"`
}

// ParseReqFrame validates and decodes a raw JSON message into a ReqFrame.
func ParseReqFrame(data []byte) (*ReqFrame, error) {
	var f ReqFrame
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	if f.Type != FrameTypeReq {
		return nil, fmt.Errorf("expected type \"req\", got %q", f.Type)
	}
	if f.ID == "" {
		return nil, fmt.Errorf("missing request id")
	}
	if f.Method == "" {
		return nil, fmt.Errorf("missing method")
	}
	return &f, nil
}

// NewResFrame builds a success response frame.
func NewResFrame(reqID string, status string, payload any) *ResFrame {
	return &ResFrame{
		Type:    FrameTypeRes,
		ID:      reqID,
		Code:    200,
		Status:  status,
		Payload: payload,
	}
}

// ErrorFrame builds an error response from an errcode.Code.
// HTTP status code, reason string and retryable flag are derived from the Code metadata.
func ErrorFrame(reqID string, c errcode.Code, message string) *ResFrame {
	return &ResFrame{
		Type:   FrameTypeRes,
		ID:     reqID,
		Code:   c.HTTP,
		Status: "error",
		Error: &ResError{
			Reason:    c.Value,
			Message:   message,
			Retryable: c.Retryable,
		},
	}
}

// NewErrorResFrame builds an error response frame (legacy, prefer ErrorFrame).
func NewErrorResFrame(reqID string, code int, reason, message string, retryable bool) *ResFrame {
	return &ResFrame{
		Type:   FrameTypeRes,
		ID:     reqID,
		Code:   code,
		Status: "error",
		Error: &ResError{
			Reason:    reason,
			Message:   message,
			Retryable: retryable,
		},
	}
}

// NewEventFrame builds a push event frame.
func NewEventFrame(event, sessionKey, runID string, seq int32, payload any) *EventFrame {
	return &EventFrame{
		Type:       FrameTypeEvent,
		Event:      event,
		SessionKey: sessionKey,
		RunID:      runID,
		Seq:        seq,
		Ts:         time.Now().UnixMilli(),
		Payload:    payload,
	}
}
