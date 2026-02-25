package ws

import (
	"encoding/json"
	"fmt"
	"log/slog"
)

// HandlerFunc processes an RPC request and returns a response payload or error.
type HandlerFunc func(conn *Conn, params json.RawMessage) (*ResFrame, error)

// Router maps RPC method names to handlers.
type Router struct {
	handlers map[string]HandlerFunc
}

// NewRouter creates a Router with no registered methods.
func NewRouter() *Router {
	return &Router{handlers: make(map[string]HandlerFunc)}
}

// Handle registers a handler for the given method name.
func (r *Router) Handle(method string, h HandlerFunc) {
	r.handlers[method] = h
}

// Dispatch routes an incoming request frame to the registered handler.
func (r *Router) Dispatch(conn *Conn, frame *ReqFrame) *ResFrame {
	h, ok := r.handlers[frame.Method]
	if !ok {
		slog.Warn("unknown rpc method", "method", frame.Method, "conn_id", conn.ID)
		return NewErrorResFrame(frame.ID, 404, "METHOD_NOT_FOUND",
			fmt.Sprintf("unknown method: %s", frame.Method), false)
	}

	res, err := h(conn, frame.Params)
	if err != nil {
		slog.Error("rpc handler error", "method", frame.Method, "err", err, "conn_id", conn.ID)
		return NewErrorResFrame(frame.ID, 500, "INTERNAL_ERROR", err.Error(), true)
	}
	if res != nil {
		res.ID = frame.ID
	}
	return res
}
