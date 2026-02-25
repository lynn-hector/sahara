package ws

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/sahara-ai/sahara/pkg/errcode"
)

// HandlerFunc processes an RPC request and returns a response payload or error.
type HandlerFunc func(conn *Conn, params json.RawMessage) (*ResFrame, error)

// Router maps RPC method names to handlers.
// All Handle() calls must complete before Dispatch() is called concurrently.
type Router struct {
	mu       sync.RWMutex
	handlers map[string]HandlerFunc
	frozen   bool
}

// NewRouter creates a Router with no registered methods.
func NewRouter() *Router {
	return &Router{handlers: make(map[string]HandlerFunc)}
}

// Handle registers a handler for the given method name.
// Must be called during initialization, before Freeze().
func (r *Router) Handle(method string, h HandlerFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		panic(fmt.Sprintf("ws.Router: cannot register handler for %q after Freeze()", method))
	}
	r.handlers[method] = h
}

// Freeze locks the router for concurrent reads. Call after all Handle() registrations.
func (r *Router) Freeze() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.frozen = true
	slog.Info("ws router frozen", "methods", len(r.handlers))
}

// Dispatch routes an incoming request frame to the registered handler.
func (r *Router) Dispatch(conn *Conn, frame *ReqFrame) *ResFrame {
	r.mu.RLock()
	h, ok := r.handlers[frame.Method]
	r.mu.RUnlock()

	if !ok {
		slog.Warn("unknown rpc method", "method", frame.Method, "conn_id", conn.ID)
		return ErrorFrame(frame.ID, errcode.GWMethodNotFound,
			fmt.Sprintf("unknown method: %s", frame.Method))
	}

	res, err := h(conn, frame.Params)
	if err != nil {
		slog.Error("rpc handler error", "method", frame.Method, "err", err, "conn_id", conn.ID)
		return ErrorFrame(frame.ID, errcode.InternalError, err.Error())
	}
	if res != nil {
		res.ID = frame.ID
	}
	return res
}
