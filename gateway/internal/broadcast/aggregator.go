package broadcast

import (
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/sahara-ai/sahara/gateway/internal/ws"
)

const (
	flushInterval = 150 * time.Millisecond
	maxBufferSize = 4096
)

// Aggregator batches consecutive delta events into a single WS push.
type Aggregator struct {
	mu      sync.Mutex
	hub     *ws.Hub
	buffers map[string]*sessionBuffer // sessionKey → buffer
	done    chan struct{}
}

type sessionBuffer struct {
	sessionKey string
	runID      string
	text       string
	seq        int32
	lastTs     int64
	timer      *time.Timer
}

// NewAggregator creates a delta aggregator.
func NewAggregator(hub *ws.Hub) *Aggregator {
	return &Aggregator{
		hub:     hub,
		buffers: make(map[string]*sessionBuffer),
		done:    make(chan struct{}),
	}
}

// Push adds a delta event to the aggregation buffer.
// Non-delta events trigger an immediate flush before forwarding.
func (a *Aggregator) Push(sessionKey string, frame *ws.EventFrame) {
	if frame.Event != "agent.delta" {
		a.Flush(sessionKey)
		data, _ := json.Marshal(frame)
		a.hub.SendToSession(sessionKey, data)
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	buf, ok := a.buffers[sessionKey]
	if !ok {
		buf = &sessionBuffer{
			sessionKey: sessionKey,
			runID:      frame.RunID,
		}
		a.buffers[sessionKey] = buf
	}

	payload, _ := frame.Payload.(map[string]any)
	text, _ := payload["text"].(string)

	buf.text += text
	buf.seq = frame.Seq
	buf.lastTs = frame.Ts

	if len(buf.text) >= maxBufferSize {
		a.flushLocked(sessionKey)
		return
	}

	if buf.timer != nil {
		buf.timer.Stop()
	}
	buf.timer = time.AfterFunc(flushInterval, func() {
		a.Flush(sessionKey)
	})
}

// Flush sends any buffered delta text immediately.
func (a *Aggregator) Flush(sessionKey string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.flushLocked(sessionKey)
}

func (a *Aggregator) flushLocked(sessionKey string) {
	buf, ok := a.buffers[sessionKey]
	if !ok || buf.text == "" {
		return
	}

	frame := ws.NewEventFrame(
		"agent.delta",
		sessionKey,
		buf.runID,
		buf.seq,
		map[string]any{"text": buf.text, "stream": "assistant"},
	)
	frame.Ts = buf.lastTs

	data, err := json.Marshal(frame)
	if err != nil {
		slog.Error("failed to marshal aggregated delta", "err", err)
		return
	}

	a.hub.SendToSession(sessionKey, data)
	buf.text = ""
	if buf.timer != nil {
		buf.timer.Stop()
		buf.timer = nil
	}
}

// Stop clears all buffers.
func (a *Aggregator) Stop() {
	a.mu.Lock()
	defer a.mu.Unlock()

	for key, buf := range a.buffers {
		if buf.timer != nil {
			buf.timer.Stop()
		}
		a.flushLocked(key)
	}
	a.buffers = nil
}
