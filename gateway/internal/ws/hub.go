package ws

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = 30 * time.Second
	maxMessageSize = 64 * 1024 // 64 KB
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// Hub manages all active WebSocket connections.
type Hub struct {
	mu     sync.RWMutex
	conns  map[string]*Conn
	sessID map[string]map[string]bool // sessionKey → set of connIDs

	router  *Router
	limiter *RateLimiter

	register   chan *Conn
	unregister chan *Conn
	done       chan struct{}
}

// Conn represents a single WebSocket connection.
type Conn struct {
	ID         string
	SessionKey string
	UserID     string

	hub  *Hub
	ws   *websocket.Conn
	send chan []byte
}

// NewHub creates a connection manager with the given RPC router.
func NewHub(router *Router) *Hub {
	return &Hub{
		conns:      make(map[string]*Conn),
		sessID:     make(map[string]map[string]bool),
		router:     router,
		limiter:    NewRateLimiter(10),
		register:   make(chan *Conn, 256),
		unregister: make(chan *Conn, 256),
		done:       make(chan struct{}),
	}
}

// Run processes register / unregister events. Call as a goroutine.
func (h *Hub) Run() {
	for {
		select {
		case conn := <-h.register:
			h.mu.Lock()
			h.conns[conn.ID] = conn
			if conn.SessionKey != "" {
				if h.sessID[conn.SessionKey] == nil {
					h.sessID[conn.SessionKey] = make(map[string]bool)
				}
				h.sessID[conn.SessionKey][conn.ID] = true
			}
			h.mu.Unlock()
			slog.Info("ws conn registered", "conn_id", conn.ID, "session_key", conn.SessionKey)

		case conn := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.conns[conn.ID]; ok {
				delete(h.conns, conn.ID)
				if conn.SessionKey != "" {
					delete(h.sessID[conn.SessionKey], conn.ID)
					if len(h.sessID[conn.SessionKey]) == 0 {
						delete(h.sessID, conn.SessionKey)
					}
				}
				close(conn.send)
			}
			h.mu.Unlock()
			h.limiter.Remove(conn.ID)
			slog.Info("ws conn unregistered", "conn_id", conn.ID)

		case <-h.done:
			return
		}
	}
}

// Stop signals the Hub loop to exit.
func (h *Hub) Stop() {
	close(h.done)
}

// Count returns the number of active connections.
func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.conns)
}

// SendToSession pushes raw data to all connections bound to the given session.
func (h *Hub) SendToSession(sessionKey string, data []byte) {
	h.mu.RLock()
	connIDs := h.sessID[sessionKey]
	conns := make([]*Conn, 0, len(connIDs))
	for cid := range connIDs {
		if c, ok := h.conns[cid]; ok {
			conns = append(conns, c)
		}
	}
	h.mu.RUnlock()

	for _, c := range conns {
		select {
		case c.send <- data:
		default:
			slog.Warn("ws send buffer full, dropping", "conn_id", c.ID)
		}
	}
}

// BindSession associates a connection with a session key (called after agent.submit).
func (h *Hub) BindSession(connID, sessionKey string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	c, ok := h.conns[connID]
	if !ok {
		return
	}
	if c.SessionKey != "" && c.SessionKey != sessionKey {
		delete(h.sessID[c.SessionKey], c.ID)
	}
	c.SessionKey = sessionKey
	if h.sessID[sessionKey] == nil {
		h.sessID[sessionKey] = make(map[string]bool)
	}
	h.sessID[sessionKey][c.ID] = true
}

// ServeWS upgrades an HTTP request to WebSocket and starts read/write pumps.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	wsConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("ws upgrade failed", "err", err)
		return
	}

	connID := r.URL.Query().Get("conn_id")
	if connID == "" {
		connID = generateConnID()
	}

	conn := &Conn{
		ID:   connID,
		hub:  h,
		ws:   wsConn,
		send: make(chan []byte, 256),
	}

	h.register <- conn

	go conn.writePump()
	go conn.readPump()
}

// readPump reads messages from the WebSocket and dispatches them.
func (c *Conn) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.ws.Close()
	}()

	c.ws.SetReadLimit(maxMessageSize)
	c.ws.SetReadDeadline(time.Now().Add(pongWait))
	c.ws.SetPongHandler(func(string) error {
		c.ws.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := c.ws.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				slog.Warn("ws read error", "conn_id", c.ID, "err", err)
			}
			return
		}

		frame, parseErr := ParseReqFrame(message)
		if parseErr != nil {
			errRes := NewErrorResFrame("", 400, "INVALID_FRAME", parseErr.Error(), false)
			data, _ := json.Marshal(errRes)
			select {
			case c.send <- data:
			default:
			}
			continue
		}

		if !c.hub.limiter.Allow(c.ID) {
			errRes := NewErrorResFrame(frame.ID, 429, "RATE_LIMITED", "too many requests", true)
			data, _ := json.Marshal(errRes)
			select {
			case c.send <- data:
			default:
			}
			continue
		}

		res := c.hub.router.Dispatch(c, frame)
		if res != nil {
			data, _ := json.Marshal(res)
			select {
			case c.send <- data:
			default:
			}
		}
	}
}

// writePump pumps messages from the send channel to the WebSocket.
func (c *Conn) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.ws.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.ws.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.ws.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

		case <-ticker.C:
			c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
