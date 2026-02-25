package ws

import (
	"log/slog"
	"sync"
)

// Hub 管理所有 WebSocket 连接
// Phase 1 实现: goroutine per conn (读+写)；连接注册/注销；心跳 ping/pong
type Hub struct {
	// connID → Conn
	conns map[string]*Conn
	mu    sync.RWMutex

	register   chan *Conn
	unregister chan string
}

// Conn 表示一个 WebSocket 连接
type Conn struct {
	ID        string
	SessionID string
	UserID    string
	// TODO Phase 1: 添加 websocket.Conn, 读写 goroutine, 心跳
}

// NewHub 创建连接管理中心
func NewHub() *Hub {
	return &Hub{
		conns:      make(map[string]*Conn),
		register:   make(chan *Conn, 64),
		unregister: make(chan string, 64),
	}
}

// Run 启动 Hub 事件循环
func (h *Hub) Run() {
	for {
		select {
		case conn := <-h.register:
			h.mu.Lock()
			h.conns[conn.ID] = conn
			h.mu.Unlock()
			slog.Info("ws conn registered", "conn_id", conn.ID)

		case connID := <-h.unregister:
			h.mu.Lock()
			delete(h.conns, connID)
			h.mu.Unlock()
			slog.Info("ws conn unregistered", "conn_id", connID)
		}
	}
}

// Count 返回当前连接数
func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.conns)
}
