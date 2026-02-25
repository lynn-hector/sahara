// ratelimit.go implements three-layer rate limiting using sliding window counters:
//
//  1. Connection-level: per WS connection (prevents a single connection from flooding)
//  2. User-level: per UserID from JWT (limits aggregate RPS across all user connections)
//  3. Global-level: total gateway RPS (protects backend from thundering herd)
//
// Each layer uses the same sliding window algorithm. A request must pass all three
// layers to be allowed. The RejectedLayer return value identifies which layer blocked.
package ws

import (
	"sync"
	"time"
)

// RejectedLayer indicates which rate-limit layer rejected a request.
type RejectedLayer int

const (
	Allowed          RejectedLayer = 0
	RejectedConn     RejectedLayer = 1
	RejectedUser     RejectedLayer = 2
	RejectedGlobal   RejectedLayer = 3
)

// RateLimitConfig holds limits for all three layers.
type RateLimitConfig struct {
	ConnRPS   int // max RPS per connection (default 10)
	UserRPS   int // max RPS per user (default 30, 0 = disabled)
	GlobalRPS int // max total RPS (default 500, 0 = disabled)
}

// RateLimiter implements three-layer sliding window rate limiting.
type RateLimiter struct {
	mu       sync.Mutex
	windows  map[string]*window // keyed by connID or "user:XXX"
	global   *window
	cfg      RateLimitConfig
	interval time.Duration
}

type window struct {
	timestamps []int64
}

// NewRateLimiter creates a three-layer limiter with the given config.
func NewRateLimiter(maxRPS int) *RateLimiter {
	return NewRateLimiterWithConfig(RateLimitConfig{
		ConnRPS:   maxRPS,
		UserRPS:   maxRPS * 3,
		GlobalRPS: 500,
	})
}

// NewRateLimiterWithConfig creates a limiter with explicit per-layer limits.
func NewRateLimiterWithConfig(cfg RateLimitConfig) *RateLimiter {
	return &RateLimiter{
		windows:  make(map[string]*window),
		global:   &window{},
		cfg:      cfg,
		interval: time.Second,
	}
}

// Allow checks if a request from the given connection is allowed (connection-level only).
// For backward compatibility with existing code.
func (rl *RateLimiter) Allow(connID string) bool {
	return rl.AllowMulti(connID, "") == Allowed
}

// AllowMulti checks all three rate-limit layers. userID may be empty (unauthenticated).
func (rl *RateLimiter) AllowMulti(connID, userID string) RejectedLayer {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now().UnixMilli()
	cutoff := now - rl.interval.Milliseconds()

	// Layer 1: connection-level
	if rl.cfg.ConnRPS > 0 {
		if !rl.checkAndRecord("conn:"+connID, now, cutoff, rl.cfg.ConnRPS) {
			return RejectedConn
		}
	}

	// Layer 2: user-level
	if rl.cfg.UserRPS > 0 && userID != "" {
		if !rl.checkAndRecord("user:"+userID, now, cutoff, rl.cfg.UserRPS) {
			return RejectedUser
		}
	}

	// Layer 3: global-level
	if rl.cfg.GlobalRPS > 0 {
		evict(rl.global, cutoff)
		if len(rl.global.timestamps) >= rl.cfg.GlobalRPS {
			return RejectedGlobal
		}
		rl.global.timestamps = append(rl.global.timestamps, now)
	}

	return Allowed
}

func (rl *RateLimiter) checkAndRecord(key string, now, cutoff int64, limit int) bool {
	w, ok := rl.windows[key]
	if !ok {
		w = &window{}
		rl.windows[key] = w
	}
	evict(w, cutoff)
	if len(w.timestamps) >= limit {
		return false
	}
	w.timestamps = append(w.timestamps, now)
	return true
}

func evict(w *window, cutoff int64) {
	i := 0
	for i < len(w.timestamps) && w.timestamps[i] < cutoff {
		i++
	}
	w.timestamps = w.timestamps[i:]
}

// Remove cleans up state for a disconnected connection.
func (rl *RateLimiter) Remove(connID string) {
	rl.mu.Lock()
	delete(rl.windows, "conn:"+connID)
	rl.mu.Unlock()
}

// RemoveUser cleans up user-level state when a user fully disconnects.
func (rl *RateLimiter) RemoveUser(userID string) {
	if userID == "" {
		return
	}
	rl.mu.Lock()
	delete(rl.windows, "user:"+userID)
	rl.mu.Unlock()
}
