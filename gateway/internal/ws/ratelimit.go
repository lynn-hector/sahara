// ratelimit.go implements per-connection rate limiting using a sliding window algorithm.
//
// Design: each connection maintains a sorted list of recent request timestamps.
// On each Allow() call, expired entries (older than 1s) are evicted, then the
// current count is compared against maxRPS. This is simple and memory-efficient
// for the expected connection scale (< 10K connections).
//
// Phase 2 will add user-level and global-level rate limiting.
package ws

import (
	"sync"
	"time"
)

// RateLimiter implements a per-connection sliding window rate limiter.
type RateLimiter struct {
	mu       sync.Mutex
	windows  map[string]*window
	maxRPS   int
	interval time.Duration
}

type window struct {
	timestamps []int64
}

// NewRateLimiter creates a limiter allowing maxRPS requests per second per connection.
func NewRateLimiter(maxRPS int) *RateLimiter {
	return &RateLimiter{
		windows:  make(map[string]*window),
		maxRPS:   maxRPS,
		interval: time.Second,
	}
}

// Allow checks if a request from the given connection is allowed.
func (rl *RateLimiter) Allow(connID string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now().UnixMilli()
	cutoff := now - rl.interval.Milliseconds()

	w, ok := rl.windows[connID]
	if !ok {
		w = &window{}
		rl.windows[connID] = w
	}

	// Evict old entries
	i := 0
	for i < len(w.timestamps) && w.timestamps[i] < cutoff {
		i++
	}
	w.timestamps = w.timestamps[i:]

	if len(w.timestamps) >= rl.maxRPS {
		return false
	}

	w.timestamps = append(w.timestamps, now)
	return true
}

// Remove cleans up state for a disconnected connection.
func (rl *RateLimiter) Remove(connID string) {
	rl.mu.Lock()
	delete(rl.windows, connID)
	rl.mu.Unlock()
}
