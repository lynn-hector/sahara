// Package runtime 管理 Gateway 与 Runtime Worker 的 gRPC 连接。
// Phase 0: 单 Worker 连接 + Health Check
// Phase 1: Worker 池 + 负载感知调度 + SubmitTask/AbortTask
package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// WorkerStatus 描述单个 Worker 的健康状态
type WorkerStatus struct {
	Addr    string
	Healthy bool
	Latency time.Duration
	Error   string
}

// Pool 管理到 Runtime Worker 的 gRPC 连接池
type Pool struct {
	mu      sync.RWMutex
	workers []*worker
}

type worker struct {
	addr string
	conn *grpc.ClientConn
}

// NewPool 创建 Worker 连接池
// addrs 格式: "host:port,host:port,..."
func NewPool(addrs string) (*Pool, error) {
	parts := strings.Split(addrs, ",")
	var workers []*worker
	for _, addr := range parts {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		workers = append(workers, &worker{addr: addr})
	}
	if len(workers) == 0 {
		return nil, fmt.Errorf("no runtime worker addresses provided")
	}
	return &Pool{workers: workers}, nil
}

// Connect 建立到所有 Worker 的 gRPC 连接
func (p *Pool) Connect(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, w := range p.workers {
		conn, err := grpc.NewClient(
			w.addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.WaitForReady(false)),
		)
		if err != nil {
			return fmt.Errorf("failed to create gRPC client for %s: %w", w.addr, err)
		}
		w.conn = conn
		slog.Info("gRPC client created", "addr", w.addr)
	}
	return nil
}

// CheckHealth 对所有 Worker 执行 gRPC Health Check
func (p *Pool) CheckHealth(ctx context.Context) []WorkerStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()

	results := make([]WorkerStatus, len(p.workers))
	var wg sync.WaitGroup

	for i, w := range p.workers {
		wg.Add(1)
		go func(idx int, wk *worker) {
			defer wg.Done()
			results[idx] = checkWorker(ctx, wk)
		}(i, w)
	}
	wg.Wait()
	return results
}

// IsReady 返回是否至少有一个 Worker 健康
func (p *Pool) IsReady(ctx context.Context) bool {
	statuses := p.CheckHealth(ctx)
	for _, s := range statuses {
		if s.Healthy {
			return true
		}
	}
	return false
}

// Close 关闭所有连接
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, w := range p.workers {
		if w.conn != nil {
			_ = w.conn.Close()
		}
	}
}

// ConnState 返回连接状态摘要 (用于日志/监控)
func (p *Pool) ConnState() map[string]string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	states := make(map[string]string, len(p.workers))
	for _, w := range p.workers {
		if w.conn == nil {
			states[w.addr] = "not_connected"
		} else {
			states[w.addr] = w.conn.GetState().String()
		}
	}
	return states
}

func checkWorker(ctx context.Context, w *worker) WorkerStatus {
	status := WorkerStatus{Addr: w.addr}

	if w.conn == nil {
		status.Error = "not connected"
		return status
	}

	// 检查底层连接状态
	state := w.conn.GetState()
	if state == connectivity.Shutdown {
		status.Error = "connection shutdown"
		return status
	}

	// 执行 gRPC Health Check RPC
	client := healthpb.NewHealthClient(w.conn)
	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	start := time.Now()
	resp, err := client.Check(callCtx, &healthpb.HealthCheckRequest{
		Service: "sahara.agent.v1.AgentService",
	})
	status.Latency = time.Since(start)

	if err != nil {
		status.Error = err.Error()
		return status
	}

	status.Healthy = resp.GetStatus() == healthpb.HealthCheckResponse_SERVING
	if !status.Healthy {
		status.Error = fmt.Sprintf("service status: %s", resp.GetStatus().String())
	}
	return status
}
