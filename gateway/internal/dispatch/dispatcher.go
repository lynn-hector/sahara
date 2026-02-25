package dispatch

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	agentv1 "github.com/sahara-ai/sahara/gen/sahara/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Dispatcher routes tasks to Runtime Workers via gRPC.
type Dispatcher struct {
	workers []*workerConn
	next    atomic.Uint64
}

type workerConn struct {
	addr   string
	conn   *grpc.ClientConn
	client agentv1.AgentServiceClient
}

// New creates a Dispatcher targeting the given worker addresses.
func New(addrs []string) *Dispatcher {
	workers := make([]*workerConn, len(addrs))
	for i, addr := range addrs {
		workers[i] = &workerConn{addr: addr}
	}
	return &Dispatcher{workers: workers}
}

// Connect establishes gRPC connections to all workers.
func (d *Dispatcher) Connect(ctx context.Context) error {
	for _, w := range d.workers {
		conn, err := grpc.NewClient(
			w.addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return fmt.Errorf("dial %s: %w", w.addr, err)
		}
		w.conn = conn
		w.client = agentv1.NewAgentServiceClient(conn)
		slog.Info("dispatcher connected to worker", "addr", w.addr)
	}
	return nil
}

// Close tears down all worker connections.
func (d *Dispatcher) Close() {
	for _, w := range d.workers {
		if w.conn != nil {
			w.conn.Close()
		}
	}
}

// SubmitResult is returned after a successful SubmitTask RPC.
type SubmitResult struct {
	RunID        string
	WorkerID     string
	AcceptedAtMs int64
	WorkerAddr   string
}

// Submit sends a SubmitTask RPC, trying workers in round-robin order.
// Returns RESOURCE_EXHAUSTED-aware retry: if one worker is full, try the next.
func (d *Dispatcher) Submit(ctx context.Context, req *agentv1.SubmitTaskRequest) (*SubmitResult, error) {
	if len(d.workers) == 0 {
		return nil, fmt.Errorf("no workers available")
	}

	start := d.next.Add(1) - 1
	var lastErr error

	for i := 0; i < len(d.workers); i++ {
		w := d.workers[(start+uint64(i))%uint64(len(d.workers))]

		callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		resp, err := w.client.SubmitTask(callCtx, req)
		cancel()

		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.ResourceExhausted {
				slog.Warn("worker full, trying next", "addr", w.addr)
				lastErr = err
				continue
			}
			return nil, fmt.Errorf("submit to %s: %w", w.addr, err)
		}

		return &SubmitResult{
			RunID:        resp.GetRunId(),
			WorkerID:     resp.GetWorkerId(),
			AcceptedAtMs: resp.GetAcceptedAtMs(),
			WorkerAddr:   w.addr,
		}, nil
	}

	return nil, fmt.Errorf("all workers busy: %w", lastErr)
}

// Abort sends an AbortTask RPC to the specific worker address.
func (d *Dispatcher) Abort(ctx context.Context, workerAddr, taskID, runID, reason string) error {
	for _, w := range d.workers {
		if w.addr == workerAddr {
			callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			_, err := w.client.AbortTask(callCtx, &agentv1.AbortTaskRequest{
				TaskId: taskID,
				RunId:  runID,
				Reason: reason,
			})
			return err
		}
	}
	return fmt.Errorf("worker %s not found in pool", workerAddr)
}
