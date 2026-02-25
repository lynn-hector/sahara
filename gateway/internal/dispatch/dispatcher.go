package dispatch

import (
	"context"
	"fmt"
	"sync/atomic"
)

// Dispatcher 负责将任务分配到 Runtime Worker
// Phase 0: 空壳
// Phase 1: 轮询调度 + gRPC client pool
type Dispatcher struct {
	workers []string      // Worker gRPC 地址列表
	next    atomic.Uint64 // 轮询索引
}

// New 创建 Dispatcher
func New(addrs []string) *Dispatcher {
	return &Dispatcher{
		workers: addrs,
	}
}

// Pick 选择下一个 Worker (Round-Robin)
func (d *Dispatcher) Pick() (string, error) {
	if len(d.workers) == 0 {
		return "", fmt.Errorf("no workers available")
	}
	idx := d.next.Add(1) - 1
	return d.workers[idx%uint64(len(d.workers))], nil
}

// Submit 提交任务到 Runtime Worker
// TODO Phase 1: 实现 gRPC SubmitTask 调用
func (d *Dispatcher) Submit(ctx context.Context, workerAddr string, taskID string) error {
	_ = ctx
	_ = workerAddr
	_ = taskID
	return fmt.Errorf("not implemented: Phase 1")
}
