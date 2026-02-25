package broadcast

// Consumer 从 Redis Streams 消费事件并推送到 WebSocket 连接
// Phase 0: 空壳
// Phase 1: XREAD 消费 events:{session_key} → 查找 WS 连接 → 推送

// TODO Phase 1:
// - Redis XREAD 消费循环
// - 150ms Delta Aggregator 限频聚合
// - session → WS conn 映射查找
// - 错误处理与重试
