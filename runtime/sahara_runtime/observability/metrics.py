"""Prometheus 指标定义。"""

from prometheus_client import Counter, Gauge, Histogram

# ── Task 指标 ──────────────────────────────────────────────
TASKS_TOTAL = Counter(
    "sahara_rt_tasks_total",
    "Total number of tasks submitted",
    ["agent_id", "status"],
)

TASKS_ACTIVE = Gauge(
    "sahara_rt_tasks_active",
    "Number of currently active tasks",
)

TASK_DURATION = Histogram(
    "sahara_rt_task_duration_seconds",
    "Task execution duration",
    ["agent_id"],
    buckets=[0.5, 1, 2, 5, 10, 30, 60, 120, 300],
)

# ── LLM 指标 ──────────────────────────────────────────────
LLM_CALLS_TOTAL = Counter(
    "sahara_rt_llm_calls_total",
    "Total LLM API calls",
    ["provider", "model"],
)

LLM_CALL_DURATION = Histogram(
    "sahara_rt_llm_call_duration_seconds",
    "LLM API call duration",
    ["provider", "model"],
    buckets=[0.5, 1, 2, 5, 10, 30, 60],
)

LLM_TOKENS_TOTAL = Counter(
    "sahara_rt_llm_tokens_total",
    "Total tokens consumed",
    ["provider", "model", "direction"],  # direction: input/output
)

LLM_ERRORS_TOTAL = Counter(
    "sahara_rt_llm_errors_total",
    "Total LLM API errors",
    ["provider", "status_code"],
)

# ── Tool 指标 ──────────────────────────────────────────────
TOOL_CALLS_TOTAL = Counter(
    "sahara_rt_tool_calls_total",
    "Total tool executions",
    ["tool_name", "success"],
)

TOOL_DURATION = Histogram(
    "sahara_rt_tool_duration_seconds",
    "Tool execution duration",
    ["tool_name"],
    buckets=[0.1, 0.5, 1, 5, 10, 30, 60],
)

# ── Event 指标 ─────────────────────────────────────────────
EVENTS_EMITTED = Counter(
    "sahara_rt_events_emitted_total",
    "Total events emitted to Redis Streams",
    ["event_type"],
)

EVENT_PUBLISH_ERRORS = Counter(
    "sahara_rt_event_publish_errors_total",
    "Failed event publish attempts",
)

# ── Worker 指标 ────────────────────────────────────────────
WORKER_CPU = Gauge("sahara_rt_worker_cpu_percent", "Worker CPU usage percent")
WORKER_MEMORY = Gauge("sahara_rt_worker_memory_bytes", "Worker memory usage bytes")
