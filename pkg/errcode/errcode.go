// Package errcode 定义 Sahara 统一业务错误码
package errcode

// 错误码格式: SAHARA_{SERVICE}_{CATEGORY}_{DETAIL}

const (
	// ── 通用 ────────────────────────────────────────────
	OK              = "SAHARA_OK"
	InternalError   = "SAHARA_INTERNAL_ERROR"
	InvalidArgument = "SAHARA_INVALID_ARGUMENT"
	NotFound        = "SAHARA_NOT_FOUND"
	Unauthorized    = "SAHARA_UNAUTHORIZED"

	// ── Gateway ─────────────────────────────────────────
	GatewayRateLimit     = "SAHARA_GW_RATE_LIMIT"
	GatewayWSClosed      = "SAHARA_GW_WS_CLOSED"
	GatewayNoWorker      = "SAHARA_GW_NO_WORKER"
	GatewayWorkerBusy    = "SAHARA_GW_WORKER_BUSY"
	GatewaySessionLocked = "SAHARA_GW_SESSION_LOCKED"

	// ── Runtime ─────────────────────────────────────────
	RuntimeBusy         = "SAHARA_RT_BUSY"
	RuntimeTaskNotFound = "SAHARA_RT_TASK_NOT_FOUND"
	RuntimeLLMError     = "SAHARA_RT_LLM_ERROR"
	RuntimeToolError    = "SAHARA_RT_TOOL_ERROR"
	RuntimeSandboxError = "SAHARA_RT_SANDBOX_ERROR"
	RuntimeTimeout      = "SAHARA_RT_TIMEOUT"
)
