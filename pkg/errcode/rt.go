package errcode

// ── Runtime 域错误码 ────────────────────────────────────
// Runtime 通过 gRPC 返回给 Gateway, Gateway 可选择直接透传或映射为 GW 码。
// 这些常量在 Go 侧仅做定义参考, Python Runtime 使用自身的 proto-generated enums。

var (
	RTBusy = Code{
		Value: "SAHARA_RT_BUSY", Domain: DomainRuntime, HTTP: 503, Retryable: true,
	}

	RTTaskNotFound = Code{
		Value: "SAHARA_RT_TASK_NOT_FOUND", Domain: DomainRuntime, HTTP: 404,
	}

	RTLLMError = Code{
		Value: "SAHARA_RT_LLM_ERROR", Domain: DomainRuntime, HTTP: 502, Retryable: true,
	}

	RTToolError = Code{
		Value: "SAHARA_RT_TOOL_ERROR", Domain: DomainRuntime, HTTP: 500,
	}

	RTSandboxError = Code{
		Value: "SAHARA_RT_SANDBOX_ERROR", Domain: DomainRuntime, HTTP: 500,
	}

	RTTimeout = Code{
		Value: "SAHARA_RT_TIMEOUT", Domain: DomainRuntime, HTTP: 504, Retryable: true,
	}
)
