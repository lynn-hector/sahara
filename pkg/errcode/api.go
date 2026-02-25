package errcode

// ── API Service 域错误码 ────────────────────────────────
// 面向 REST HTTP 客户端, 覆盖 CRUD、权限、业务校验等场景。
// Phase 2 实现 API Service 时逐步补充。

var (
	APIResourceNotFound = Code{
		Value: "SAHARA_API_RESOURCE_NOT_FOUND", Domain: DomainAPI, HTTP: 404,
	}

	APIQuotaExceeded = Code{
		Value: "SAHARA_API_QUOTA_EXCEEDED", Domain: DomainAPI, HTTP: 429, Retryable: true,
	}

	APIValidationFailed = Code{
		Value: "SAHARA_API_VALIDATION_FAILED", Domain: DomainAPI, HTTP: 422,
	}
)
