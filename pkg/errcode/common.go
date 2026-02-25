package errcode

// ── 公共错误码 ──────────────────────────────────────────
// Gateway 和 API Service 共用, 适用于所有服务的通用错误场景。

var (
	OK = Code{
		Value: "SAHARA_OK", Domain: DomainCommon, HTTP: 200,
	}

	InternalError = Code{
		Value: "SAHARA_INTERNAL_ERROR", Domain: DomainCommon, HTTP: 500, Retryable: true,
	}

	InvalidArgument = Code{
		Value: "SAHARA_INVALID_ARGUMENT", Domain: DomainCommon, HTTP: 400,
	}

	NotFound = Code{
		Value: "SAHARA_NOT_FOUND", Domain: DomainCommon, HTTP: 404,
	}

	Unauthorized = Code{
		Value: "SAHARA_UNAUTHORIZED", Domain: DomainCommon, HTTP: 401,
	}

	Forbidden = Code{
		Value: "SAHARA_FORBIDDEN", Domain: DomainCommon, HTTP: 403,
	}

	TooManyRequests = Code{
		Value: "SAHARA_TOO_MANY_REQUESTS", Domain: DomainCommon, HTTP: 429, Retryable: true,
	}

	ServiceUnavailable = Code{
		Value: "SAHARA_SERVICE_UNAVAILABLE", Domain: DomainCommon, HTTP: 503, Retryable: true,
	}
)
