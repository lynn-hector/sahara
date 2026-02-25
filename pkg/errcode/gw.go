package errcode

// ── Gateway 域错误码 ────────────────────────────────────
// 面向 WebSocket 客户端, 覆盖连接、帧解析、限频、任务提交等场景。

var (
	GWInvalidFrame = Code{
		Value: "SAHARA_GW_INVALID_FRAME", Domain: DomainGateway, HTTP: 400,
	}

	GWInvalidParams = Code{
		Value: "SAHARA_GW_INVALID_PARAMS", Domain: DomainGateway, HTTP: 400,
	}

	GWMethodNotFound = Code{
		Value: "SAHARA_GW_METHOD_NOT_FOUND", Domain: DomainGateway, HTTP: 404,
	}

	GWRateLimit = Code{
		Value: "SAHARA_GW_RATE_LIMIT", Domain: DomainGateway, HTTP: 429, Retryable: true,
	}

	GWSubmitFailed = Code{
		Value: "SAHARA_GW_SUBMIT_FAILED", Domain: DomainGateway, HTTP: 503, Retryable: true,
	}

	GWNoWorker = Code{
		Value: "SAHARA_GW_NO_WORKER", Domain: DomainGateway, HTTP: 503, Retryable: true,
	}

	GWWorkerBusy = Code{
		Value: "SAHARA_GW_WORKER_BUSY", Domain: DomainGateway, HTTP: 503, Retryable: true,
	}

	GWWSClosed = Code{
		Value: "SAHARA_GW_WS_CLOSED", Domain: DomainGateway, HTTP: 410,
	}

	GWSessionLocked = Code{
		Value: "SAHARA_GW_SESSION_LOCKED", Domain: DomainGateway, HTTP: 409, Retryable: true,
	}
)
