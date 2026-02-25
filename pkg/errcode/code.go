// Package errcode 定义 Sahara 统一业务错误码。
//
// 设计原则:
//   - 错误码按 domain 拆分文件: common.go / gw.go / api.go / rt.go
//   - 每个 Code 携带元数据 (HTTP 状态码、可重试标志), 消费方无需 switch
//   - Gateway 只关注 common + gw, API Service 只关注 common + api
//   - 命名格式: SAHARA_{DOMAIN}_{DETAIL}, 如 SAHARA_GW_RATE_LIMIT
//
// 演进路线:
//   - Phase 1: Go 原生常量, Gateway + API 直接引用
//   - Phase 2+: 如需 TS SDK, 可迁移到 contracts/ YAML + codegen
package errcode

import "fmt"

// Domain 标识错误码所属的服务域。
type Domain string

const (
	DomainCommon  Domain = "common"
	DomainGateway Domain = "gateway"
	DomainAPI     Domain = "api"
	DomainRuntime Domain = "runtime"
)

// Code 是一个携带元数据的业务错误码。
//
// 典型使用方式:
//
//	frame := ws.ErrorFrame(reqID, errcode.GWRateLimit, "too many requests")
//
// 其中 ErrorFrame 内部读取 Code.HTTP / Code.Value / Code.Retryable 构造响应帧。
type Code struct {
	// Value 是客户端可解析的错误码字符串, 如 "SAHARA_GW_RATE_LIMIT"
	Value string

	// Domain 标识错误码的服务域
	Domain Domain

	// HTTP 是推荐的 HTTP/WS 状态码 (如 429, 503)
	HTTP int

	// Retryable 提示客户端是否可以安全重试
	Retryable bool
}

// String 返回错误码的字符串表示, 便于日志输出。
func (c Code) String() string {
	return c.Value
}

// Error 实现 error 接口, 允许 Code 直接作为 error 使用。
func (c Code) Error() string {
	return fmt.Sprintf("%s (HTTP %d)", c.Value, c.HTTP)
}

// Is 判断两个 Code 是否相同 (基于 Value 字段)。
func (c Code) Is(other Code) bool {
	return c.Value == other.Value
}
