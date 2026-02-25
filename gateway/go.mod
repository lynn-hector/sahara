module github.com/sahara-ai/sahara/gateway

go 1.25.0

require (
	github.com/gorilla/websocket v1.5.3
	github.com/redis/go-redis/v9 v9.18.0
	github.com/sahara-ai/sahara/gen v0.0.0
	github.com/sahara-ai/sahara/pkg v0.0.0
	google.golang.org/grpc v1.79.1
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
)

replace (
	github.com/sahara-ai/sahara/gen => ../gen/go
	github.com/sahara-ai/sahara/pkg => ../pkg
)
