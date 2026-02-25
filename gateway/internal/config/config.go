package config

import (
	"log/slog"
	"os"
)

// Config 是 Gateway 的配置
type Config struct {
	// 监听地址
	Addr string

	// Redis 连接
	RedisURL string

	// Runtime Worker gRPC 地址列表 (逗号分隔)
	RuntimeAddrs string

	// 日志级别 ("debug" / "info" / "warn" / "error")
	LogLevelStr string
}

// Load 从环境变量加载配置
func Load() *Config {
	return &Config{
		Addr:         envOr("ADDR", ":8080"),
		RedisURL:     envOr("REDIS_URL", "redis://localhost:6379"),
		RuntimeAddrs: envOr("RUNTIME_ADDRS", "localhost:50051"),
		LogLevelStr:  envOr("LOG_LEVEL", "info"),
	}
}

// LogLevel 返回 slog.Level
func (c *Config) LogLevel() slog.Level {
	switch c.LogLevelStr {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
