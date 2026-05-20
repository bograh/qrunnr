package config

import (
	"os"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	HTTP     HTTPConfig
	Postgres PostgresConfig
	Redis    RedisConfig
	Worker   WorkerConfig
	Retry    RetryConfig
}

type HTTPConfig struct {
	Port string
}

type PostgresConfig struct {
	DatabaseURL string
}

type RedisConfig struct {
	RedisURL string
}

type WorkerConfig struct {
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
}

type RetryConfig struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

func Load() (Config, error) {
	_ = godotenv.Load()

	return Config{
		HTTP: HTTPConfig{
			Port: env("PORT", "8080"),
		},
		Postgres: PostgresConfig{
			DatabaseURL: os.Getenv("DATABASE_URL"),
		},
		Redis: RedisConfig{
			RedisURL: os.Getenv("REDIS_URL"),
		},
		Worker: WorkerConfig{
			HeartbeatInterval: durationEnv("HEARTBEAT_INTERVAL", 10*time.Second),
			HeartbeatTimeout:  durationEnv("HEARTBEAT_TIMEOUT", 30*time.Second),
		},
		Retry: RetryConfig{
			BaseDelay: durationEnv("RETRY_BASE_DELAY", time.Second),
			MaxDelay:  durationEnv("RETRY_MAX_DELAY", time.Hour),
		},
	}, nil
}

func env(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
