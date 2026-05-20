package config

import "time"

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
	return Config{}, nil
}
