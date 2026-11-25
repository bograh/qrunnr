package config

import (
	"errors"
	"fmt"
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
	Admin    AdminConfig
	LogLevel string
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
	// HeartbeatTimeout is how long an executing task may go without a
	// heartbeat before the watchdog reclaims it. It is also the lease length
	// reported to workers on claim.
	HeartbeatTimeout time.Duration
	WatchdogInterval time.Duration
}

type RetryConfig struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

// AdminConfig protects the /admin dashboard with HTTP basic auth when both
// fields are set.
type AdminConfig struct {
	Username string
	Password string
}

func (a AdminConfig) AuthEnabled() bool { return a.Username != "" && a.Password != "" }

func Load() (Config, error) {
	_ = godotenv.Load()

	var errs []error
	duration := func(key string, fallback time.Duration) time.Duration {
		d, err := durationEnv(key, fallback)
		if err != nil {
			errs = append(errs, err)
		}
		return d
	}

	cfg := Config{
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
			HeartbeatInterval: duration("HEARTBEAT_INTERVAL", 10*time.Second),
			HeartbeatTimeout:  duration("HEARTBEAT_TIMEOUT", 30*time.Second),
			WatchdogInterval:  duration("WATCHDOG_INTERVAL", 15*time.Second),
		},
		Retry: RetryConfig{
			BaseDelay: duration("RETRY_BASE_DELAY", time.Second),
			MaxDelay:  duration("RETRY_MAX_DELAY", time.Hour),
		},
		Admin: AdminConfig{
			Username: os.Getenv("ADMIN_USERNAME"),
			Password: os.Getenv("ADMIN_PASSWORD"),
		},
		LogLevel: env("LOG_LEVEL", "info"),
	}

	if (cfg.Admin.Username == "") != (cfg.Admin.Password == "") {
		errs = append(errs, errors.New("ADMIN_USERNAME and ADMIN_PASSWORD must be set together"))
	}
	if cfg.Retry.MaxDelay < cfg.Retry.BaseDelay {
		errs = append(errs, errors.New("RETRY_MAX_DELAY must be greater than or equal to RETRY_BASE_DELAY"))
	}
	return cfg, errors.Join(errs...)
}

func env(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func durationEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback, fmt.Errorf("%s: invalid duration %q", key, value)
	}
	if parsed <= 0 {
		return fallback, fmt.Errorf("%s: duration must be positive, got %q", key, value)
	}
	return parsed, nil
}
