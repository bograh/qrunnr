package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	for _, k := range []string{"PORT", "DATABASE_URL", "REDIS_URL", "HEARTBEAT_TIMEOUT", "WATCHDOG_INTERVAL",
		"RETRY_BASE_DELAY", "RETRY_MAX_DELAY", "ADMIN_USERNAME", "ADMIN_PASSWORD", "LOG_LEVEL", "HEARTBEAT_INTERVAL"} {
		t.Setenv(k, "")
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTP.Port != "8080" || cfg.Worker.HeartbeatTimeout != 30*time.Second || cfg.Worker.WatchdogInterval != 15*time.Second ||
		cfg.Retry.BaseDelay != time.Second || cfg.Retry.MaxDelay != time.Hour || cfg.LogLevel != "info" || cfg.Admin.AuthEnabled() {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	t.Setenv("HEARTBEAT_TIMEOUT", "soon")
	t.Setenv("WATCHDOG_INTERVAL", "-5s")
	t.Setenv("ADMIN_USERNAME", "ops")
	t.Setenv("ADMIN_PASSWORD", "")
	_, err := Load()
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"HEARTBEAT_TIMEOUT", "WATCHDOG_INTERVAL", "ADMIN_USERNAME and ADMIN_PASSWORD"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected error to mention %s, got %v", want, err)
		}
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv("HEARTBEAT_TIMEOUT", "45s")
	t.Setenv("RETRY_BASE_DELAY", "250ms")
	t.Setenv("ADMIN_USERNAME", "ops")
	t.Setenv("ADMIN_PASSWORD", "secret")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Worker.HeartbeatTimeout != 45*time.Second || cfg.Retry.BaseDelay != 250*time.Millisecond || !cfg.Admin.AuthEnabled() {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}
