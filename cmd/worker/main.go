package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"taskforge.dev/internal/model"
)

type workerConfig struct {
	APIURL            string
	WorkerID          string
	Queue             string
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
	RequestTimeout    time.Duration
}

type worker struct {
	config workerConfig
	client *http.Client
}

type claimRequest struct {
	WorkerID string `json:"worker_id"`
	Queue    string `json:"queue,omitempty"`
}

type ackRequest struct {
	TaskID   string         `json:"task_id"`
	WorkerID string         `json:"worker_id"`
	Result   map[string]any `json:"result,omitempty"`
}

type nackRequest struct {
	TaskID   string `json:"task_id"`
	WorkerID string `json:"worker_id"`
	Error    string `json:"error"`
}

type heartbeatRequest struct {
	TaskID   string `json:"task_id"`
	WorkerID string `json:"worker_id"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg := loadConfig()
	w := worker{
		config: cfg,
		client: &http.Client{Timeout: cfg.RequestTimeout},
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("worker %s polling %s queue=%q", cfg.WorkerID, cfg.APIURL, cfg.Queue)
	return w.run(ctx)
}

func loadConfig() workerConfig {
	_ = godotenv.Load()

	return workerConfig{
		APIURL:            strings.TrimRight(env("TASKFORGE_API_URL", "http://localhost:8080"), "/"),
		WorkerID:          env("WORKER_ID", hostnameWorkerID()),
		Queue:             os.Getenv("QUEUE"),
		PollInterval:      durationEnv("WORKER_POLL_INTERVAL", time.Second),
		HeartbeatInterval: durationEnv("HEARTBEAT_INTERVAL", 10*time.Second),
		RequestTimeout:    durationEnv("WORKER_REQUEST_TIMEOUT", 15*time.Second),
	}
}

func (w worker) run(ctx context.Context) error {
	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()

	for {
		if err := w.pollOnce(ctx); err != nil {
			log.Printf("worker poll failed: %v", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (w worker) pollOnce(ctx context.Context) error {
	task, ok, err := w.claim(ctx)
	if err != nil || !ok {
		return err
	}

	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	heartbeatErr := make(chan error, 1)
	go func() {
		heartbeatErr <- w.heartbeatLoop(taskCtx, task.ID)
	}()

	result, execErr := execute(task)
	cancel()
	if err := <-heartbeatErr; err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("heartbeat stopped for task %s: %v", task.ID, err)
	}

	if execErr != nil {
		return w.nack(ctx, task.ID, execErr.Error())
	}
	return w.ack(ctx, task.ID, result)
}

func (w worker) claim(ctx context.Context) (model.ClaimedTask, bool, error) {
	var task model.ClaimedTask
	status, err := w.postJSON(ctx, "/workers/claim", claimRequest{
		WorkerID: w.config.WorkerID,
		Queue:    w.config.Queue,
	}, &task)
	if err != nil {
		return model.ClaimedTask{}, false, err
	}
	if status == http.StatusNoContent {
		return model.ClaimedTask{}, false, nil
	}
	if status != http.StatusOK {
		return model.ClaimedTask{}, false, fmt.Errorf("claim returned status %d", status)
	}
	return task, true, nil
}

func (w worker) heartbeatLoop(ctx context.Context, taskID string) error {
	ticker := time.NewTicker(w.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			status, err := w.postJSON(ctx, "/workers/heartbeat", heartbeatRequest{
				TaskID:   taskID,
				WorkerID: w.config.WorkerID,
			}, nil)
			if err != nil {
				return err
			}
			if status != http.StatusOK {
				return fmt.Errorf("heartbeat returned status %d", status)
			}
		}
	}
}

func (w worker) ack(ctx context.Context, taskID string, result map[string]any) error {
	status, err := w.postJSON(ctx, "/workers/ack", ackRequest{
		TaskID:   taskID,
		WorkerID: w.config.WorkerID,
		Result:   result,
	}, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("ack returned status %d", status)
	}
	log.Printf("task %s acknowledged", taskID)
	return nil
}

func (w worker) nack(ctx context.Context, taskID string, message string) error {
	status, err := w.postJSON(ctx, "/workers/nack", nackRequest{
		TaskID:   taskID,
		WorkerID: w.config.WorkerID,
		Error:    message,
	}, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("nack returned status %d", status)
	}
	log.Printf("task %s rejected: %s", taskID, message)
	return nil
}

func (w worker) postJSON(ctx context.Context, path string, request any, response any) (int, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return 0, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.config.APIURL+path, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if response != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if err := json.NewDecoder(resp.Body).Decode(response); err != nil && !errors.Is(err, io.EOF) {
			return resp.StatusCode, err
		}
		return resp.StatusCode, nil
	}

	if resp.StatusCode >= 400 {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return resp.StatusCode, fmt.Errorf("%s %s returned %d: %s", req.Method, req.URL.Path, resp.StatusCode, strings.TrimSpace(string(payload)))
	}
	return resp.StatusCode, nil
}

func execute(task model.ClaimedTask) (map[string]any, error) {
	var payload map[string]any
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		return nil, err
	}

	if fail, _ := payload["fail"].(bool); fail {
		message, _ := payload["error"].(string)
		if message == "" {
			message = "payload requested failure"
		}
		return nil, errors.New(message)
	}

	if sleepMS, ok := number(payload["sleep_ms"]); ok && sleepMS > 0 {
		time.Sleep(time.Duration(sleepMS) * time.Millisecond)
	}

	return map[string]any{
		"ok":          true,
		"worker_id":   env("WORKER_ID", hostnameWorkerID()),
		"finished_at": time.Now().UTC().Format(time.RFC3339Nano),
	}, nil
}

func number(value any) (int64, bool) {
	switch typed := value.(type) {
	case float64:
		return int64(typed), true
	case int64:
		return typed, true
	default:
		return 0, false
	}
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

func hostnameWorkerID() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		return "worker-1"
	}
	return hostname
}
