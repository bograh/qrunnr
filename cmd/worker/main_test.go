package main

import (
	"encoding/json"
	"testing"

	"qrunnr.dev/internal/model"
)

func TestExecuteSuccess(t *testing.T) {
	payload := json.RawMessage(`{"action":"send_email"}`)
	result, err := execute(model.ClaimedTask{Payload: payload})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result["ok"] != true {
		t.Fatalf("expected ok result, got %+v", result)
	}
}

func TestExecuteFailureFromPayload(t *testing.T) {
	payload := json.RawMessage(`{"fail":true,"error":"downstream failed"}`)
	_, err := execute(model.ClaimedTask{Payload: payload})
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "downstream failed" {
		t.Fatalf("expected downstream failed, got %q", err.Error())
	}
}
