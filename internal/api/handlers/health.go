package handlers

import (
	"context"
	"net/http"
	"time"
)

// Healthz reports that the process is serving HTTP.
func Healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Readyz returns a handler that reports 503 when check fails, e.g. when
// Postgres or Redis is unreachable.
func Readyz(check func(ctx context.Context) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if check != nil {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if err := check(ctx); err != nil {
				writeError(w, http.StatusServiceUnavailable, "not ready: "+err.Error())
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	}
}
