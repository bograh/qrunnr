package middleware

import (
	"log/slog"
	"net/http"
	"runtime/debug"

	chimiddleware "github.com/go-chi/chi/v5/middleware"
)

// Recovery converts a handler panic into a 500 JSON response and logs the
// panic value with its stack trace. http.ErrAbortHandler is re-panicked so
// net/http can abort the connection as intended.
func Recovery(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				rec := recover()
				if rec == nil {
					return
				}
				if rec == http.ErrAbortHandler {
					panic(rec)
				}
				logger.ErrorContext(r.Context(), "panic recovered",
					slog.Any("panic", rec),
					slog.String("method", r.Method),
					slog.String("path", r.URL.Path),
					slog.String("request_id", chimiddleware.GetReqID(r.Context())),
					slog.String("stack", string(debug.Stack())),
				)
				if r.Header.Get("Connection") == "Upgrade" {
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":"internal server error"}` + "\n"))
			}()
			next.ServeHTTP(w, r)
		})
	}
}
