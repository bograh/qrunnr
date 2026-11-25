package admin_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"taskforge.dev/internal/admin"
	"taskforge.dev/internal/api/router"
	"taskforge.dev/internal/model"
	"taskforge.dev/internal/storage/memory"
)

type fixture struct {
	server *httptest.Server
	queues *memory.QueueStore
	tasks  *memory.TaskStore
}

func newFixture(t *testing.T, opts admin.Options) *fixture {
	t.Helper()
	queues := memory.NewQueueStore()
	tasks := memory.NewTaskStore(queues)
	opts.Queues, opts.Tasks = queues, tasks
	opts.Logger = slog.New(slog.DiscardHandler)
	if opts.LiveInterval == 0 {
		opts.LiveInterval = 50 * time.Millisecond
	}
	dashboard := admin.New(opts)
	srv := httptest.NewServer(router.New(router.Options{
		QueueStore: queues, TaskStore: tasks, Logger: opts.Logger, Admin: dashboard.Routes(),
	}))
	t.Cleanup(func() {
		dashboard.Close()
		srv.Close()
	})
	return &fixture{server: srv, queues: queues, tasks: tasks}
}

func (f *fixture) do(t *testing.T, method, path string, form url.Values, headers map[string]string) (*http.Response, string) {
	t.Helper()
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req, err := http.NewRequest(method, f.server.URL+path, body)
	if err != nil {
		t.Fatal(err)
	}
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func htmx(target string) map[string]string {
	h := map[string]string{"HX-Request": "true"}
	if target != "" {
		h["HX-Target"] = target
	}
	return h
}

func (f *fixture) queue(t *testing.T, name string, maxRetries int) {
	t.Helper()
	if _, err := f.queues.Create(context.Background(), model.NewQueue(name, maxRetries, nil)); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) enqueue(t *testing.T, queue string) model.Task {
	t.Helper()
	task, err := f.tasks.Create(context.Background(), queue, model.NewTask("", json.RawMessage(`{"user_id":"abc123"}`), time.Now(), 5))
	if err != nil {
		t.Fatal(err)
	}
	return task
}

// deadTask enqueues a task on a max_retries=0 queue and fails it once.
func (f *fixture) deadTask(t *testing.T, queue string) string {
	t.Helper()
	f.enqueue(t, queue)
	claimed, ok, err := f.tasks.ClaimForWorker(context.Background(), "w1", queue, time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	if err := f.tasks.Nack(context.Background(), claimed.ID, "w1", "smtp timeout"); err != nil {
		t.Fatal(err)
	}
	return claimed.ID
}

func expectContains(t *testing.T, body string, wants ...string) {
	t.Helper()
	for _, w := range wants {
		if !strings.Contains(body, w) {
			t.Fatalf("expected body to contain %q\n---\n%s", w, body)
		}
	}
}

func TestOverviewRendersQueuesAndStats(t *testing.T) {
	f := newFixture(t, admin.Options{})
	f.queue(t, "email_jobs", 3)
	f.enqueue(t, "email_jobs")
	f.enqueue(t, "email_jobs")

	resp, body := f.do(t, http.MethodGet, "/admin/", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("expected html, got %s", ct)
	}
	expectContains(t, body, "<!doctype html>", "email_jobs", `sse-connect="/admin/events?view=overview"`, "/admin/static/app.css?v=")

	resp, _ = f.do(t, http.MethodGet, "/admin", nil, nil)
	if resp.StatusCode != http.StatusMovedPermanently || resp.Header.Get("Location") != "/admin/" {
		t.Fatalf("expected /admin to redirect to /admin/, got %d %s", resp.StatusCode, resp.Header.Get("Location"))
	}
}

func TestStaticAssetsServed(t *testing.T) {
	f := newFixture(t, admin.Options{})
	for path, ctype := range map[string]string{
		"/admin/static/app.css":                "text/css",
		"/admin/static/vendor/htmx.min.js":     "javascript",
		"/admin/static/vendor/htmx-ext-sse.js": "javascript",
	} {
		resp, body := f.do(t, http.MethodGet, path, nil, nil)
		if resp.StatusCode != http.StatusOK || !strings.Contains(resp.Header.Get("Content-Type"), ctype) || len(body) == 0 {
			t.Fatalf("%s: status %d type %s len %d", path, resp.StatusCode, resp.Header.Get("Content-Type"), len(body))
		}
	}
}

func TestBasicAuth(t *testing.T) {
	f := newFixture(t, admin.Options{Username: "ops", Password: "s3cret"})

	resp, _ := f.do(t, http.MethodGet, "/admin/", nil, nil)
	if resp.StatusCode != http.StatusUnauthorized || resp.Header.Get("WWW-Authenticate") == "" {
		t.Fatalf("expected 401 challenge, got %d", resp.StatusCode)
	}
	req, _ := http.NewRequest(http.MethodGet, f.server.URL+"/admin/", nil)
	req.SetBasicAuth("ops", "wrong")
	if r, _ := http.DefaultClient.Do(req); r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 with wrong password, got %d", r.StatusCode)
	}
	req.SetBasicAuth("ops", "s3cret")
	r, err := http.DefaultClient.Do(req)
	if err != nil || r.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 with credentials, got %v %v", r.StatusCode, err)
	}
	// The worker API is not behind dashboard auth.
	resp, _ = f.do(t, http.MethodGet, "/queues", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected API unaffected by dashboard auth, got %d", resp.StatusCode)
	}
}

func TestWritesRequireHTMXHeader(t *testing.T) {
	f := newFixture(t, admin.Options{})
	form := url.Values{"name": {"jobs"}, "max_retries": {"3"}}

	resp, _ := f.do(t, http.MethodPost, "/admin/queues", form, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected cross-site style post to be rejected, got %d", resp.StatusCode)
	}
	if qs, _ := f.queues.List(context.Background()); len(qs) != 0 {
		t.Fatal("queue must not be created without the HX-Request header")
	}
}

func TestQueueManagement(t *testing.T) {
	f := newFixture(t, admin.Options{})

	_, body := f.do(t, http.MethodPost, "/admin/queues", url.Values{"name": {"jobs"}, "max_retries": {"4"}, "rate_limit": {"25"}}, htmx("manage-queues"))
	expectContains(t, body, "Created queue jobs.", `id="manage-queues"`, `value="25"`)
	q, err := f.queues.GetByName(context.Background(), "jobs")
	if err != nil || q.MaxRetries != 4 || q.RateLimit == nil || *q.RateLimit != 25 {
		t.Fatalf("queue not created as submitted: %+v %v", q, err)
	}

	_, body = f.do(t, http.MethodPost, "/admin/queues", url.Values{"name": {"jobs"}, "max_retries": {"1"}}, htmx("manage-queues"))
	expectContains(t, body, "already exists")
	_, body = f.do(t, http.MethodPost, "/admin/queues", url.Values{"name": {"bad name!"}, "max_retries": {"1"}}, htmx("manage-queues"))
	expectContains(t, body, "Name must be 1-64 characters")
	_, body = f.do(t, http.MethodPost, "/admin/queues", url.Values{"name": {"x"}, "max_retries": {"99"}}, htmx("manage-queues"))
	expectContains(t, body, "Max_retries must be an integer between 0 and 25")

	_, body = f.do(t, http.MethodPost, "/admin/queues/jobs/rate-limit", url.Values{"rate_limit": {""}}, htmx("manage-queues"))
	expectContains(t, body, "Removed the rate limit on jobs.")
	if q, _ := f.queues.GetByName(context.Background(), "jobs"); q.RateLimit != nil {
		t.Fatal("expected rate limit cleared")
	}
	_, body = f.do(t, http.MethodPost, "/admin/queues/jobs/rate-limit", url.Values{"rate_limit": {"0"}}, htmx("manage-queues"))
	expectContains(t, body, "Rate limit must be a whole number of at least 1")

	f.enqueue(t, "jobs")
	_, body = f.do(t, http.MethodDelete, "/admin/queues/jobs", nil, htmx("manage-queues"))
	expectContains(t, body, "still has pending")
}

func TestTaskListFragmentAndFilters(t *testing.T) {
	f := newFixture(t, admin.Options{})
	f.queue(t, "a", 3)
	f.queue(t, "b", 3)
	ta := f.enqueue(t, "a")
	tb := f.enqueue(t, "b")

	resp, body := f.do(t, http.MethodGet, "/admin/tasks?queue=a", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	expectContains(t, body, "<!doctype html>", `id="task-filters"`, ta.ID[:8])
	if strings.Contains(body, tb.ID[:8]) {
		t.Fatal("queue filter not applied")
	}

	_, frag := f.do(t, http.MethodGet, "/admin/tasks?status=pending", nil, htmx("task-results"))
	if strings.Contains(frag, "<!doctype html>") || strings.Contains(frag, `id="task-filters"`) {
		t.Fatal("htmx request targeting results must get only the fragment")
	}
	expectContains(t, frag, ta.ID[:8], tb.ID[:8])

	_, frag = f.do(t, http.MethodGet, "/admin/tasks?status=bogus", nil, htmx("task-results"))
	expectContains(t, frag, "Unknown status bogus.")
	_, frag = f.do(t, http.MethodGet, "/admin/tasks?from=yesterday", nil, htmx("task-results"))
	expectContains(t, frag, "Could not read the &#39;created after&#39; time.")
	_, frag = f.do(t, http.MethodGet, "/admin/tasks?from=2999-01-01T00:00", nil, htmx("task-results"))
	expectContains(t, frag, "No tasks match these filters.")
}

func TestTaskDetailShowsHistoryAndCancels(t *testing.T) {
	f := newFixture(t, admin.Options{})
	f.queue(t, "jobs", 3)
	task := f.enqueue(t, "jobs")

	resp, body := f.do(t, http.MethodGet, "/admin/tasks/"+task.ID, nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	expectContains(t, body, task.ID, "Enqueued as pending", "&#34;user_id&#34;: &#34;abc123&#34;", "Cancel task")

	_, frag := f.do(t, http.MethodPost, "/admin/tasks/"+task.ID+"/cancel", url.Values{"queue": {"jobs"}}, htmx("task-detail"))
	expectContains(t, frag, "Task cancelled.", "pending → cancelled")
	_, frag = f.do(t, http.MethodPost, "/admin/tasks/"+task.ID+"/cancel", url.Values{"queue": {"jobs"}}, htmx("task-detail"))
	expectContains(t, frag, "Only pending tasks can be cancelled")

	resp, body = f.do(t, http.MethodGet, "/admin/tasks/00000000-0000-4000-8000-000000000000", nil, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown task, got %d", resp.StatusCode)
	}
	expectContains(t, body, "No task with this ID exists.")
}

func TestDeadTaskDetailAndReplay(t *testing.T) {
	f := newFixture(t, admin.Options{})
	f.queue(t, "jobs", 0)
	id := f.deadTask(t, "jobs")

	_, body := f.do(t, http.MethodGet, "/admin/tasks/"+id, nil, nil)
	expectContains(t, body, "dead-letter queue", "smtp timeout", "executing → dead", "Replay")

	_, frag := f.do(t, http.MethodPost, "/admin/dlq/replay", url.Values{"selected": {"jobs:" + id}}, htmx("task-detail"))
	expectContains(t, frag, "Replayed 1 task.", "Replayed from dead-letter queue", "replayed as task")
}

func TestDLQBulkReplayAndDiscard(t *testing.T) {
	f := newFixture(t, admin.Options{})
	f.queue(t, "jobs", 0)
	first := f.deadTask(t, "jobs")
	second := f.deadTask(t, "jobs")

	_, body := f.do(t, http.MethodGet, "/admin/dlq?queue=jobs", nil, nil)
	expectContains(t, body, `value="jobs:`+first+`"`, `value="jobs:`+second+`"`, "smtp timeout")

	form := url.Values{"selected": {"jobs:" + first, "jobs:" + second}, "queue": {"jobs"}, "page": {"1"}}
	_, frag := f.do(t, http.MethodPost, "/admin/dlq/replay", form, htmx("dlq-results"))
	expectContains(t, frag, "Replayed 2 tasks.", "1×")
	if strings.Contains(frag, "<!doctype html>") {
		t.Fatal("expected fragment response")
	}
	dead, _ := f.tasks.DeadLetterTask(first)
	if dead.ReplayCount != 1 {
		t.Fatalf("expected replay count 1, got %d", dead.ReplayCount)
	}
	if tasks, _ := f.tasks.ListTasks(context.Background(), model.TaskFilter{Queue: "jobs", Status: model.TaskStatusPending}); len(tasks) != 2 {
		t.Fatalf("expected 2 replayed pending tasks, got %d", len(tasks))
	}

	form = url.Values{"selected": {"jobs:" + first, "jobs:00000000-0000-4000-8000-000000000000"}}
	_, frag = f.do(t, http.MethodPost, "/admin/dlq/discard", form, htmx("dlq-results"))
	expectContains(t, frag, "Discarded 1 task. 1 task could not be discarded")
	if _, ok := f.tasks.DeadLetterTask(first); ok {
		t.Fatal("expected first task discarded")
	}

	_, frag = f.do(t, http.MethodPost, "/admin/dlq/replay", url.Values{}, htmx("dlq-results"))
	expectContains(t, frag, "Select at least one task.")
}

func TestWorkersPage(t *testing.T) {
	f := newFixture(t, admin.Options{})
	f.tasks.ReplaceWorkerForTest("fresh-worker", "", time.Now())
	f.tasks.ReplaceWorkerForTest("stale-worker", "", time.Now().Add(-30*time.Second))

	_, body := f.do(t, http.MethodGet, "/admin/workers", nil, nil)
	expectContains(t, body, "fresh-worker", "stale-worker", "stale", "idle", `sse-connect="/admin/events?view=workers"`)
}

func TestEventsStreamRendersFragments(t *testing.T) {
	f := newFixture(t, admin.Options{LiveInterval: 20 * time.Millisecond})
	f.queue(t, "email_jobs", 3)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, f.server.URL+"/admin/events?view=overview", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("expected event stream, got %s", resp.Header.Get("Content-Type"))
	}

	// Read two complete events to prove the stream keeps updating.
	reader := bufio.NewReader(resp.Body)
	events := 0
	var event strings.Builder
	for events < 2 {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read stream: %v", err)
		}
		if line == "\n" {
			got := event.String()
			if !strings.HasPrefix(got, "event: overview\n") || !strings.Contains(got, "data: ") || !strings.Contains(got, "email_jobs") {
				t.Fatalf("unexpected event: %q", got)
			}
			for _, l := range strings.Split(strings.TrimSuffix(got, "\n"), "\n")[1:] {
				if !strings.HasPrefix(l, "data: ") {
					t.Fatalf("every payload line must be a data field, got %q", l)
				}
			}
			events++
			event.Reset()
			continue
		}
		event.WriteString(line)
	}

	resp2, _ := f.do(t, http.MethodGet, "/admin/events?view=nope", nil, nil)
	if resp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for unknown view, got %d", resp2.StatusCode)
	}
}

func TestCloseEndsOpenStreams(t *testing.T) {
	queues := memory.NewQueueStore()
	tasks := memory.NewTaskStore(queues)
	dashboard := admin.New(admin.Options{Queues: queues, Tasks: tasks, Logger: slog.New(slog.DiscardHandler), LiveInterval: time.Hour})
	srv := httptest.NewServer(dashboard.Routes())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/events?view=workers")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, reader)
		close(done)
	}()
	dashboard.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not end after Close")
	}
}
