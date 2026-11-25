// Package views holds the admin dashboard's templ components.
package views

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"qrunnr.dev/internal/model"
	webadmin "qrunnr.dev/web/admin"
)

// FlashKind selects the styling of a flash message.
type FlashKind string

const (
	FlashSuccess FlashKind = "success"
	FlashError   FlashKind = "error"
)

// Flash is a one-off message shown after an action.
type Flash struct {
	Kind    FlashKind
	Message string
}

type OverviewData struct {
	Queues      []model.QueueOverview
	Workers     []model.WorkerStatus
	GeneratedAt time.Time
}

type overviewTotals struct {
	Depth, Executing, DoneLastMinute, FailedLastMinute, DLQ int
	ActiveWorkers, StaleWorkers                             int
}

func (d OverviewData) totals() overviewTotals {
	var t overviewTotals
	for _, q := range d.Queues {
		t.Depth += q.Stats.Depth
		t.Executing += q.Stats.ExecutingCount
		t.DoneLastMinute += q.Stats.DoneLastMinute
		t.FailedLastMinute += q.Stats.FailedLastMinute
		t.DLQ += q.Stats.DLQDepth
	}
	for _, w := range d.Workers {
		if w.Stale {
			t.StaleWorkers++
		} else {
			t.ActiveWorkers++
		}
	}
	return t
}

type TaskFilterForm struct {
	Queue, Status, Worker, TaskID, From, To string
	Page                                    int
}

func (f TaskFilterForm) active() bool {
	return f.Queue != "" || f.Status != "" || f.Worker != "" || f.TaskID != "" || f.From != "" || f.To != ""
}

type TaskListData struct {
	Form       TaskFilterForm
	QueueNames []string
	Tasks      []model.TaskSummary
	HasNext    bool
	Flash      Flash
}

type TaskDetailData struct {
	TaskID string
	Task   *model.TaskSummary
	Dead   *model.DeadLetterTask
	Events []model.TaskEvent
	Flash  Flash
}

func (d TaskDetailData) transitions() []model.TaskEvent {
	out := []model.TaskEvent{}
	for _, e := range d.Events {
		if e.Kind != model.TaskEventHeartbeat {
			out = append(out, e)
		}
	}
	return out
}

// heartbeats returns heartbeat events newest first, capped at limit.
func (d TaskDetailData) heartbeats(limit int) ([]model.TaskEvent, int) {
	out := []model.TaskEvent{}
	total := 0
	for i := len(d.Events) - 1; i >= 0; i-- {
		if d.Events[i].Kind != model.TaskEventHeartbeat {
			continue
		}
		total++
		if len(out) < limit {
			out = append(out, d.Events[i])
		}
	}
	return out, total
}

type DLQFilterForm struct {
	Queue, From, To string
	Page            int
}

type DLQData struct {
	Form       DLQFilterForm
	QueueNames []string
	Tasks      []model.DeadLetterTask
	HasNext    bool
	Flash      Flash
}

var queueStatuses = []model.TaskStatus{
	model.TaskStatusPending, model.TaskStatusRetry, model.TaskStatusExecuting,
	model.TaskStatusDone, model.TaskStatusCancelled,
}

func asset(name string) string {
	return "/admin/static/" + name + "?v=" + webadmin.AssetVersion
}

const timeLayout = "2006-01-02 15:04:05"

func fmtTime(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	return t.UTC().Format(timeLayout)
}

func fmtTimePtr(t *time.Time) string {
	if t == nil {
		return "—"
	}
	return fmtTime(*t)
}

func isoTime(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// relative renders t relative to now, e.g. "12s ago" or "in 3m".
func relative(t time.Time, now time.Time) string {
	d := now.Sub(t)
	switch {
	case d > -time.Second && d < time.Second:
		return "now"
	case d < 0:
		return "in " + compactDuration(-d)
	default:
		return compactDuration(d) + " ago"
	}
}

func relativePtr(t *time.Time, now time.Time) string {
	if t == nil {
		return ""
	}
	return relative(*t, now)
}

func compactDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return strconv.Itoa(int(d.Seconds())) + "s"
	case d < time.Hour:
		return fmt.Sprintf("%dm %02ds", int(d.Minutes()), int(d.Seconds())%60)
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh %02dm", int(d.Hours()), int(d.Minutes())%60)
	default:
		return fmt.Sprintf("%dd %02dh", int(d.Hours())/24, int(d.Hours())%24)
	}
}

func ageSeconds(seconds int) string {
	if seconds <= 0 {
		return "—"
	}
	return compactDuration(time.Duration(seconds) * time.Second)
}

func throughput(doneLastMinute int) string {
	if doneLastMinute == 0 {
		return "0"
	}
	return strconv.FormatFloat(float64(doneLastMinute)/60, 'f', 2, 64)
}

func errorRate(done, failed int) string {
	if done+failed == 0 {
		return "—"
	}
	return strconv.FormatFloat(100*float64(failed)/float64(done+failed), 'f', 1, 64) + "%"
}

func rateLimitLabel(limit *int) string {
	if limit == nil {
		return "unlimited"
	}
	return strconv.Itoa(*limit) + "/s"
}

func rateLimitValue(limit *int) string {
	if limit == nil {
		return ""
	}
	return strconv.Itoa(*limit)
}

func itoa(n int) string { return strconv.Itoa(n) }

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func prettyJSON(raw []byte) string {
	if len(bytes.TrimSpace(raw)) == 0 || string(bytes.TrimSpace(raw)) == "null" {
		return ""
	}
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return string(raw)
	}
	return out.String()
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}

func eventTitle(e model.TaskEvent) string {
	switch e.Kind {
	case model.TaskEventReplay:
		return "Replayed from dead-letter queue"
	case model.TaskEventHeartbeat:
		return "Heartbeat"
	}
	if e.FromStatus == "" {
		return "Enqueued as " + string(e.ToStatus)
	}
	return string(e.FromStatus) + " → " + string(e.ToStatus)
}

// linkedTaskID extracts the other task referenced by a replay message.
func linkedTaskID(e model.TaskEvent) string {
	for _, prefix := range []string{"replayed as task ", "replayed from dead-letter task "} {
		if id, ok := strings.CutPrefix(e.Message, prefix); ok {
			return id
		}
	}
	return ""
}

func taskURL(id string) string { return "/admin/tasks/" + url.PathEscape(id) }

func taskListURL(f TaskFilterForm, page int) string {
	v := url.Values{}
	set := func(k, val string) {
		if val != "" {
			v.Set(k, val)
		}
	}
	set("queue", f.Queue)
	set("status", f.Status)
	set("worker", f.Worker)
	set("task_id", f.TaskID)
	set("from", f.From)
	set("to", f.To)
	if page > 1 {
		v.Set("page", strconv.Itoa(page))
	}
	if len(v) == 0 {
		return "/admin/tasks"
	}
	return "/admin/tasks?" + v.Encode()
}

func dlqListURL(f DLQFilterForm, page int) string {
	v := url.Values{}
	if f.Queue != "" {
		v.Set("queue", f.Queue)
	}
	if f.From != "" {
		v.Set("from", f.From)
	}
	if f.To != "" {
		v.Set("to", f.To)
	}
	if page > 1 {
		v.Set("page", strconv.Itoa(page))
	}
	if len(v) == 0 {
		return "/admin/dlq"
	}
	return "/admin/dlq?" + v.Encode()
}

func queueTasksURL(name string) string { return "/admin/tasks?queue=" + url.QueryEscape(name) }
func queueDLQURL(name string) string   { return "/admin/dlq?queue=" + url.QueryEscape(name) }

func selectedValue(queueName, id string) string { return queueName + ":" + id }

// hxVals encodes values for an hx-vals attribute.
func hxVals(kv ...string) string {
	m := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func modelStatus(s string) model.TaskStatus { return model.TaskStatus(s) }

func orDash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

func suffix(rel string) string {
	if rel == "" {
		return ""
	}
	return " (" + rel + ")"
}
