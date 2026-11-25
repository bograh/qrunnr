package views

import (
	"sort"
	"strconv"
	"time"

	"qrunnr.dev/internal/model"
)

// tone is the semantic colour of a status: ok, warn, fail, info, run, or muted.
type tone string

const (
	toneOK    tone = "ok"
	toneWarn  tone = "warn"
	toneFail  tone = "fail"
	toneInfo  tone = "info"
	toneRun   tone = "run"
	toneMuted tone = "muted"
)

// Thresholds behind the overview verdict.
const (
	errorRateWarn   = 0.05
	errorRateFail   = 0.25
	oldestAgeWarn   = 5 * 60 // seconds
	workerStaleSecs = 20
)

func statusTone(status model.TaskStatus) tone {
	switch status {
	case model.TaskStatusDone:
		return toneOK
	case model.TaskStatusRetry:
		return toneWarn
	case model.TaskStatusDead:
		return toneFail
	case model.TaskStatusExecuting, model.TaskStatusClaimed:
		return toneRun
	case model.TaskStatusPending:
		return toneInfo
	default:
		return toneMuted
	}
}

// markClass is the small square status mark used everywhere instead of pills.
func markClass(t tone) string {
	switch t {
	case toneOK:
		return "inline-block size-2 shrink-0 bg-ok"
	case toneWarn:
		return "inline-block size-2 shrink-0 bg-warn"
	case toneFail:
		return "inline-block size-2 shrink-0 bg-fail"
	case toneInfo:
		return "inline-block size-2 shrink-0 bg-info"
	case toneRun:
		return "inline-block size-2 shrink-0 bg-run"
	default:
		return "inline-block size-2 shrink-0 border border-ink-3"
	}
}

func toneTextClass(t tone) string {
	switch t {
	case toneOK:
		return "text-ok"
	case toneWarn:
		return "text-warn"
	case toneFail:
		return "text-fail"
	case toneInfo:
		return "text-info"
	case toneRun:
		return "text-run"
	default:
		return "text-ink-3"
	}
}

// edgeClass marks a table row's health with an inset bar on its first cell.
func edgeClass(t tone) string {
	switch t {
	case toneFail:
		return "shadow-[inset_3px_0_0_var(--color-fail)]"
	case toneWarn:
		return "shadow-[inset_3px_0_0_var(--color-warn)]"
	default:
		return ""
	}
}

func flashTone(kind FlashKind) tone {
	if kind == FlashError {
		return toneFail
	}
	return toneOK
}

func errorRatio(done, failed int) float64 {
	if done+failed == 0 {
		return 0
	}
	return float64(failed) / float64(done+failed)
}

func errorRateTone(done, failed int) tone {
	switch r := errorRatio(done, failed); {
	case done+failed == 0:
		return toneMuted
	case r >= errorRateFail:
		return toneFail
	case r >= errorRateWarn:
		return toneWarn
	default:
		return toneMuted
	}
}

// queueHealth grades a queue and lists the reasons, worst first.
func queueHealth(q model.QueueOverview) (tone, []string) {
	s := q.Stats
	worst := toneOK
	reasons := []string{}
	raise := func(t tone, reason string) {
		if t == toneFail || (t == toneWarn && worst == toneOK) {
			worst = t
		}
		reasons = append(reasons, reason)
	}

	if r := errorRatio(s.DoneLastMinute, s.FailedLastMinute); s.DoneLastMinute+s.FailedLastMinute > 0 && r >= errorRateWarn {
		t := toneWarn
		if r >= errorRateFail {
			t = toneFail
		}
		raise(t, errorRate(s.DoneLastMinute, s.FailedLastMinute)+" of executions failed in the last minute")
	}
	if s.DLQDepth > 0 {
		raise(toneWarn, plural(s.DLQDepth, "dead letter", "dead letters")+" waiting for review")
	}
	if s.OldestPendingAgeSeconds >= oldestAgeWarn {
		raise(toneWarn, "oldest task has waited "+ageSeconds(s.OldestPendingAgeSeconds))
	}
	return worst, reasons
}

type queueRow struct {
	model.QueueOverview
	Tone    tone
	Reasons []string
}

// queueRows orders queues needing attention first (failing, then warning),
// then by name.
func (d OverviewData) queueRows() []queueRow {
	rows := make([]queueRow, 0, len(d.Queues))
	for _, q := range d.Queues {
		t, reasons := queueHealth(q)
		rows = append(rows, queueRow{QueueOverview: q, Tone: t, Reasons: reasons})
	}
	rank := map[tone]int{toneFail: 0, toneWarn: 1}
	sort.SliceStable(rows, func(i, j int) bool {
		ri, iok := rank[rows[i].Tone]
		rj, jok := rank[rows[j].Tone]
		if !iok {
			ri = 2
		}
		if !jok {
			rj = 2
		}
		if ri != rj {
			return ri < rj
		}
		return rows[i].Queue.Name < rows[j].Queue.Name
	})
	return rows
}

func (d OverviewData) maxDepth() int {
	m := 0
	for _, q := range d.Queues {
		m = max(m, q.Stats.Depth)
	}
	return m
}

type verdictLine struct {
	Tone    tone
	Text    string
	Details []string
}

// verdict is the one-sentence answer to "is anything wrong?", computed from
// live data only.
func (d OverviewData) verdict() verdictLine {
	if len(d.Queues) == 0 {
		return verdictLine{Tone: toneMuted, Text: "No queues yet. Create one below to start accepting tasks."}
	}

	t := d.totals()
	v := verdictLine{Tone: toneOK}
	attention := 0
	for _, row := range d.queueRows() {
		if row.Tone == toneOK {
			continue
		}
		attention++
		if row.Tone == toneFail {
			v.Tone = toneFail
		} else if v.Tone != toneFail {
			v.Tone = toneWarn
		}
		v.Details = append(v.Details, row.Queue.Name+": "+row.Reasons[0])
	}
	if t.Depth > 0 && t.ActiveWorkers == 0 {
		if v.Tone != toneFail {
			v.Tone = toneWarn
		}
		v.Details = append(v.Details, plural(t.Depth, "task is", "tasks are")+" queued but no worker has checked in for 60 seconds")
	} else if t.StaleWorkers > 0 {
		if v.Tone == toneOK {
			v.Tone = toneWarn
		}
		v.Details = append(v.Details, plural(t.StaleWorkers, "worker has", "workers have")+" gone quiet for over 20 seconds")
	}

	switch {
	case attention > 0:
		v.Text = itoa(attention) + " of " + plural(len(d.Queues), "queue needs", "queues need") + " attention."
		if attention == 1 {
			v.Text = "1 of " + itoa(len(d.Queues)) + " queues needs attention."
		}
		if len(d.Queues) == 1 {
			v.Text = "The queue needs attention."
		}
	case v.Tone != toneOK:
		v.Text = "Queues are healthy, but workers need a look."
	case len(d.Queues) == 1:
		v.Text = "The queue is healthy."
	default:
		v.Text = "All " + itoa(len(d.Queues)) + " queues are healthy."
	}
	return v
}

func plural(n int, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return strconv.Itoa(n) + " " + many
}

// pctStyle renders a CSS width for a fraction clamped to [0, 1]. A non-zero
// value never renders thinner than 2% so small counts stay visible.
func pctStyle(n, of int) string {
	if of <= 0 || n <= 0 {
		return "width: 0%"
	}
	pct := float64(n) / float64(of) * 100
	if pct > 100 {
		pct = 100
	}
	if pct < 2 {
		pct = 2
	}
	return "width: " + strconv.FormatFloat(pct, 'f', 1, 64) + "%"
}

// freshness describes how recently a worker checked in, against the 20s
// stale threshold.
func freshness(lastSeen, now time.Time) (style string, t tone) {
	age := int(now.Sub(lastSeen).Seconds())
	if age < 0 {
		age = 0
	}
	switch {
	case age >= workerStaleSecs:
		t = toneFail
	case age >= workerStaleSecs/2:
		t = toneWarn
	default:
		t = toneMuted
	}
	return pctStyle(min(age, workerStaleSecs), workerStaleSecs), t
}

func barFillClass(t tone) string {
	switch t {
	case toneWarn:
		return "block h-full bg-warn"
	case toneFail:
		return "block h-full bg-fail"
	case toneOK:
		return "block h-full bg-ok"
	default:
		return "block h-full bg-ink-3"
	}
}

func eventTone(e model.TaskEvent) tone {
	switch e.Kind {
	case model.TaskEventReplay:
		return toneInfo
	case model.TaskEventHeartbeat:
		return toneMuted
	}
	return statusTone(e.ToStatus)
}
