package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openabstractions/abstraction-facade/go-core/resolution"
)

// outageSink is the sink-outage fault (conformance/faults/sink-outage): while
// down, every write fails and every rebind is the typed resolution error. A
// held message's next successful write waits until release.
type outageSink struct {
	mu         sync.Mutex
	down       bool
	delivered  []Record
	rebinds    int
	writeError string
	status     string
	cause      string
	hold       string
	held       bool
	release    chan struct{}
}

func (s *outageSink) Write(r Record) error {
	s.mu.Lock()
	if s.down {
		s.mu.Unlock()
		return errors.New(s.writeError)
	}
	if s.hold != "" && r.Msg == s.hold {
		s.hold, s.held = "", true
		release := s.release
		s.mu.Unlock()
		<-release
		s.mu.Lock()
	}
	defer s.mu.Unlock()
	s.delivered = append(s.delivered, r)
	return nil
}

func (s *outageSink) holdNext(msg string) {
	s.mu.Lock()
	s.hold, s.release = msg, make(chan struct{})
	s.mu.Unlock()
}

func (s *outageSink) isHeld() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.held
}

func (s *outageSink) releaseHeld() {
	s.mu.Lock()
	close(s.release)
	s.mu.Unlock()
}

func (s *outageSink) Rebind(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rebinds++
	if s.down {
		return &ResolutionError{Status: resolution.ErrorStatus(s.status), Capability: "abstraction.logging",
			Contract: "abstraction.logging/sink@1", LookedFor: "the installed runtime", Err: errors.New(s.cause)}
	}
	return nil
}

func (s *outageSink) set(down bool) {
	s.mu.Lock()
	s.down = down
	s.mu.Unlock()
}

func (s *outageSink) snapshot() ([]Record, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Record(nil), s.delivered...), s.rebinds
}

// lockedBuffer is a writer the delivery goroutine and the test share.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) lines() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	text := strings.TrimRight(b.buf.String(), "\n")
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}

// captureReports sends the default reporter's lines to a buffer for one test.
func captureReports(t *testing.T) *lockedBuffer {
	t.Helper()
	out := &lockedBuffer{}
	saved := reportTo
	reportTo = func() io.Writer { return out }
	t.Cleanup(func() { reportTo = saved })
	return out
}

type outageCounts struct {
	Accepted  uint64 `json:"accepted"`
	Dropped   uint64 `json:"dropped"`
	Written   uint64 `json:"written"`
	Failed    uint64 `json:"failed"`
	Abandoned uint64 `json:"abandoned"`
	Queued    int    `json:"queued"`
	Failing   bool   `json:"failing"`
}

type outageDefaults struct {
	Capacity         int    `json:"capacity"`
	InitialBackoffMS int64  `json:"initial_backoff_ms"`
	MaxBackoffMS     int64  `json:"max_backoff_ms"`
	WriteTimeoutMS   int64  `json:"write_timeout_ms"`
	JitterPercent    int    `json:"jitter_percent"`
	GapMessage       string `json:"gap_message"`
}

func checkOutageDefaults(want outageDefaults, options AsyncOptions) error {
	sink := NewAsyncSink(&outageSink{}, options)
	got := outageDefaults{sink.opts.Capacity, sink.opts.InitialBackoff.Milliseconds(), sink.opts.MaxBackoff.Milliseconds(), sink.opts.WriteTimeout.Milliseconds(), int(BackoffJitter * 100), GapMessage}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := sink.Close(ctx); err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("writer defaults %+v, want %+v", got, want)
	}
	return nil
}

// Keep the existing public constants usable in their original untyped contexts.
var _ int64 = DefaultAsyncCapacity
var _ float32 = BackoffJitter

func countsOf(c AsyncCounts) outageCounts {
	return outageCounts{c.Accepted, c.Dropped, c.Written, c.Failed, c.Abandoned, c.Queued, c.Failing}
}

type outageFixture struct {
	Options struct {
		Capacity         int    `json:"capacity"`
		InitialBackoffMS int    `json:"initial_backoff_ms"`
		MaxBackoffMS     int    `json:"max_backoff_ms"`
		Program          string `json:"program"`
	} `json:"options"`
	Outage struct {
		WriteError  string `json:"write_error"`
		RebindError struct {
			Status string `json:"status"`
			Cause  string `json:"cause"`
		} `json:"rebind_error"`
	} `json:"outage"`
	Steps []struct {
		Defaults *outageDefaults `json:"defaults"`
		Log      []string        `json:"log"`
		Await    string          `json:"await"`
		Outage   *bool           `json:"outage"`
		Hold     string          `json:"hold"`
		Release  bool            `json:"release"`
		Counts   *outageCounts   `json:"counts"`
		Close    bool            `json:"close"`
	} `json:"steps"`
	Expect struct {
		Counts            outageCounts `json:"counts"`
		LastErrorContains string       `json:"last_error_contains"`
		FailureStatus     string       `json:"failure_status"`
		Transitions       []string     `json:"transitions"`
		Delivered         []string     `json:"delivered"`
		Gap               struct {
			Level      int               `json:"level"`
			Msg        string            `json:"msg"`
			Attrs      map[string]string `json:"attrs"`
			Timestamps []string          `json:"timestamps"`
			Hop        int               `json:"hop"`
			By         string            `json:"by"`
			Program    string            `json:"program"`
		} `json:"gap"`
		GapsDropped []string `json:"gaps_dropped"`
		Reporter    []string `json:"reporter"`
	} `json:"expect"`
}

func loadOutageFixture(t *testing.T) outageFixture {
	t.Helper()
	path := privateInput(t, "..", "..", "..", "conformance", "faults", "sink-outage", "sink-outage.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var f outageFixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	return f
}

// The shared sink-outage fault [LOG-S12, LOG-S13]: every language runs the same
// steps and asserts the same counts, transitions, delivered order, gap record
// and default reporter lines.
func TestSinkOutageFixture(t *testing.T) {
	f := loadOutageFixture(t)
	for _, reporter := range []bool{false, true} {
		t.Run(map[bool]string{false: "callbacks", true: "default reporter"}[reporter], func(t *testing.T) {
			inner := &outageSink{writeError: f.Outage.WriteError, status: f.Outage.RebindError.Status, cause: f.Outage.RebindError.Cause}
			var mu sync.Mutex
			var transitions []string
			var failure error
			opts := AsyncOptions{
				Capacity:       f.Options.Capacity,
				InitialBackoff: time.Duration(f.Options.InitialBackoffMS) * time.Millisecond,
				MaxBackoff:     time.Duration(f.Options.MaxBackoffMS) * time.Millisecond,
				Program:        f.Options.Program,
			}
			var lines *lockedBuffer
			reported := func() int {
				if reporter {
					return len(lines.lines())
				}
				mu.Lock()
				defer mu.Unlock()
				return len(transitions)
			}
			if reporter {
				lines = captureReports(t)
			} else {
				note := func(name string) { mu.Lock(); transitions = append(transitions, name); mu.Unlock() }
				opts.OnFailure = func(err error, _ AsyncCounts) { mu.Lock(); failure = err; mu.Unlock(); note("failure") }
				opts.OnOverflow = func(AsyncCounts) { note("overflow") }
				opts.OnRecovery = func(AsyncCounts) { note("recovery") }
			}
			sink := NewAsyncSink(inner, opts)
			logger := slog.New(NewHandler(sink, &Options{Program: f.Options.Program}))
			var final AsyncCounts
			for i, step := range f.Steps {
				switch {
				case step.Defaults != nil:
					if err := checkOutageDefaults(*step.Defaults, AsyncOptions{}); err != nil {
						t.Fatal(err)
					}
				case step.Log != nil:
					for _, msg := range step.Log {
						logger.Info(msg)
					}
				case step.Outage != nil:
					inner.set(*step.Outage)
				case step.Hold != "":
					inner.holdNext(step.Hold)
				case step.Release:
					inner.releaseHeld()
				case step.Await == "held":
					eventually(t, "the held delivery", inner.isHeld)
				case step.Counts != nil:
					if got := countsOf(sink.Counts()); got != *step.Counts {
						t.Fatalf("step %d counts %+v, want %+v", i, got, *step.Counts)
					}
				case step.Close:
					c, err := closeWithin(t, sink, 5*time.Second)
					if err != nil {
						t.Fatal(err)
					}
					final = c
				case step.Await == "settled":
					eventually(t, "settled", func() bool {
						c := sink.Counts()
						return c.Queued == 0 && !c.Failing && c.Written+c.Failed+c.Abandoned == c.Accepted
					})
				case step.Await == "failing":
					eventually(t, "the failure report", func() bool { return sink.Counts().Failing && reported() >= 1 })
				case step.Await == "retry":
					eventually(t, "a retry during the outage", func() bool { _, rebinds := inner.snapshot(); return rebinds >= 2 })
				default:
					t.Fatalf("step %d: unknown step", i)
				}
			}
			if got := countsOf(final); got != f.Expect.Counts {
				t.Fatalf("final counts %+v, want %+v", got, f.Expect.Counts)
			}
			if !strings.Contains(final.LastError, f.Expect.LastErrorContains) {
				t.Fatalf("last error %q does not contain %q", final.LastError, f.Expect.LastErrorContains)
			}
			delivered, _ := inner.snapshot()
			var messages []string
			for _, r := range delivered {
				messages = append(messages, r.Msg)
			}
			if fmt.Sprint(messages) != fmt.Sprint(f.Expect.Delivered) {
				t.Fatalf("delivered %q, want %q", messages, f.Expect.Delivered)
			}
			gap := delivered[2]
			want := f.Expect.Gap
			if int(gap.Level) != want.Level || gap.Msg != want.Msg || len(gap.Identity) != 1 || gap.Identity[0].Hop != want.Hop ||
				gap.Identity[0].By != want.By || gap.Identity[0].Program != want.Program {
				t.Fatalf("gap record %+v", gap)
			}
			instant := regexp.MustCompile(`^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$`)
			for k, v := range want.Attrs {
				if gap.Attrs[k] != v {
					t.Fatalf("gap attribute %s = %q, want %q", k, gap.Attrs[k], v)
				}
			}
			for _, k := range want.Timestamps {
				if !instant.MatchString(gap.Attrs[k]) {
					t.Fatalf("gap attribute %s = %q is not a fixed-width instant", k, gap.Attrs[k])
				}
			}
			// Every drop is reported by exactly one gap record; a later gap record
			// begins where the previous one ended.
			var gaps []string
			var reportedTotal uint64
			previousUntil := ""
			for _, r := range delivered {
				if r.Msg != GapMessage {
					continue
				}
				gaps = append(gaps, r.Attrs["dropped"])
				n, err := strconv.ParseUint(r.Attrs["dropped"], 10, 64)
				if err != nil {
					t.Fatalf("gap dropped %q: %v", r.Attrs["dropped"], err)
				}
				reportedTotal += n
				for _, k := range want.Timestamps {
					if !instant.MatchString(r.Attrs[k]) {
						t.Fatalf("gap attribute %s = %q is not a fixed-width instant", k, r.Attrs[k])
					}
				}
				if r.Attrs["since"] > r.Attrs["until"] {
					t.Fatalf("gap since %s after until %s", r.Attrs["since"], r.Attrs["until"])
				}
				if previousUntil != "" && r.Attrs["since"] != previousUntil {
					t.Fatalf("later gap since %s, want the previous until %s", r.Attrs["since"], previousUntil)
				}
				previousUntil = r.Attrs["until"]
			}
			if fmt.Sprint(gaps) != fmt.Sprint(f.Expect.GapsDropped) {
				t.Fatalf("gap records dropped %q, want %q", gaps, f.Expect.GapsDropped)
			}
			if reportedTotal != final.Dropped {
				t.Fatalf("gap records report %d dropped, counts say %d", reportedTotal, final.Dropped)
			}
			if reporter {
				if got := lines.lines(); fmt.Sprint(got) != fmt.Sprint(f.Expect.Reporter) {
					t.Fatalf("reporter lines %q, want %q", got, f.Expect.Reporter)
				}
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if fmt.Sprint(transitions) != fmt.Sprint(f.Expect.Transitions) {
				t.Fatalf("transitions %v, want %v", transitions, f.Expect.Transitions)
			}
			var refused *ResolutionError
			if !errors.As(failure, &refused) || string(refused.Status) != f.Expect.FailureStatus {
				t.Fatalf("failure reported %T %v, want the %s resolution error", failure, failure, f.Expect.FailureStatus)
			}
		})
	}
}

func TestSinkOutageDefaultsRejectWriterDrift(t *testing.T) {
	f := loadOutageFixture(t)
	for _, step := range f.Steps {
		if step.Defaults != nil {
			if err := checkOutageDefaults(*step.Defaults, AsyncOptions{Capacity: step.Defaults.Capacity + 1}); err == nil {
				t.Fatal("writer default drift accepted")
			}
			return
		}
	}
	t.Fatal("fixture has no defaults step")
}

// A 2,000-record burst while the service is down drops exactly the newest
// 2000 - 1024, reports the overflow once, and the gap record carries the count.
func TestAsyncSinkBurstDuringOutageDropsTheNewestAndTheGapCarriesTheCount(t *testing.T) {
	inner := &outageSink{writeError: "down", status: "runtime_unavailable", cause: "down"}
	var overflows, failures, recoveries int
	var mu sync.Mutex
	s := NewAsyncSink(inner, AsyncOptions{
		InitialBackoff: 5 * time.Millisecond, MaxBackoff: 20 * time.Millisecond, Program: "burst",
		OnOverflow: func(AsyncCounts) { mu.Lock(); overflows++; mu.Unlock() },
		OnFailure:  func(error, AsyncCounts) { mu.Lock(); failures++; mu.Unlock() },
		OnRecovery: func(AsyncCounts) { mu.Lock(); recoveries++; mu.Unlock() },
	})
	inner.set(true)
	for i := range 2000 {
		_ = s.Write(asyncRecord(fmt.Sprintf("b%04d", i)))
	}
	c := s.Counts()
	if c.Accepted != 1024 || c.Dropped != 2000-1024 || c.Queued != 1024 {
		t.Fatalf("counts during the outage %+v", c)
	}
	eventually(t, "a retry during the outage", func() bool { _, rebinds := inner.snapshot(); return s.Counts().Failing && rebinds >= 2 })
	inner.set(false)
	final, err := closeWithin(t, s, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if final.Written != 1024 || final.Failed != 0 || final.Dropped != 976 {
		t.Fatalf("final counts %+v", final)
	}
	delivered, _ := inner.snapshot()
	if len(delivered) != 1025 || delivered[0].Msg != GapMessage || delivered[0].Attrs["dropped"] != "976" ||
		delivered[0].Level != LevelWarn || delivered[1].Msg != "b0000" || delivered[1024].Msg != "b1023" {
		t.Fatalf("delivered %d records, gap %+v", len(delivered), delivered[0])
	}
	mu.Lock()
	defer mu.Unlock()
	if overflows != 1 || failures != 1 || recoveries != 1 {
		t.Fatalf("overflow %d, failure %d, recovery %d reports", overflows, failures, recoveries)
	}
}

// With no callbacks, an outage and its recovery are exactly two lines on
// standard error, and no record is among them.
func TestAsyncSinkWithoutCallbacksReportsTwoLinesOnStandardError(t *testing.T) {
	inner := &outageSink{writeError: "pipe closed", status: "runtime_unavailable", cause: "gone"}
	inner.set(true)
	var counts AsyncCounts
	stderr := stderrOf(t, func() {
		s := NewAsyncSink(inner, AsyncOptions{InitialBackoff: 5 * time.Millisecond, MaxBackoff: 10 * time.Millisecond})
		for i := range 3 {
			_ = s.Write(asyncRecord(fmt.Sprint("secret record ", i)))
		}
		eventually(t, "a retry", func() bool { _, rebinds := inner.snapshot(); return rebinds >= 3 })
		inner.set(false)
		var err error
		if counts, err = closeWithin(t, s, 5*time.Second); err != nil {
			t.Fatal(err)
		}
	})
	lines := strings.Split(strings.TrimRight(stderr, "\n"), "\n")
	want := []string{
		"abstraction.logging: service unreachable (runtime_unavailable); 3 queued, 0 dropped",
		"abstraction.logging: delivering again; 0 dropped",
	}
	if fmt.Sprint(lines) != fmt.Sprint(want) || strings.Contains(stderr, "secret record") {
		t.Fatalf("stderr %q, want %q", stderr, want)
	}
	if counts.Written != 3 || counts.Failed != 0 {
		t.Fatalf("counts %+v", counts)
	}
}

// Close at its deadline during an outage counts the head record Failed and the
// rest Abandoned.
func TestAsyncSinkCloseAtTheDeadlineDuringAnOutageCountsAbandoned(t *testing.T) {
	inner := &outageSink{writeError: "down", status: "runtime_unavailable", cause: "down"}
	inner.set(true)
	s := NewAsyncSink(inner, AsyncOptions{InitialBackoff: 5 * time.Millisecond, MaxBackoff: 10 * time.Millisecond, OnFailure: quiet})
	for i := range 5 {
		if err := s.Write(asyncRecord(fmt.Sprint("a", i))); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "the failure", func() bool { return s.Counts().Failing })
	c, err := closeWithin(t, s, 100*time.Millisecond)
	if !errors.Is(err, ErrCloseDeadline) {
		t.Fatalf("close: %v", err)
	}
	if c.Accepted != 5 || c.Failed != 1 || c.Abandoned != 4 || c.Written != 0 || c.Queued != 0 {
		t.Fatalf("counts %+v", c)
	}
}

// The default reporter names a failure by its resolution status, timeout, or
// write_failed [LOG-S13].
func TestFailureClassNamesTheResolutionStatusOrTimeout(t *testing.T) {
	for err, want := range map[error]string{
		&ResolutionError{Status: resolution.RuntimeUnavailable}: "runtime_unavailable",
		fmt.Errorf("wrapped: %w", ErrWriteTimeout):              "timeout",
		context.DeadlineExceeded:                                "timeout",
		errors.New("connection reset"):                          "write_failed",
	} {
		if got := failureClass(err); got != want {
			t.Fatalf("class of %v = %s, want %s", err, got, want)
		}
	}
}

// A record dropped while the queue drains after a recovery, followed by a
// second failure before the queue empties, is reported by that failure's gap
// record, which begins where the first gap record ended [LOG-S13].
func TestAsyncSinkDropDuringTheDrainIsReportedByTheNextFailuresGapRecord(t *testing.T) {
	inner := &outageSink{writeError: "down", status: "runtime_unavailable", cause: "down"}
	inner.set(true)
	s := NewAsyncSink(inner, AsyncOptions{Capacity: 2, InitialBackoff: 5 * time.Millisecond, MaxBackoff: 10 * time.Millisecond,
		OnFailure: quiet})
	for _, msg := range []string{"a", "b", "c"} {
		_ = s.Write(asyncRecord(msg))
	}
	eventually(t, "the failure", func() bool { return s.Counts().Failing })
	inner.holdNext("a")
	inner.set(false)
	eventually(t, "the held delivery of a", inner.isHeld)
	if err := s.Write(asyncRecord("d")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("write during the drain: %v", err)
	}
	inner.set(true)
	inner.releaseHeld()
	eventually(t, "the second failure", func() bool { c := s.Counts(); return c.Failing && c.Written == 1 })
	inner.set(false)
	final, err := closeWithin(t, s, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	delivered, _ := inner.snapshot()
	var got []string
	for _, r := range delivered {
		if r.Msg == GapMessage {
			got = append(got, "gap "+r.Attrs["dropped"])
			continue
		}
		got = append(got, r.Msg)
	}
	if fmt.Sprint(got) != fmt.Sprint([]string{"gap 1", "a", "gap 1", "b"}) || final.Dropped != 2 || final.Written != 2 {
		t.Fatalf("delivered %q, counts %+v", got, final)
	}
	if delivered[2].Attrs["since"] != delivered[0].Attrs["until"] {
		t.Fatalf("second gap since %s, want the first gap's until %s", delivered[2].Attrs["since"], delivered[0].Attrs["until"])
	}
}
