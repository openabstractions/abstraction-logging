package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	identity "github.com/openabstractions/abstraction-identity"
	logging "github.com/openabstractions/abstraction-logging/go"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	client "github.com/openabstractions/abstraction-logging/go/client"
	"github.com/openabstractions/abstraction-logging/go/internal/sinkclock"
)

// ReadAboutCaller (LOG-P9) returns only rows the runtime's owned writer stamped
// as about the proven caller, from since onward, under one caller-log check.
func TestReadAboutCallerReturnsOnlyStampedRowsForTheCaller(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var aboutChecks, otherChecks atomic.Int64
	refuse := atomic.Bool{}
	policy := appPermit()
	policy.Check = func(_ context.Context, _ *identity.Peer, q ApplicationHistoryQuery) ([]bool, error) {
		if !q.AboutCaller {
			otherChecks.Add(1)
			allowed := make([]bool, len(q.Emitters))
			for i := range allowed {
				allowed[i] = true
			}
			return allowed, nil
		}
		if q.Selection != 0 || len(q.Emitters) != 0 || q.Target != (wire.ApplicationLogResource{}) {
			t.Errorf("caller-log query carried a target: %+v", q)
		}
		aboutChecks.Add(1)
		if refuse.Load() {
			return nil, errNotPermittedForTest
		}
		return []bool{}, nil
	}
	sink, reader, _, account := applicationTestHost(t, policy)
	runtimeProgram := filepath.Join(t.TempDir(), "runtime")
	if err := os.WriteFile(runtimeProgram, []byte("runtime"), 0o600); err != nil {
		t.Fatal(err)
	}
	resource, err := logging.PrepareApplicationResource(runtimeProgram)
	if err != nil {
		t.Fatal(err)
	}
	runtimeWriter, err := logging.WithEmitter(sink, logging.Emitter{Version: 1, Account: account, Program: resource.Program, Key: resource.CapturedFileKey})
	if err != nil {
		t.Fatal(err)
	}
	aboutSelf, err := logging.AboutCaller(runtimeWriter, self)
	if err != nil {
		t.Fatal(err)
	}
	aboutOther, err := logging.AboutCaller(runtimeWriter, filepath.Join(t.TempDir(), "other"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := logging.AboutCaller(runtimeWriter, "relative"); err == nil {
		t.Fatal("an unproven relative program was accepted as a caller")
	}
	since := time.Now().UTC().Add(-time.Minute)
	old := appRecord("about me, before the window")
	old.Time = logging.Timestamp{Time: since.Add(-time.Hour)}
	for _, w := range []struct {
		out logging.Sink
		r   logging.Record
	}{{aboutSelf, old}, {aboutSelf, appRecord("about me")}, {aboutOther, appRecord("about another caller")}, {runtimeWriter, appRecord("runtime record about nobody")}} {
		if err := w.out.Write(w.r); err != nil {
			t.Fatal(err)
		}
	}
	// The caller's own writes carry its emitter, never a subject stamp.
	appWrite(t, sink, account, self, "written by the caller itself")

	ctx := context.Background()
	page, err := reader.ReadAboutCallerContext(ctx, since, "", 16, 65536)
	if err != nil || page.Outcome != wire.HistoryPageOutcomePage || len(page.Records) != 1 || page.Records[0].Msg != "about me" || !page.AtEnd {
		t.Fatalf("about caller: %+v %v", page, err)
	}
	if aboutChecks.Load() == 0 || otherChecks.Load() != 0 {
		t.Fatalf("caller-log admission checks about=%d other=%d", aboutChecks.Load(), otherChecks.Load())
	}
	for k := range page.Records[0].Attrs {
		if strings.HasPrefix(k, "_oa") {
			t.Fatalf("private stamp leaked into the public record: %v", page.Records[0].Attrs)
		}
	}
	// The cursor binds since; another window is a gap with the cursor kept.
	changed, err := reader.ReadAboutCallerContext(ctx, since.Add(time.Second), page.Next, 16, 65536)
	if err != nil || changed.Outcome != wire.HistoryPageOutcomeGap || changed.Next != page.Next || len(changed.Records) != 0 {
		t.Fatalf("changed since: %+v %v", changed, err)
	}
	// An application-log cursor cannot resume a caller-log read.
	all, err := reader.ReadContext(ctx, wire.ApplicationLogTarget{Selection: wire.ApplicationLogSelectionAllApps}, "", 16, 65536)
	if err != nil || all.Outcome != wire.HistoryPageOutcomePage || len(all.Records) != 5 {
		t.Fatalf("all apps still sees every attributed row: %+v %v", all, err)
	}
	crossed, err := reader.ReadAboutCallerContext(ctx, since, all.Next, 16, 65536)
	if err != nil || crossed.Outcome != wire.HistoryPageOutcomeGap {
		t.Fatalf("crossed cursor: %+v %v", crossed, err)
	}
	timing := appRecord("abstraction.inference/chat@3")
	timing.Attrs = map[string]string{"oa.load_ms": "12"}
	if err := aboutSelf.Write(timing); err != nil {
		t.Fatal(err)
	}
	if err := aboutOther.Write(timing); err != nil {
		t.Fatal(err)
	}
	followed, err := reader.ObserveAboutCallerContext(ctx, since, page.Next, 16, 65536, 0)
	if err != nil || followed.Outcome != wire.HistoryPageOutcomePage || len(followed.Records) != 1 || followed.Records[0].Attrs["oa.load_ms"] != "12" {
		t.Fatalf("follow own timing: %+v %v", followed, err)
	}
	refuse.Store(true)
	denied, err := reader.ObserveAboutCallerContext(ctx, since, "", 16, 65536, 500)
	if err != nil || denied.Outcome != wire.HistoryPageOutcomeNotPermitted || len(denied.Records) != 0 {
		t.Fatalf("refused: %+v %v", denied, err)
	}
}

// A fresh read seeks and filters by time; the sink-written seek itself is
// held by the logging package's seek tests, which control the sink clock.
func TestReadAboutCallerFiltersEarlierRowsAndSealsItsMode(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	policy := appPermit()
	policy.Check = func(context.Context, *identity.Peer, ApplicationHistoryQuery) ([]bool, error) { return []bool{}, nil }
	sink, reader, _, account := applicationTestHost(t, policy)
	runtimeProgram := filepath.Join(t.TempDir(), "runtime")
	if err := os.WriteFile(runtimeProgram, []byte("runtime"), 0o600); err != nil {
		t.Fatal(err)
	}
	resource, err := logging.PrepareApplicationResource(runtimeProgram)
	if err != nil {
		t.Fatal(err)
	}
	runtimeWriter, err := logging.WithEmitter(sink, logging.Emitter{Version: 1, Account: account, Program: resource.Program, Key: resource.CapturedFileKey})
	if err != nil {
		t.Fatal(err)
	}
	about, err := logging.AboutCaller(runtimeWriter, self)
	if err != nil {
		t.Fatal(err)
	}
	since := time.Now().UTC().Add(-time.Minute)
	for i := 0; i < 20; i++ {
		old := appRecord(fmt.Sprintf("earlier request %d", i))
		old.Time = logging.Timestamp{Time: since.Add(-2*time.Hour + time.Duration(i)*time.Second)}
		if err := about.Write(old); err != nil {
			t.Fatal(err)
		}
	}
	if err := about.Write(appRecord("in the window")); err != nil {
		t.Fatal(err)
	}
	page, err := reader.ReadAboutCallerContext(context.Background(), since, "", 16, 65536)
	if err != nil || page.Outcome != wire.HistoryPageOutcomePage || len(page.Records) != 1 || page.Records[0].Msg != "in the window" || !page.AtEnd {
		t.Fatalf("seek: %d records, outcome %v, end %v, %v", len(page.Records), page.Outcome, page.AtEnd, err)
	}
	// A caller-log cursor cannot resume Read, and the reverse is a gap too.
	crossed, err := reader.ReadContext(context.Background(), wire.ApplicationLogTarget{Selection: wire.ApplicationLogSelectionAllApps}, page.Next, 16, 65536)
	if err != nil || crossed.Outcome != wire.HistoryPageOutcomeGap || crossed.Next != page.Next {
		t.Fatalf("caller-log cursor fed to Read: %+v %v", crossed, err)
	}
}

// More than 256 rows before the window: only the seek reaches the window on
// the first page; a walk from the earliest row returns an empty page.
func TestReadAboutCallerSeeksPastMoreThanAPageBeforeSince(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	clock := now.Add(-30 * time.Minute)
	t.Cleanup(sinkclock.Set(func() time.Time { return clock }))
	policy := appPermit()
	policy.Check = func(context.Context, *identity.Peer, ApplicationHistoryQuery) ([]bool, error) { return []bool{}, nil }
	sink, reader, _, account := applicationTestHost(t, policy)
	runtimeProgram := filepath.Join(t.TempDir(), "runtime")
	if err := os.WriteFile(runtimeProgram, []byte("runtime"), 0o600); err != nil {
		t.Fatal(err)
	}
	resource, err := logging.PrepareApplicationResource(runtimeProgram)
	if err != nil {
		t.Fatal(err)
	}
	runtimeWriter, err := logging.WithEmitter(sink, logging.Emitter{Version: 1, Account: account, Program: resource.Program, Key: resource.CapturedFileKey})
	if err != nil {
		t.Fatal(err)
	}
	about, err := logging.AboutCaller(runtimeWriter, self)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 600; i++ {
		clock = now.Add(-30*time.Minute + time.Duration(i)*time.Second)
		earlier := appRecord("earlier request")
		earlier.Time = logging.Timestamp{Time: clock}
		if err := about.Write(earlier); err != nil {
			t.Fatal(err)
		}
	}
	clock = now
	window := appRecord("in the window")
	window.Time = logging.Timestamp{Time: now}
	if err := about.Write(window); err != nil {
		t.Fatal(err)
	}
	page, err := reader.ReadAboutCallerContext(context.Background(), now.Add(-time.Minute), "", 16, 65536)
	if err != nil || page.Outcome != wire.HistoryPageOutcomePage || len(page.Records) != 1 || page.Records[0].Msg != "in the window" {
		t.Fatalf("seek: %d records, outcome %v, end %v, %v", len(page.Records), page.Outcome, page.AtEnd, err)
	}
}

var errNotPermittedForTest = &testRefusal{}

// Exercise the public service over retained legacy rows, then a complete
// service/sink restart. The sink's exact instant monotonicity is held by
// TestSinkInstantContinuesAcrossARestart; this checks caller filtering and
// cursor refusal through reader@2 rather than the sink's private scan.
func TestReadAboutCallerUpgradeSeekAndRestartGap(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	clock := now.Add(-30 * time.Minute)
	t.Cleanup(sinkclock.Set(func() time.Time { return clock }))
	path := filepath.Join(t.TempDir(), "rows")
	var legacy strings.Builder
	for i := 0; i < 600; i++ {
		row := appRecord("legacy runtime row")
		row.Time = logging.Timestamp{Time: now.Add(-2*time.Hour + time.Duration(i)*time.Second)}
		encoded, err := row.Encode()
		if err != nil {
			t.Fatal(err)
		}
		legacy.Write(encoded)
	}
	if err := os.WriteFile(path, []byte(legacy.String()), 0600); err != nil {
		t.Fatal(err)
	}
	policy := appPermit()
	policy.Check = func(_ context.Context, _ *identity.Peer, q ApplicationHistoryQuery) ([]bool, error) {
		if !q.AboutCaller || q.Selection != 0 || len(q.Emitters) != 0 || q.Target != (wire.ApplicationLogResource{}) {
			t.Errorf("read expanded its caller-log scope: %+v", q)
		}
		return []bool{}, nil
	}
	sink, endpoint, _, account, stop := applicationTestHostAtPath(t, policy, path)
	writer := func(sink *logging.FileSink, subject string) logging.Sink {
		t.Helper()
		resource, err := logging.PrepareApplicationResource(self)
		if err != nil {
			t.Fatal(err)
		}
		emitter, err := logging.WithEmitter(sink, logging.Emitter{Version: 1, Account: account, Program: resource.Program, Key: resource.CapturedFileKey})
		if err != nil {
			t.Fatal(err)
		}
		about, err := logging.AboutCaller(emitter, subject)
		if err != nil {
			t.Fatal(err)
		}
		return about
	}
	clock = now
	if err := writer(sink, self).Write(appRecord("own failure after upgrade")); err != nil {
		t.Fatal(err)
	}
	since := now.Add(-time.Minute)
	reader := client.NewApplicationReader(endpoint)
	page, err := reader.ReadAboutCallerContext(t.Context(), since, "", 16, 65536)
	if err != nil || page.Outcome != wire.HistoryPageOutcomePage || len(page.Records) != 1 || page.Records[0].Msg != "own failure after upgrade" || !page.AtEnd {
		t.Fatalf("legacy seek did not reach own failure on first page: %+v %v", page, err)
	}
	stop() // joins the old service and closes its sink before reopening.
	clock = now.Add(-10 * time.Minute)
	sink, endpoint, _, _, _ = applicationTestHostAtPath(t, policy, path)
	if err := writer(sink, self).Write(appRecord("own failure after restart")); err != nil {
		t.Fatal(err)
	}
	if err := writer(sink, filepath.Join(t.TempDir(), "other-caller")).Write(appRecord("another caller's failure")); err != nil {
		t.Fatal(err)
	}
	reader = client.NewApplicationReader(endpoint)
	gap, err := reader.ReadAboutCallerContext(t.Context(), since, page.Next, 16, 65536)
	if err != nil || gap.Outcome != wire.HistoryPageOutcomeGap || gap.Next != page.Next || len(gap.Records) != 0 {
		t.Fatalf("restart cursor silently resumed: %+v %v", gap, err)
	}
	fresh, err := reader.ReadAboutCallerContext(t.Context(), since, "", 16, 65536)
	if err != nil || fresh.Outcome != wire.HistoryPageOutcomePage || len(fresh.Records) != 2 || fresh.Records[0].Msg != "own failure after upgrade" || fresh.Records[1].Msg != "own failure after restart" || !fresh.AtEnd {
		t.Fatalf("fresh restarted caller read: %+v %v", fresh, err)
	}
	for _, row := range fresh.Records {
		for key := range row.Attrs {
			if strings.HasPrefix(key, "_oa") {
				t.Fatalf("private stamp disclosed: %s", key)
			}
		}
	}
}

type testRefusal struct{}

func (*testRefusal) Error() string { return "not permitted" }
