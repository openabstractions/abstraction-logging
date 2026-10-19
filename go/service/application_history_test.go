package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	identity "github.com/openabstractions/abstraction-identity"
	"github.com/openabstractions/abstraction-identity/listen"
	logging "github.com/openabstractions/abstraction-logging/go"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	client "github.com/openabstractions/abstraction-logging/go/client"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var appTestID atomic.Int64

func applicationTestHost(t *testing.T, policy ApplicationHistoryPolicy) (*logging.FileSink, *client.ApplicationReader, *client.ApplicationObserver, string) {
	t.Helper()
	sink, ep, _, account := applicationTestHostAt(t, policy)
	return sink, client.NewApplicationReader(ep), client.NewApplicationObserver(ep), account
}
func applicationTestHostAt(t *testing.T, policy ApplicationHistoryPolicy) (*logging.FileSink, string, *Host, string) {
	t.Helper()
	account, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	sink, err := logging.OpenRetainedFileSink(filepath.Join(t.TempDir(), "rows"), logging.LogsRetention{MaxUse: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	ep := listen.Endpoint(fmt.Sprintf("app-log-%d-%d", os.Getpid(), appTestID.Add(1)))
	// A macOS socket endpoint cannot carry Program proof; the installed
	// runtime serves logging-v1 over XPC there, and service_test.go checks
	// the socket refusal.
	if err := listen.CanEver(ep, listen.Program); err != nil {
		sink.Close()
		t.Skipf("this platform's socket endpoint cannot carry Program proof: %v", err)
	}
	h, err := Listen(ep, sink)
	if err != nil {
		sink.Close()
		t.Fatal(err)
	}
	if err = h.EnableApplicationHistoryPolicy(policy); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("application host did not stop")
		}
		sink.Close()
	})
	return sink, ep, h, account.Uid
}
func appRecord(message string) logging.Record {
	return logging.Record{Schema: 1, Time: logging.Timestamp{Time: time.Now().UTC()}, Msg: message}
}
func appPermit() ApplicationHistoryPolicy {
	return ApplicationHistoryPolicy{Fits: func(*identity.Peer, ApplicationHistoryQuery) bool { return true }, Check: func(_ context.Context, _ *identity.Peer, q ApplicationHistoryQuery) ([]bool, error) {
		a := make([]bool, len(q.Emitters))
		for i := range a {
			a[i] = true
		}
		return a, nil
	}}
}
func appWrite(t *testing.T, s *logging.FileSink, account, program, message string) {
	t.Helper()
	if err := s.WriteAttributed(appRecord(message), logging.Emitter{Version: 1, Account: account, Program: program, Key: identity.ProgramKey(program)}); err != nil {
		t.Fatal(err)
	}
}

func TestApplicationHistoryFiltersBeforeQuotaAndChecksOneFinalBatch(t *testing.T) {
	a := filepath.Join(t.TempDir(), "a")
	b := filepath.Join(t.TempDir(), "b")
	var final atomic.Int64
	policy := appPermit()
	policy.Check = func(_ context.Context, _ *identity.Peer, q ApplicationHistoryQuery) ([]bool, error) {
		if len(q.Emitters) > 0 {
			final.Add(1)
		}
		result := make([]bool, len(q.Emitters))
		for i, e := range q.Emitters {
			result[i] = e.Program == a
		}
		return result, nil
	}
	s, r, _, acct := applicationTestHost(t, policy)
	appWrite(t, s, acct, b, strings.Repeat("large excluded", 200))
	appWrite(t, s, acct, a, "selected")
	appWrite(t, s, "other-account", a, "other-account")
	if err := s.Write(appRecord("legacy")); err != nil {
		t.Fatal(err)
	}
	p, err := r.ReadContext(context.Background(), wire.ApplicationLogTarget{Selection: wire.ApplicationLogSelectionAllApps}, "", 1, 512)
	if err != nil || p.Outcome != wire.HistoryPageOutcomePage || len(p.Records) != 1 || p.Records[0].Msg != "selected" || !p.AtEnd || final.Load() != 1 {
		t.Fatalf("filter/quota/snapshot: %+v %v final=%d", p, err, final.Load())
	}
}

func TestApplicationHistoryEmptyProgressAndTargetBoundCursor(t *testing.T) {
	a := filepath.Join(t.TempDir(), "a")
	b := filepath.Join(t.TempDir(), "b")
	s, r, _, acct := applicationTestHost(t, appPermit())
	for i := 0; i < logging.MaxHistoryScanRows; i++ {
		appWrite(t, s, acct, b, "excluded")
	}
	appWrite(t, s, acct, a, "selected")
	target := wire.ApplicationLogTarget{Selection: wire.ApplicationLogSelectionOneApp, Program: a}
	p, err := r.ReadContext(context.Background(), target, "", 1, 512)
	if err != nil || p.Outcome != wire.HistoryPageOutcomePage || len(p.Records) != 0 || p.AtEnd || p.Next == "" {
		t.Fatalf("empty progress: %+v %v", p, err)
	}
	next, err := r.ReadContext(context.Background(), target, p.Next, 1, 512)
	if err != nil || len(next.Records) != 1 || next.Records[0].Msg != "selected" || !next.AtEnd {
		t.Fatalf("next: %+v %v", next, err)
	}
	gap, err := r.ReadContext(context.Background(), wire.ApplicationLogTarget{Selection: wire.ApplicationLogSelectionOneApp, Program: b}, p.Next, 1, 512)
	if err != nil || gap.Outcome != wire.HistoryPageOutcomeGap || gap.Next != p.Next {
		t.Fatalf("foreign selection: %+v %v", gap, err)
	}
	forged := p.Next[:len(p.Next)-2] + "AA"
	gap, err = r.ReadContext(context.Background(), target, forged, 1, 512)
	if err != nil || gap.Outcome != wire.HistoryPageOutcomeGap {
		t.Fatalf("forged: %+v %v", gap, err)
	}
}

func TestApplicationHistoryResumeRetainsCapturedAliasKey(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "image")
	alias := filepath.Join(dir, "alias")
	if err := os.WriteFile(a, []byte("image"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(a, alias); err != nil {
		t.Fatal(err)
	}
	s, r, _, acct := applicationTestHost(t, appPermit())
	appWrite(t, s, acct, a, "one")
	appWrite(t, s, acct, a, "two")
	target := wire.ApplicationLogTarget{Selection: wire.ApplicationLogSelectionOneApp, Program: alias}
	p, err := r.ReadContext(context.Background(), target, "", 1, 512)
	if err != nil || len(p.Records) != 1 || p.AtEnd {
		t.Fatalf("first alias: %+v %v", p, err)
	}
	if err = os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(alias, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	p, err = r.ReadContext(context.Background(), target, p.Next, 1, 512)
	if err != nil || len(p.Records) != 1 || p.Records[0].Msg != "two" {
		t.Fatalf("recaptured selector: %+v %v", p, err)
	}
}

func TestApplicationHistoryPolicyRunsOutsideSinkLockAndRevokesEmptyPage(t *testing.T) {
	var s *logging.FileSink
	var deny atomic.Bool
	var calls atomic.Int64
	policy := appPermit()
	policy.Check = func(_ context.Context, _ *identity.Peer, q ApplicationHistoryQuery) ([]bool, error) {
		calls.Add(1)
		if deny.Load() {
			return nil, errors.New("blocked")
		}
		if len(q.Emitters) > 0 {
			if err := s.Write(appRecord("callback")); err != nil {
				return nil, err
			}
		}
		a := make([]bool, len(q.Emitters))
		for i := range a {
			a[i] = true
		}
		return a, nil
	}
	sink, r, o, acct := applicationTestHost(t, policy)
	s = sink
	program := filepath.Join(t.TempDir(), "app")
	appWrite(t, s, acct, program, "selected")
	target := wire.ApplicationLogTarget{Selection: wire.ApplicationLogSelectionOneApp, Program: program}
	p, err := r.ReadContext(context.Background(), target, "", 1, 512)
	if err != nil || len(p.Records) != 1 {
		t.Fatalf("lock held across callback: %+v %v", p, err)
	}
	p, err = r.ReadContext(context.Background(), target, p.Next, 1, 512)
	if err != nil || len(p.Records) != 0 || !p.AtEnd {
		t.Fatalf("consume excluded callback: %+v %v", p, err)
	}
	baseline := calls.Load()
	type reply struct {
		page wire.ApplicationLogPage
		err  error
	}
	result := make(chan reply, 1)
	go func() {
		page, err := o.ObserveContext(context.Background(), target, p.Next, 1, 512, 250)
		result <- reply{page, err}
	}()
	deadline := time.Now().Add(time.Second)
	for calls.Load() == baseline && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	deny.Store(true)
	refused := <-result
	if refused.err != nil || refused.page.Outcome != wire.HistoryPageOutcomeNotPermitted || len(refused.page.Records) != 0 || refused.page.Next != p.Next || refused.page.AtEnd {
		t.Fatalf("revoked empty observe: %+v, %v", refused.page, refused.err)
	}
}

func TestApplicationHistoryEmitterBatchCapacityAdvancesOnlyAcceptedPhysicalRows(t *testing.T) {
	policy := appPermit()
	policy.Fits = func(_ *identity.Peer, q ApplicationHistoryQuery) bool { return len(q.Emitters) <= 1 }
	s, r, _, acct := applicationTestHost(t, policy)
	a := filepath.Join(t.TempDir(), "a")
	b := filepath.Join(t.TempDir(), "b")
	appWrite(t, s, acct, a, "one")
	appWrite(t, s, acct, b, "two")
	target := wire.ApplicationLogTarget{Selection: wire.ApplicationLogSelectionAllApps}
	p, err := r.ReadContext(context.Background(), target, "", 256, 65536)
	if err != nil || len(p.Records) != 1 || p.AtEnd {
		t.Fatalf("capacity first: %+v %v", p, err)
	}
	p, err = r.ReadContext(context.Background(), target, p.Next, 256, 65536)
	if err != nil || len(p.Records) != 1 || p.Records[0].Msg != "two" || !p.AtEnd {
		t.Fatalf("capacity next: %+v %v", p, err)
	}
}

type forgedAppStampWriter struct{ listen.FrameClient }

func (w forgedAppStampWriter) WriteFrame(frame []byte) error {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(frame, &envelope); err != nil {
		return err
	}
	var args map[string]json.RawMessage
	if err := json.Unmarshal(envelope["arguments"], &args); err != nil {
		return err
	}
	var record map[string]json.RawMessage
	if err := json.Unmarshal(args["record"], &record); err != nil {
		return err
	}
	record["_oa_emitter"] = json.RawMessage(`{"version":1,"account":"forged-account","program":"forged-program"}`)
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	args["record"] = encoded
	encoded, err = json.Marshal(args)
	if err != nil {
		return err
	}
	envelope["arguments"] = encoded
	encoded, err = json.Marshal(envelope)
	if err != nil {
		return err
	}
	return w.FrameClient.WriteFrame(encoded)
}

func TestApplicationSinkStampComesOnlyFromNativeReceivingPeer(t *testing.T) {
	s, ep, _, account := applicationTestHostAt(t, appPermit())
	writer := wire.NewSinkClient(forgedAppStampWriter{listen.FrameClient{Endpoint: ep, Timeout: time.Second, MaxFrame: 1 << 20}})
	if err := writer.Write(wire.Record{Schema: 1, Time: time.Now().UTC().Format(time.RFC3339Nano), Msg: "forged reserved stamp", Attrs: map[string]string{"program": "forged-attribute"}, Identity: []wire.Attestation{{By: "claim", Verified: true, Exe: "forged-exe"}}}); err != nil {
		t.Fatal(err)
	}
	var rows []logging.HistoryRow
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		scan, err := s.ScanApplicationHistory("")
		if err != nil {
			t.Fatal(err)
		}
		rows = scan.Rows
		if len(rows) > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if len(rows) != 1 || rows[0].Emitter == nil || rows[0].InvalidEmitter || rows[0].Emitter.Account != account {
		t.Fatalf("receiving stamp: %+v", rows)
	}
	image, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if !identity.SameSubjectProgram(rows[0].Emitter.Program, image) {
		t.Fatalf("caller controls emitter: %+v", rows[0].Emitter)
	}
	if len(rows[0].Record.Identity) == 0 || rows[0].Record.Identity[0].Exe != "forged-exe" {
		t.Fatal("submitted claim was lost")
	}
}

func TestApplicationObserverDisconnectReleasesCapacity(t *testing.T) {
	_, ep, h, _ := applicationTestHostAt(t, appPermit())
	observer, err := client.NewApplicationObserver(ep).WithTimeout(35 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := observer.ObserveContext(ctx, wire.ApplicationLogTarget{Selection: wire.ApplicationLogSelectionAllApps}, "", 1, 512, 30000)
		done <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for len(h.observers) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(h.observers) != 1 {
		t.Fatal("observer did not wait")
	}
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for (len(h.observers) != 0 || len(h.calls) != 0) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(h.observers) != 0 || len(h.calls) != 0 {
		t.Fatalf("disconnected call retained slots: observers=%d calls=%d", len(h.observers), len(h.calls))
	}
}
