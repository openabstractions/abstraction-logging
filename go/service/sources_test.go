package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	identity "github.com/openabstractions/abstraction-identity"
	"github.com/openabstractions/abstraction-identity/listen"
	logging "github.com/openabstractions/abstraction-logging/go"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	loggingclient "github.com/openabstractions/abstraction-logging/go/client"
)

type sourceFixture struct {
	id        string
	describe  atomic.Int32
	read      atomic.Int32
	denied    bool
	identity  bool
	entries   []SourceEntry
	readErr   error
	readWait  chan struct{}
	readGo    chan struct{}
	endCursor string
	endErr    error
	endCalls  atomic.Int32
	endWait   chan struct{}
	endGo     chan struct{}
}

func (f *sourceFixture) ID() string { return f.id }

func (f *sourceFixture) Describe(context.Context) (wire.SourceDescriptor, error) {
	f.describe.Add(1)
	if f.denied {
		return wire.SourceDescriptor{}, ErrSourcePermissionDenied
	}
	return wire.SourceDescriptor{ID: f.id, Availability: wire.SourceAvailabilityAvailable, Limitations: []wire.SourceLimitation{wire.SourceLimitationRetention, wire.SourceLimitationNativeRedaction}}, nil
}

func (f *sourceFixture) Read(ctx context.Context, cursor string, maxRecords, maxBytes int64) (NativePage, error) {
	f.read.Add(1)
	if f.readWait != nil {
		select {
		case f.readWait <- struct{}{}:
		case <-ctx.Done():
			return NativePage{}, ctx.Err()
		}
		select {
		case <-f.readGo:
		case <-ctx.Done():
			return NativePage{}, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return NativePage{}, err
	}
	if f.denied {
		return NativePage{}, ErrSourcePermissionDenied
	}
	if f.readErr != nil {
		return NativePage{}, f.readErr
	}
	if cursor != "" {
		return NativePage{Outcome: wire.PageOutcomePage, Next: cursor, AtEnd: true}, nil
	}
	entries := append([]SourceEntry(nil), f.entries...)
	if f.identity && len(entries) > 0 {
		entries[0].Record.Identity = []wire.Attestation{{By: "identity/windows", Verified: true, Hop: 1}}
	}
	return NativePage{Outcome: wire.PageOutcomePage, Entries: entries, Next: "fixture:1", AtEnd: true}, nil
}

func (f *sourceFixture) CurrentCursor(ctx context.Context) (string, error) {
	f.endCalls.Add(1)
	if f.endWait != nil {
		select {
		case f.endWait <- struct{}{}:
		case <-ctx.Done():
			return "", ctx.Err()
		}
		select {
		case <-f.endGo:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return f.endCursor, f.endErr
}

type sourceWithoutCurrentPosition struct{ SourceAdapter }

func TestSourceServiceMapsNativeReadFailuresWithoutHidingPermissionOrCancellation(t *testing.T) {
	fixture := &sourceFixture{id: "linux/system-journal", readErr: errors.New("native query failed")}
	service, err := NewSourceService(fixture)
	if err != nil {
		t.Fatal(err)
	}
	page, err := service.Read(context.Background(), fixture.id, "", 10, 65536)
	if err != nil || page.Outcome != wire.HistoryPageOutcomeUnavailable || len(page.Records) != 0 || page.Next != "" {
		t.Fatalf("native failure page %+v, %v", page, err)
	}
	fixture.readErr = ErrSourcePermissionDenied
	if _, err := service.Read(context.Background(), fixture.id, "", 10, 65536); !errors.Is(err, ErrSourcePermissionDenied) {
		t.Fatalf("permission failure = %v", err)
	}
	fixture.readErr = context.Canceled
	if _, err := service.Read(context.Background(), fixture.id, "", 10, 65536); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled adapter read = %v", err)
	}
}

func TestSourceStartAtEndReturnsSealedEmptyOrNativeBaseline(t *testing.T) {
	fixture := &sourceFixture{id: "linux/system-journal", endCursor: "journal:latest"}
	service, err := NewSourceService(fixture)
	if err != nil {
		t.Fatal(err)
	}
	page, err := service.StartAtEnd(context.Background(), fixture.id)
	if err != nil || page.Outcome != wire.HistoryPageOutcomePage || len(page.Records) != 0 || !page.AtEnd || page.Next == "" {
		t.Fatalf("start at nonempty end: %+v, %v", page, err)
	}
	if native, source := service.open(page.Next); source != fixture.id || native != "journal:latest" {
		t.Fatalf("baseline cursor opened as %q/%q", source, native)
	}
	restarted, err := NewSourceService(fixture)
	if err != nil {
		t.Fatal(err)
	}
	gap, err := restarted.Read(context.Background(), fixture.id, page.Next, 1, 65536)
	if err != nil || gap.Outcome != wire.HistoryPageOutcomeGap || gap.Next != page.Next || len(gap.Records) != 0 {
		t.Fatalf("reader restart did not preserve a gap: %+v, %v", gap, err)
	}
	fixture.endCursor = ""
	empty, err := service.StartAtEnd(context.Background(), fixture.id)
	if err != nil || empty.Outcome != wire.HistoryPageOutcomePage || len(empty.Records) != 0 || !empty.AtEnd || empty.Next == "" {
		t.Fatalf("start at empty end: %+v, %v", empty, err)
	}
	if native, source := service.open(empty.Next); source != fixture.id || native != "" {
		t.Fatalf("empty baseline cursor opened as %q/%q", source, native)
	}
	fixture.endErr = errors.New("native query failed")
	unavailable, err := service.StartAtEnd(context.Background(), fixture.id)
	if err != nil || unavailable.Outcome != wire.HistoryPageOutcomeUnavailable || unavailable.Next != "" || len(unavailable.Records) != 0 {
		t.Fatalf("current-position failure: %+v, %v", unavailable, err)
	}
	unsupportedService, err := NewSourceService(sourceWithoutCurrentPosition{SourceAdapter: fixture})
	if err != nil {
		t.Fatal(err)
	}
	unsupported, err := unsupportedService.StartAtEnd(context.Background(), fixture.id)
	if err != nil || unsupported.Outcome != wire.HistoryPageOutcomeUnsupported || len(unsupported.Records) != 0 {
		t.Fatalf("missing optional current-position operation: %+v, %v", unsupported, err)
	}
}

func TestSourceServiceBindsCursorAndBoundsRecordProvenance(t *testing.T) {
	first := &sourceFixture{id: "linux/system-journal", entries: []SourceEntry{{
		Record: wire.Record{Schema: 1, Time: "2026-09-29T10:11:12.000000Z", Level: 8, Msg: "fixture event", Attrs: map[string]string{
			"SYSLOG_IDENTIFIER": "fixture", "PASSWORD": "drop", "db.secret": "drop", "_PID": "42",
			"API_KEY": "drop", "Set-Cookie": "drop", "Cookie": "drop",
		}},
		Producer: &wire.ProducerClaim{Provider: "fixture-provider", Program: "fixture", PID: 42},
	}}}
	second := &sourceFixture{id: "linux/user-journal"}
	service, err := NewSourceService(first, second)
	if err != nil {
		t.Fatal(err)
	}
	if got := service.IDs(); len(got) != 2 || got[0] != first.id || got[1] != second.id {
		t.Fatalf("catalog IDs %v", got)
	}
	page, err := service.Read(context.Background(), first.id, "", 1, 65536)
	if err != nil || page.Outcome != wire.HistoryPageOutcomePage || len(page.Records) != 1 || page.Records[0].SourceID != first.id || page.Records[0].Record.Identity != nil {
		t.Fatalf("first page %+v, %v", page, err)
	}
	if page.Records[0].Producer == nil || page.Records[0].Producer.Program != "fixture" {
		t.Fatalf("producer claim lost: %+v", page.Records[0])
	}
	attrs := page.Records[0].Record.Attrs
	if attrs["PASSWORD"] != "" || attrs["db.secret"] != "" || attrs["API_KEY"] != "" || attrs["Set-Cookie"] != "" || attrs["Cookie"] != "" || attrs["_PID"] != "42" || attrs["SYSLOG_IDENTIFIER"] != "fixture" {
		t.Fatalf("structured secret fields not filtered: %#v", attrs)
	}
	cross, err := service.Read(context.Background(), second.id, page.Next, 1, 65536)
	if err != nil || cross.Outcome != wire.HistoryPageOutcomeGap || cross.Next != page.Next || len(cross.Records) != 0 {
		t.Fatalf("cross-source cursor: %+v, %v", cross, err)
	}
	resumed, err := service.Read(context.Background(), first.id, page.Next, 1, 65536)
	if err != nil || resumed.Outcome != wire.HistoryPageOutcomePage || !resumed.AtEnd || len(resumed.Records) != 0 {
		t.Fatalf("resume: %+v, %v", resumed, err)
	}
	restarted, err := NewSourceService(first, second)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := restarted.Read(context.Background(), first.id, page.Next, 1, 65536)
	if err != nil || stale.Outcome != wire.HistoryPageOutcomeGap || stale.Next != page.Next {
		t.Fatalf("restart cursor: %+v, %v", stale, err)
	}
	invalid, err := service.Read(context.Background(), first.id, "", 0, 65536)
	if err != nil || invalid.Outcome != wire.HistoryPageOutcomeInvalid || invalid.Next != "" {
		t.Fatalf("limits: %+v, %v", invalid, err)
	}
}

func TestSourceServiceRefusesAttestedRowsAndOversizedEntries(t *testing.T) {
	for _, fixture := range []*sourceFixture{
		{id: "windows/application", identity: true, entries: []SourceEntry{{Record: wire.Record{Schema: 1, Time: "2026-09-29T10:11:12.000000Z", Msg: "fixture"}}}},
		{id: "windows/system", entries: []SourceEntry{{Record: wire.Record{Schema: 1, Time: "2026-09-29T10:11:12.000000Z", Msg: "too large"}}}},
	} {
		service, err := NewSourceService(fixture)
		if err != nil {
			t.Fatal(err)
		}
		maxBytes := int64(65536)
		if fixture.id == "windows/system" {
			maxBytes = 1
		}
		page, err := service.Read(context.Background(), fixture.id, "", 1, maxBytes)
		if err != nil {
			t.Fatal(err)
		}
		want := wire.HistoryPageOutcomeCorrupt
		if fixture.id == "windows/system" {
			want = wire.HistoryPageOutcomeRecordTooLarge
		}
		if page.Outcome != want || len(page.Records) != 0 || page.Next != "" {
			t.Fatalf("%s refusal %+v, want %v", fixture.id, page, want)
		}
	}
}

func TestSourceReaderRightsBeforeProbeAndRead(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("current Program proof limitation prevents the native IPC test")
	}
	fixture := &sourceFixture{id: "windows/application", entries: []SourceEntry{{
		Record: wire.Record{Schema: 1, Time: "2026-09-29T10:11:12.000000Z", Msg: "fixture event"},
	}}}
	endpoint := listen.Endpoint(fmt.Sprintf("ls-%d", os.Getpid()))
	host, err := Listen(endpoint, discardSink{})
	if err != nil {
		t.Fatal(err)
	}
	var denied atomic.Bool
	var resources atomic.Int32
	if err := host.EnableSourceReader([]SourceAdapter{fixture}, func(_ context.Context, _ *identity.Peer, action, resource string) error {
		resources.Add(1)
		if action != SourceReadAction || resource != SourceReadResource(fixture.id) {
			t.Errorf("wrong source permission pair %q/%q", action, resource)
		}
		if denied.Load() {
			return errors.New("source denied")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !host.SourceReaderAvailable() {
		t.Fatal("configured source reader is not ready")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- host.Serve(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("source host did not stop")
		}
	}()
	client := loggingclient.NewSourceReaderWithTransport(listen.FrameClient{Endpoint: endpoint, Timeout: time.Second, MaxFrame: 1 << 20})
	denied.Store(true)
	catalog, err := client.SourcesContext(context.Background())
	if err != nil || len(catalog.Sources) != 1 || catalog.Sources[0].Availability != wire.SourceAvailabilityPermissionDenied {
		t.Fatalf("denied catalog %+v, %v", catalog, err)
	}
	if fixture.describe.Load() != 0 || fixture.read.Load() != 0 {
		t.Fatalf("native adapter called before source authorization: describe=%d read=%d", fixture.describe.Load(), fixture.read.Load())
	}
	refused, err := client.ReadContext(context.Background(), fixture.id, "", 1, 65536)
	if err != nil || refused.Outcome != wire.HistoryPageOutcomeNotPermitted || len(refused.Records) != 0 || refused.Next != "" || refused.AtEnd {
		t.Fatalf("denied source read: %+v, %v", refused, err)
	}
	denied.Store(false)
	catalog, err = client.SourcesContext(context.Background())
	if err != nil || len(catalog.Sources) != 1 || catalog.Sources[0].Availability != wire.SourceAvailabilityAvailable {
		t.Fatalf("allowed catalog %+v, %v", catalog, err)
	}
	page, err := client.ReadContext(context.Background(), fixture.id, "", 1, 65536)
	if err != nil || page.Outcome != wire.HistoryPageOutcomePage || len(page.Records) != 1 || page.Records[0].Record.Identity != nil {
		t.Fatalf("allowed page %+v, %v", page, err)
	}
	if fixture.describe.Load() != 1 || fixture.read.Load() != 1 || resources.Load() != 5 {
		t.Fatalf("calls: describe=%d read=%d policy=%d", fixture.describe.Load(), fixture.read.Load(), resources.Load())
	}
}

func TestSourceReaderChecksStartAtEndRightsBeforeAndAfterProbe(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("current Program proof limitation prevents the native IPC test")
	}
	fixture := &sourceFixture{id: "windows/application", endCursor: "fixture:current", endWait: make(chan struct{}, 1), endGo: make(chan struct{})}
	endpoint := listen.Endpoint(fmt.Sprintf("ls-start-end-%d", os.Getpid()))
	host, err := Listen(endpoint, discardSink{})
	if err != nil {
		t.Fatal(err)
	}
	var denied atomic.Bool
	if err := host.EnableSourceReader([]SourceAdapter{fixture}, func(context.Context, *identity.Peer, string, string) error {
		if denied.Load() {
			return errors.New("revoked")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- host.Serve(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("source host did not stop")
		}
	}()
	client := loggingclient.NewSourceReaderWithTransport(listen.FrameClient{Endpoint: endpoint, Timeout: time.Second, MaxFrame: 1 << 20})
	denied.Store(true)
	refused, err := client.StartAtEndContext(context.Background(), fixture.id)
	if err != nil || refused.Outcome != wire.HistoryPageOutcomeNotPermitted || len(refused.Records) != 0 || refused.Next != "" || fixture.endCalls.Load() != 0 {
		t.Fatalf("denied start-at-end: %+v, %v; probes=%d", refused, err, fixture.endCalls.Load())
	}
	denied.Store(false)
	type reply struct {
		page wire.SourcePage
		err  error
	}
	result := make(chan reply, 1)
	go func() {
		page, err := client.StartAtEndContext(context.Background(), fixture.id)
		result <- reply{page, err}
	}()
	select {
	case <-fixture.endWait:
	case <-time.After(2 * time.Second):
		t.Fatal("current-position adapter did not block")
	}
	denied.Store(true)
	close(fixture.endGo)
	select {
	case refused := <-result:
		if refused.err != nil || refused.page.Outcome != wire.HistoryPageOutcomeNotPermitted || len(refused.page.Records) != 0 || refused.page.Next != "" || refused.page.AtEnd {
			t.Fatalf("revoked current-end returned %+v, %v", refused.page, refused.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked current-position query did not finish")
	}
}

func TestSourceReaderRechecksRightsAfterBlockedRead(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("current Program proof limitation prevents the native IPC test")
	}
	fixture := &sourceFixture{id: "windows/application", readWait: make(chan struct{}, 1), readGo: make(chan struct{}), entries: []SourceEntry{{
		Record: wire.Record{Schema: 1, Time: "2026-09-29T10:11:12.000000Z", Msg: "fixture event"},
	}}}
	endpoint := listen.Endpoint(fmt.Sprintf("ls-revoke-%d", os.Getpid()))
	host, err := Listen(endpoint, discardSink{})
	if err != nil {
		t.Fatal(err)
	}
	var denied atomic.Bool
	if err := host.EnableSourceReader([]SourceAdapter{fixture}, func(context.Context, *identity.Peer, string, string) error {
		if denied.Load() {
			return errors.New("revoked")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- host.Serve(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("source host did not stop")
		}
	}()
	client := loggingclient.NewSourceReaderWithTransport(listen.FrameClient{Endpoint: endpoint, Timeout: time.Second, MaxFrame: 1 << 20})
	type reply struct {
		page wire.SourcePage
		err  error
	}
	result := make(chan reply, 1)
	go func() {
		page, err := client.ReadContext(context.Background(), fixture.id, "", 1, 65536)
		result <- reply{page, err}
	}()
	select {
	case <-fixture.readWait:
	case <-time.After(2 * time.Second):
		t.Fatal("adapter read did not block")
	}
	denied.Store(true)
	close(fixture.readGo)
	select {
	case refused := <-result:
		if refused.err != nil || refused.page.Outcome != wire.HistoryPageOutcomeNotPermitted || len(refused.page.Records) != 0 || refused.page.Next != "" || refused.page.AtEnd {
			t.Fatalf("revoked read returned %+v, %v", refused.page, refused.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked read did not finish")
	}
}

type discardSink struct{}

func (discardSink) Write(logging.Record) error { return nil }
