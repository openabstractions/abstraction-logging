package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	identity "github.com/openabstractions/abstraction-identity"
	"github.com/openabstractions/abstraction-identity/listen"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	loggingclient "github.com/openabstractions/abstraction-logging/go/client"
)

type fakeProtected struct {
	covers   bool
	mu       sync.Mutex
	requests []ProtectedSourceRequest
	result   []byte
	err      error
	during   func()
}

func (f *fakeProtected) Covers(sourceID string) bool {
	return f.covers && sourceID == ProtectedSourceID
}

func (f *fakeProtected) Read(_ context.Context, peer *identity.Peer, request []byte) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, err := DecodeProtectedSourceRequest(request)
	if err != nil || peer == nil {
		return nil, fmt.Errorf("fake: %v", err)
	}
	f.requests = append(f.requests, r)
	if f.during != nil {
		f.during()
	}
	return f.result, f.err
}

func (f *fakeProtected) last(t *testing.T) ProtectedSourceRequest {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) == 0 {
		t.Fatal("the protected path was not asked")
	}
	return f.requests[len(f.requests)-1]
}

// protectedHost serves a denied Security fixture with p as its protected path
// and a source policy that refuses while refuse is set.
func protectedHost(t *testing.T, p ProtectedSourceReads, refuse *atomic.Bool) *loggingclient.SourceReader {
	t.Helper()
	if runtime.GOOS == "darwin" {
		t.Skip("current Program proof limitation prevents the native IPC test")
	}
	fixture := &sourceFixture{id: ProtectedSourceID, denied: true}
	endpoint := listen.Endpoint(fmt.Sprintf("lp-%d-%d", os.Getpid(), protectedSerial.Add(1)))
	host, err := Listen(endpoint, discardSink{})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.EnableSourceReader([]SourceAdapter{fixture}, func(context.Context, *identity.Peer, string, string) error {
		if refuse.Load() {
			return errors.New("source refused")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if p != nil {
		if err := host.EnableProtectedSources(p); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- host.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("source host did not stop")
		}
	})
	if err := host.EnableProtectedSources(p); err == nil {
		time.Sleep(50 * time.Millisecond)
		if err := host.EnableProtectedSources(p); err == nil {
			t.Error("protected sources changed while serving")
		}
	}
	return loggingclient.NewSourceReaderWithTransport(listen.FrameClient{Endpoint: endpoint, Timeout: 2 * time.Second, MaxFrame: 1 << 20})
}

var protectedSerial atomic.Int64

func serviceCode(err error) (string, string) {
	var refusal *wire.ServiceError
	if errors.As(err, &refusal) {
		return string(refusal.Code), refusal.Message
	}
	return "", ""
}

// TestProtectedSourceRead is LOG-N1's runtime half: a denied permitted read
// asks the protected path for the exact request, reports its waiting states,
// and returns the helper's page checked, sanitized and sealed like any page.
func TestProtectedSourceRead(t *testing.T) {
	var refuse atomic.Bool
	p := &fakeProtected{covers: true, err: ErrOSAuthorizationRequired}
	client := protectedHost(t, p, &refuse)
	ctx := context.Background()
	page, err := client.ReadContext(ctx, ProtectedSourceID, "", 4, 4096)
	if err != nil || page.Outcome != wire.HistoryPageOutcomeOSAuthorizationRequired || page.Next != "" || len(page.Records) != 0 {
		t.Fatalf("first read: %+v, %v", page, err)
	}
	if got := p.last(t); got != (ProtectedSourceRequest{Source: ProtectedSourceID, MaxRecords: 4, MaxBytes: 4096}) {
		t.Fatalf("request %+v", got)
	}
	p.err = ErrOSAuthorizationPending
	if page, err := client.ReadContext(ctx, ProtectedSourceID, "", 4, 4096); err != nil || page.Outcome != wire.HistoryPageOutcomeOSAuthorizationPending || page.Next != "" || len(page.Records) != 0 {
		t.Fatalf("pending read: %+v, %v", page, err)
	}
	native := securityPage()
	native.Entries[0].Record.Attrs = map[string]string{"TargetUserName": "reinis", "password": "hunter2"}
	p.err = nil
	if p.result, err = encodeProtectedPage(ProtectedSourceID, native); err != nil {
		t.Fatal(err)
	}
	page, err = client.ReadContext(ctx, ProtectedSourceID, "", 4, 4096)
	if err != nil || page.Outcome != wire.HistoryPageOutcomePage || len(page.Records) != 1 {
		t.Fatalf("delivered page %+v %v", page, err)
	}
	if attrs := page.Records[0].Record.Attrs; attrs["password"] != "" || attrs["TargetUserName"] != "reinis" {
		t.Fatalf("the helper's page was not sanitized: %v", attrs)
	}
	if page.Next == "" || page.Next == native.Next || strings.Contains(page.Next, "42:") {
		t.Fatalf("the native continuation left unsealed: %q", page.Next)
	}
	p.err = ErrOSAuthorizationRequired
	if next, err := client.ReadContext(ctx, ProtectedSourceID, page.Next, 4, 4096); err != nil || next.Outcome != wire.HistoryPageOutcomeOSAuthorizationRequired || next.Next != page.Next || len(next.Records) != 0 {
		t.Fatalf("next page: %+v, %v", next, err)
	}
	if got := p.last(t); got.Cursor != native.Next {
		t.Fatalf("the helper would read from %q, want the opened native cursor %q", got.Cursor, native.Next)
	}
	p.err = nil
	p.during = func() { refuse.Store(true) }
	page, err = client.ReadContext(ctx, ProtectedSourceID, "", 4, 4096)
	if err != nil || page.Outcome != wire.HistoryPageOutcomeNotPermitted || len(page.Records) != 0 || page.Next != "" {
		t.Fatalf("a revoked source right still returned the page: %+v, %v", page, err)
	}
}

// TestProtectedSourceRefusals: no trusted helper keeps the denial; the
// person's refusal and an elevated denial are forbidden; a refused source
// right never reaches the protected path; a malformed helper page is not
// returned.
func TestProtectedSourceRefusals(t *testing.T) {
	ctx := context.Background()
	t.Run("no trusted helper", func(t *testing.T) {
		var refuse atomic.Bool
		p := &fakeProtected{covers: false, err: ErrOSAuthorizationRequired}
		client := protectedHost(t, p, &refuse)
		page, err := client.ReadContext(ctx, ProtectedSourceID, "", 1, 65536)
		if err != nil || page.Outcome != wire.HistoryPageOutcomeForbidden || len(page.Records) != 0 || page.Next != "" {
			t.Fatalf("%+v, %v", page, err)
		}
		if len(p.requests) != 0 {
			t.Fatal("an uncovered source asked the protected path")
		}
		nilClient := protectedHost(t, nil, &refuse)
		if page, err := nilClient.ReadContext(ctx, ProtectedSourceID, "", 1, 65536); err != nil || page.Outcome != wire.HistoryPageOutcomeForbidden {
			t.Fatalf("no protected path: %+v, %v", page, err)
		}
	})
	for name, cause := range map[string]error{"refused": ErrProtectedRefused, "denied elevated": ErrProtectedDenied} {
		t.Run(name, func(t *testing.T) {
			var refuse atomic.Bool
			client := protectedHost(t, &fakeProtected{covers: true, err: cause}, &refuse)
			page, err := client.ReadContext(ctx, ProtectedSourceID, "", 1, 65536)
			if err != nil || page.Outcome != wire.HistoryPageOutcomeForbidden || len(page.Records) != 0 || page.Next != "" {
				t.Fatalf("%+v, %v", page, err)
			}
		})
	}
	t.Run("source right refused first", func(t *testing.T) {
		var refuse atomic.Bool
		refuse.Store(true)
		p := &fakeProtected{covers: true, err: ErrOSAuthorizationRequired}
		client := protectedHost(t, p, &refuse)
		if page, err := client.ReadContext(ctx, ProtectedSourceID, "", 1, 65536); err != nil || page.Outcome != wire.HistoryPageOutcomeNotPermitted {
			t.Fatalf("%+v, %v", page, err)
		}
		if len(p.requests) != 0 {
			t.Fatal("a caller without the source right reached the protected path")
		}
	})
	t.Run("malformed helper pages", func(t *testing.T) {
		var refuse atomic.Bool
		p := &fakeProtected{covers: true}
		client := protectedHost(t, p, &refuse)
		other, _ := encodeProtectedPage("windows/system", securityPage())
		many := securityPage()
		many.Entries = append(many.Entries, many.Entries[0], many.Entries[0])
		tooMany, _ := encodeProtectedPage(ProtectedSourceID, many)
		attested := securityPage()
		attested.Entries[0].Record.Identity = []wire.Attestation{{By: "identity/windows", Verified: true, Hop: 1}}
		forged, _ := encodeProtectedPage(ProtectedSourceID, attested)
		for name, want := range map[string]struct {
			result  []byte
			outcome wire.HistoryPageOutcome
		}{
			"another source": {other, wire.HistoryPageOutcomeUnavailable},
			"not a page":     {[]byte("not json"), wire.HistoryPageOutcomeUnavailable},
			"too many":       {tooMany, wire.HistoryPageOutcomeUnavailable},
			"attested row":   {forged, wire.HistoryPageOutcomeCorrupt},
		} {
			p.result = want.result
			page, err := client.ReadContext(ctx, ProtectedSourceID, "", 2, 65536)
			if err != nil || page.Outcome != want.outcome || len(page.Records) != 0 {
				t.Fatalf("%s: %+v %v", name, page, err)
			}
		}
	})
}
