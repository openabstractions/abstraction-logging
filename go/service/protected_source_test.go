package service

import (
	"context"
	"errors"
	"testing"

	"github.com/openabstractions/abstraction-facade/go-core/protected"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

type fixedAdapter struct {
	page NativePage
	err  error
	got  []any
}

func (a *fixedAdapter) ID() string { return ProtectedSourceID }
func (a *fixedAdapter) Describe(context.Context) (wire.SourceDescriptor, error) {
	return wire.SourceDescriptor{ID: ProtectedSourceID, Availability: wire.SourceAvailabilityAvailable}, nil
}
func (a *fixedAdapter) Read(_ context.Context, cursor string, maxRecords, maxBytes int64) (NativePage, error) {
	a.got = []any{cursor, maxRecords, maxBytes}
	return a.page, a.err
}

func securityPage() NativePage {
	return NativePage{Outcome: wire.PageOutcomePage, Next: "42:abcd", AtEnd: true, Entries: []SourceEntry{{
		Record:   wire.Record{Schema: 1, Time: "2026-10-07T12:00:00.000000Z", Level: 0, Msg: "An account was successfully logged on."},
		Producer: &wire.ProducerClaim{Provider: "Microsoft-Windows-Security-Auditing", PID: 4, UID: -1, GID: -1},
	}}}
}

// TestProtectedSourceAct is the helper's half of LOG-N1: the exact request
// reaches the platform adapter, and its page, refusal or failure comes back.
func TestProtectedSourceAct(t *testing.T) {
	request, err := EncodeProtectedSourceRequest(ProtectedSourceRequest{Source: ProtectedSourceID, Cursor: "41:abcd", MaxRecords: 16, MaxBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	adapter := &fixedAdapter{page: securityPage()}
	adapters := map[string]SourceAdapter{ProtectedSourceID: adapter}
	result, failure := runProtectedSourceRead(context.Background(), request, adapters)
	if failure != "" {
		t.Fatalf("failure %q", failure)
	}
	if adapter.got[0] != "41:abcd" || adapter.got[1] != int64(16) || adapter.got[2] != int64(4096) {
		t.Fatalf("the adapter read %v", adapter.got)
	}
	page, err := decodeProtectedPage(ProtectedSourceID, result)
	if err != nil {
		t.Fatal(err)
	}
	want := securityPage()
	if page.Outcome != want.Outcome || page.Next != want.Next || !page.AtEnd || len(page.Entries) != 1 || page.Entries[0].Record.Msg != want.Entries[0].Record.Msg || page.Entries[0].Producer.Provider != want.Entries[0].Producer.Provider {
		t.Fatalf("round trip: %+v", page)
	}
	if _, err := decodeProtectedPage("windows/system", result); err == nil {
		t.Fatal("a page for another source was accepted")
	}
	adapter.err = ErrSourcePermissionDenied
	if result, failure := runProtectedSourceRead(context.Background(), request, adapters); failure != protected.FailureDenied || result != nil {
		t.Fatalf("native denial: %q %q", result, failure)
	}
	adapter.err = errors.New("wevtutil failed")
	if _, failure := runProtectedSourceRead(context.Background(), request, adapters); failure != protected.FailureFailed {
		t.Fatalf("failure: %q", failure)
	}
	if _, failure := runProtectedSourceRead(context.Background(), request, map[string]SourceAdapter{}); failure != protected.FailureFailed {
		t.Fatalf("no adapter on this platform: %q", failure)
	}
}

// TestProtectedSourceRequestGrammar: the helper reads only the one spelling
// of a Security request inside the source reader's bounds.
func TestProtectedSourceRequestGrammar(t *testing.T) {
	good := ProtectedSourceRequest{Source: ProtectedSourceID, Cursor: "", MaxRecords: 256, MaxBytes: 65536}
	data, err := EncodeProtectedSourceRequest(good)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeProtectedSourceRequest(data); err != nil || got != good {
		t.Fatalf("%+v %v", got, err)
	}
	for _, bad := range []ProtectedSourceRequest{
		{Source: "windows/system", MaxRecords: 1, MaxBytes: 1},
		{Source: ProtectedSourceID, MaxRecords: 0, MaxBytes: 1},
		{Source: ProtectedSourceID, MaxRecords: 257, MaxBytes: 1},
		{Source: ProtectedSourceID, MaxRecords: 1, MaxBytes: 65537},
		{Source: ProtectedSourceID, Cursor: "a b", MaxRecords: 1, MaxBytes: 1},
		{Source: ProtectedSourceID, Cursor: string(make([]byte, maxSourceCursorBytes/2+1)), MaxRecords: 1, MaxBytes: 1},
	} {
		if _, err := EncodeProtectedSourceRequest(bad); err == nil {
			t.Fatalf("encoded %+v", bad)
		}
	}
	for _, raw := range []string{
		`{"source":"windows/security","cursor":"","max_records":1,"max_bytes":1,"query":"*"}`,
		`{"source":"windows/security","cursor":"","max_records":1,"max_bytes":1} {}`,
		`{"max_bytes":1,"max_records":1,"cursor":"","source":"windows/security"}`,
		`{"source":"windows/security", "cursor":"","max_records":1,"max_bytes":1}`,
		`{"source":"windows/security","cursor":"","max_records":1}`,
		`{"source":"\\\\host\\Security","cursor":"","max_records":1,"max_bytes":1}`,
	} {
		if _, err := DecodeProtectedSourceRequest([]byte(raw)); err == nil {
			t.Fatalf("decoded %s", raw)
		}
		if _, failure := runProtectedSourceRead(context.Background(), []byte(raw), map[string]SourceAdapter{ProtectedSourceID: &fixedAdapter{page: securityPage()}}); failure != protected.FailureFailed {
			t.Fatalf("acted on %s", raw)
		}
	}
}
