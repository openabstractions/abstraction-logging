package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/openabstractions/abstraction-facade/go-core/protected"
	identity "github.com/openabstractions/abstraction-identity"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

// ProtectedSourceID is the one source a protected page reads (LOG-N1).
const ProtectedSourceID = "windows/security"

// ProtectedSourceReads is the runtime's path to a protected native page
// (LOG-N1): the system-access question, the ticket and the helper's result.
type ProtectedSourceReads interface {
	// Covers reports whether sourceID has a trusted protected path now.
	Covers(sourceID string) bool
	// Read returns the helper's result for exactly this caller and encoded
	// request, once, or one of ErrOSAuthorizationRequired,
	// ErrOSAuthorizationPending, ErrProtectedRefused or ErrProtectedDenied.
	Read(ctx context.Context, peer *identity.Peer, request []byte) ([]byte, error)
}

// Error codes of a protected source read (LOG-N1). ServiceErrorCode is an
// open vocabulary; older readers keep the word.
const (
	CodeOSAuthorizationRequired = "os_authorization_required"
	CodeOSAuthorizationPending  = "os_authorization_pending"
)

var (
	// ErrOSAuthorizationRequired: the system-access question waits for the person.
	ErrOSAuthorizationRequired = errors.New("logging source: the person's approval and the administrator prompt are required")
	// ErrOSAuthorizationPending: the person said once; the helper's page has not arrived.
	ErrOSAuthorizationPending = errors.New("logging source: waiting for the administrator-approved read")
	// ErrProtectedRefused: the person refused this read.
	ErrProtectedRefused = errors.New("logging source: the person refused the administrator-approved read")
	// ErrProtectedDenied: the platform refused the read even elevated.
	ErrProtectedDenied = errors.New("logging source: native access denied even with administrator approval")
)

func protectedOutcome(err error) bool {
	return errors.Is(err, ErrOSAuthorizationRequired) || errors.Is(err, ErrOSAuthorizationPending) || errors.Is(err, ErrProtectedRefused) || errors.Is(err, ErrProtectedDenied)
}

// EnableProtectedSources installs the runtime's protected path before the
// host starts serving (LOG-N1). A nil path keeps native denials forbidden.
func (h *Host) EnableProtectedSources(p ProtectedSourceReads) error {
	h.lifecycle.Lock()
	defer h.lifecycle.Unlock()
	if h.serving || h.ctx.Err() != nil {
		return errors.New("logging service: configure protected sources before Serve")
	}
	h.protected = p
	return nil
}

// readProtected reads sourceID through the runtime's protected path after
// the unelevated read was denied. The helper's page passes the same checks,
// sanitizing and sealing as an adapter's page.
func (r *receiver) readProtected(ctx context.Context, peer *identity.Peer, sourceID, cursor string, maxRecords, maxBytes int64) (wire.SourcePage, error) {
	page, err := r.sources.readWith(ctx, sourceID, cursor, maxRecords, maxBytes, func(_ SourceAdapter, native string) (NativePage, error) {
		request, err := EncodeProtectedSourceRequest(ProtectedSourceRequest{Source: sourceID, Cursor: native, MaxRecords: maxRecords, MaxBytes: maxBytes})
		if err != nil {
			return NativePage{}, err
		}
		result, err := r.protected.Read(ctx, peer, request)
		if err != nil {
			return NativePage{}, err
		}
		return decodeProtectedPage(sourceID, result)
	})
	refuse := func(outcome wire.HistoryPageOutcome) (wire.SourcePage, error) {
		return wire.SourcePage{Outcome: outcome, Records: []wire.SourceRecord{}, Next: cursor}, nil
	}
	switch {
	case errors.Is(err, ErrOSAuthorizationRequired):
		return refuse(wire.HistoryPageOutcomeOSAuthorizationRequired)
	case errors.Is(err, ErrOSAuthorizationPending):
		return refuse(wire.HistoryPageOutcomeOSAuthorizationPending)
	case errors.Is(err, ErrProtectedRefused):
		return refuse(wire.HistoryPageOutcomeForbidden)
	case errors.Is(err, ErrProtectedDenied):
		return refuse(wire.HistoryPageOutcomeForbidden)
	}
	return page, err
}

// ProtectedSourceRequest is the frozen request of one protected page: the
// source, the opened native cursor and the caller's bounds (LOG-N1). The
// runtime freezes it when it admits the system-access question and the
// helper receives exactly it.
type ProtectedSourceRequest struct {
	Source     string `json:"source"`
	Cursor     string `json:"cursor"`
	MaxRecords int64  `json:"max_records"`
	MaxBytes   int64  `json:"max_bytes"`
}

var errProtectedRequest = errors.New("logging source: protected request outside its grammar")

func (r ProtectedSourceRequest) valid() bool {
	if r.Source != ProtectedSourceID || len(r.Cursor) > maxSourceCursorBytes/2 || r.MaxRecords < 1 || r.MaxRecords > 256 || r.MaxBytes < 1 || r.MaxBytes > 65536 {
		return false
	}
	for _, c := range []byte(r.Cursor) {
		if c < 0x21 || c > 0x7e {
			return false
		}
	}
	return true
}

// EncodeProtectedSourceRequest is the request's one spelling; the same
// request always encodes to the same bytes, which bind the ticket.
func EncodeProtectedSourceRequest(r ProtectedSourceRequest) ([]byte, error) {
	if !r.valid() {
		return nil, errProtectedRequest
	}
	return json.Marshal(r)
}

// DecodeProtectedSourceRequest reads a request strictly: no unknown field,
// no trailing data, and only the grammar EncodeProtectedSourceRequest writes.
func DecodeProtectedSourceRequest(data []byte) (ProtectedSourceRequest, error) {
	var r ProtectedSourceRequest
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil || d.More() || !r.valid() {
		return ProtectedSourceRequest{}, errProtectedRequest
	}
	again, err := json.Marshal(r)
	if err != nil || !bytes.Equal(again, data) {
		return ProtectedSourceRequest{}, errProtectedRequest
	}
	return r, nil
}

// RunProtectedSourceRead is the helper's act for
// protected.OperationSecurityLogPage (FAC-H1, LOG-N1): the platform's own
// adapter for the Security channel, with its fixed arguments, the request's
// opened cursor and bounds. A native refusal is protected.FailureDenied.
func RunProtectedSourceRead(ctx context.Context, request []byte) ([]byte, string) {
	return runProtectedSourceRead(ctx, request, nativePlatformSourceAdapters(protectedCommandRunner{}))
}

func runProtectedSourceRead(ctx context.Context, request []byte, adapters map[string]SourceAdapter) ([]byte, string) {
	r, err := DecodeProtectedSourceRequest(request)
	if err != nil {
		return nil, protected.FailureFailed
	}
	adapter := adapters[r.Source]
	if adapter == nil {
		return nil, protected.FailureFailed
	}
	page, err := adapter.Read(ctx, r.Cursor, r.MaxRecords, r.MaxBytes)
	if errors.Is(err, ErrSourcePermissionDenied) {
		return nil, protected.FailureDenied
	}
	if err != nil {
		return nil, protected.FailureFailed
	}
	result, err := encodeProtectedPage(r.Source, page)
	if err != nil {
		return nil, protected.FailureFailed
	}
	return result, ""
}

// encodeProtectedPage carries an adapter's page in the source page's own
// codec; Next stays the native continuation, sealed only by the runtime.
func encodeProtectedPage(source string, page NativePage) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, err = nil, fmt.Errorf("logging source: protected page encoding: %v", r)
		}
	}()
	result := wire.SourcePage{Outcome: historyOutcome(page.Outcome), Records: make([]wire.SourceRecord, 0, len(page.Entries)), Next: page.Next, AtEnd: page.AtEnd}
	for _, entry := range page.Entries {
		result.Records = append(result.Records, wire.SourceRecord{SourceID: source, Record: entry.Record, Producer: entry.Producer})
	}
	return wire.EncodeSourcePage(&result), nil
}

// decodeProtectedPage reads the helper's result back into an adapter page
// for the request's source. SourceService then checks it as it checks any
// adapter's page.
func decodeProtectedPage(source string, data []byte) (NativePage, error) {
	page, err := wire.DecodeSourcePage(data)
	if err != nil {
		return NativePage{}, err
	}
	outcome, ok := providerOutcome(page.Outcome)
	if !ok {
		return NativePage{}, errors.New("logging source: protected page has a receiving policy outcome")
	}
	out := NativePage{Outcome: outcome, Next: page.Next, AtEnd: page.AtEnd, Entries: make([]SourceEntry, 0, len(page.Records))}
	for _, record := range page.Records {
		if record.SourceID != source {
			return NativePage{}, errors.New("logging source: protected page names another source")
		}
		out.Entries = append(out.Entries, SourceEntry{Record: record.Record, Producer: record.Producer})
	}
	return out, nil
}
