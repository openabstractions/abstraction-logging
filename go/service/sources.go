package service

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	identity "github.com/openabstractions/abstraction-identity"
	"github.com/openabstractions/abstraction-identity/listen"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

const maxSourceCursorBytes = 8192

var sourceIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._/-]{0,127}$`)

var ErrSourcePermissionDenied = errors.New("logging source: native permission denied")

const SourceReadAction = "abstraction.logging/history.read"

func SourceReadResource(id string) string { return "source:" + id }

// SourceEntry is normalized native event data. Record.Identity must be empty;
// Producer contains source claims, never authenticated OA caller identity.
type SourceEntry struct {
	Record   wire.Record
	Producer *wire.ProducerClaim
}

// NativePage is one bounded result from a single platform source. Next is the
// adapter's continuation and is sealed by SourceService before it is exposed.
type NativePage struct {
	Outcome wire.PageOutcome
	Entries []SourceEntry
	Next    string
	AtEnd   bool
}

// SourceAdapter reads one fixed native source. Describe and Read are called
// only after the caller's exact source right has been checked. Implementations
// must honor ctx and the supplied entry/encoded-byte bounds.
type SourceAdapter interface {
	ID() string
	Describe(context.Context) (wire.SourceDescriptor, error)
	Read(context.Context, string, int64, int64) (NativePage, error)
}

// SourceCurrentPosition is optional. It returns the adapter's opaque native
// cursor at the current end, or an empty cursor when the source is empty.
type SourceCurrentPosition interface {
	CurrentCursor(context.Context) (string, error)
}

// SourcePolicy decides the exact action/resource pair for a source. The action
// is SourceReadAction and the resource is SourceReadResource(id). A history
// grant on "abstraction.logging/history" does not grant source access.
// Wrap ErrSourcePolicyUnavailable or ErrHistoryPolicyUnavailable when the
// decision could not be obtained; every other error is a refusal.
type SourcePolicy func(context.Context, *identity.Peer, string, string) error

var ErrSourcePolicyUnavailable = errors.New("logging source: policy unavailable")

// SourceService composes fixed source adapters and owns source-bound cursor
// protection. It does not merge, collect or persist native entries.
type SourceService struct {
	seal     cipher.AEAD
	adapters map[string]SourceAdapter
}

func NewSourceService(adapters ...SourceAdapter) (*SourceService, error) {
	if len(adapters) > 32 {
		return nil, errors.New("logging source: source catalogue exceeds 32 entries")
	}
	key := [32]byte{}
	if _, err := rand.Read(key[:]); err != nil {
		return nil, fmt.Errorf("logging source: create cursor key: %w", err)
	}
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	seal, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	service := &SourceService{seal: seal, adapters: make(map[string]SourceAdapter, len(adapters))}
	for _, adapter := range adapters {
		if adapter == nil {
			return nil, errors.New("logging source: nil adapter")
		}
		id := adapter.ID()
		if !sourceIDPattern.MatchString(id) || strings.Contains(id, "..") {
			return nil, fmt.Errorf("logging source: invalid fixed source id %q", id)
		}
		if _, exists := service.adapters[id]; exists {
			return nil, fmt.Errorf("logging source: duplicate source id %q", id)
		}
		service.adapters[id] = adapter
	}
	return service, nil
}

// EnableSourceReader installs fixed native adapters and an explicit source
// policy before the host starts serving.
func (h *Host) EnableSourceReader(adapters []SourceAdapter, policy SourcePolicy) error {
	if policy == nil {
		return errors.New("logging service: explicit source policy required")
	}
	if len(adapters) == 0 {
		return errors.New("logging service: at least one fixed source adapter is required")
	}
	service, err := NewSourceService(adapters...)
	if err != nil {
		return err
	}
	h.lifecycle.Lock()
	defer h.lifecycle.Unlock()
	if h.serving || h.ctx.Err() != nil {
		return errors.New("logging service: configure source reader before Serve")
	}
	h.sources, h.sourcePolicy = service, policy
	return nil
}

func (h *Host) SourceReaderAvailable() bool {
	h.lifecycle.Lock()
	defer h.lifecycle.Unlock()
	return h.sources != nil && h.sourcePolicy != nil
}

type sourceReceiver struct{ receiver *receiver }

func (r *sourceReceiver) Ready() (bool, string) {
	return r.receiver.sourceReady()
}

func (r *sourceReceiver) DescribeMetadata() ([]string, map[string]string) {
	return r.receiver.describeSourceMetadata()
}

func (r *sourceReceiver) Sources() (wire.SourceCatalog, error) {
	return r.receiver.Sources()
}

func (r *sourceReceiver) Read(sourceID, cursor string, maxRecords, maxBytes int64) (wire.SourcePage, error) {
	return r.receiver.readSource(sourceID, cursor, maxRecords, maxBytes)
}

func (r *sourceReceiver) StartAtEnd(sourceID string) (wire.SourcePage, error) {
	return r.receiver.startSourceAtEnd(sourceID)
}

func (r *receiver) sourceReady() (bool, string) {
	if r.sources == nil || r.sourcePolicy == nil {
		return false, "source adapters or source policy are unavailable"
	}
	return true, ""
}

func (r *receiver) describeSourceMetadata() ([]string, map[string]string) {
	return []string{"fixed source IDs", "per-source exact rights", "bounded direct reads", "no cross-source ordering"}, nil
}

func (r *receiver) Sources() (wire.SourceCatalog, error) {
	if r.sources == nil || r.sourcePolicy == nil {
		return wire.SourceCatalog{Outcome: wire.SourceCatalogOutcomeUnavailable, Sources: []wire.SourceDescriptor{}}, nil
	}
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	peer, err := r.sourcePeer()
	if err != nil {
		return wire.SourceCatalog{}, err
	}
	catalog := wire.SourceCatalog{Outcome: wire.SourceCatalogOutcomeCatalog, Sources: make([]wire.SourceDescriptor, 0, len(r.sources.IDs()))}
	for _, id := range r.sources.IDs() {
		if err := ctx.Err(); err != nil {
			return wire.SourceCatalog{}, err
		}
		if err := r.sourcePolicy(ctx, peer, SourceReadAction, SourceReadResource(id)); err != nil {
			if errors.Is(err, ErrSourcePolicyUnavailable) || errors.Is(err, ErrHistoryPolicyUnavailable) || ctx.Err() != nil {
				return wire.SourceCatalog{Outcome: wire.SourceCatalogOutcomeUnavailable, Sources: []wire.SourceDescriptor{}}, nil
			}
			catalog.Sources = append(catalog.Sources, wire.SourceDescriptor{ID: id, Availability: wire.SourceAvailabilityPermissionDenied, Limitations: []wire.SourceLimitation{}})
			continue
		}
		catalog.Sources = append(catalog.Sources, r.sources.Describe(ctx, id))
	}
	return catalog, nil
}

func (r *receiver) readSource(sourceID, cursor string, maxRecords, maxBytes int64) (wire.SourcePage, error) {
	refuse := func(outcome wire.HistoryPageOutcome) (wire.SourcePage, error) {
		return wire.SourcePage{Outcome: outcome, Records: []wire.SourceRecord{}, Next: cursor}, nil
	}
	if maxRecords < 1 || maxRecords > 256 || maxBytes < 1 || maxBytes > 65536 {
		return refuse(wire.HistoryPageOutcomeInvalid)
	}
	if r.sources == nil || r.sourcePolicy == nil {
		return refuse(wire.HistoryPageOutcomeUnavailable)
	}
	if !r.sources.Has(sourceID) {
		return refuse(wire.HistoryPageOutcomeUnsupported)
	}
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	peer, err := r.sourcePeer()
	if err != nil {
		return wire.SourcePage{}, err
	}
	if outcome := r.authorizeSource(peer, sourceID); outcome != wire.HistoryPageOutcomePage {
		return refuse(outcome)
	}
	page, err := r.sources.Read(ctx, sourceID, cursor, maxRecords, maxBytes)
	if errors.Is(err, ErrSourcePermissionDenied) {
		if r.protected == nil || !r.protected.Covers(sourceID) {
			return refuse(wire.HistoryPageOutcomeForbidden)
		}
		page, err = r.readProtected(ctx, peer, sourceID, cursor, maxRecords, maxBytes)
	}
	if err != nil {
		return wire.SourcePage{}, err
	}
	peer, err = r.sourcePeer()
	if err != nil {
		return wire.SourcePage{}, err
	}
	if outcome := r.authorizeSource(peer, sourceID); outcome != wire.HistoryPageOutcomePage {
		return refuse(outcome)
	}
	return page, err
}

func (r *receiver) startSourceAtEnd(sourceID string) (wire.SourcePage, error) {
	refuse := func(outcome wire.HistoryPageOutcome) (wire.SourcePage, error) {
		return wire.SourcePage{Outcome: outcome, Records: []wire.SourceRecord{}, Next: ""}, nil
	}
	if r.sources == nil || r.sourcePolicy == nil {
		return refuse(wire.HistoryPageOutcomeUnavailable)
	}
	if !r.sources.Has(sourceID) {
		return refuse(wire.HistoryPageOutcomeUnsupported)
	}
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	peer, err := r.sourcePeer()
	if err != nil {
		return wire.SourcePage{}, err
	}
	if outcome := r.authorizeSource(peer, sourceID); outcome != wire.HistoryPageOutcomePage {
		return refuse(outcome)
	}
	page, err := r.sources.StartAtEnd(ctx, sourceID)
	if errors.Is(err, ErrSourcePermissionDenied) {
		return refuse(wire.HistoryPageOutcomeForbidden)
	}
	if err != nil {
		return wire.SourcePage{}, err
	}
	peer, err = r.sourcePeer()
	if err != nil {
		return wire.SourcePage{}, err
	}
	if outcome := r.authorizeSource(peer, sourceID); outcome != wire.HistoryPageOutcomePage {
		return refuse(outcome)
	}
	return page, nil
}

func (r *receiver) authorizeSource(peer *identity.Peer, sourceID string) wire.HistoryPageOutcome {
	if err := r.sourcePolicy(r.ctx, peer, SourceReadAction, SourceReadResource(sourceID)); err != nil {
		if errors.Is(err, ErrSourcePolicyUnavailable) || errors.Is(err, ErrHistoryPolicyUnavailable) || r.ctx != nil && r.ctx.Err() != nil {
			return wire.HistoryPageOutcomeUnavailable
		}
		return wire.HistoryPageOutcomeNotPermitted
	}
	return wire.HistoryPageOutcomePage
}

func (r *receiver) sourcePeer() (*identity.Peer, error) {
	peer, err := r.call.Peer()
	if err != nil {
		return nil, &wire.ServiceError{Code: "caller_unavailable", Message: "caller identity could not be rechecked"}
	}
	if _, err = peer.Path.AtLeast(listen.Program.Path); err != nil {
		return nil, &wire.ServiceError{Code: "identity_required", Message: "kernel program identity required"}
	}
	if _, err = peer.User.AtLeast(listen.Program.User); err != nil {
		return nil, &wire.ServiceError{Code: "identity_required", Message: "kernel user identity required"}
	}
	if _, err = peer.Process.AtLeast(listen.Program.Process); err != nil {
		return nil, &wire.ServiceError{Code: "identity_required", Message: "kernel process identity required"}
	}
	return peer, nil
}

func (s *SourceService) IDs() []string {
	ids := make([]string, 0, len(s.adapters))
	for id := range s.adapters {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (s *SourceService) Has(id string) bool {
	_, ok := s.adapters[id]
	return ok
}

func (s *SourceService) Describe(ctx context.Context, id string) wire.SourceDescriptor {
	adapter, ok := s.adapters[id]
	if !ok {
		return wire.SourceDescriptor{ID: id, Availability: wire.SourceAvailabilityUnsupported, Limitations: []wire.SourceLimitation{wire.SourceLimitationVisibilityUnknown}}
	}
	descriptor, err := adapter.Describe(ctx)
	if ctx.Err() != nil {
		return wire.SourceDescriptor{ID: id, Availability: wire.SourceAvailabilityUnavailable, Limitations: []wire.SourceLimitation{wire.SourceLimitationVisibilityUnknown}}
	}
	if errors.Is(err, ErrSourcePermissionDenied) {
		return wire.SourceDescriptor{ID: id, Availability: wire.SourceAvailabilityPermissionDenied, Limitations: []wire.SourceLimitation{wire.SourceLimitationNativeAccess}}
	}
	if err != nil || descriptor.ID != id || !validAvailability(descriptor.Availability) {
		return wire.SourceDescriptor{ID: id, Availability: wire.SourceAvailabilityUnavailable, Limitations: []wire.SourceLimitation{wire.SourceLimitationVisibilityUnknown}}
	}
	if len(descriptor.Limitations) > 5 {
		return wire.SourceDescriptor{ID: id, Availability: wire.SourceAvailabilityUnavailable, Limitations: []wire.SourceLimitation{wire.SourceLimitationVisibilityUnknown}}
	}
	if descriptor.Limitations == nil {
		limitation := wire.SourceLimitationVisibilityUnknown
		if descriptor.Availability == wire.SourceAvailabilityPermissionDenied {
			limitation = wire.SourceLimitationNativeAccess
		}
		descriptor.Limitations = []wire.SourceLimitation{limitation}
	}
	return descriptor
}

func validAvailability(value wire.SourceAvailability) bool {
	return value == wire.SourceAvailabilityAvailable || value == wire.SourceAvailabilityPermissionDenied || value == wire.SourceAvailabilityUnavailable || value == wire.SourceAvailabilityUnsupported
}

func (s *SourceService) Read(ctx context.Context, sourceID, cursor string, maxRecords, maxBytes int64) (wire.SourcePage, error) {
	return s.readWith(ctx, sourceID, cursor, maxRecords, maxBytes, func(adapter SourceAdapter, native string) (NativePage, error) {
		return adapter.Read(ctx, native, maxRecords, maxBytes)
	})
}

// readWith is Read with the native page supplied by read, which receives the
// source's adapter and the opened native cursor. Every page passes the same
// checks, sanitizing and sealing whoever read it.
func (s *SourceService) readWith(ctx context.Context, sourceID, cursor string, maxRecords, maxBytes int64, read func(SourceAdapter, string) (NativePage, error)) (wire.SourcePage, error) {
	refuse := func(outcome wire.HistoryPageOutcome) (wire.SourcePage, error) {
		return wire.SourcePage{Outcome: outcome, Records: []wire.SourceRecord{}, Next: cursor}, nil
	}
	if maxRecords < 1 || maxRecords > 256 || maxBytes < 1 || maxBytes > 65536 {
		return refuse(wire.HistoryPageOutcomeInvalid)
	}
	if len(cursor) > maxSourceCursorBytes {
		return refuse(wire.HistoryPageOutcomeGap)
	}
	adapter, ok := s.adapters[sourceID]
	if !ok {
		return refuse(wire.HistoryPageOutcomeUnsupported)
	}
	if err := ctx.Err(); err != nil {
		return wire.SourcePage{}, err
	}
	nativeCursor := ""
	if cursor != "" {
		var decodedSource string
		nativeCursor, decodedSource = s.open(cursor)
		if decodedSource != sourceID {
			return refuse(wire.HistoryPageOutcomeGap)
		}
	}
	page, err := read(adapter, nativeCursor)
	if err != nil {
		if ctx.Err() != nil {
			return wire.SourcePage{}, ctx.Err()
		}
		if errors.Is(err, ErrSourcePermissionDenied) || protectedOutcome(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return wire.SourcePage{}, err
		}
		return refuse(wire.HistoryPageOutcomeUnavailable)
	}
	if err := ctx.Err(); err != nil {
		return wire.SourcePage{}, err
	}
	if !validPageOutcome(page.Outcome) {
		return refuse(wire.HistoryPageOutcomeUnavailable)
	}
	if page.Outcome != wire.PageOutcomePage {
		if len(page.Entries) != 0 || page.AtEnd {
			return refuse(wire.HistoryPageOutcomeUnavailable)
		}
		return refuse(historyOutcome(page.Outcome))
	}
	if int64(len(page.Entries)) > maxRecords || len(page.Next) > maxSourceCursorBytes/2 || (len(page.Entries) > 0 && page.Next == nativeCursor) {
		return refuse(wire.HistoryPageOutcomeUnavailable)
	}
	result := wire.SourcePage{Outcome: wire.HistoryPageOutcomePage, Records: make([]wire.SourceRecord, 0, len(page.Entries)), AtEnd: page.AtEnd}
	var used int64
	for _, entry := range page.Entries {
		if entry.Record.Schema != 1 || len(entry.Record.Identity) != 0 {
			return refuse(wire.HistoryPageOutcomeCorrupt)
		}
		if _, err := wire.Decode(wire.Encode(&entry.Record)); err != nil {
			return refuse(wire.HistoryPageOutcomeCorrupt)
		}
		if entry.Record.Attrs != nil {
			entry.Record.Attrs = withoutSensitiveFields(entry.Record.Attrs)
		}
		record := wire.SourceRecord{SourceID: sourceID, Record: entry.Record, Producer: entry.Producer}
		size := int64(wire.SourceRecordEncodedSize(&record))
		if size > maxBytes && len(result.Records) == 0 {
			return refuse(wire.HistoryPageOutcomeRecordTooLarge)
		}
		if used+size > maxBytes {
			return refuse(wire.HistoryPageOutcomeUnavailable)
		}
		used += size
		result.Records = append(result.Records, record)
	}
	if !page.AtEnd && len(page.Entries) == 0 {
		return refuse(wire.HistoryPageOutcomeUnavailable)
	}
	result.Next, err = s.sealCursor(sourceID, page.Next)
	if err != nil || len(result.Next) > maxSourceCursorBytes || (len(result.Records) > 0 && result.Next == cursor) {
		return refuse(wire.HistoryPageOutcomeUnavailable)
	}
	return result, nil
}

func (s *SourceService) StartAtEnd(ctx context.Context, sourceID string) (wire.SourcePage, error) {
	refuse := func(outcome wire.HistoryPageOutcome) (wire.SourcePage, error) {
		return wire.SourcePage{Outcome: outcome, Records: []wire.SourceRecord{}, Next: ""}, nil
	}
	adapter, ok := s.adapters[sourceID]
	if !ok {
		return refuse(wire.HistoryPageOutcomeUnsupported)
	}
	position, ok := adapter.(SourceCurrentPosition)
	if !ok {
		return refuse(wire.HistoryPageOutcomeUnsupported)
	}
	if err := ctx.Err(); err != nil {
		return wire.SourcePage{}, err
	}
	nativeCursor, err := position.CurrentCursor(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return wire.SourcePage{}, ctx.Err()
		}
		if errors.Is(err, ErrSourcePermissionDenied) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return wire.SourcePage{}, err
		}
		return refuse(wire.HistoryPageOutcomeUnavailable)
	}
	if err := ctx.Err(); err != nil {
		return wire.SourcePage{}, err
	}
	if len(nativeCursor) > maxSourceCursorBytes/2 {
		return refuse(wire.HistoryPageOutcomeUnavailable)
	}
	next, err := s.sealCursor(sourceID, nativeCursor)
	if err != nil || len(next) > maxSourceCursorBytes {
		return refuse(wire.HistoryPageOutcomeUnavailable)
	}
	return wire.SourcePage{Outcome: wire.HistoryPageOutcomePage, Records: []wire.SourceRecord{}, Next: next, AtEnd: true}, nil
}

func validPageOutcome(value wire.PageOutcome) bool {
	return value == wire.PageOutcomePage || value == wire.PageOutcomeGap || value == wire.PageOutcomeUnavailable || value == wire.PageOutcomeInvalidRequest || value == wire.PageOutcomeRecordTooLarge || value == wire.PageOutcomeCorrupt || value == wire.PageOutcomeUnsupported
}

func (s *SourceService) sealCursor(sourceID, native string) (string, error) {
	plain := []byte(sourceID + "\x00" + native)
	nonce := make([]byte, s.seal.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := s.seal.Seal(nonce, nonce, plain, nil)
	return base64.RawURLEncoding.EncodeToString(sealed), nil
}

func (s *SourceService) open(cursor string) (native, sourceID string) {
	sealed, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil || len(sealed) < s.seal.NonceSize()+s.seal.Overhead() {
		return "", ""
	}
	nonce, ciphertext := sealed[:s.seal.NonceSize()], sealed[s.seal.NonceSize():]
	plain, err := s.seal.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", ""
	}
	i := strings.IndexByte(string(plain), 0)
	if i <= 0 {
		return "", ""
	}
	return string(plain[i+1:]), string(plain[:i])
}

func withoutSensitiveFields(attrs map[string]string) map[string]string {
	if len(attrs) == 0 {
		return attrs
	}
	result := make(map[string]string, len(attrs))
	for name, value := range attrs {
		key := strings.ToLower(name)
		key = strings.NewReplacer("_", "", "-", "", ".", "", " ", "").Replace(key)
		sensitive := false
		for _, marker := range []string{"password", "passwd", "secret", "token", "authorization", "credential", "privatekey", "accesskey", "apikey", "cookie"} {
			if strings.Contains(key, marker) {
				sensitive = true
				break
			}
		}
		if !sensitive {
			result[name] = value
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}
