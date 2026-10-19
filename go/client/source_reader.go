package client

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/openabstractions/abstraction-identity/listen"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

var fixedSourceID = regexp.MustCompile(`^[a-z0-9][a-z0-9._/-]{0,127}$`)

const maxSourceCursorBytes = 8192

type SourceDescriptor = wire.SourceDescriptor
type SourceCatalog = wire.SourceCatalog
type SourceRecord = wire.SourceRecord
type SourcePage = wire.SourcePage
type SourceAvailability = wire.SourceAvailability
type SourceCatalogOutcome = wire.SourceCatalogOutcome
type SourceLimitation = wire.SourceLimitation
type ProducerClaim = wire.ProducerClaim

const (
	SourceLimitationRetention         = wire.SourceLimitationRetention
	SourceLimitationNativeRedaction   = wire.SourceLimitationNativeRedaction
	SourceLimitationCollectionPolicy  = wire.SourceLimitationCollectionPolicy
	SourceLimitationNativeAccess      = wire.SourceLimitationNativeAccess
	SourceLimitationVisibilityUnknown = wire.SourceLimitationVisibilityUnknown
)

func SourceAvailabilityValues() []SourceAvailability     { return wire.SourceAvailabilityValues() }
func SourceCatalogOutcomeValues() []SourceCatalogOutcome { return wire.SourceCatalogOutcomeValues() }
func SourceLimitationValues() []SourceLimitation         { return wire.SourceLimitationValues() }

// SourceReader binds the versioned fixed-source interface. It never selects a
// path or a native query on the caller's behalf.
type SourceReader struct{ transport listen.FrameClient }

func NewSourceReader(endpoint string) *SourceReader {
	return NewSourceReaderWithTransport(listen.FrameClient{Endpoint: endpoint})
}

func NewSourceReaderWithTransport(transport listen.FrameClient) *SourceReader {
	return &SourceReader{transport: transport.WithDefaults(DefaultTimeout, 1<<20)}
}

func (c *SourceReader) SourcesContext(ctx context.Context) (SourceCatalog, error) {
	if err := ctx.Err(); err != nil {
		return SourceCatalog{}, err
	}
	catalog, err := wire.NewSourceReaderClient(c.transport.WithContext(ctx)).Sources()
	if err != nil {
		return SourceCatalog{}, err
	}
	if !catalog.Outcome.Known() {
		return SourceCatalog{}, errors.New("logging: invalid source catalogue outcome")
	}
	if catalog.Outcome != wire.SourceCatalogOutcomeCatalog {
		if len(catalog.Sources) != 0 {
			return SourceCatalog{}, errors.New("logging: refused source catalogue contains entries")
		}
		return catalog, nil
	}
	if len(catalog.Sources) > 32 {
		return SourceCatalog{}, errors.New("logging: source catalogue exceeds limit")
	}
	seen := make(map[string]struct{}, len(catalog.Sources))
	for _, source := range catalog.Sources {
		if !fixedSourceID.MatchString(source.ID) || strings.Contains(source.ID, "..") || !validSourceAvailability(source.Availability) {
			return SourceCatalog{}, errors.New("logging: invalid source catalogue")
		}
		if len(source.Limitations) > 5 {
			return SourceCatalog{}, errors.New("logging: source limitation count exceeds limit")
		}
		if _, exists := seen[source.ID]; exists {
			return SourceCatalog{}, errors.New("logging: duplicate source catalogue entry")
		}
		seen[source.ID] = struct{}{}
	}
	return catalog, nil
}

func (c *SourceReader) ReadContext(ctx context.Context, sourceID, cursor string, maxRecords, maxBytes int64) (SourcePage, error) {
	if err := ctx.Err(); err != nil {
		return SourcePage{}, err
	}
	if !fixedSourceID.MatchString(sourceID) || strings.Contains(sourceID, "..") {
		return SourcePage{}, errors.New("logging: invalid fixed source identifier")
	}
	if len(cursor) > maxSourceCursorBytes {
		return SourcePage{}, errors.New("logging: source cursor exceeds limit")
	}
	page, err := wire.NewSourceReaderClient(c.transport.WithContext(ctx)).Read(sourceID, cursor, maxRecords, maxBytes)
	if err != nil {
		return SourcePage{}, err
	}
	return validateSourcePage(page, sourceID, cursor, maxRecords, maxBytes)
}

// StartAtEndContext returns an opaque baseline cursor without returning native
// records. ReadContext can use that cursor to begin after the observed end.
func (c *SourceReader) StartAtEndContext(ctx context.Context, sourceID string) (SourcePage, error) {
	if err := ctx.Err(); err != nil {
		return SourcePage{}, err
	}
	if !fixedSourceID.MatchString(sourceID) || strings.Contains(sourceID, "..") {
		return SourcePage{}, errors.New("logging: invalid fixed source identifier")
	}
	page, err := wire.NewSourceReaderClient(c.transport.WithContext(ctx)).StartAtEnd(sourceID)
	if err != nil {
		return SourcePage{}, err
	}
	if page.Outcome == wire.HistoryPageOutcomePage {
		if len(page.Records) != 0 || !page.AtEnd || page.Next == "" || len(page.Next) > maxSourceCursorBytes {
			return SourcePage{}, errors.New("logging: invalid source end cursor")
		}
		return page, nil
	}
	return validateSourcePage(page, sourceID, "", 1, 1)
}

func validSourceAvailability(value wire.SourceAvailability) bool {
	return value == wire.SourceAvailabilityAvailable || value == wire.SourceAvailabilityPermissionDenied || value == wire.SourceAvailabilityUnavailable || value == wire.SourceAvailabilityUnsupported
}

func validateSourcePage(page wire.SourcePage, sourceID, cursor string, maxRecords, maxBytes int64) (wire.SourcePage, error) {
	if !page.Outcome.Known() {
		return wire.SourcePage{}, errors.New("logging: invalid source page outcome")
	}
	if page.Outcome == wire.HistoryPageOutcomePage {
		var size int64
		for _, record := range page.Records {
			if record.SourceID != sourceID || len(record.Record.Identity) != 0 || record.Record.Schema != 1 {
				return wire.SourcePage{}, errors.New("logging: invalid source record provenance")
			}
			size += int64(wire.SourceRecordEncodedSize(&record))
		}
		if maxRecords < 1 || maxRecords > 256 || maxBytes < 1 || maxBytes > 65536 || int64(len(page.Records)) > maxRecords || size > maxBytes || page.Next == "" || (len(page.Records) > 0 && page.Next == cursor) || (!page.AtEnd && len(page.Records) == 0) {
			return wire.SourcePage{}, errors.New("logging: invalid bounded source page")
		}
	} else if len(page.Records) != 0 || page.Next != cursor || page.AtEnd {
		return wire.SourcePage{}, errors.New("logging: refusal changed source continuation")
	}
	return page, nil
}
