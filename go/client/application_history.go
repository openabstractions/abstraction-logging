package client

import (
	"context"
	"errors"
	identity "github.com/openabstractions/abstraction-identity"
	"github.com/openabstractions/abstraction-identity/listen"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"time"
	"unicode/utf8"
)

// ApplicationLogPermissionTarget encodes the owning requested one-app target
// for an explicit Asks permission@1 request. It performs no filesystem capture
// and accepts no caller FileKey. Rights prepares receiving evidence at edit.
func ApplicationLogPermissionTarget(program string) ([]byte, error) {
	if !utf8.ValidString(program) || !identity.ValidSubjectProgram(program) {
		return nil, errors.New("logging: clean native subject program required")
	}
	target := wire.EncodeApplicationLogResource(&wire.ApplicationLogResource{Program: program})
	if len(target) > 16<<10 {
		return nil, errors.New("logging: application target exceeds 16 KiB")
	}
	return target, nil
}

// ApplicationReader binds only reader@2. Resolution never substitutes reader@1.
type ApplicationReader struct{ transport listen.FrameClient }

func NewApplicationReader(endpoint string) *ApplicationReader {
	return NewApplicationReaderWithTransport(listen.FrameClient{Endpoint: endpoint})
}
func NewApplicationReaderWithTransport(t listen.FrameClient) *ApplicationReader {
	return &ApplicationReader{t.WithDefaults(DefaultTimeout, 1<<20)}
}
func (c *ApplicationReader) ReadContext(ctx context.Context, target ApplicationLogTarget, cursor string, maxRecords, maxBytes int64) (ApplicationLogPage, error) {
	if err := ctx.Err(); err != nil {
		return ApplicationLogPage{}, err
	}
	page, err := wire.NewApplicationHistoryReaderClient(c.transport.WithContext(ctx)).Read(target, cursor, maxRecords, maxBytes)
	if err != nil {
		return ApplicationLogPage{}, err
	}
	return validateApplicationHistoryPage(page, cursor, maxRecords, maxBytes)
}

// ApplicationObserver waits once per bounded physical scan at observer@2.
type ApplicationObserver struct{ transport listen.FrameClient }

func NewApplicationObserver(endpoint string) *ApplicationObserver {
	return NewApplicationObserverWithTransport(listen.FrameClient{Endpoint: endpoint})
}
func NewApplicationObserverWithTransport(t listen.FrameClient) *ApplicationObserver {
	return &ApplicationObserver{t.WithDefaults(DefaultTimeout, 1<<20)}
}
func (c *ApplicationObserver) WithTimeout(timeout time.Duration) (*ApplicationObserver, error) {
	if timeout <= 0 || timeout > 35*time.Second {
		return nil, errors.New("logging: timeout must be positive and at most 35 seconds")
	}
	clone := *c
	clone.transport.Timeout = timeout
	return &clone, nil
}
func (c *ApplicationObserver) ObserveContext(ctx context.Context, target ApplicationLogTarget, cursor string, maxRecords, maxBytes, waitMS int64) (ApplicationLogPage, error) {
	if err := ctx.Err(); err != nil {
		return ApplicationLogPage{}, err
	}
	if waitMS < 0 || waitMS > 30000 {
		return ApplicationLogPage{}, errors.New("logging: wait_ms outside 0..30000")
	}
	page, err := wire.NewApplicationHistoryObserverClient(c.transport.WithContext(ctx)).Observe(target, cursor, maxRecords, maxBytes, waitMS)
	if err != nil {
		return ApplicationLogPage{}, err
	}
	return validateApplicationHistoryPage(page, cursor, maxRecords, maxBytes)
}

func validateApplicationHistoryPage(page ApplicationLogPage, cursor string, maxRecords, maxBytes int64) (ApplicationLogPage, error) {
	if !page.Outcome.Known() {
		return ApplicationLogPage{}, errors.New("logging: invalid application history outcome")
	}
	if page.Outcome == wire.HistoryPageOutcomePage {
		var size int64
		for _, record := range page.Records {
			size += int64(len(wire.Encode(&record)))
		}
		if maxRecords < 1 || maxRecords > 256 || maxBytes < 1 || maxBytes > 65536 || int64(len(page.Records)) > maxRecords || size > maxBytes || page.Next == "" || !page.AtEnd && page.Next == cursor || len(page.Records) > 0 && page.Next == cursor {
			return ApplicationLogPage{}, errors.New("logging: invalid bounded application history response")
		}
	} else if len(page.Records) != 0 || page.Next != cursor || page.AtEnd {
		return ApplicationLogPage{}, errors.New("logging: refusal changed application continuation")
	}
	return page, nil
}
