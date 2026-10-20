// Package client supplies logging through the machine's service. It contains no
// file sink or embedded provider and never falls back to writing a local store.
package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/openabstractions/abstraction-facade/go-core/bootstrap"
	"os"
	"time"

	"github.com/openabstractions/abstraction-identity/listen"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

// EnvEndpoint overrides the framed service endpoint. The legacy raw-record
// ABSTRACTION_LOG_SERVICE endpoint deliberately has a different name.
const EnvEndpoint = "ABSTRACTION_LOG_ENDPOINT"

// DefaultTimeout is how long a sink, reader or observer waits for one call
// when its transport names no timeout and the call's context no deadline. A
// service that consults a policy, such as the runtime's rights decision on
// history reads, answers within it. A transport timeout, WithTimeout or a
// context deadline replaces it.
const DefaultTimeout = 2 * time.Second

type Record = wire.Record
type HistoryUsage = wire.HistoryUsage
type HistoryUsageOutcome = wire.HistoryUsageOutcome
type HistoryUsageResult = wire.HistoryUsageResult

func HistoryUsageOutcomeValues() []HistoryUsageOutcome { return wire.HistoryUsageOutcomeValues() }

type Client struct {
	transport listen.FrameClient
	initErr   error
}

// DefaultEndpoint resolves the service endpoint without opening a file or
// starting a provider. Availability is reported by the actual operation.
// DefaultEndpoint returns empty when the account namespace cannot be resolved.
func DefaultEndpoint() string { endpoint, _ := checkedDefaultEndpoint(); return endpoint }
func checkedDefaultEndpoint() (string, error) {
	if endpoint := os.Getenv(EnvEndpoint); endpoint != "" {
		return endpoint, nil
	}
	return bootstrap.InstalledEndpoint("logging-v1")
}
func emptyEndpointError() error {
	_, err := checkedDefaultEndpoint()
	if err != nil {
		return err
	}
	return fmt.Errorf("logging: empty endpoint")
}

func Discover() *Client {
	endpoint, err := checkedDefaultEndpoint()
	c := New(endpoint)
	c.initErr = err
	return c
}

func New(endpoint string) *Client {
	c := NewWithTransport(listen.FrameClient{Endpoint: endpoint})
	if endpoint == "" {
		c.initErr = emptyEndpointError()
	}
	return c
}

// NewWithTransport retains the caller's endpoint, server trust and waiting limits.
func NewWithTransport(transport listen.FrameClient) *Client {
	c := &Client{transport: transport.WithDefaults(DefaultTimeout, 1<<20)}
	if transport.Endpoint == "" && transport.Dialer == nil {
		c.initErr = emptyEndpointError()
	}
	return c
}

// Write submits a schema-defined record. Success is not a persistence receipt.
func (c *Client) Write(record Record) error {
	return c.WriteContext(context.Background(), record)
}

// WriteContext bounds submission waiting; timeout leaves delivery unresolved.
func (c *Client) WriteContext(ctx context.Context, record Record) error {
	if c.initErr != nil {
		return c.initErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return wire.NewSinkClient(c.transport.WithContext(ctx)).Write(record)
}

// Log supplies protocol metadata so applications only name the event.
func (c *Client) Log(level int64, message string, attrs map[string]string) error {
	return c.LogContext(context.Background(), level, message, attrs)
}

// LogContext supplies metadata and uses the caller waiting context.
func (c *Client) LogContext(ctx context.Context, level int64, message string, attrs map[string]string) error {
	return c.WriteContext(ctx, Record{Schema: 1, Time: wire.FormatTimestamp(time.Now()),
		Level: level, Msg: message, Attrs: attrs,
		Identity: []wire.Attestation{{By: "self", UID: -1, GID: -1, PID: -1}}})
}

// Reader binds only the generated history interface.
type Reader struct {
	transport listen.FrameClient
	initErr   error
}

func NewReader(endpoint string) *Reader {
	c := NewReaderWithTransport(listen.FrameClient{Endpoint: endpoint})
	if endpoint == "" {
		c.initErr = emptyEndpointError()
	}
	return c
}

// NewReaderWithTransport retains the caller's endpoint, server trust and waiting limits.
func NewReaderWithTransport(transport listen.FrameClient) *Reader {
	c := &Reader{transport: transport.WithDefaults(DefaultTimeout, 1<<20)}
	if transport.Endpoint == "" && transport.Dialer == nil {
		c.initErr = emptyEndpointError()
	}
	return c
}

// ReadContext returns bounded retained history with opaque continuation state.
// A gap requires the application to explicitly choose a fresh empty cursor.
func (c *Reader) ReadContext(ctx context.Context, cursor string, maxRecords, maxBytes int64) (wire.Page, error) {
	if c.initErr != nil {
		return wire.Page{}, c.initErr
	}
	if err := ctx.Err(); err != nil {
		return wire.Page{}, err
	}
	page, err := wire.NewHistoryReaderClient(c.transport.WithContext(ctx)).Read(cursor, maxRecords, maxBytes)
	if err != nil {
		return wire.Page{}, err
	}
	return validateHistoryPage(page, cursor, maxRecords, maxBytes)
}

// UsageContext reads the provider's effective retained-history usage through
// the same native boundary as Read. Older providers may return unknown_method.
func (c *Reader) UsageContext(ctx context.Context) (wire.HistoryUsageResult, error) {
	if c.initErr != nil {
		return wire.HistoryUsageResult{}, c.initErr
	}
	if err := ctx.Err(); err != nil {
		return wire.HistoryUsageResult{}, err
	}
	return wire.NewHistoryReaderClient(c.transport.WithContext(ctx)).Usage()
}

func validateHistoryPage(page wire.Page, cursor string, maxRecords, maxBytes int64) (wire.Page, error) {
	if page.Outcome == wire.PageOutcomePage {
		var size int64
		for _, record := range page.Records {
			size += int64(len(wire.Encode(&record)))
		}
		if maxRecords < 1 || maxRecords > 256 || maxBytes < 1 || maxBytes > 65536 || int64(len(page.Records)) > maxRecords || size > maxBytes || page.Next == "" || (len(page.Records) > 0 && page.Next == cursor) || (!page.AtEnd && len(page.Records) == 0) {
			return wire.Page{}, errors.New("logging: invalid bounded history response")
		}
	} else if len(page.Records) != 0 || page.Next != cursor || page.AtEnd {
		return wire.Page{}, errors.New("logging: refusal changed history continuation")
	}
	return page, nil
}
