// Package client supplies logging through the machine's service. It contains no
// file sink or embedded provider and never falls back to writing a local store.
package client

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/openabstractions/abstraction-identity/listen"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

// EnvEndpoint overrides the framed service endpoint. The legacy raw-record
// ABSTRACTION_LOG_SERVICE endpoint deliberately has a different name.
const EnvEndpoint = "ABSTRACTION_LOG_ENDPOINT"

type Record = wire.Record

type Client struct {
	transport listen.FrameClient
}

// DefaultEndpoint resolves the service endpoint without opening a file or
// starting a provider. Availability is reported by the actual operation.
func DefaultEndpoint() string {
	if endpoint := os.Getenv(EnvEndpoint); endpoint != "" {
		return endpoint
	}
	return listen.Endpoint("logging-v1")
}

func Discover() *Client { return New(DefaultEndpoint()) }

func New(endpoint string) *Client {
	return NewWithTransport(listen.FrameClient{Endpoint: endpoint})
}

// NewWithTransport retains the caller's endpoint, server trust and waiting limits.
func NewWithTransport(transport listen.FrameClient) *Client {
	return &Client{transport: transport.WithDefaults(2*time.Second, 1<<20)}
}

// Write submits a schema-defined record. Success is not a persistence receipt.
func (c *Client) Write(record Record) error {
	return c.WriteContext(context.Background(), record)
}

// WriteContext bounds submission waiting; timeout leaves delivery unresolved.
func (c *Client) WriteContext(ctx context.Context, record Record) error {
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
	return c.WriteContext(ctx, Record{Schema: 1, Time: time.Now().UTC().Format("2006-01-02T15:04:05.000000Z"),
		Level: level, Msg: message, Attrs: attrs,
		Identity: []wire.Attestation{{By: "self", Uid: -1, Gid: -1, Pid: -1}}})
}

// Reader binds only the generated history interface.
type Reader struct{ transport listen.FrameClient }

func NewReader(endpoint string) *Reader {
	return NewReaderWithTransport(listen.FrameClient{Endpoint: endpoint})
}

// NewReaderWithTransport retains the caller's endpoint, server trust and waiting limits.
func NewReaderWithTransport(transport listen.FrameClient) *Reader {
	return &Reader{transport: transport.WithDefaults(2*time.Second, 1<<20)}
}

// ReadContext returns bounded retained history with opaque continuation state.
// A gap requires the application to explicitly choose a fresh empty cursor.
func (c *Reader) ReadContext(ctx context.Context, cursor string, maxRecords, maxBytes int64) (wire.Page, error) {
	if err := ctx.Err(); err != nil {
		return wire.Page{}, err
	}
	page, err := wire.NewHistoryReaderClient(c.transport.WithContext(ctx)).Read(cursor, maxRecords, maxBytes)
	if err != nil {
		return wire.Page{}, err
	}
	return validateHistoryPage(page, cursor, maxRecords, maxBytes)
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
