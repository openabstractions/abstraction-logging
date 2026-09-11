// Package client supplies logging through the machine's service. It contains no
// file sink or embedded provider and never falls back to writing a local store.
package client

import (
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
	return &Client{transport: listen.FrameClient{Endpoint: endpoint, Timeout: 2 * time.Second, MaxFrame: 1 << 20}}
}

// Write submits a schema-defined record. Success is not a persistence receipt.
func (c *Client) Write(record Record) error {
	return wire.NewSinkClient(&c.transport).Write(record)
}

// Log supplies protocol metadata so applications only name the event.
func (c *Client) Log(level int64, message string, attrs map[string]string) error {
	return c.Write(Record{Schema: 1, Time: time.Now().UTC().Format("2006-01-02T15:04:05.000000Z"),
		Level: level, Msg: message, Attrs: attrs,
		Identity: []wire.Attestation{{By: "self", Uid: -1, Gid: -1, Pid: -1}}})
}
