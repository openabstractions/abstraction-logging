package client

import (
	"context"
	"errors"
	"github.com/openabstractions/abstraction-identity/listen"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"time"
)

// Observer binds the optional long-poll interface. Its default waiting budget is two seconds.
type Observer struct{ transport listen.FrameClient }

func NewObserver(endpoint string) *Observer {
	return NewObserverWithTransport(listen.FrameClient{Endpoint: endpoint})
}

// NewObserverWithTransport retains the caller's endpoint, server trust and waiting limits.
func NewObserverWithTransport(transport listen.FrameClient) *Observer {
	return &Observer{transport: transport.WithDefaults(2*time.Second, 1<<20)}
}

// WithTimeout returns a reusable independent binding. A caller context may shorten this budget.
func (c *Observer) WithTimeout(timeout time.Duration) (*Observer, error) {
	if timeout <= 0 || timeout > 35*time.Second {
		return nil, errors.New("logging: timeout must be positive and at most 35 seconds")
	}
	copy := *c
	copy.transport.Timeout = timeout
	return &copy, nil
}

// ObserveContext bounds waiting independently of logging. Transport cancellation gives no page.
func (c *Observer) ObserveContext(ctx context.Context, cursor string, maxRecords, maxBytes, waitMS int64) (wire.Page, error) {
	if err := ctx.Err(); err != nil {
		return wire.Page{}, err
	}
	if waitMS < 0 || waitMS > 30000 {
		return wire.Page{}, errors.New("logging: wait_ms outside 0..30000")
	}
	page, err := wire.NewHistoryObserverClient(c.transport.WithContext(ctx)).Observe(cursor, maxRecords, maxBytes, waitMS)
	if err != nil {
		return wire.Page{}, err
	}
	return validateHistoryPage(page, cursor, maxRecords, maxBytes)
}
