package logging

import (
	"context"
	"log/slog"
	"sync"

	"github.com/openabstractions/abstraction-facade/go-core/bootstrap"
	"github.com/openabstractions/abstraction-identity/listen"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"github.com/openabstractions/abstraction-logging/go/client"
)

// clientSink writes provider records to an already-resolved service client.
type clientSink struct{ client *client.Client }

// NewClientSink adapts an already-resolved logging client — for example the one
// a facade Machine's ResolveLog returns under the caller's own trust — to Sink.
// Each record crosses as its contract encoding. Write waits within the client's
// own limits; WriteContext also honours ctx.
func NewClientSink(c *client.Client) Sink { return clientSink{client: c} }

// NewClientHandler returns the slog handler writing to an already-resolved
// logging client. It is NewHandler over NewClientSink.
func NewClientHandler(c *client.Client, opts *Options) *Handler {
	return NewHandler(NewClientSink(c), opts)
}

// NewRebindingClientSink adapts a resolved logging client to Sink and rebinds
// through resolve, for example a facade Machine's ResolveLog under the caller's
// own trust. Under an AsyncSink, a failed delivery calls resolve again before
// each retry [LOG-S12]; a failed resolve leaves the previous client in place.
func NewRebindingClientSink(bound *client.Client, resolve func(context.Context) (*client.Client, error)) Sink {
	return &rebindingClientSink{bound: bound, resolve: resolve}
}

type rebindingClientSink struct {
	mu      sync.Mutex
	bound   *client.Client
	resolve func(context.Context) (*client.Client, error)
}

func (s *rebindingClientSink) Write(r Record) error { return s.WriteContext(context.Background(), r) }

func (s *rebindingClientSink) WriteContext(ctx context.Context, r Record) error {
	s.mu.Lock()
	bound := s.bound
	s.mu.Unlock()
	return clientSink{client: bound}.WriteContext(ctx, r)
}

func (s *rebindingClientSink) Rebind(ctx context.Context) error {
	c, err := s.resolve(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.bound = c
	s.mu.Unlock()
	return nil
}

func (s clientSink) Write(r Record) error { return s.WriteContext(context.Background(), r) }

func (s clientSink) WriteContext(ctx context.Context, r Record) error {
	encoded, err := r.Encode()
	if err != nil {
		return err
	}
	record, err := wire.Decode(encoded)
	if err != nil {
		return err
	}
	return s.client.WriteContext(ctx, *record)
}

// Resolve selects the installed runtime and resolves abstraction.logging/sink@1
// now, returning a handler bound to that service. It is the eager form of the
// handler Default returns, for a program that reports at startup whether its
// logging was adopted. A selection, trust or resolution failure is returned;
// a *ResolutionError carries the resolver's typed status. Once bound, records
// are delivered as Default delivers them [LOG-S12].
func Resolve(ctx context.Context, program string) (*Handler, error) {
	handler := newAdoptedHandler(newResolvedSink(), &Options{Program: program})
	if err := handler.Check(ctx); err != nil {
		return nil, err
	}
	return handler, nil
}

// ResolveVerified resolves through the resolver at an explicit endpoint with
// independent server trust, the eager form of NewResolvedHandler. It never
// selects the installed runtime, and its resolution errors name "the explicit
// endpoint <endpoint>".
func ResolveVerified(ctx context.Context, program, endpoint string, server listen.ServerExpectation) (*Handler, error) {
	handler := newAdoptedHandler(resolvedSinkFor(endpoint, server), &Options{Program: program})
	if err := handler.Check(ctx); err != nil {
		return nil, err
	}
	return handler, nil
}

// Check resolves a handler whose service binding is selected lazily (Default,
// NewResolvedHandler) now, and reports why it cannot deliver. A handler over any
// other sink has nothing to resolve and returns nil. A successful binding is kept
// for later records.
func (h *Handler) Check(ctx context.Context) error {
	if sink, ok := h.sink.(*adoptedSink); ok {
		return sink.check(ctx)
	}
	return nil
}

func resolvedSinkFor(endpoint string, server listen.ServerExpectation) *resolvedSink {
	if server.Process != nil {
		process := *server.Process
		server.Process = &process
	}
	sink := newResolvedSink()
	sink.explicit = true
	sink.selectInstalled = func(context.Context) (bootstrap.Selection, error) {
		return bootstrap.Selection{Endpoint: endpoint, Server: server}, nil
	}
	return sink
}

var _ slog.Handler = (*Handler)(nil)
