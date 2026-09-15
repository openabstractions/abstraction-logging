package logging

import (
	"context"
	"log/slog"

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
// a *ResolutionError carries the resolver's typed status. Default is unchanged.
func Resolve(ctx context.Context, program string) (*Handler, error) {
	sink := newResolvedSink()
	if _, err := sink.binding(ctx); err != nil {
		return nil, err
	}
	return NewHandler(sink, &Options{Program: program}), nil
}

// ResolveVerified is Resolve with an explicit resolver endpoint and independent
// server trust, the eager form of NewResolvedHandler.
func ResolveVerified(ctx context.Context, program, endpoint string, server listen.ServerExpectation) (*Handler, error) {
	sink := resolvedSinkFor(endpoint, server)
	if _, err := sink.binding(ctx); err != nil {
		return nil, err
	}
	return NewHandler(sink, &Options{Program: program}), nil
}

// Check resolves a handler whose service binding is selected lazily (Default,
// NewResolvedHandler) now, and reports why it cannot deliver. A handler over any
// other sink has nothing to resolve and returns nil. A successful binding is kept
// for later records.
func (h *Handler) Check(ctx context.Context) error {
	if sink, ok := h.sink.(*resolvedSink); ok {
		_, err := sink.binding(ctx)
		return err
	}
	return nil
}

func resolvedSinkFor(endpoint string, server listen.ServerExpectation) *resolvedSink {
	if server.Process != nil {
		process := *server.Process
		server.Process = &process
	}
	sink := newResolvedSink()
	sink.selectInstalled = func(context.Context) (bootstrap.Selection, error) {
		return bootstrap.Selection{Endpoint: endpoint, Server: server}, nil
	}
	return sink
}

var _ slog.Handler = (*Handler)(nil)
