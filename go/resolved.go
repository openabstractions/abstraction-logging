package logging

import (
	"context"
	"fmt"
	"github.com/openabstractions/abstraction-identity/listen"
	"log/slog"
	"time"

	"github.com/openabstractions/abstraction-facade/go-core/bootstrap"
	facade "github.com/openabstractions/abstraction-facade/go-core/go/abstraction/facade"
	"github.com/openabstractions/abstraction-facade/go-core/resolution"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"github.com/openabstractions/abstraction-logging/go/client"
)

// ResolutionError retains a typed service refusal from the runtime catalogue.
type ResolutionError struct{ Status string }

func (e *ResolutionError) Error() string { return fmt.Sprintf("logging resolution: %s", e.Status) }

type resolvedSink struct {
	gate            chan struct{}
	bound           *client.Client
	selectInstalled func(context.Context) (bootstrap.Selection, error)
}

// NewResolvedHandler selects a resolver using independent caller/host trust
// configuration. The selected logging binding is retained after resolution.
// Handle exposes waiting and trust errors; slog.Logger normally discards them.
func NewResolvedHandler(program, endpoint string, server listen.ServerExpectation) slog.Handler {
	if server.Process != nil {
		process := *server.Process
		server.Process = &process
	}
	sink := newResolvedSink()
	sink.selectInstalled = func(context.Context) (bootstrap.Selection, error) {
		return bootstrap.Selection{Endpoint: endpoint, Server: server}, nil
	}
	return NewHandler(sink, &Options{Program: program})
}

func newResolvedSink() *resolvedSink         { return &resolvedSink{gate: make(chan struct{}, 1)} }
func (s *resolvedSink) Write(r Record) error { return s.WriteContext(context.Background(), r) }
func (s *resolvedSink) binding(ctx context.Context) (*client.Client, error) {
	select {
	case s.gate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-s.gate }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.bound != nil {
		return s.bound, nil
	}
	selectInstalled := s.selectInstalled
	if selectInstalled == nil {
		selectInstalled = bootstrap.SelectInstalled
	}
	selected, err := selectInstalled(ctx)
	if err != nil {
		return nil, err
	}
	result, err := resolution.NewVerifiedClient(selected.Endpoint, 2*time.Second, selected.Server).Resolve(ctx, facade.ResolveRequest{
		Capability: "abstraction.logging", Contracts: []string{"abstraction.logging/sink@1"}, Scope: facade.ScopeLocal,
	})
	if err != nil {
		return nil, err
	}
	if result.Status != facade.ResolutionStatusResolved {
		return nil, &ResolutionError{Status: result.Status}
	}
	if result.Reference.Scope != facade.ScopeLocal || result.Reference.Transport != resolution.LocalTransport {
		return nil, &ResolutionError{Status: "unsupported_transport"}
	}
	transport, err := resolution.BindLocal(ctx, *result.Reference, &selected.Server, nil)
	if err != nil {
		return nil, err
	}
	s.bound = client.NewWithTransport(transport)
	return s.bound, nil
}
func (s *resolvedSink) WriteContext(ctx context.Context, r Record) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	c, err := s.binding(ctx)
	if err != nil {
		return err
	}
	encoded, err := r.Encode()
	if err != nil {
		return err
	}
	record, err := wire.Decode(encoded)
	if err != nil {
		return err
	}
	return c.WriteContext(ctx, *record)
}
