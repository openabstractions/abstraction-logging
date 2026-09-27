package logging

import (
	"context"
	"errors"
	"github.com/openabstractions/abstraction-identity/listen"
	"io/fs"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/openabstractions/abstraction-facade/go-core/bootstrap"
	facade "github.com/openabstractions/abstraction-facade/go-core/go/abstraction/facade"
	"github.com/openabstractions/abstraction-facade/go-core/resolution"
	"github.com/openabstractions/abstraction-logging/go/client"
)

// ResolutionError is the facade's typed resolution failure; see resolution.Error.
type ResolutionError = resolution.Error

type resolvedSink struct {
	gate            chan struct{}
	bound           *client.Client
	selectInstalled func(context.Context) (bootstrap.Selection, error)
	// activate starts the selected installation's runtime. Default discovery
	// leaves selectInstalled and activate nil and activates through
	// bootstrap.ActivateInstalled on Windows. A sink given its selection (an
	// explicit endpoint) never activates; a fixture sets activate to declare
	// its selection the installed one.
	activate func(context.Context, bootstrap.Selection) error
	// explicit marks a resolver endpoint the application named. A resolution
	// error then looks for "the explicit endpoint <endpoint>".
	explicit bool
}

// NewResolvedHandler returns a handler that resolves abstraction.logging/sink@1
// lazily through the explicit endpoint, trusting it by server. Handle or an
// explicit Handler.Check triggers the first resolution.
// Construction performs no resolution. The handler never selects or
// activates the installed runtime. Resolution errors name "the explicit
// endpoint <endpoint>". The selected logging binding is retained after
// resolution. Handle exposes waiting and trust errors; slog.Logger normally
// discards them.
func NewResolvedHandler(program, endpoint string, server listen.ServerExpectation) slog.Handler {
	return newAdoptedHandler(resolvedSinkFor(endpoint, server), &Options{Program: program})
}

func newResolvedSink() *resolvedSink         { return &resolvedSink{gate: make(chan struct{}, 1)} }
func (s *resolvedSink) Write(r Record) error { return s.WriteContext(context.Background(), r) }

func (s *resolvedSink) binding(ctx context.Context) (*client.Client, error) {
	return s.bind(ctx, ctx, false)
}

// Rebind forgets the bound service and resolves it again: it selects the
// installed runtime, or uses the explicit endpoint it was given, checks its
// trust and resolves abstraction.logging/sink@1 [LOG-S12]. A failure leaves the
// sink unbound.
func (s *resolvedSink) Rebind(ctx context.Context) error {
	_, err := s.bind(ctx, ctx, true)
	return err
}

// bind resolves within ctx. When default discovery finds the installed
// runtime's resolver endpoint absent, it starts that runtime once within the
// caller's budget (bootstrap.DefaultActivationBudget without a deadline) and
// asks again within 2 s.
func (s *resolvedSink) bind(caller, ctx context.Context, again bool) (*client.Client, error) {
	select {
	case s.gate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-s.gate }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if again {
		s.bound = nil
	}
	if s.bound != nil {
		return s.bound, nil
	}
	const capability, contract = "abstraction.logging", "abstraction.logging/sink@1"
	selectInstalled, activate := s.selectInstalled, s.activate
	if selectInstalled == nil {
		selectInstalled = bootstrap.SelectInstalled
		if activate == nil && runtime.GOOS == "windows" {
			activate = bootstrap.ActivateInstalled
		}
	}
	selected, err := selectInstalled(ctx)
	if err != nil {
		return nil, resolution.Unreachable(ctx, err, capability, contract, "the installed runtime")
	}
	lookedFor := "the installed runtime at " + selected.Endpoint
	if s.explicit {
		lookedFor = "the explicit endpoint " + selected.Endpoint
	}
	resolver := resolution.NewVerifiedClient(selected.Endpoint, 2*time.Second, selected.Server)
	request := facade.ResolveRequest{Capability: capability, Contracts: []string{contract}, Scope: facade.ScopeLocal}
	result, err := resolver.Resolve(ctx, request)
	if err != nil && activate != nil && caller.Err() == nil && errors.Is(err, fs.ErrNotExist) && !errors.Is(err, listen.ErrServerUntrusted) {
		budget, cancelBudget := caller, context.CancelFunc(func() {})
		if _, ok := caller.Deadline(); !ok {
			budget, cancelBudget = context.WithTimeout(caller, bootstrap.DefaultActivationBudget)
		}
		activation := activate(budget, selected)
		cancelBudget()
		if errors.Is(activation, bootstrap.ErrUpgradeInProgress) {
			return nil, &ResolutionError{Status: resolution.UpgradeInProgress, Capability: capability, Contract: contract, LookedFor: lookedFor, Err: activation}
		}
		if activation != nil {
			err = errors.Join(err, activation)
		} else {
			retry, cancelRetry := context.WithTimeout(caller, 2*time.Second)
			defer cancelRetry()
			ctx = retry
			result, err = resolver.Resolve(ctx, request)
		}
	}
	if err != nil {
		return nil, resolution.Unreachable(ctx, err, capability, contract, lookedFor)
	}
	if result.Status != facade.ResolutionStatusResolved {
		return nil, &ResolutionError{Status: resolution.ErrorStatus(result.Status.String()), Capability: capability, Contract: contract, LookedFor: lookedFor}
	}
	if result.Reference.Scope != facade.ScopeLocal || result.Reference.Transport != resolution.LocalTransport {
		return nil, &ResolutionError{Status: resolution.UnsupportedTransport, Capability: capability, Contract: contract, LookedFor: lookedFor,
			Scope: result.Reference.Scope, Transport: result.Reference.Transport}
	}
	transport, err := resolution.BindLocal(ctx, *result.Reference, &selected.Server, nil)
	if err != nil {
		return nil, err
	}
	s.bound = client.NewWithTransport(transport)
	return s.bound, nil
}

// WriteContext binds within 2 s of ctx, plus an activation's own budget, and
// then writes within 2 s.
func (s *resolvedSink) WriteContext(ctx context.Context, r Record) error {
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	c, err := s.bind(ctx, bounded, false)
	cancel()
	if err != nil {
		return err
	}
	ctx, cancel = context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return clientSink{client: c}.WriteContext(ctx, r)
}

// adoptedSink is the sink under Default, Resolve and NewResolvedHandler. Until
// its first binding, each write resolves synchronously and an absent service
// is that write's resolution error [LOG-S11]. Once bound, records go through an
// AsyncSink that retries and rebinds when the service disappears [LOG-S12].
type adoptedSink struct {
	resolved *resolvedSink
	opts     AsyncOptions
	mu       sync.Mutex
	queue    *AsyncSink
}

func newAdoptedHandler(resolved *resolvedSink, opts *Options) *Handler {
	o := Options{}
	if opts != nil {
		o = *opts
	}
	sink := &adoptedSink{resolved: resolved, opts: AsyncOptions{
		Program: o.Program, OnOverflow: o.OnOverflow, OnFailure: o.OnFailure, OnRecovery: o.OnRecovery,
	}}
	return NewHandler(sink, &o)
}

func (s *adoptedSink) Write(r Record) error { return s.WriteContext(context.Background(), r) }

// WriteContext refuses a record whose caller already cancelled, as the
// synchronous sink did, and otherwise never waits once bound.
func (s *adoptedSink) WriteContext(ctx context.Context, r Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	queue, err := s.bind(ctx)
	if err != nil {
		return err
	}
	return queue.Write(r)
}

func (s *adoptedSink) bind(ctx context.Context) (*AsyncSink, error) {
	s.mu.Lock()
	queue := s.queue
	s.mu.Unlock()
	if queue != nil {
		return queue, nil
	}
	// Binding waits 2 s; an activation of a stopped installed runtime runs
	// within the caller's own budget.
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := s.resolved.bind(ctx, bounded, false); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queue == nil {
		s.queue = NewAsyncSink(s.resolved, s.opts)
	}
	return s.queue, nil
}

func (s *adoptedSink) async() *AsyncSink {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queue
}
