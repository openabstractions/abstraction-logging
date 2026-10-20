package logging

import (
	"context"
	"errors"
	"fmt"
	"github.com/openabstractions/abstraction-identity/listen"
	"io/fs"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
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
	// everBound is set by the first successful binding.
	everBound atomic.Bool
	act       activation
}

// activation is the background start of a stopped installed runtime. At most
// one runs at a time, and a failed one is remembered: no new start begins
// before retryAt, whose wait doubles from activationRetry up to
// activationRetryMax.
type activation struct {
	mu      sync.Mutex
	running chan struct{} // closed when the start in flight returns
	err     error         // the last start's result
	retryAt time.Time
	delay   time.Duration
}

// activationRetry and activationRetryMax bound how often a runtime that fails
// to start is started again.
var activationRetry, activationRetryMax = 5 * time.Second, time.Minute

// NewResolvedHandler returns a handler that resolves abstraction.logging/sink@1
// lazily through the explicit endpoint, trusting it by server. Handle or an
// explicit Handler.Check triggers the first resolution.
// Construction performs no resolution. The handler never selects or
// activates the installed runtime. Resolution errors name "the explicit
// endpoint <endpoint>". The selected logging binding is retained after
// resolution. Handle exposes waiting and trust errors; slog.Logger normally
// discards them.
func NewResolvedHandler(program, endpoint string, server listen.ServerExpectation) *Handler {
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
// runtime's resolver endpoint absent, it starts that runtime in the background
// (see activation), waits for the start within caller, and asks again within
// 2 s.
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
		activation := s.activation(caller, activate, selected)
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
	s.everBound.Store(true)
	return s.bound, nil
}

// activation starts the selected runtime in the background within
// bootstrap.DefaultActivationBudget, or joins the start in flight, and waits
// for it within caller. A caller that stops waiting leaves the start running
// for the next binding. After a failed start it starts nothing until the retry
// time and returns that failure.
func (s *resolvedSink) activation(caller context.Context, activate func(context.Context, bootstrap.Selection) error, selected bootstrap.Selection) error {
	s.act.mu.Lock()
	done := s.act.running
	if done == nil {
		if s.act.err != nil && time.Now().Before(s.act.retryAt) {
			err := s.act.err
			s.act.mu.Unlock()
			return err
		}
		done = make(chan struct{})
		s.act.running = done
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), bootstrap.DefaultActivationBudget)
			err := activate(ctx, selected)
			cancel()
			s.act.mu.Lock()
			s.act.running, s.act.err = nil, err
			if err == nil {
				s.act.delay = 0
			} else {
				s.act.delay = min(max(2*s.act.delay, activationRetry), activationRetryMax)
				s.act.retryAt = time.Now().Add(s.act.delay)
			}
			s.act.mu.Unlock()
			close(done)
		}()
	}
	s.act.mu.Unlock()
	select {
	case <-done:
	case <-caller.Done():
		return fmt.Errorf("runtime activation still in progress: %w", caller.Err())
	}
	s.act.mu.Lock()
	defer s.act.mu.Unlock()
	return s.act.err
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

// adoptedSink is the sink under Default, Resolve and NewResolvedHandler. Every
// record goes into an AsyncSink from the first write, so a log statement never
// waits on resolution or on the runtime's start. The delivery goroutine binds,
// starts a stopped installed runtime, and retries with backoff while the
// service is absent; the queue keeps up to its capacity meanwhile [LOG-S11,
// LOG-S12].
type adoptedSink struct {
	resolved *resolvedSink
	// native receives the records while the machine offers no logging
	// service, when the adopter installed one [LOG-S15].
	native slog.Handler
	opts   AsyncOptions
	mu     sync.Mutex
	queue  *AsyncSink
	// closed is set by close; a later write gets ErrSinkClosed and starts
	// no delivery goroutine.
	closed bool
}

func newAdoptedHandler(resolved *resolvedSink, opts *Options) *Handler {
	o := Options{}
	if opts != nil {
		o = *opts
	}
	sink := &adoptedSink{resolved: resolved, native: o.Native, opts: AsyncOptions{
		Program: o.Program, OnOverflow: o.OnOverflow, OnFailure: o.OnFailure, OnRecovery: o.OnRecovery,
	}}
	return NewHandler(sink, &o)
}

func (s *adoptedSink) Write(r Record) error { return s.WriteContext(context.Background(), r) }

// WriteContext refuses a record whose caller already cancelled and otherwise
// queues it without waiting.
func (s *adoptedSink) WriteContext(ctx context.Context, r Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	queue := s.start()
	if queue == nil {
		return ErrSinkClosed
	}
	return queue.Write(r)
}

// start returns the queue, starting it on the first write, and nil after close
// when no record was ever written.
func (s *adoptedSink) start() *AsyncSink {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queue == nil && !s.closed {
		var inner Sink = s.resolved
		if s.native != nil {
			inner = &nativeFallback{resolved: s.resolved, native: s.native}
		}
		s.queue = NewAsyncSink(inner, s.opts)
	}
	return s.queue
}

// check binds now within 2 s, plus the wait for a runtime start, and keeps the
// binding for later records.
func (s *adoptedSink) check(ctx context.Context) error {
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := s.resolved.bind(ctx, bounded, false)
	return err
}

// close drains the queue until ctx is done. A sink that never bound tries once
// more within ctx; when that fails it stops at once, counts every queued record
// Failed, and returns the resolution error [LOG-S11]. With a native handler and
// a machine that offers no logging service, the queue drains to native
// [LOG-S15].
func (s *adoptedSink) close(ctx context.Context) (AsyncCounts, error) {
	s.mu.Lock()
	s.closed = true
	queue := s.queue
	s.mu.Unlock()
	if queue == nil {
		return AsyncCounts{}, nil
	}
	if !s.resolved.everBound.Load() {
		if err := s.check(ctx); err != nil && (s.native == nil || !notOffered(err)) {
			stopped, stop := context.WithCancel(ctx)
			stop()
			//unchecked: the queue closes on a cancelled context only to count what it held; the resolution error is the one returned
			counts, _ := queue.Close(stopped)
			counts.Failed, counts.Abandoned = counts.Failed+counts.Abandoned, 0
			counts.LastError = err.Error()
			return counts, fmt.Errorf("logging: %d records not delivered: %w", counts.Failed, err)
		}
	}
	return queue.Close(ctx)
}

func (s *adoptedSink) async() *AsyncSink {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queue
}
