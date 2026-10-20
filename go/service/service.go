// Package service connects the generated logging interface to a provider using
// the shared identity-bound listener. Only this side holds the sink.
package service

import (
	"context"
	"crypto/cipher"
	"errors"
	"fmt"
	"os/user"
	"runtime"
	"sync"
	"time"

	identity "github.com/openabstractions/abstraction-identity"
	"github.com/openabstractions/abstraction-identity/listen"
	logging "github.com/openabstractions/abstraction-logging/go"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	watch "github.com/openabstractions/abstraction-watch/go"
)

type Host struct {
	listener          listen.Listener
	out               logging.Sink
	owner             string
	ctx               context.Context
	cancel            context.CancelFunc
	closeOnce         sync.Once
	workers           sync.WaitGroup
	calls             listen.CallAdmission
	observers         chan struct{}
	lifecycle         sync.Mutex
	serving           bool
	history           HistoryPolicy
	sources           *SourceService
	sourcePolicy      SourcePolicy
	protected         ProtectedSourceReads
	applicationPolicy ApplicationHistoryPolicy
	applicationSeal   cipher.AEAD
	// OnError reports refused requests and provider failures to the host operator.
	// It may be called concurrently. It is never a response to a one-way call.
	OnError func(error)
	// OnStopped is called when admission stops, before active writes drain.
	// Assign it before Serve. It must return promptly.
	OnStopped func()
}

func Listen(endpoint string, out logging.Sink) (*Host, error) {
	if out == nil {
		return nil, errors.New("logging service: nil provider")
	}
	owner, err := user.Current()
	if err != nil {
		return nil, err
	}
	if owner.Uid == "" {
		return nil, errors.New("logging service: owner unavailable")
	}
	l, err := listen.ListenFramed(endpoint, listen.Program)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	seal, err := newApplicationSeal()
	if err != nil {
		cancel()
		l.Close()
		return nil, err
	}
	return &Host{listener: listen.Sessions(l, listen.SessionOptions{MaxSessions: 64}), out: out, owner: owner.Uid, ctx: ctx, cancel: cancel, calls: listen.NewCallAdmission(64), observers: make(chan struct{}, 32), applicationSeal: seal}, nil
}

func (h *Host) Close() error {
	var err error
	h.closeOnce.Do(func() { h.cancel(); err = h.listener.Close() })
	return err
}

func (h *Host) Serve(ctx context.Context) error {
	h.lifecycle.Lock()
	if h.serving || h.ctx.Err() != nil {
		h.lifecycle.Unlock()
		return errors.New("logging service: host cannot serve twice or after close")
	}
	h.serving = true
	h.lifecycle.Unlock()
	stop := context.AfterFunc(ctx, func() { _ = h.Close() })
	defer stop()
	defer h.workers.Wait()
	defer func() {
		if h.OnStopped != nil {
			h.OnStopped()
		}
	}()
	defer h.Close()
	for {
		connection, err := h.listener.Accept()
		if err != nil {
			if h.ctx.Err() != nil || ctx.Err() != nil {
				return nil
			}
			return err
		}
		release, admitted := h.calls.Take(connection)
		if !admitted {
			continue
		}
		h.workers.Add(1)
		go func() {
			defer h.workers.Done()
			defer release()
			defer connection.Close()
			requestContext, cancel := context.WithTimeout(h.ctx, 35*time.Second)
			defer cancel()
			call, err := listen.ReceiveFramed(requestContext, connection, listen.Program, 1<<20)
			if call != nil {
				defer call.Close()
			}
			if err == nil {
				h.lifecycle.Lock()
				policy := h.history
				applicationPolicy := h.applicationPolicy
				sources, sourcePolicy, protected := h.sources, h.sourcePolicy, h.protected
				h.lifecycle.Unlock()
				handler := &receiver{out: h.out, call: call, owner: h.owner, observers: h.observers, history: policy, sources: sources, sourcePolicy: sourcePolicy, protected: protected, ctx: requestContext, applicationPolicy: applicationPolicy, applicationSeal: h.applicationSeal}
				var name string
				name, err = wire.ServiceName(call.Frame)
				if err == nil {
					switch name {
					case "abstraction.logging/reader@1", "abstraction.logging/observer@1", "abstraction.logging/reader@2", "abstraction.logging/observer@2", "abstraction.logging/source-reader@1", wire.EndpointContract, "abstraction.facade/endpoint@2":
						var reply []byte
						reply, err = wire.ServeEndpoint(call.Frame, "openabstractions", "",
							&wire.SinkDispatcher{Handler: handler},
							&wire.HistoryReaderDispatcher{Handler: handler},
							&wire.HistoryObserverDispatcher{Handler: handler},
							&wire.ApplicationHistoryReaderDispatcher{Handler: &applicationReceiver{handler}},
							&wire.ApplicationHistoryObserverDispatcher{Handler: &applicationReceiver{handler}},
							&wire.SourceReaderDispatcher{Handler: &sourceReceiver{receiver: handler}})
						if err == nil {
							err = call.Reply(reply)
						}
					default:
						// A sink write is one-way: no reply, including for an unknown service.
						dispatch := wire.SinkDispatcher{Handler: handler}
						err = dispatch.WriteFrame(call.Frame)
					}
				}
			}
			if err != nil && h.OnError != nil && h.ctx.Err() == nil {
				h.OnError(err)
			}
		}()
	}
}

type receiver struct {
	out               logging.Sink
	call              *listen.FramedCall
	owner             string
	observers         chan struct{}
	history           HistoryPolicy
	sources           *SourceService
	sourcePolicy      SourcePolicy
	protected         ProtectedSourceReads
	applicationPolicy ApplicationHistoryPolicy
	applicationSeal   cipher.AEAD
	ctx               context.Context
}

// Legacy untyped constants remain source-compatible for Go callers. Service
// errors use the generated ServiceErrorCode members.
const (
	CodeForbidden         = "forbidden"
	CodePolicyUnavailable = "policy_unavailable"
)

// HistoryPolicy authorizes history reading and observation for the rechecked
// receiving peer, after same-account proof and before records are read or
// returned. It must honor ctx and be safe for concurrent calls. Wrap
// ErrHistoryPolicyUnavailable when the decision cannot be obtained; every other
// error is a refusal.
type HistoryPolicy func(context.Context, *identity.Peer) error

// ErrHistoryPolicyUnavailable distinguishes a failed decision lookup from refusal.
var ErrHistoryPolicyUnavailable = errors.New("logging service: history policy unavailable")

// EnableHistoryPolicy narrows history reading and observation to callers the
// policy authorizes. Configure it before Serve. Writes are unaffected.
func (h *Host) EnableHistoryPolicy(policy HistoryPolicy) error {
	h.lifecycle.Lock()
	defer h.lifecycle.Unlock()
	if h.serving || h.ctx.Err() != nil {
		return errors.New("logging service: configure history policy before Serve")
	}
	if policy == nil {
		return errors.New("logging service: explicit history policy required")
	}
	h.history = policy
	return nil
}

func (r *receiver) Write(value wire.Record) error {
	observed, err := r.call.Peer()
	if err != nil {
		return err
	}
	path, err := observed.Path.AtLeast(listen.Program.Path)
	if err != nil {
		return err
	}
	user, err := observed.User.AtLeast(listen.Program.User)
	if err != nil {
		return err
	}
	process, err := observed.Process.AtLeast(listen.Program.Process)
	if err != nil {
		return err
	}
	// The adapter converts between the generated client record and the existing
	// provider record; it does not replace either codec or copy wire field names.
	record, err := logging.DecodeRecord(wire.Encode(&value))
	if err != nil {
		return fmt.Errorf("logging service: %w", err)
	}
	peer := logging.Attestation{By: "identity/" + runtime.GOOS, Verified: true,
		Exe: path, UID: logging.Unknown, GID: logging.Unknown, PID: process.PID}
	if user.Kind == "windows" {
		peer.User = user.SID
	} else {
		peer.UID, peer.GID = user.UID, user.GID
	}
	// Preserve the submitted provenance chain as claims and append this service's
	// own observation. Never promote a submitted 'verified' field to authority.
	attested := (&logging.Server{}).Accept(record, peer, nil)
	if out, ok := r.out.(logging.AttributedSink); ok {
		if provider, owned := r.out.(applicationHistoryProvider); owned && provider.ApplicationHistoryAvailable() {
			subject, err := identity.SubjectOf(observed, listen.Program)
			if err != nil {
				return err
			}
			return out.WriteAttributed(attested, logging.Emitter{Version: 1, Account: subject.Account, Program: subject.Program, Key: identity.ProgramKey(subject.Program)})
		}
	}
	return r.out.Write(attested)
}

// HistoryAvailable describes the selected provider, independently of write readiness.
type historyReader interface {
	Read(string, int64, int64) (wire.Page, error)
}

func (h *Host) HistoryAvailable() bool {
	_, ok := h.out.(historyReader)
	return ok
}

func (r *receiver) authorizeHistory() error {
	peer, err := r.call.Peer()
	if err != nil {
		return &wire.ServiceError{Code: wire.ServiceErrorCodeCallerUnavailable, Message: "caller identity could not be rechecked"}
	}
	_, verdict := identity.AdmitOwner(peer, identity.Need{User: listen.Program.User}, r.owner)
	switch verdict {
	case identity.OwnerAdmitted:
	case identity.OwnerIdentityRequired, identity.OwnerUnavailable:
		return &wire.ServiceError{Code: wire.ServiceErrorCodeIdentityRequired, Message: "kernel user identity required"}
	case identity.OwnerOtherAccount:
		return &wire.ServiceError{Code: wire.ServiceErrorCodeWrongUser, Message: "history belongs to another user"}
	default:
		return &wire.ServiceError{Code: wire.ServiceErrorCodeCallerUnavailable, Message: "caller identity could not be rechecked"}
	}
	if r.history == nil {
		return nil
	}
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	err = r.history(ctx, peer)
	if err == nil && ctx.Err() == nil {
		return nil
	}
	if ctx.Err() != nil || errors.Is(err, ErrHistoryPolicyUnavailable) {
		return &wire.ServiceError{Code: wire.ServiceErrorCodePolicyUnavailable, Message: "history policy decision unavailable"}
	}
	return &wire.ServiceError{Code: wire.ServiceErrorCodeForbidden, Message: "history reading not permitted"}
}

func (r *receiver) Read(cursor string, maxRecords, maxBytes int64) (wire.Page, error) {
	if err := r.authorizeHistory(); err != nil {
		return wire.Page{}, err
	}
	if maxRecords < 1 || maxRecords > 256 || maxBytes < 1 || maxBytes > 65536 {
		return wire.Page{Outcome: wire.PageOutcomeInvalidRequest, Records: []wire.Record{}, Next: cursor}, nil
	}
	if source, ok := r.out.(historyReader); ok {
		return source.Read(cursor, maxRecords, maxBytes)
	}
	return wire.Page{Outcome: wire.PageOutcomeUnavailable, Records: []wire.Record{}, Next: cursor}, nil
}

func (r *receiver) Usage() (wire.HistoryUsageResult, error) {
	if err := r.authorizeHistory(); err != nil {
		return wire.HistoryUsageResult{}, err
	}
	result := wire.HistoryUsageResult{Outcome: wire.HistoryUsageOutcomeUnavailable}
	if source, ok := r.out.(interface {
		Usage() (wire.HistoryUsageResult, error)
	}); ok {
		var err error
		result, err = source.Usage()
		if err != nil {
			return wire.HistoryUsageResult{}, err
		}
	}
	if err := r.authorizeHistory(); err != nil {
		return wire.HistoryUsageResult{}, err
	}
	return result, nil
}

// ObservationAvailable reports native notification support for this provider.
func (h *Host) ObservationAvailable() bool { _, ok := h.out.(logging.HistoryObserver); return ok }
func (r *receiver) Observe(cursor string, maxRecords, maxBytes, waitMS int64) (wire.Page, error) {
	if err := r.authorizeHistory(); err != nil {
		return wire.Page{}, err
	}
	refusal := func(outcome wire.PageOutcome) (wire.Page, error) {
		return wire.Page{Outcome: outcome, Records: []wire.Record{}, Next: cursor}, nil
	}
	if maxRecords < 1 || maxRecords > 256 || maxBytes < 1 || maxBytes > 65536 || waitMS < 0 || waitMS > 30000 {
		return refusal(wire.PageOutcomeInvalidRequest)
	}
	source, ok := r.out.(logging.HistoryObserver)
	if !ok {
		return refusal(wire.PageOutcomeUnsupported)
	}
	// A source with its own gate bounds its waiters (WATCH-W2); the host
	// bound covers only a source that has none.
	if _, gated := r.out.(historyGated); !gated && waitMS > 0 {
		select {
		case r.observers <- struct{}{}:
			defer func() { <-r.observers }()
		default:
			return refusal(wire.PageOutcomeUnavailable)
		}
	}
	// A source that takes the recheck runs it after every read (WATCH-W4);
	// any other source is rechecked once after it returns.
	if rechecked, ok := r.out.(historyRechecked); ok {
		return rechecked.ObserveRechecked(r.call.WaitContext(), cursor, maxRecords, maxBytes, waitMS, r.authorizeHistory)
	}
	page, err := source.ObserveContext(r.call.WaitContext(), cursor, maxRecords, maxBytes, waitMS)
	if err != nil {
		return wire.Page{}, err
	}
	if err = r.authorizeHistory(); err != nil {
		return wire.Page{}, err
	}
	return page, nil
}

// historyGated is a history source that waits on its own watch.Gate.
type historyGated interface{ HistoryGate() *watch.Gate }

// historyRechecked is a history source whose wait rechecks the caller's
// authority after every read.
type historyRechecked interface {
	ObserveRechecked(ctx context.Context, cursor string, maxRecords, maxBytes, waitMS int64, recheck func() error) (wire.Page, error)
}
