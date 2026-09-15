// Package service connects the generated logging interface to a provider using
// the shared identity-bound listener. Only this side holds the sink.
package service

import (
	"context"
	"errors"
	"fmt"
	"os/user"
	"runtime"
	"strconv"
	"sync"
	"time"

	identity "github.com/openabstractions/abstraction-identity"
	"github.com/openabstractions/abstraction-identity/listen"
	logging "github.com/openabstractions/abstraction-logging/go"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

type Host struct {
	listener  listen.Listener
	out       logging.Sink
	owner     string
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	workers   sync.WaitGroup
	calls     chan struct{}
	observers chan struct{}
	lifecycle sync.Mutex
	serving   bool
	history   HistoryPolicy
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
	l, err := listen.Listen(endpoint)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Host{listener: l, out: out, owner: owner.Uid, ctx: ctx, cancel: cancel, calls: make(chan struct{}, 64), observers: make(chan struct{}, 32)}, nil
}

func (h *Host) Close() error {
	var err error
	h.closeOnce.Do(func() { h.cancel(); err = h.listener.Close() })
	return err
}

func (h *Host) Serve(ctx context.Context) error {
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
		select {
		case h.calls <- struct{}{}:
		default:
			connection.Close()
			continue
		}
		h.workers.Add(1)
		go func() {
			defer h.workers.Done()
			defer func() { <-h.calls }()
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
				h.lifecycle.Unlock()
				handler := &receiver{out: h.out, call: call, owner: h.owner, observers: h.observers, history: policy, ctx: requestContext}
				var name string
				name, err = wire.ServiceName(call.Frame)
				if err == nil {
					if name == "abstraction.logging/reader@1" {
						dispatcher := wire.HistoryReaderDispatcher{Handler: handler}
						var reply []byte
						reply, err = dispatcher.ExchangeFrame(call.Frame)
						if err == nil {
							err = call.Reply(reply)
						}
					} else if name == "abstraction.logging/observer@1" {
						dispatcher := wire.HistoryObserverDispatcher{Handler: handler}
						reply, dispatchErr := dispatcher.ExchangeFrame(call.Frame)
						err = dispatchErr
						if err == nil {
							err = call.Reply(reply)
						}
					} else {
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
	out       logging.Sink
	call      *listen.FramedCall
	owner     string
	observers chan struct{}
	history   HistoryPolicy
	ctx       context.Context
}

// History policy refusal codes returned as service errors.
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
	return r.out.Write(attested)
}

// HistoryAvailable describes the selected provider, independently of write readiness.
func (h *Host) HistoryAvailable() bool {
	_, ok := h.out.(wire.HistoryReader)
	return ok
}

func (r *receiver) authorizeHistory() error {
	peer, err := r.call.Peer()
	if err != nil {
		return &wire.ServiceError{Code: "caller_unavailable", Message: "caller identity could not be rechecked"}
	}
	who, err := peer.User.AtLeast(listen.Program.User)
	if err != nil {
		return &wire.ServiceError{Code: "identity_required", Message: "kernel user identity required"}
	}
	principal := ""
	if who.Kind == "windows" {
		principal = who.SID
	} else if who.Kind == "posix" {
		principal = strconv.Itoa(who.UID)
	}
	if principal == "" || principal != r.owner {
		return &wire.ServiceError{Code: "wrong_user", Message: "history belongs to another user"}
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
		return &wire.ServiceError{Code: CodePolicyUnavailable, Message: "history policy decision unavailable"}
	}
	return &wire.ServiceError{Code: CodeForbidden, Message: "history reading not permitted"}
}

func (r *receiver) Read(cursor string, maxRecords, maxBytes int64) (wire.Page, error) {
	if err := r.authorizeHistory(); err != nil {
		return wire.Page{}, err
	}
	if maxRecords < 1 || maxRecords > 256 || maxBytes < 1 || maxBytes > 65536 {
		return wire.Page{Outcome: wire.PageOutcomeInvalidRequest, Records: []wire.Record{}, Next: cursor}, nil
	}
	if source, ok := r.out.(wire.HistoryReader); ok {
		return source.Read(cursor, maxRecords, maxBytes)
	}
	return wire.Page{Outcome: wire.PageOutcomeUnavailable, Records: []wire.Record{}, Next: cursor}, nil
}

// ObservationAvailable reports native notification support for this provider.
func (h *Host) ObservationAvailable() bool { _, ok := h.out.(logging.HistoryObserver); return ok }
func (r *receiver) Observe(cursor string, maxRecords, maxBytes, waitMS int64) (wire.Page, error) {
	if err := r.authorizeHistory(); err != nil {
		return wire.Page{}, err
	}
	refusal := func(outcome string) (wire.Page, error) {
		return wire.Page{Outcome: outcome, Records: []wire.Record{}, Next: cursor}, nil
	}
	if maxRecords < 1 || maxRecords > 256 || maxBytes < 1 || maxBytes > 65536 || waitMS < 0 || waitMS > 30000 {
		return refusal(wire.PageOutcomeInvalidRequest)
	}
	source, ok := r.out.(logging.HistoryObserver)
	if !ok {
		return refusal(wire.PageOutcomeUnsupported)
	}
	if waitMS > 0 {
		select {
		case r.observers <- struct{}{}:
			defer func() { <-r.observers }()
		default:
			return refusal(wire.PageOutcomeUnavailable)
		}
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
