// Package service connects the generated logging interface to a provider using
// the shared identity-bound listener. Only this side holds the sink.
package service

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/openabstractions/abstraction-identity/listen"
	logging "github.com/openabstractions/abstraction-logging/go"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

type Host struct {
	listener  listen.Listener
	out       logging.Sink
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	workers   sync.WaitGroup
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
	l, err := listen.Listen(endpoint)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Host{listener: l, out: out, ctx: ctx, cancel: cancel}, nil
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
		h.workers.Add(1)
		go func() {
			defer h.workers.Done()
			defer connection.Close()
			requestContext, cancel := context.WithTimeout(h.ctx, 5*time.Second)
			defer cancel()
			call, err := listen.ReceiveFramed(requestContext, connection, listen.Program, 1<<20)
			if call != nil {
				defer call.Close()
			}
			if err == nil {
				handler := &receiver{out: h.out, call: call}
				dispatch := wire.SinkDispatcher{Handler: handler}
				err = dispatch.WriteFrame(call.Frame)
			}
			if err != nil && h.OnError != nil && h.ctx.Err() == nil {
				h.OnError(err)
			}
		}()
	}
}

type receiver struct {
	out  logging.Sink
	call *listen.FramedCall
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
