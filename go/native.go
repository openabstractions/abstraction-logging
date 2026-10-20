package logging

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/openabstractions/abstraction-facade/go-core/bootstrap"
	facade "github.com/openabstractions/abstraction-facade/go-core/go/abstraction/facade"
	"github.com/openabstractions/abstraction-facade/go-core/resolution"
)

// NativeRecheckInterval is how long records go to the native handler before
// the machine is asked again whether it offers the logging service [LOG-S15].
var NativeRecheckInterval = 30 * time.Second

// errDeliveredNative is a delivery the native handler took. AsyncSink counts
// it Native.
var errDeliveredNative = errors.New("logging: record delivered to the native handler")

// DefaultWithNative is Default with the native handler an adopter installs
// beside the facade [LOG-S15]. While this machine answers that it offers no
// logging service (no OA runtime is installed, or the runtime has no logging
// registration), records go to native, typically
// slog.NewTextHandler(os.Stderr, nil), and are counted Native. The machine is
// asked again every NativeRecheckInterval, and a runtime installed later takes
// over without a restart. Every other failure stays the resolution error of
// LOG-S11, and records wait in the queue for the service. A nil native is
// Default.
func DefaultWithNative(program string, native slog.Handler) *Handler {
	return NewDefaultHandler(&Options{Program: program, Native: native})
}

// notOffered reports a definite answer that this machine offers no logging
// service: no trusted runtime installation, a platform no runtime supports, or
// the resolver's unavailable [LOG-S15].
func notOffered(err error) bool {
	if errors.Is(err, bootstrap.ErrNoTrustedInstallation) {
		return true
	}
	var refused *ResolutionError
	if errors.As(err, &refused) {
		return refused.Status == resolution.ErrorStatus(facade.ResolutionStatusUnavailable.String()) || refused.Platform != ""
	}
	return false
}

// nativeFallback is the inner sink of an adopted handler given a native
// handler. It binds and writes as the resolved sink does, and sends records to
// native while the machine offers no logging service.
type nativeFallback struct {
	resolved *resolvedSink
	native   slog.Handler
	mu       sync.Mutex
	active   bool      // records go to native
	recheck  time.Time // the next time the machine is asked again
}

func (f *nativeFallback) Write(r Record) error { return f.WriteContext(context.Background(), r) }

// WriteContext delivers to the service, or to native while the fallback is
// active and its recheck is not due. It returns errDeliveredNative for a record
// native took.
func (f *nativeFallback) WriteContext(ctx context.Context, r Record) error {
	f.mu.Lock()
	active, due := f.active, !time.Now().Before(f.recheck)
	f.mu.Unlock()
	if active && !due {
		return f.toNative(ctx, r)
	}
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	c, err := f.resolved.bind(ctx, bounded, false)
	cancel()
	if f.fallsBack(err) {
		f.enter(ctx, err)
		return f.toNative(ctx, r)
	}
	if err != nil {
		return err
	}
	f.leave(ctx)
	ctx, cancel = context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return clientSink{client: c}.WriteContext(ctx, r)
}

// Rebind resolves again. Before the first binding, an answer that the machine
// offers no logging service activates the fallback, and the retry's record
// goes to native.
func (f *nativeFallback) Rebind(ctx context.Context) error {
	err := f.resolved.Rebind(ctx)
	if f.fallsBack(err) {
		f.enter(ctx, err)
		return nil
	}
	return err
}

// fallsBack reports a not-offered answer before the first binding. A failure
// after a successful binding is that binding's, and its records wait for the
// service [LOG-S12, LOG-S15].
func (f *nativeFallback) fallsBack(err error) bool {
	return notOffered(err) && !f.resolved.everBound.Load()
}

func (f *nativeFallback) enter(ctx context.Context, cause error) {
	f.mu.Lock()
	was := f.active
	f.active, f.recheck = true, time.Now().Add(NativeRecheckInterval)
	f.mu.Unlock()
	if !was {
		f.line(ctx, slog.LevelWarn, "abstraction.logging/sink@1 not offered: "+failureClass(cause))
	}
}

func (f *nativeFallback) leave(ctx context.Context) {
	f.mu.Lock()
	was := f.active
	f.active = false
	f.mu.Unlock()
	if was {
		f.line(ctx, slog.LevelInfo, "abstraction.logging/sink@1 offered again; records go to the service")
	}
}

func (f *nativeFallback) line(ctx context.Context, level slog.Level, msg string) {
	if f.native.Enabled(ctx, level) {
		//unchecked: the native handler is the adopter's; a failure there has nowhere else to go
		_ = f.native.Handle(ctx, slog.NewRecord(time.Now(), level, msg, 0))
	}
}

// toNative hands the record to native with its time, level, message and
// attributes. The identity chain stays behind: no receiving service stamps it.
// A record native does not enable, or fails to handle, is still counted Native.
func (f *nativeFallback) toNative(ctx context.Context, r Record) error {
	level := slog.Level(r.Level)
	if f.native.Enabled(ctx, level) {
		at := r.Time.Time
		if at.IsZero() {
			at = time.Now()
		}
		record := slog.NewRecord(at, level, r.Msg, 0)
		keys := make([]string, 0, len(r.Attrs)+1)
		for k := range r.Attrs {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		for _, k := range keys {
			record.AddAttrs(slog.String(k, r.Attrs[k]))
		}
		if r.Job != "" {
			record.AddAttrs(slog.String("job", r.Job))
		}
		//unchecked: the native handler is the adopter's; a failure there has nowhere else to go
		_ = f.native.Handle(ctx, record)
	}
	return errDeliveredNative
}
