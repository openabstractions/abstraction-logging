package logging

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"
)

// DefaultCloseTimeout bounds Handler.Close when its context has no deadline,
// as Python's ServiceHandler.close bounds its own.
const DefaultCloseTimeout = 5 * time.Second

// Default returns a handler bound lazily to the resolved logging service.
// Handle queues each record and returns at once. An AsyncSink binds in the
// background, starts a stopped installed runtime, retries and rebinds while the
// service is absent, and reports each transition on standard error [LOG-S11,
// LOG-S12, LOG-S13]. Nothing falls back to a file or stderr. Close returns the
// resolution error when records were logged and the handler never bound, and
// Check returns it at once.
//
// Default returns the *Handler so the program can drain it before exit:
//
//	h := logging.Default("my-app")
//	defer h.Close(context.Background())
//	slog.SetDefault(slog.New(h))
func Default(program string) *Handler {
	return NewDefaultHandler(&Options{Program: program})
}

// NewDefaultHandler is Default with Options, whose OnFailure, OnRecovery and
// OnOverflow replace the standard-error reporter.
func NewDefaultHandler(opts *Options) *Handler {
	return newAdoptedHandler(newResolvedSink(), opts)
}

// LegacyDefault explicitly selects environment/file discovery and stderr when
// unconfigured. It preserves the pre-service Default behavior for local adopters.
func LegacyDefault(program string) slog.Handler {
	sink := LegacyAuto(program)
	if sink == Sink(DiscardSink{}) {
		return slog.NewTextHandler(os.Stderr, nil)
	}
	return NewHandler(sink, &Options{Program: program})
}

// Handler adapts this package's Sink to log/slog.
//
// This is the entire Go integration, and its smallness is the point. An
// application does not learn a new logging API, does not change a call site, and
// does not take a dependency in its libraries. It changes where the default
// logger points:
//
//	h := logging.Default("modelget")
//	defer h.Close(context.Background())
//	slog.SetDefault(slog.New(h))
//
// Every slog.Info in the program and in every library it imports now lands in a
// sink that other processes and other machines can read. SLF4J earned adoption
// by being the thing you could add without rewriting anything; this copies that,
// and refuses to compete with slog itself.
type Handler struct {
	sink   Sink
	opts   Options
	attrs  map[string]string
	groups []string
}

type Options struct {
	// Program names the emitter in its hop-0 claim.
	Program string
	// Level is the minimum level written. Zero value is LevelInfo.
	Level Level
	// Job ties every record from this handler to a unit of work. Set it on a
	// handler derived per job rather than globally.
	Job string
	// Claim overrides the computed hop-0 attestation, for tests.
	Claim *Attestation
	// OnFailure, OnRecovery and OnOverflow receive the transitions of a handler
	// bound through the facade (Default, NewDefaultHandler, Resolve,
	// NewResolvedHandler); see AsyncOptions. With none set, each transition is
	// one line on standard error. Other handlers ignore them.
	OnFailure  func(error, AsyncCounts)
	OnRecovery func(AsyncCounts)
	OnOverflow func(AsyncCounts)
	// Native receives the records of a handler bound through the facade while
	// the machine offers no logging service; see DefaultWithNative [LOG-S15].
	// Other handlers ignore it.
	Native slog.Handler
}

func NewHandler(sink Sink, opts *Options) *Handler {
	h := &Handler{sink: sink}
	if opts != nil {
		h.opts = *opts
	}
	return h
}

// Counts returns the delivery counts of a handler bound through the facade, or
// of a handler over an AsyncSink. Other handlers, and one not yet bound, report
// zero counts.
func (h *Handler) Counts() AsyncCounts {
	if queue := h.queue(); queue != nil {
		return queue.Counts()
	}
	return AsyncCounts{}
}

// Close drains the queue of a handler bound through the facade, or of a handler
// over an AsyncSink, until ctx is done, or for DefaultCloseTimeout when ctx has
// no deadline; see AsyncSink.Close. A facade handler
// that never bound tries to bind once more within ctx; when that fails it stops
// at once and returns the resolution error, with every queued record counted
// Failed [LOG-S11]. Handle stays fail-open under
// slog; Close is where the program sees the error. Other handlers have nothing
// to drain.
func (h *Handler) Close(ctx context.Context) (AsyncCounts, error) {
	if _, ok := ctx.Deadline(); !ok {
		bounded, cancel := context.WithTimeout(ctx, DefaultCloseTimeout)
		defer cancel()
		ctx = bounded
	}
	if adopted, ok := h.sink.(*adoptedSink); ok {
		return adopted.close(ctx)
	}
	if queue := h.queue(); queue != nil {
		return queue.Close(ctx)
	}
	return AsyncCounts{}, nil
}

func (h *Handler) queue() *AsyncSink {
	switch sink := h.sink.(type) {
	case *adoptedSink:
		return sink.async()
	case *AsyncSink:
		return sink
	}
	return nil
}

func (h *Handler) Enabled(_ context.Context, l slog.Level) bool {
	return Level(l) >= h.opts.Level
}

func (h *Handler) Handle(ctx context.Context, r slog.Record) error {
	claim := Claim(h.opts.Program)
	if h.opts.Claim != nil {
		claim = *h.opts.Claim
	}
	rec := Record{
		Schema:   SchemaVersion,
		Time:     At(r.Time),
		Level:    Level(r.Level),
		Msg:      r.Message,
		Identity: Identity{claim},
		Job:      h.opts.Job,
	}
	if r.Time.IsZero() {
		rec.Time = At(Now())
	}

	// Attributes are flattened to strings because this record crosses a language
	// boundary. JSON numbers are how an int64 quietly becomes a float64, and a
	// log line that silently rounds a byte count is worse than one that quotes
	// it.
	attrs := make(map[string]string, len(h.attrs)+r.NumAttrs())
	for k, v := range h.attrs {
		attrs[k] = v
	}
	r.Attrs(func(a slog.Attr) bool {
		put(attrs, h.groups, a)
		return true
	})
	if len(attrs) > 0 {
		rec.Attrs = attrs
	}
	if sink, ok := h.sink.(interface {
		WriteContext(context.Context, Record) error
	}); ok {
		return sink.WriteContext(ctx, rec)
	}
	return h.sink.Write(rec)
}

func (h *Handler) WithAttrs(as []slog.Attr) slog.Handler {
	next := h.clone()
	for _, a := range as {
		put(next.attrs, next.groups, a)
	}
	return next
}

func (h *Handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	next := h.clone()
	next.groups = append(next.groups, name)
	return next
}

func (h *Handler) clone() *Handler {
	attrs := make(map[string]string, len(h.attrs))
	for k, v := range h.attrs {
		attrs[k] = v
	}
	return &Handler{
		sink:   h.sink,
		opts:   h.opts,
		attrs:  attrs,
		groups: append([]string(nil), h.groups...),
	}
}

// put flattens one attribute, expanding groups into dotted keys. A nested map
// would be the obvious alternative, and it is the wrong one: the value of a
// shared sink is that `grep job=abc` works, and dotted keys keep every value on
// the line that mentions it. A slog.LogValuer is resolved first, as slog's
// own handlers do, so a value that logs itself as a group flattens like one.
func put(dst map[string]string, groups []string, a slog.Attr) {
	a.Value = a.Value.Resolve()
	key := a.Key
	for i := len(groups) - 1; i >= 0; i-- {
		key = groups[i] + "." + key
	}
	if a.Value.Kind() == slog.KindGroup {
		for _, g := range a.Value.Group() {
			put(dst, append(groups, a.Key), g)
		}
		return
	}
	dst[key] = fmt.Sprint(a.Value.Any())
}

// compile-time proof this is usable wherever slog.Handler is expected
var _ slog.Handler = (*Handler)(nil)
