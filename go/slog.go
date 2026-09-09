package logging

import (
	"context"
	"fmt"
	"log/slog"
	"os"
)

// Default is the handler a program should install: the machine's sink when
// one is configured, else text on stderr — what slog does before anyone calls
// SetDefault. An unconfigured daemon that says nothing is a defect in this
// layer, and two daemons carried the same fallback before it lived here.
func Default(program string) slog.Handler {
	sink := Auto(program)
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
//	slog.SetDefault(slog.New(logging.Default("modelget")))
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
}

func NewHandler(sink Sink, opts *Options) *Handler {
	h := &Handler{sink: sink}
	if opts != nil {
		h.opts = *opts
	}
	return h
}

func (h *Handler) Enabled(_ context.Context, l slog.Level) bool {
	return Level(l) >= h.opts.Level
}

func (h *Handler) Handle(_ context.Context, r slog.Record) error {
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
// the line that mentions it.
func put(dst map[string]string, groups []string, a slog.Attr) {
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
