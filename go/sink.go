package logging

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// A Sink is somewhere records go. It is the only interface in this package, and
// it is one method wide on purpose.
//
// The interesting Sinks are the ones that are not in this process: a file on a
// share that a NAS is also writing to, a system service, a supervisor. That is
// the whole reason this exists — slog.Handler and Python's logging.Handler
// already cover everything that stays inside one program.
type Sink interface {
	Write(Record) error
}

// FileSink appends records to one file, one JSON object per line.
//
// Several processes on several machines may hold the same file open at once, and
// that is the intended use rather than an edge case. It works because of one
// property: a write smaller than PIPE_BUF to a file opened O_APPEND is atomic,
// so concurrent writers interleave whole lines rather than shredding each
// other's. That is also why a record is written with a single Write call and why
// oversized records are refused instead of being split.
type FileSink struct {
	mu   sync.Mutex
	f    *os.File
	path string

	// MaxLine caps one encoded record. Beyond this the atomicity argument above
	// stops holding and concurrent writers could interleave halves of two lines,
	// producing a file that is neither valid JSON nor recoverable. Truncating
	// the message is the honest failure: the line survives, marked.
	MaxLine int
}

// DefaultMaxLine is conservative on purpose. POSIX guarantees atomicity only up
// to PIPE_BUF (4096 on Linux) for pipes; for regular files O_APPEND writes are
// atomic at the filesystem level and in practice far larger writes are safe, but
// SMB is in this path and its guarantees are weaker. 3800 leaves room for the
// framing this adds around a message.
const DefaultMaxLine = 3800

// OpenFileSink opens (creating if needed) an append-only sink.
func OpenFileSink(path string) (*FileSink, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileSink{f: f, path: path, MaxLine: DefaultMaxLine}, nil
}

func (s *FileSink) Path() string { return s.path }

func (s *FileSink) Write(r Record) error {
	b, err := r.Encode()
	if err != nil {
		return err
	}
	max := s.MaxLine
	if max <= 0 {
		max = DefaultMaxLine
	}
	if len(b) > max {
		b, err = s.truncated(r, max)
		if err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = s.f.Write(b)
	return err
}

// truncated rebuilds an oversized record as a shorter one that says so. Dropping
// the line would lose the very event most likely to matter — the one carrying a
// giant error payload.
func (s *FileSink) truncated(r Record, max int) ([]byte, error) {
	over := r
	over.Attrs = map[string]string{"logging.truncated": "true"}
	if n := len(r.Attrs); n > 0 {
		over.Attrs["logging.dropped_attrs"] = fmt.Sprint(n)
	}
	// Shrink the message until the whole line fits. Encoding is cheap and this
	// path is rare.
	msg := r.Msg
	for {
		over.Msg = msg
		b, err := over.Encode()
		if err != nil {
			return nil, err
		}
		if len(b) <= max || msg == "" {
			return b, nil
		}
		cut := len(msg) / 2
		if cut < 1 {
			cut = 1
		}
		msg = msg[:cut]
	}
}

func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}

// MultiSink writes to several sinks. A failure in one does not stop the others,
// because a log line reaching two of three places beats an exception in the
// caller's hot path.
type MultiSink []Sink

func (m MultiSink) Write(r Record) error {
	var first error
	for _, s := range m {
		if err := s.Write(r); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// DiscardSink is the default when nothing is configured.
//
// SLF4J's equivalent prints a warning and drops everything, and that is the
// right behaviour: a library that logs must never fail because the application
// chose not to configure logging. What it must also never do is decide on the
// application's behalf where the logs go.
type DiscardSink struct{}

func (DiscardSink) Write(Record) error { return nil }

// EnvSink is the discovery step: a sink configured by the environment rather
// than by the code that logs.
//
// This is the part that makes the abstraction useful across processes. A
// supervisor started by a scheduler, a container on a NAS and a CLI on a laptop
// all read the same variable and all end up appending to the same file, without
// any of them containing a path.
const EnvSink = "ABSTRACTION_LOG"

// FromEnv returns the configured sink, or DiscardSink when there is none.
// Returning a working no-op rather than an error is deliberate: not configuring
// logging is a legitimate choice, not a failure.
func FromEnv() Sink {
	path := os.Getenv(EnvSink)
	if path == "" {
		return DiscardSink{}
	}
	s, err := OpenFileSink(path)
	if err != nil {
		// Say so once, on stderr, and carry on. Refusing to start because a log
		// file could not be opened would make logging the most dangerous
		// component in the system.
		fmt.Fprintf(os.Stderr, "logging: %s=%s could not be opened (%v); logging is disabled\n", EnvSink, path, err)
		return DiscardSink{}
	}
	return s
}

// Now is a seam for tests.
var Now = func() time.Time { return time.Now() }

// PerUser returns the conventional per-user sink path under root.
//
// The separation comes from the directory's permissions, which the OS enforces,
// not from the name — the name only makes it findable. Create the directory
// 0700 and the guarantee is real; create it 0777 and this is a labelling scheme.
// Separation() will tell you which you got.
func PerUser(root, user string) string {
	return filepath.Join(root, "logs", user+".jsonl")
}
