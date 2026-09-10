package logging

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"
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
	// producing a file that is neither valid JSON nor recoverable. Shrinking the
	// record is the honest failure: the line survives, marked. A cap too small
	// for any record at all is ErrLineCap, and nothing is written.
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

// ErrLineCap reports that no record fits: the cap is smaller than the smallest
// line this sink can encode, which is an instant, a level, a schema number and
// the shrink mark. It is returned instead of writing, because a line over the
// cap breaks the interleaving property for every other writer sharing the file,
// and it is distinguishable from an I/O failure so a caller can tell a
// misconfigured cap from a full disk.
var ErrLineCap = errors.New("logging: line cap smaller than the smallest record")

// truncated rebuilds an oversized record as a shorter one that says so. Dropping
// the line would lose the very event most likely to matter — the one carrying a
// giant error payload.
func (s *FileSink) truncated(r Record, max int) ([]byte, error) {
	stages, err := degraded(r)
	if err != nil {
		return nil, err
	}
	for _, over := range stages {
		b, err := shrunk(over, max)
		if err != nil {
			return nil, err
		}
		if b != nil {
			return b, nil
		}
	}
	return nil, fmt.Errorf("%w: cap %d", ErrLineCap, max)
}

// degraded is the ladder: the marked record, then the same record with one more
// piece of retained metadata dropped at each step. It is finite, and every step
// after the first narrows the frame strictly, so the shrink below cannot cycle.
//
// The first step may grow the record, because marking it costs bytes and an
// unmarked shrunk line would be a lie. Every step after it is taken only when it
// makes the frame strictly smaller: shedding a field to buy nothing loses
// evidence for free.
func degraded(r Record) ([]Record, error) {
	over := r
	over.Attrs = map[string]string{"logging.truncated": "true"}
	if n := len(r.Attrs); n > 0 {
		over.Attrs["logging.dropped_attrs"] = strconv.Itoa(n)
	}
	stages := []Record{over}
	shed := func(next Record) error {
		narrower, err := narrows(next, over)
		if err != nil || !narrower {
			return err
		}
		stages = append(stages, next)
		over = next
		return nil
	}
	if over.Job != "" {
		next := over
		next.Job = ""
		next.Attrs = marked(over.Attrs, "logging.dropped_job", "true")
		if err := shed(next); err != nil {
			return nil, err
		}
	}
	if len(over.Identity) > 0 {
		next := over
		next.Attrs = marked(over.Attrs, "logging.dropped_hops", strconv.Itoa(len(over.Identity)))
		next.Identity = nil
		if err := shed(next); err != nil {
			return nil, err
		}
	}
	return stages, nil
}

func marked(attrs map[string]string, key, value string) map[string]string {
	next := make(map[string]string, len(attrs)+1)
	for k, v := range attrs {
		next[k] = v
	}
	next[key] = value
	return next
}

// narrows compares frames — the encoded record without its message — so that the
// comparison is about what the step shed rather than about how long the message
// happens to be.
func narrows(a, b Record) (bool, error) {
	x, err := frame(a)
	if err != nil {
		return false, err
	}
	y, err := frame(b)
	if err != nil {
		return false, err
	}
	return x < y, nil
}

func frame(r Record) (int, error) {
	r.Msg = ""
	b, err := r.Encode()
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

// shrunk halves the message until the line fits. It returns nil when even the
// empty message does not fit inside this stage's frame, which is the caller's
// signal to shed more metadata rather than to give up.
func shrunk(over Record, max int) ([]byte, error) {
	for {
		b, err := over.Encode()
		if err != nil {
			return nil, err
		}
		if len(b) <= max {
			return b, nil
		}
		if over.Msg == "" {
			return nil, nil
		}
		over.Msg = cut(over.Msg, len(over.Msg)/2)
	}
}

// cut shortens s to at most n bytes, ending on a character boundary. Slicing at
// an arbitrary index splits a rune, and half a character is not the UTF-8 the
// record claims to be — Go's encoder hides that by substituting U+FFFD, which
// corrupts the message quietly and leaves a binding that does not substitute
// writing invalid JSON. n is always below len(s), so the result is always
// strictly shorter and the loop above always terminates.
func cut(s string, n int) string {
	if n >= len(s) {
		n = len(s) - 1
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	if n < 0 {
		return ""
	}
	return s[:n]
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
