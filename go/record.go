// Package logging is not another logging facade.
//
// Go has log/slog and Python has logging. Both are already the SLF4J shape — an
// interface applications code against, with handlers swapped underneath — and
// both are in their standard libraries. Writing a third would be inventing a
// competitor to something already standard, which is the failure this project is
// supposed to avoid.
//
// What neither of them can do is leave the process.
//
// slog.Handler is an in-process interface. Python's logging.Handler is an
// in-process interface. Neither can send a record to a supervisor on another
// machine, and neither can tell you what a NAS was doing while your laptop was
// asleep — which is precisely the situation the rest of this repository exists
// for. A download running under jobd on a Synology emits log lines that no
// slog.Handler on the PC will ever see.
//
// So this package supplies the one part that is missing: a RECORD FORMAT that
// several languages agree on, and a SINK that outlives the process writing to
// it. The facade stays whatever the language already has. Applications keep
// using slog. They gain a handler.
//
//	slog.SetDefault(slog.New(logging.NewHandler(sink, nil)))
//
// That is the whole integration. Nothing above has to learn a new logging API,
// which is the difference between an abstraction people adopt and one they are
// asked to migrate to.
package logging

import (
	"encoding/json"
	"fmt"

	job "github.com/openabstractions/abstraction-job/go"
)

// SchemaVersion is the version of the record format. It is checked on read, so
// a newer writer cannot be silently misread by an older reader — the same rule
// the job record follows, and for the same reason: these files are the
// interface between programs that were not built together.
const SchemaVersion = 1

// Level is deliberately the numeric scale slog uses (Debug -4, Info 0, Warn 4,
// Error 8) rather than an enum of names.
//
// Numbers survive translation between languages that disagree about which levels
// exist. Python has no TRACE and no FATAL; Go has no CRITICAL; java.util.logging
// has SEVERE and FINEST. A name-based format forces every binding to invent a
// mapping and get it subtly wrong. A signed integer with named landmarks lets a
// Python CRITICAL land between Error and whatever is above it without anybody
// having to agree on what to call it.
type Level int

const (
	LevelDebug Level = -4
	LevelInfo  Level = 0
	LevelWarn  Level = 4
	LevelError Level = 8
)

// String gives the nearest landmark name, with an offset when it falls between
// two — "ERROR+4" rather than a lie about which level it was.
func (l Level) String() string {
	name := func(base Level, s string) string {
		switch {
		case l == base:
			return s
		case l > base:
			return fmt.Sprintf("%s+%d", s, int(l-base))
		default:
			return fmt.Sprintf("%s-%d", s, int(base-l))
		}
	}
	switch {
	case l < LevelInfo:
		return name(LevelDebug, "DEBUG")
	case l < LevelWarn:
		return name(LevelInfo, "INFO")
	case l < LevelError:
		return name(LevelWarn, "WARN")
	default:
		return name(LevelError, "ERROR")
	}
}

// Record is one log line, in the form every language writes and every language
// reads.
//
// It is deliberately close to slog's model and to Python's LogRecord, so that
// both bindings are thin. Anything a specific logger knows that this does not is
// carried in Attrs rather than being added here — the same "opaque payload"
// discipline that keeps job.Record from having to change every time downloading
// learns something new.
type Record struct {
	Schema int       `json:"schema"`
	Time   Timestamp `json:"time"`
	Level  Level     `json:"level"`
	Msg    string    `json:"msg"`

	// Identity is who wrote this, and who says so — an ordered chain rather than
	// one answer. Hop 0 is the writer's own claim; every service that handles the
	// record appends what it could independently establish, and nothing is ever
	// removed. See identity.go.
	Identity Identity `json:"identity,omitempty"`

	// Job ties a line to a unit of work in the job store, when there is one.
	// This is the field that makes a shared sink worth having: "what was the NAS
	// doing with my download while the laptop was asleep" becomes a filter
	// rather than an archaeology exercise.
	Job string `json:"job,omitempty"`

	// Attrs are the structured key/values. Strings only, on purpose: this
	// crosses a language boundary, and JSON numbers are the classic way to turn
	// an int64 into a float64 without anybody noticing.
	Attrs map[string]string `json:"attrs,omitempty"`
}

// Timestamp is job.Timestamp. One layer says what an instant looks like on the
// wire; two said it here until 2026-09-06, and the copies had already drifted in
// how they parsed while both cited the same cross-language bug as their reason.
type Timestamp = job.Timestamp

var At = job.At

// Encode writes one line of JSON, newline terminated. One record per line is
// what makes a shared sink appendable by several processes at once and readable
// by tail, grep and every log tool already installed.
func (r Record) Encode() ([]byte, error) {
	if r.Schema == 0 {
		r.Schema = SchemaVersion
	}
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// DecodeRecord reads one line, and refuses a schema it does not understand
// rather than guessing at fields it has never seen.
func DecodeRecord(line []byte) (Record, error) {
	var r Record
	if err := json.Unmarshal(line, &r); err != nil {
		return r, err
	}
	if r.Schema != SchemaVersion {
		return r, fmt.Errorf("logging: record schema %d, this build understands %d", r.Schema, SchemaVersion)
	}
	return r, nil
}
