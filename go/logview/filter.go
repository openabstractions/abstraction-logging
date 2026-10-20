// Package logview narrows already-authorized record pages for display.
package logview

import (
	"errors"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type RecordFilter struct {
	level        int64
	contract     string
	tag          string
	since, until time.Time
}

// Parse uses numeric logging levels and RFC 3339 timestamps. Logging's wire
// level is an integer rather than a generated enum.
func Parse(level, tag, since, until string) (RecordFilter, error) {
	f := RecordFilter{level: -100, tag: tag}
	if len(tag) > 256 || !utf8.ValidString(tag) {
		return f, errors.New("logs: invalid tag")
	}
	if level != "" {
		switch strings.ToLower(level) {
		case "debug":
			f.level = -4
		case "info":
			f.level = 0
		case "warn":
			f.level = 4
		case "error":
			f.level = 8
		default:
			n, err := strconv.ParseInt(level, 10, 64)
			if err != nil || n < -100 || n > 100 {
				return f, errors.New("logs: invalid level")
			}
			f.level = n
		}
	}
	for _, field := range []struct {
		text string
		at   *time.Time
	}{{since, &f.since}, {until, &f.until}} {
		if field.text != "" {
			at, err := time.Parse(time.RFC3339Nano, field.text)
			if err != nil {
				return f, errors.New("logs: time filters require RFC 3339")
			}
			*field.at = at
		}
	}
	if !f.since.IsZero() && !f.until.IsZero() && f.until.Before(f.since) {
		return f, errors.New("logs: until precedes since")
	}
	return f, nil
}

// ParseContract adds a module or exact service filter to the display filters.
// It narrows authorized rows by their canonical service name in Record.Msg.
func ParseContract(contract, level, tag, since, until string) (RecordFilter, error) {
	f, err := Parse(level, tag, since, until)
	if err != nil {
		return f, err
	}
	if len(contract) > 256 || !utf8.ValidString(contract) {
		return f, errors.New("logs: invalid contract")
	}
	if contract != "" {
		for _, c := range contract {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || strings.ContainsRune("._/@-", c)) {
				return f, errors.New("logs: invalid contract")
			}
		}
		if strings.Contains(contract, "..") || strings.Contains(contract, "//") || strings.HasPrefix(contract, "/") || strings.HasSuffix(contract, "/") {
			return f, errors.New("logs: invalid contract")
		}
		if !strings.HasPrefix(contract, "abstraction.") {
			contract = "abstraction." + contract
		}
		f.contract = contract
	}
	return f, nil
}

// Keep performs display matching only. The caller identifies its existing
// sink-loss record so loss disclosure remains visible through every filter.
func (f RecordFilter) Keep(r wire.Record, lossMessage string) bool {
	if lossMessage != "" && r.Msg == lossMessage {
		return true
	}
	if f.contract != "" && r.Msg != f.contract && !strings.HasPrefix(r.Msg, f.contract+"/") {
		return false
	}
	if r.Level < f.level {
		return false
	}
	if f.tag != "" {
		key, value, hasValue := strings.Cut(f.tag, "=")
		actual, ok := r.Attrs[key]
		if !ok || hasValue && actual != value {
			return false
		}
	}
	if !f.since.IsZero() || !f.until.IsZero() {
		at, err := time.Parse(time.RFC3339Nano, r.Time)
		if err != nil || !f.since.IsZero() && at.Before(f.since) || !f.until.IsZero() && at.After(f.until) {
			return false
		}
	}
	return true
}
