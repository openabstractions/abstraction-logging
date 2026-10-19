package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

const nativeCommandOutputLimit = 8 << 20

var errNativeOutputLimit = errors.New("logging source: native command output exceeded limit")

type nativeCommandRunner interface {
	CommandPath(string) (string, error)
	Run(context.Context, string, ...string) ([]byte, error)
}

type execNativeCommandRunner struct{}

func (execNativeCommandRunner) CommandPath(name string) (string, error) {
	return nativeCommandPath(name)
}

func (execNativeCommandRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return runNativeCommand(ctx, nativeCommandEnvironment(), name, args...)
}

// protectedCommandRunner is the elevated helper's runner (LOG-N1, FAC-H1):
// the same fixed command paths, and a fixed environment built from the
// system directory, never this process's own.
type protectedCommandRunner struct{}

func (protectedCommandRunner) CommandPath(name string) (string, error) {
	return nativeCommandPath(name)
}

func (protectedCommandRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	env, err := protectedCommandEnvironment()
	if err != nil {
		return nil, err
	}
	return runNativeCommand(ctx, env, name, args...)
}

func runNativeCommand(ctx context.Context, env []string, name string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, args...)
	command.Env = env
	stdout := &cappedOutput{limit: nativeCommandOutputLimit}
	stderr := &cappedOutput{limit: 32 << 10}
	command.Stdout, command.Stderr = stdout, stderr
	err := command.Run()
	if stdout.exceeded {
		return nil, errNativeOutputLimit
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w: %s", name, err, strings.TrimSpace(string(stderr.Bytes())))
	}
	return stdout.Bytes(), nil
}

func nativeCommandEnvironment() []string {
	env := os.Environ()
	fixed := env[:0]
	for _, value := range env {
		key, _, _ := strings.Cut(value, "=")
		if !strings.EqualFold(key, "LC_ALL") {
			fixed = append(fixed, value)
		}
	}
	return append(fixed, "LC_ALL=C")
}

// cappedOutput drains all output while retaining only a bounded prefix. It
// avoids leaving a child blocked on a full pipe after the memory limit.
type cappedOutput struct {
	bytes.Buffer
	limit    int
	exceeded bool
}

func (b *cappedOutput) Write(p []byte) (int, error) {
	remaining := b.limit - b.Len()
	if remaining > 0 {
		if len(p) < remaining {
			_, _ = b.Buffer.Write(p)
		} else {
			_, _ = b.Buffer.Write(p[:remaining])
		}
	}
	if len(p) > remaining {
		b.exceeded = true
	}
	return len(p), nil
}

type unavailableSource struct{ id string }

type nativeSourceEntry struct {
	entry  SourceEntry
	cursor string
}

func boundedNativePage(sourceID string, entries []nativeSourceEntry, cursor string, maxRecords, maxBytes int64) NativePage {
	count := len(entries)
	if int64(count) > maxRecords {
		count = int(maxRecords)
	}
	page := NativePage{Outcome: wire.PageOutcomePage, Entries: make([]SourceEntry, 0, count), Next: cursor, AtEnd: len(entries) <= int(maxRecords)}
	var used int64
	for _, candidate := range entries[:count] {
		entry := candidate.entry
		if entry.Record.Attrs != nil {
			entry.Record.Attrs = withoutSensitiveFields(entry.Record.Attrs)
		}
		record := wire.SourceRecord{SourceID: sourceID, Record: entry.Record, Producer: entry.Producer}
		size := int64(wire.SourceRecordEncodedSize(&record))
		if used+size > maxBytes {
			if len(page.Entries) == 0 {
				return NativePage{Outcome: wire.PageOutcomeRecordTooLarge, Next: cursor}
			}
			page.AtEnd = false
			return page
		}
		used += size
		page.Entries = append(page.Entries, entry)
		page.Next = candidate.cursor
	}
	if len(entries) > count {
		page.AtEnd = false
	}
	return page
}

func (s unavailableSource) ID() string { return s.id }
func (s unavailableSource) Describe(context.Context) (wire.SourceDescriptor, error) {
	return wire.SourceDescriptor{ID: s.id, Availability: wire.SourceAvailabilityUnsupported, Limitations: []wire.SourceLimitation{wire.SourceLimitationVisibilityUnknown}}, nil
}
func (unavailableSource) Read(context.Context, string, int64, int64) (NativePage, error) {
	return NativePage{Outcome: wire.PageOutcomeUnsupported}, nil
}

func NativeSourceAdapters() []SourceAdapter {
	available := nativePlatformSourceAdapters(execNativeCommandRunner{})
	ids := []string{"windows/application", "windows/system", "windows/security", "linux/system-journal", "linux/user-journal", "macos/oslog"}
	result := make([]SourceAdapter, 0, len(ids))
	for _, id := range ids {
		if adapter := available[id]; adapter != nil {
			result = append(result, adapter)
		} else {
			result = append(result, unavailableSource{id: id})
		}
	}
	return result
}
