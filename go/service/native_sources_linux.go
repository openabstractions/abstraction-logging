//go:build linux

package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

type journalSource struct {
	id       string
	user     bool
	runner   nativeCommandRunner
	pathOnce sync.Once
	path     string
	pathErr  error
}

func nativePlatformSourceAdapters(runner nativeCommandRunner) map[string]SourceAdapter {
	return map[string]SourceAdapter{
		"linux/system-journal": &journalSource{id: "linux/system-journal", runner: runner},
		"linux/user-journal":   &journalSource{id: "linux/user-journal", user: true, runner: runner},
	}
}

func (s *journalSource) ID() string { return s.id }

func (s *journalSource) Describe(context.Context) (wire.SourceDescriptor, error) {
	if _, err := s.resolvePath(); err != nil {
		return wire.SourceDescriptor{}, err
	}
	return wire.SourceDescriptor{ID: s.id, Availability: wire.SourceAvailabilityAvailable, Limitations: []wire.SourceLimitation{
		wire.SourceLimitationRetention, wire.SourceLimitationCollectionPolicy, wire.SourceLimitationNativeAccess,
	}}, nil
}

func (s *journalSource) Read(ctx context.Context, cursor string, maxRecords, maxBytes int64) (NativePage, error) {
	path, err := s.resolvePath()
	if err != nil {
		return NativePage{}, err
	}
	limit := strconv.FormatInt(maxRecords+1, 10)
	base := []string{"--no-pager", "--quiet", "--output=json", "--utc"}
	if s.user {
		base = append(base, "--user")
	} else {
		base = append(base, "--system")
	}
	if cursor != "" {
		anchor, err := s.runner.Run(ctx, path, append(append([]string(nil), base...), "--cursor="+cursor, "--lines=+1")...)
		if err != nil {
			if errors.Is(journalCommandError(err), errJournalCursorUnavailable) {
				return NativePage{Outcome: wire.PageOutcomeGap}, nil
			}
			return NativePage{}, journalCommandError(err)
		}
		entries, _, err := parseJournal(anchor)
		if err != nil || len(entries) != 1 || entries[0].nativeCursor != cursor {
			return NativePage{Outcome: wire.PageOutcomeGap}, nil
		}
		base = append(base, "--after-cursor="+cursor)
	}
	args := append(base, "--lines=+"+limit, "--show-cursor")
	output, err := s.runner.Run(ctx, path, args...)
	if err != nil {
		if cursor != "" && errors.Is(journalCommandError(err), errJournalCursorUnavailable) {
			return NativePage{Outcome: wire.PageOutcomeGap}, nil
		}
		return NativePage{}, journalCommandError(err)
	}
	entries, _, err := parseJournal(output)
	if err != nil {
		return NativePage{Outcome: wire.PageOutcomeCorrupt}, nil
	}
	candidates := make([]nativeSourceEntry, len(entries))
	for i, entry := range entries {
		candidates[i] = nativeSourceEntry{entry: entry.entry, cursor: entry.nativeCursor}
	}
	return boundedNativePage(s.id, candidates, cursor, maxRecords, maxBytes), nil
}

func (s *journalSource) CurrentCursor(ctx context.Context) (string, error) {
	path, err := s.resolvePath()
	if err != nil {
		return "", err
	}
	args := []string{"--no-pager", "--quiet", "--output=json", "--utc"}
	if s.user {
		args = append(args, "--user")
	} else {
		args = append(args, "--system")
	}
	args = append(args, "--lines=1", "--reverse", "--show-cursor")
	output, err := s.runner.Run(ctx, path, args...)
	if err != nil {
		return "", journalCommandError(err)
	}
	entries, _, err := parseJournal(output)
	if err != nil {
		return "", err
	}
	if len(entries) == 0 {
		return "", nil
	}
	if len(entries) != 1 {
		return "", errors.New("journal current position returned multiple entries")
	}
	return entries[0].nativeCursor, nil
}

func (s *journalSource) resolvePath() (string, error) {
	s.pathOnce.Do(func() {
		if s.path != "" {
			return
		}
		path, err := s.runner.CommandPath("journalctl")
		if err != nil {
			s.pathErr = fmt.Errorf("journalctl unavailable: %w", err)
			return
		}
		s.path = path
	})
	return s.path, s.pathErr
}

type journalEntry struct {
	entry        SourceEntry
	nativeCursor string
}

func parseJournal(output []byte) ([]journalEntry, string, error) {
	var entries []journalEntry
	cursor := ""
	reader := bufio.NewScanner(bytes.NewReader(output))
	reader.Buffer(make([]byte, 64<<10), nativeCommandOutputLimit)
	for reader.Scan() {
		line := strings.TrimSpace(reader.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "-- cursor: ") {
			cursor = strings.TrimSpace(strings.TrimPrefix(line, "-- cursor: "))
			continue
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &fields); err != nil {
			return nil, "", err
		}
		get := func(key string) string {
			var value string
			_ = json.Unmarshal(fields[key], &value)
			return value
		}
		nativeCursor := get("__CURSOR")
		if nativeCursor == "" {
			return nil, "", errors.New("journal entry has no cursor")
		}
		micros, err := strconv.ParseInt(get("__REALTIME_TIMESTAMP"), 10, 64)
		if err != nil {
			return nil, "", err
		}
		priority, _ := strconv.ParseInt(get("PRIORITY"), 10, 64)
		message := get("MESSAGE")
		attrs := make(map[string]string)
		for _, name := range []string{"_HOSTNAME", "_SYSTEMD_UNIT", "_SYSTEMD_CGROUP", "SYSLOG_IDENTIFIER", "SYSLOG_FACILITY", "_TRANSPORT", "_PID", "_UID", "_GID", "_COMM", "_EXE"} {
			if value := get(name); value != "" {
				attrs[name] = value
			}
		}
		uid, _ := strconv.ParseInt(get("_UID"), 10, 64)
		gid, _ := strconv.ParseInt(get("_GID"), 10, 64)
		pid, _ := strconv.ParseInt(get("_PID"), 10, 64)
		program := get("_COMM")
		if program == "" {
			program = get("SYSLOG_IDENTIFIER")
		}
		entries = append(entries, journalEntry{
			entry: SourceEntry{Record: wire.Record{Schema: 1, Time: time.UnixMicro(micros).UTC().Format(time.RFC3339Nano), Level: priority, Msg: message, Attrs: attrs},
				Producer: &wire.ProducerClaim{Provider: get("_TRANSPORT"), Program: program, Exe: get("_EXE"), PID: pid, UID: uid, GID: gid}},
			nativeCursor: nativeCursor,
		})
	}
	if err := reader.Err(); err != nil {
		return nil, "", err
	}
	return entries, cursor, nil
}

func journalCommandError(err error) error {
	text := strings.ToLower(err.Error())
	if strings.Contains(text, "permission denied") || strings.Contains(text, "access denied") || strings.Contains(text, "not authorized") {
		return fmt.Errorf("%w: %v", ErrSourcePermissionDenied, err)
	}
	if strings.Contains(text, "failed to seek to cursor:") {
		return fmt.Errorf("%w: %v", errJournalCursorUnavailable, err)
	}
	return err
}

var errJournalCursorUnavailable = errors.New("logging source: journal cursor unavailable")
