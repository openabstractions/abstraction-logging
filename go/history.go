package logging

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

// Read supplies bounded history for a service-owned append-only sink. Cursors
// belong to this open sink instance; reopening requires an explicit restart.
// This optional provider interface carries no path in its application result.
// External in-place rewriting is outside the append-only provider lifecycle.
func (s *FileSink) Read(cursor string, maxRecords, maxBytes int64) (wire.Page, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readHistoryLocked(cursor, maxRecords, maxBytes)
}
func (s *FileSink) readHistoryLocked(cursor string, maxRecords, maxBytes int64) (wire.Page, error) {
	refuse := func(outcome string) (wire.Page, error) {
		return wire.Page{Outcome: outcome, Records: []wire.Record{}, Next: cursor}, nil
	}
	if maxRecords < 1 || maxRecords > 256 || maxBytes < 1 || maxBytes > 65536 {
		return refuse(wire.PageOutcomeInvalidRequest)
	}
	if s.historyEpoch == "" {
		var key [16]byte
		if _, err := rand.Read(key[:]); err != nil {
			return refuse(wire.PageOutcomeUnavailable)
		}
		s.historyEpoch = hex.EncodeToString(key[:])
	}
	var offset int64
	if cursor != "" {
		parts := strings.Split(cursor, ":")
		if len(parts) != 2 || parts[0] != s.historyEpoch {
			return refuse(wire.PageOutcomeGap)
		}
		var err error
		offset, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil || offset < 0 || strconv.FormatInt(offset, 10) != parts[1] {
			return refuse(wire.PageOutcomeGap)
		}
	}
	current, err := s.f.Stat()
	if err != nil {
		return refuse(wire.PageOutcomeUnavailable)
	}
	f, err := os.Open(s.path)
	if err != nil {
		return refuse(wire.PageOutcomeUnavailable)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return refuse(wire.PageOutcomeUnavailable)
	}
	if !os.SameFile(current, info) || offset > info.Size() {
		return refuse(wire.PageOutcomeGap)
	}
	if offset > 0 {
		var prev [1]byte
		if _, err := f.ReadAt(prev[:], offset-1); err != nil || prev[0] != '\n' {
			return refuse(wire.PageOutcomeGap)
		}
	}
	page := wire.Page{Outcome: wire.PageOutcomePage, Records: []wire.Record{}}
	var used int64
	for offset < info.Size() && int64(len(page.Records)) < maxRecords {
		// An oversized stored line must not cause an unbounded allocation.
		remaining := min(info.Size()-offset, int64(65537))
		line, err := bufio.NewReader(io.NewSectionReader(f, offset, remaining)).ReadBytes('\n')
		if len(line) > 65536 {
			return refuse(wire.PageOutcomeRecordTooLarge)
		}
		if err != nil {
			return refuse(wire.PageOutcomeCorrupt)
		}
		record, err := wire.Decode(line)
		if err != nil {
			return refuse(wire.PageOutcomeCorrupt)
		}
		size := int64(len(wire.Encode(record)))
		if used+size > maxBytes {
			if len(page.Records) == 0 {
				return refuse(wire.PageOutcomeRecordTooLarge)
			}
			break
		}
		page.Records = append(page.Records, *record)
		used += size
		offset += int64(len(line))
	}
	page.Next = s.historyEpoch + ":" + strconv.FormatInt(offset, 10)
	page.AtEnd = offset == info.Size()
	return page, nil
}

// HistoryObserver is an optional native provider interface. Implementations
// honor ctx, bound waiting and preserve the HistoryReader cursor/page rules.
type HistoryObserver interface {
	ObserveContext(context.Context, string, int64, int64, int64) (wire.Page, error)
}

const MaxHistoryWaiters = 32

// ObserveContext waits only when a bounded read finds no records at current end.
// Channel capture and read share the writer mutex. External writers do not
// provide notifications; service-owned FileSink writes are required.
func (s *FileSink) ObserveContext(ctx context.Context, cursor string, maxRecords, maxBytes, waitMS int64) (wire.Page, error) {
	if e := ctx.Err(); e != nil {
		return wire.Page{}, e
	}
	if waitMS < 0 || waitMS > 30000 {
		return wire.Page{Outcome: wire.PageOutcomeInvalidRequest, Records: []wire.Record{}, Next: cursor}, nil
	}
	s.mu.Lock()
	if s.historyChanged == nil {
		s.historyChanged = make(chan struct{})
	}
	changed := s.historyChanged
	page, e := s.readHistoryLocked(cursor, maxRecords, maxBytes)
	if e != nil || page.Outcome != wire.PageOutcomePage || len(page.Records) > 0 || !page.AtEnd || waitMS == 0 {
		s.mu.Unlock()
		return page, e
	}
	if s.historyWaiters >= MaxHistoryWaiters {
		s.mu.Unlock()
		return wire.Page{Outcome: wire.PageOutcomeUnavailable, Records: []wire.Record{}, Next: cursor}, nil
	}
	s.historyWaiters++
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.historyWaiters--; s.mu.Unlock() }()
	timer := time.NewTimer(time.Duration(waitMS) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return wire.Page{}, ctx.Err()
	case <-changed:
	case <-timer.C:
	}
	if e = ctx.Err(); e != nil {
		return wire.Page{}, e
	}
	return s.Read(cursor, maxRecords, maxBytes)
}
