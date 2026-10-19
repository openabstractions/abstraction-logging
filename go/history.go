package logging

import (
	"context"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"time"
)

// EndCursor is the cursor word that begins at the current end of retained
// history (wire.HistoryCursorWords).
const EndCursor = "end"

// Read supplies bounded history for a service-owned append-only sink. Cursors
// belong to this open sink instance; reopening requires an explicit restart.
// EndCursor starts at the current end and returns no records.
// This optional provider interface carries no path in its application result.
// External in-place rewriting is outside the append-only provider lifecycle.
func (s *FileSink) Read(cursor string, maxRecords, maxBytes int64) (wire.Page, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readHistoryLocked(cursor, maxRecords, maxBytes)
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
	// Reread from the position the first read resolved, so EndCursor keeps the
	// end fixed when the call began. A refusal keeps the caller's cursor.
	page, e = s.Read(page.Next, maxRecords, maxBytes)
	if e == nil && page.Outcome != wire.PageOutcomePage {
		page.Next = cursor
	}
	return page, e
}
