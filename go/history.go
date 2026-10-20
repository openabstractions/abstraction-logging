package logging

import (
	"context"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	watch "github.com/openabstractions/abstraction-watch/go"
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

// HistoryGate is the shared waiter history observers wait on.
func (s *FileSink) HistoryGate() *watch.Gate { return s.historyGate }

// ObserveContext waits only when a bounded read finds no records at current
// end, on the sink's shared waiter (watch.Gate). The waiter is armed before
// each read, so a write after the read still wakes it. External writers do not
// provide notifications; service-owned FileSink writes are required. It
// rechecks nothing; a service uses ObserveRechecked.
func (s *FileSink) ObserveContext(ctx context.Context, cursor string, maxRecords, maxBytes, waitMS int64) (wire.Page, error) {
	return s.ObserveRechecked(ctx, cursor, maxRecords, maxBytes, waitMS, func() error { return nil })
}

// ObserveRechecked is ObserveContext with the service's authority recheck,
// which runs after every read and before a page returns (WATCH-W4). Its error
// is returned as is, with no page.
func (s *FileSink) ObserveRechecked(ctx context.Context, cursor string, maxRecords, maxBytes, waitMS int64, recheck func() error) (wire.Page, error) {
	if e := ctx.Err(); e != nil {
		return wire.Page{}, e
	}
	if waitMS < 0 || waitMS > 30000 {
		return wire.Page{Outcome: wire.PageOutcomeInvalidRequest, Records: []wire.Record{}, Next: cursor}, nil
	}
	// Each reread starts from the position the previous read resolved, so
	// EndCursor keeps the end fixed when the call began.
	var page wire.Page
	position := cursor
	outcome, e := s.historyGate.Wait(ctx, time.Duration(waitMS)*time.Millisecond, watch.Probe{
		Read: func() (bool, error) {
			p, e := s.Read(position, maxRecords, maxBytes)
			if e != nil {
				return false, e
			}
			page = p
			if p.Outcome != wire.PageOutcomePage {
				return true, nil
			}
			position = p.Next
			return len(p.Records) > 0 || !p.AtEnd, nil
		},
		Recheck: recheck,
	})
	if e != nil {
		return wire.Page{}, e
	}
	switch outcome {
	case watch.Busy, watch.Failed:
		return wire.Page{Outcome: wire.PageOutcomeUnavailable, Records: []wire.Record{}, Next: cursor}, nil
	}
	// A refusal keeps the caller's cursor.
	if page.Outcome != wire.PageOutcomePage {
		page.Next = cursor
	}
	return page, nil
}
