package logging

import (
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"time"
)

// Usage snapshots the retained provider's owned segments and effective policy.
// Plain append-only sinks do not know an active retention budget.
func (s *FileSink) Usage() (wire.HistoryUsageResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	unavailable := wire.HistoryUsageResult{Outcome: wire.HistoryUsageOutcomeUnavailable}
	if s.retention == nil || s.historyClosed || s.f == nil {
		return unavailable, nil
	}
	if s.retention.maxRetentionBounded && s.retention.policy.MaxRetention%time.Millisecond != 0 {
		return unavailable, nil // this wire cannot report the active budget exactly
	}
	segments, outcome := s.historySegmentsLocked()
	if outcome != wire.PageOutcomePage {
		return unavailable, nil
	}
	maxUse := s.retention.policy.MaxUse
	usage := &wire.HistoryUsage{}
	if s.retention.maxUseBounded {
		usage.MaxUse = &maxUse
	}
	if s.retention.maxRetentionBounded {
		milliseconds := s.retention.policy.MaxRetention.Milliseconds()
		usage.MaxRetentionMs = &milliseconds
	}
	for _, segment := range segments {
		bytes := segment.info.Size()
		if bytes < 0 || usage.RetainedBytes > (1<<63-1)-bytes {
			return unavailable, nil
		}
		usage.RetainedBytes += bytes
	}
	if usage.RetainedBytes > 0 {
		first, err := s.scanHistoryLocked("", 1, MaxHistoryScanBytes, 0)
		if err != nil {
			return unavailable, err
		}
		if first.Outcome == wire.PageOutcomeCorrupt || first.Outcome == wire.PageOutcomeRecordTooLarge {
			return wire.HistoryUsageResult{Outcome: wire.HistoryUsageOutcomeCorrupt}, nil
		}
		if first.Outcome != wire.PageOutcomePage || len(first.Rows) != 1 {
			return unavailable, nil
		}
		usage.Oldest = first.Rows[0].Record.Time
	}
	return wire.HistoryUsageResult{Outcome: wire.HistoryUsageOutcomeAvailable, Usage: usage}, nil
}
