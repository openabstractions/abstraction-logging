package logging

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

const MaxHistoryScanRows = 256
const MaxHistoryScanBytes = 1 << 20

// HistoryRow is a copied physical row from the owned provider. Positions are
// native provider continuations; the receiving @2 service seals them.
type HistoryRow struct {
	Record         wire.Record
	Emitter        *Emitter
	InvalidEmitter bool
	Before, After  string
}
type HistoryScan struct {
	Outcome     wire.PageOutcome
	Rows        []HistoryRow
	Start, Next string
	AtEnd       bool
	Changed     <-chan struct{}
}

func (s *FileSink) ApplicationHistoryAvailable() bool { return s.retention != nil }

func (s *FileSink) ScanApplicationHistory(cursor string) (HistoryScan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retention == nil {
		return HistoryScan{Outcome: wire.PageOutcomeUnsupported}, nil
	}
	return s.scanHistoryLocked(cursor, MaxHistoryScanRows, MaxHistoryScanBytes, 0)
}

// WaitApplicationHistory uses the snapshot's captured signal and the existing
// provider waiter limit. No policy is called under this mutex.
func (s *FileSink) WaitApplicationHistory(ctx context.Context, changed <-chan struct{}, waitMS int64) error {
	if waitMS < 0 || waitMS > 30000 {
		return errors.New("logging: invalid wait")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.historyClosed {
		s.mu.Unlock()
		return os.ErrClosed
	}
	if s.historyWaiters >= MaxHistoryWaiters {
		s.mu.Unlock()
		return ErrHistoryWaiters
	}
	s.historyWaiters++
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.historyWaiters--; s.mu.Unlock() }()
	timer := time.NewTimer(time.Duration(waitMS) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-changed:
	case <-timer.C:
	}
	return ctx.Err()
}

var ErrHistoryWaiters = errors.New("logging: history wait capacity")

func (s *FileSink) scanHistoryLocked(cursor string, rowLimit, byteLimit int, projectionBytes int64) (HistoryScan, error) {
	refuse := func(out wire.PageOutcome) (HistoryScan, error) { return HistoryScan{Outcome: out}, nil }
	if s.historyClosed || s.f == nil {
		return refuse(wire.PageOutcomeUnavailable)
	}
	if s.historyChanged == nil {
		s.historyChanged = make(chan struct{})
	}
	if s.historyEpoch == "" {
		var key [16]byte
		if _, err := rand.Read(key[:]); err != nil {
			return refuse(wire.PageOutcomeUnavailable)
		}
		s.historyEpoch = hex.EncodeToString(key[:])
	}
	position := func(offset int64) string { return s.historyEpoch + ":" + strconv.FormatInt(offset, 10) }
	offset := int64(0)
	if cursor != "" && cursor != EndCursor {
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
	var segments []historySegment
	if s.retention != nil {
		if err := s.collectRetainedLocked(0); err != nil {
			if errors.Is(err, errRetainedHistoryChanged) || errors.Is(err, os.ErrNotExist) {
				return refuse(wire.PageOutcomeGap)
			}
			return refuse(wire.PageOutcomeUnavailable)
		}
		info, err := s.activeRetainedInfoLocked()
		if err != nil || s.retention.base > (1<<63-1)-info.Size() {
			return refuse(wire.PageOutcomeUnavailable)
		}
		segments = append(segments, s.retention.segments...)
		segments = append(segments, historySegment{s.retention.name, s.retention.base, info})
	} else {
		info, err := s.f.Stat()
		if err != nil {
			return refuse(wire.PageOutcomeUnavailable)
		}
		segments = []historySegment{{s.path, 0, info}}
	}
	earliest := segments[0].base
	last := segments[len(segments)-1]
	end := last.base + last.info.Size()
	if cursor == "" {
		offset = earliest
	} else if cursor == EndCursor {
		offset = end
	}
	if offset < earliest || offset > end {
		return refuse(wire.PageOutcomeGap)
	}
	scan := HistoryScan{Outcome: wire.PageOutcomePage, Start: position(offset), Rows: []HistoryRow{}, Changed: s.historyChanged}
	scanned := 0
	var projected int64
	for i, segment := range segments {
		segmentEnd := segment.base + segment.info.Size()
		if offset > segmentEnd || offset == segmentEnd && i < len(segments)-1 {
			continue
		}
		if offset < segment.base {
			if cursor != "" {
				return refuse(wire.PageOutcomeGap)
			}
			offset = segment.base
		}
		var f *os.File
		var err error
		if s.retention == nil {
			f, err = os.Open(s.path)
		} else if i == len(segments)-1 {
			f, err = s.retention.root.Open(segment.name)
		} else {
			f, err = s.retention.archive.Open(segment.name)
		}
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return refuse(wire.PageOutcomeGap)
			}
			return refuse(wire.PageOutcomeUnavailable)
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return refuse(wire.PageOutcomeUnavailable)
		}
		if !info.Mode().IsRegular() || !os.SameFile(info, segment.info) || s.retention != nil && info.Size() != segment.info.Size() {
			f.Close()
			return refuse(wire.PageOutcomeGap)
		}
		local := offset - segment.base
		if local > 0 {
			var prev [1]byte
			if _, err = f.ReadAt(prev[:], local-1); err != nil || prev[0] != '\n' {
				f.Close()
				return refuse(wire.PageOutcomeGap)
			}
		}
		for offset < segmentEnd && len(scan.Rows) < rowLimit {
			available := int64(byteLimit - scanned)
			if available == 0 {
				break
			}
			remaining := min(segmentEnd-offset, int64(65537), available)
			line, err := bufio.NewReader(io.NewSectionReader(f, local, remaining)).ReadBytes('\n')
			if len(line) > 65536 {
				f.Close()
				return refuse(wire.PageOutcomeRecordTooLarge)
			}
			if err != nil {
				// Exhausting this call's physical byte budget keeps the complete
				// row for the next call; even the partial read stays in the bound.
				if errors.Is(err, io.EOF) && remaining == available && remaining < segmentEnd-offset && len(scan.Rows) > 0 {
					break
				}
				f.Close()
				return refuse(wire.PageOutcomeCorrupt)
			}
			record, emitter, invalid, err := decodeStoredRow(line, s.retention != nil)
			if err != nil {
				f.Close()
				return refuse(wire.PageOutcomeCorrupt)
			}
			// The legacy unfiltered projection stops at its output quota, as
			// before @2. Scoped callers defer quotas until after authorization.
			if projectionBytes > 0 {
				size := int64(len(wire.Encode(&record)))
				if projected+size > projectionBytes {
					if len(scan.Rows) == 0 {
						f.Close()
						return refuse(wire.PageOutcomeRecordTooLarge)
					}
					break
				}
				projected += size
			}
			row := HistoryRow{Record: record, Emitter: emitter, InvalidEmitter: invalid, Before: position(offset)}
			offset += int64(len(line))
			local += int64(len(line))
			scanned += len(line)
			row.After = position(offset)
			scan.Rows = append(scan.Rows, row)
		}
		if err = f.Close(); err != nil {
			return refuse(wire.PageOutcomeUnavailable)
		}
		if offset < segmentEnd || len(scan.Rows) >= rowLimit {
			break
		}
	}
	scan.Next = position(offset)
	scan.AtEnd = offset == end
	return scan, nil
}

func (s *FileSink) readHistoryLocked(cursor string, maxRecords, maxBytes int64) (wire.Page, error) {
	refuse := func(out wire.PageOutcome) (wire.Page, error) {
		return wire.Page{Outcome: out, Records: []wire.Record{}, Next: cursor}, nil
	}
	if maxRecords < 1 || maxRecords > 256 || maxBytes < 1 || maxBytes > 65536 {
		return refuse(wire.PageOutcomeInvalidRequest)
	}
	scan, err := s.scanHistoryLocked(cursor, int(maxRecords), MaxHistoryScanBytes, maxBytes)
	if err != nil {
		return wire.Page{}, err
	}
	if scan.Outcome != wire.PageOutcomePage {
		return refuse(scan.Outcome)
	}
	page := wire.Page{Outcome: wire.PageOutcomePage, Records: []wire.Record{}, Next: scan.Next, AtEnd: scan.AtEnd}
	used := int64(0)
	for _, row := range scan.Rows {
		size := int64(len(wire.Encode(&row.Record)))
		if used+size > maxBytes {
			if len(page.Records) == 0 {
				return refuse(wire.PageOutcomeRecordTooLarge)
			}
			page.Next = row.Before
			page.AtEnd = false
			break
		}
		page.Records = append(page.Records, row.Record)
		used += size
	}
	return page, nil
}
