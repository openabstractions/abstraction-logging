package logging

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	cursorcodec "github.com/openabstractions/abstraction-watch/go/cursor"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/openabstractions/abstraction-identity/fileid"
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

func (s *FileSink) scanHistoryLocked(cursor string, rowLimit, byteLimit int, projectionBytes int64) (HistoryScan, error) {
	refuse := func(out wire.PageOutcome) (HistoryScan, error) { return HistoryScan{Outcome: out}, nil }
	if s.historyClosed || s.f == nil {
		return refuse(wire.PageOutcomeUnavailable)
	}
	if s.historyEpoch == "" {
		var key [16]byte
		if _, err := rand.Read(key[:]); err != nil {
			return refuse(wire.PageOutcomeUnavailable)
		}
		s.historyEpoch = hex.EncodeToString(key[:])
	}
	position := s.historyPosition
	offset := int64(0)
	if cursor != "" && cursor != EndCursor {
		envelope, ok := (cursorcodec.Codec{Separator: ":"}).Parse(cursor)
		if !ok || !envelope.Bound(s.historyEpoch, "") {
			return refuse(wire.PageOutcomeGap)
		}
		var err error
		offset, err = strconv.ParseInt(envelope.Payload, 10, 64)
		if err != nil || offset < 0 || strconv.FormatInt(offset, 10) != envelope.Payload {
			return refuse(wire.PageOutcomeGap)
		}
	}
	segments, outcome := s.historySegmentsLocked()
	if outcome != wire.PageOutcomePage {
		return refuse(outcome)
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
	scan := HistoryScan{Outcome: wire.PageOutcomePage, Start: position(offset), Rows: []HistoryRow{}}
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
		f, err := s.openHistorySegmentLocked(segment, i == len(segments)-1)
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
		if !info.Mode().IsRegular() || !segment.heldBy(f) || s.retention != nil && info.Size() != segment.info.Size() {
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
			physicalCap := 65536
			if s.retention != nil {
				physicalCap = MaxHistoryScanBytes
			}
			remaining := min(segmentEnd-offset, int64(physicalCap)+1, available)
			line, err := bufio.NewReader(io.NewSectionReader(f, local, remaining)).ReadBytes('\n')
			if len(line) > physicalCap {
				f.Close()
				return refuse(wire.PageOutcomeRecordTooLarge)
			}
			if err != nil {
				// Exhausting this call's physical byte budget keeps the complete
				// row for the next call; even the partial read stays in the bound.
				if errors.Is(err, io.EOF) && remaining == available && remaining < segmentEnd-offset {
					if len(scan.Rows) > 0 {
						break
					}
					// No partial-row cursor is exposed. A first row beyond the
					// physical work budget is unavailable through this profile.
					f.Close()
					return refuse(wire.PageOutcomeRecordTooLarge)
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

func (s *FileSink) historyPosition(offset int64) string {
	return (cursorcodec.Codec{Separator: ":"}).Encode(cursorcodec.Envelope{Epoch: s.historyEpoch, Payload: strconv.FormatInt(offset, 10)})
}

// historySegmentsLocked lists the retained segments, oldest first, with the
// active file last.
func (s *FileSink) historySegmentsLocked() ([]historySegment, wire.PageOutcome) {
	if s.retention == nil {
		info, key, err := heldSegment(s.f)
		if err != nil {
			return nil, wire.PageOutcomeUnavailable
		}
		return []historySegment{{s.path, 0, info, key}}, wire.PageOutcomePage
	}
	if err := s.collectRetainedLocked(0); err != nil {
		if errors.Is(err, errRetainedHistoryChanged) || errors.Is(err, os.ErrNotExist) {
			return nil, wire.PageOutcomeGap
		}
		return nil, wire.PageOutcomeUnavailable
	}
	info, key, err := s.activeRetainedInfoLocked()
	if err != nil || s.retention.base > (1<<63-1)-info.Size() {
		return nil, wire.PageOutcomeUnavailable
	}
	segments := append([]historySegment(nil), s.retention.segments...)
	return append(segments, historySegment{s.retention.name, s.retention.base, info, key}), wire.PageOutcomePage
}

func (s *FileSink) openHistorySegmentLocked(segment historySegment, active bool) (*os.File, error) {
	switch {
	case s.retention == nil:
		return os.Open(s.path)
	case active:
		return s.retention.root.Open(segment.name)
	default:
		return s.retention.archive.Open(segment.name)
	}
}

// seekSlack starts a time seek this much earlier, so rows written slightly
// out of clock order around since are still scanned and filtered.
const seekSlack = time.Minute

// SeekApplicationHistory returns a native position at a row start from which a
// scan reaches every row written at or after since (LOG-P9). Retained segments
// last written before since are skipped by their modification time; inside
// the first remaining segment a bounded binary search on the sink-written
// instant (_oa_at, set under the append lock) places the position at a row
// appended more than seekSlack before since. A row without that instant, from
// an earlier runtime, is placed by its record time. Rows are still filtered by
// record time after the seek. Gap and unavailable follow the scan.
func (s *FileSink) SeekApplicationHistory(since time.Time) (string, wire.PageOutcome) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retention == nil {
		return "", wire.PageOutcomeUnsupported
	}
	if s.historyClosed || s.f == nil {
		return "", wire.PageOutcomeUnavailable
	}
	if s.historyEpoch == "" {
		var key [16]byte
		if _, err := rand.Read(key[:]); err != nil {
			return "", wire.PageOutcomeUnavailable
		}
		s.historyEpoch = hex.EncodeToString(key[:])
	}
	segments, outcome := s.historySegmentsLocked()
	if outcome != wire.PageOutcomePage {
		return "", outcome
	}
	target := since.Add(-seekSlack)
	i := 0
	for i < len(segments)-1 && segments[i].info.ModTime().Before(target) {
		i++
	}
	segment := segments[i]
	f, err := s.openHistorySegmentLocked(segment, i == len(segments)-1)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", wire.PageOutcomeGap
		}
		return "", wire.PageOutcomeUnavailable
	}
	defer f.Close()
	if !segment.heldBy(f) {
		return "", wire.PageOutcomeGap
	}
	lo, hi := int64(0), segment.info.Size()
	for hi-lo > 4096 {
		mid := lo + (hi-lo)/2
		start, at, ok := rowTimeAfter(f, mid, hi)
		if ok && at.Before(target) {
			lo = start
		} else {
			hi = mid
		}
	}
	return s.historyPosition(segment.base + lo), wire.PageOutcomePage
}

// rowTimeAfter finds the first row starting after offset-1 and before limit,
// and returns its start and sink-written instant.
func rowTimeAfter(f *os.File, offset, limit int64) (int64, time.Time, bool) {
	start := offset
	reader := bufio.NewReader(io.NewSectionReader(f, offset-1, limit-offset+1))
	if offset > 0 {
		skipped, err := reader.ReadBytes('\n')
		if err != nil {
			return 0, time.Time{}, false
		}
		start = offset - 1 + int64(len(skipped))
	}
	if start >= limit {
		return 0, time.Time{}, false
	}
	head := make([]byte, len(atPlaceholder)+1)
	n, _ := f.ReadAt(head, start)
	if at, ok := storedAt(head[:n]); ok {
		return start, at, true
	}
	// A row without a sink instant was written before this provider stamped
	// rows (an earlier runtime) or by an unattributed writer. Earlier rows can
	// only precede the first stamped row, so their record time places them.
	line, err := bufio.NewReader(io.NewSectionReader(f, start, min(limit-start, MaxHistoryScanBytes))).ReadBytes('\n')
	if err != nil {
		return 0, time.Time{}, false
	}
	var row struct {
		Time string `json:"time"`
	}
	if json.Unmarshal(line, &row) != nil {
		return 0, time.Time{}, false
	}
	at, err := time.Parse(time.RFC3339Nano, row.Time)
	return start, at, err == nil
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

// heldBy reports whether f, opened on the segment's name, holds the file the
// listing recorded. The identity owner reads both keys from open handles
// (fileid.FileKeyOf, ID-F1). A rotation or a replacement since the listing
// makes it another file, and both history readers then answer a gap.
func (segment historySegment) heldBy(f *os.File) bool {
	key, err := fileid.FileKeyOf(f)
	return err == nil && segment.key != "" && key == segment.key
}

// heldSegment reads a segment's FileInfo and file key from the one open
// handle f, so both describe the file f holds whatever its name names now.
func heldSegment(f *os.File) (os.FileInfo, string, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, "", err
	}
	key, err := fileid.FileKeyOf(f)
	if err != nil {
		return nil, "", err
	}
	return info, key, nil
}

// entryKey reads the file key of the entry name in root from a handle opened
// on it. Callers refuse a link or another non-regular entry with Lstat first.
func entryKey(root *os.Root, name string) (string, error) {
	f, err := root.Open(name)
	if err != nil {
		return "", err
	}
	defer f.Close()
	return fileid.FileKeyOf(f)
}
