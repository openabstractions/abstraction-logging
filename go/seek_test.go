package logging

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

// seekSink opens a retained sink whose clock the test sets; every attributed
// row carries the clock as its sink-written instant and its record time.
func seekSink(t *testing.T, path string, maxUse int64, clock *time.Time) (*FileSink, Emitter) {
	t.Helper()
	s, err := OpenRetainedFileSink(path, LogsRetention{MaxUse: maxUse})
	if err != nil {
		t.Fatal(err)
	}
	s.retention.now = func() time.Time { return *clock }
	program := filepath.Join(filepath.Dir(path), "runtime")
	if _, err := os.Stat(program); err != nil {
		if err := os.WriteFile(program, []byte("runtime"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	resource, err := PrepareApplicationResource(program)
	if err != nil {
		t.Fatal(err)
	}
	return s, Emitter{Version: 1, Account: "account", Program: resource.Program, Key: resource.CapturedFileKey}
}

func seekWrite(t *testing.T, s *FileSink, e Emitter, msg string, recordTime time.Time) {
	t.Helper()
	if err := s.WriteAttributed(Record{Schema: 1, Time: Timestamp{Time: recordTime}, Msg: msg}, e); err != nil {
		t.Fatal(err)
	}
}

func seekTo(t *testing.T, s *FileSink, since time.Time) string {
	t.Helper()
	position, outcome := s.SeekApplicationHistory(since)
	if outcome != wire.PageOutcomePage {
		t.Fatalf("seek outcome %v", outcome)
	}
	return position
}

// The search uses the instant the sink wrote, not the writer's record time:
// a batch flushed after the failure with record times more than a minute
// older cannot move the position past the failure's row, and 600 earlier
// rows are skipped rather than walked (LOG-P9).
func TestSeekUsesTheSinkInstantAcrossOutOfOrderRows(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	clock := now.Add(-30 * time.Minute)
	s, e := seekSink(t, filepath.Join(t.TempDir(), "records.jsonl"), 8<<20, &clock)
	defer s.Close()
	for i := 0; i < 600; i++ {
		clock = now.Add(-30*time.Minute + time.Duration(i)*time.Second)
		seekWrite(t, s, e, "earlier request", clock)
	}
	clock = now.Add(-3 * time.Minute)
	seekWrite(t, s, e, "refused at 14:02", clock)
	clock = now.Add(-time.Minute)
	for i := 0; i < 300; i++ {
		seekWrite(t, s, e, "flushed batch", now.Add(-20*time.Minute))
	}
	since := now.Add(-4 * time.Minute)
	scan, err := s.ScanApplicationHistory(seekTo(t, s, since))
	if err != nil || scan.Outcome != wire.PageOutcomePage {
		t.Fatalf("scan from seek: %+v %v", scan.Outcome, err)
	}
	found := -1
	for i, row := range scan.Rows {
		if row.Record.Msg == "refused at 14:02" {
			found = i
			break
		}
	}
	if found < 0 {
		t.Fatal("the seek passed the failure's row")
	}
	if found > 64 {
		t.Fatalf("the seek walked %d earlier rows instead of skipping them", found)
	}
}

// Sealed segments last written before since are skipped by modification time;
// one written within the slack before since is kept; the position lies in the
// first kept segment, at or before its first row appended within the slack.
func TestSeekSkipsKeepsAndFindsAcrossSegments(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	clock := now.Add(-40 * time.Minute)
	dir := t.TempDir()
	path := filepath.Join(dir, "records.jsonl")
	s, e := seekSink(t, path, 64<<10, &clock)
	for i := 0; i < 360; i++ {
		clock = now.Add(-40*time.Minute + time.Duration(i)*5*time.Second)
		seekWrite(t, s, e, "request", clock)
	}
	segments := append([]historySegment(nil), s.retention.segments...)
	archive := s.retention.archive.Name()
	if len(segments) < 4 {
		t.Fatalf("only %d sealed segments", len(segments))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Each sealed segment's mtime is the instant of its last row, as a live
	// runtime would leave it.
	last := make([]time.Time, len(segments))
	for i, segment := range segments {
		f, err := os.Open(filepath.Join(archive, segment.name))
		if err != nil {
			t.Fatal(err)
		}
		lines := bufio.NewScanner(f)
		lines.Buffer(make([]byte, 0, 1<<16), 1<<20)
		for lines.Scan() {
			if at, ok := storedAt(lines.Bytes()); ok {
				last[i] = at
			}
		}
		f.Close()
		if err := os.Chtimes(filepath.Join(archive, segment.name), last[i], last[i]); err != nil {
			t.Fatal(err)
		}
	}
	clock = now
	s, _ = seekSink(t, path, 64<<10, &clock)
	defer s.Close()
	for _, c := range []struct {
		name  string
		kept  int
		since time.Time
	}{
		{"skip", 2, last[1].Add(seekSlack + time.Second)},
		{"keep in slack", 2, last[2].Add(seekSlack / 2)},
		{"find", 3, last[2].Add(last[3].Sub(last[2])/2 + seekSlack)},
	} {
		reopened := s.retention.segments
		var base, end int64 = -1, -1
		for _, segment := range reopened {
			if segment.name == segments[c.kept].name {
				base, end = segment.base, segment.base+segment.info.Size()
			}
		}
		position := seekTo(t, s, c.since)
		offset := int64(-1)
		for o := base; o <= end; o++ {
			if s.historyPosition(o) == position {
				offset = o
				break
			}
		}
		if base < 0 || offset < 0 {
			t.Fatalf("%s: position %s is not in segment %s [%d,%d)", c.name, position, segments[c.kept].name, base, end)
		}
		// No row appended within the slack before since lies before the position.
		scan, err := s.ScanApplicationHistory(s.historyPosition(base))
		if err != nil || scan.Outcome != wire.PageOutcomePage {
			t.Fatalf("%s: scan %v %v", c.name, scan.Outcome, err)
		}
		target := c.since.Add(-seekSlack)
		for _, row := range scan.Rows {
			if row.Before == position {
				break
			}
			at, err := time.Parse(time.RFC3339Nano, row.Record.Time)
			if err != nil || !at.Before(target) {
				t.Fatalf("%s: row at %s precedes the position", c.name, row.Record.Time)
			}
		}
	}
}

// After an upgrade the active segment can hold many rows an earlier runtime
// wrote without a sink instant. The search places them by record time, so a
// window after them is still reached on the first page (LOG-P9, R-05a).
func TestSeekPlacesRowsWithoutAnInstantByRecordTime(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	clock := now.Add(-30 * time.Minute)
	path := filepath.Join(t.TempDir(), "records.jsonl")
	// An earlier runtime wrote these rows: no sink instant.
	var earlier bytes.Buffer
	for i := 0; i < 600; i++ {
		line, err := Record{Schema: 1, Time: Timestamp{Time: now.Add(-2*time.Hour + time.Duration(i)*time.Second)}, Msg: "earlier runtime row"}.Encode()
		if err != nil {
			t.Fatal(err)
		}
		earlier.Write(line)
	}
	if err := os.WriteFile(path, earlier.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	s, e := seekSink(t, path, 8<<20, &clock)
	defer s.Close()
	for i := 0; i < 100; i++ {
		clock = now.Add(-30*time.Minute + time.Duration(i)*time.Second)
		seekWrite(t, s, e, "stamped request", clock)
	}
	clock = now.Add(-3 * time.Minute)
	seekWrite(t, s, e, "refused after the upgrade", clock)
	scan, err := s.ScanApplicationHistory(seekTo(t, s, now.Add(-4*time.Minute)))
	if err != nil || scan.Outcome != wire.PageOutcomePage {
		t.Fatalf("scan from seek: %v %v", scan.Outcome, err)
	}
	for _, row := range scan.Rows {
		if row.Record.Msg == "refused after the upgrade" {
			return
		}
	}
	t.Fatalf("the window was not reached on the first page of %d rows", len(scan.Rows))
}

// A reopened sink continues its instants from the last one written, so a
// restart with a slower clock cannot append rows that sort before the target.
func TestSinkInstantContinuesAcrossARestart(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	clock := now
	path := filepath.Join(t.TempDir(), "records.jsonl")
	s, e := seekSink(t, path, 8<<20, &clock)
	seekWrite(t, s, e, "before the restart", clock)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	clock = now.Add(-10 * time.Minute)
	s, e = seekSink(t, path, 8<<20, &clock)
	defer s.Close()
	seekWrite(t, s, e, "after the restart", clock)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSuffix(data, []byte("\n")), []byte("\n"))
	if len(lines) != 2 {
		t.Fatalf("%d rows", len(lines))
	}
	first, ok1 := storedAt(lines[0])
	second, ok2 := storedAt(lines[1])
	if !ok1 || !ok2 || second.Before(first) {
		t.Fatalf("instants %v then %v", first, second)
	}
}

// The encoder's row stays unchanged; the append stamps a copy.
func TestAppendLeavesTheEncodedRowUnchanged(t *testing.T) {
	clock := time.Now().UTC()
	s, e := seekSink(t, filepath.Join(t.TempDir(), "records.jsonl"), 8<<20, &clock)
	defer s.Close()
	line, err := s.EncodeAttributedExact(Record{Schema: 1, Time: Timestamp{Time: clock}, Msg: "m"}, e, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	kept := append([]byte(nil), line...)
	if err := s.appendEncoded(line); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(line, kept) {
		t.Fatal("append changed the caller's row")
	}
}

// Every row a retained sink appends carries its instant, so a row written
// through the unattributed Write path with a forged early record time cannot
// make the search pass the window (LOG-P9).
func TestSeekIgnoresForgedRecordTimesOnUnattributedRows(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	clock := now.Add(-30 * time.Minute)
	path := filepath.Join(t.TempDir(), "records.jsonl")
	s, e := seekSink(t, path, 8<<20, &clock)
	defer s.Close()
	for i := 0; i < 300; i++ {
		clock = now.Add(-30*time.Minute + time.Duration(i)*time.Second)
		seekWrite(t, s, e, "earlier request", clock)
	}
	clock = now.Add(-3 * time.Minute)
	seekWrite(t, s, e, "refused at 14:02", clock)
	clock = now.Add(-time.Minute)
	for i := 0; i < 300; i++ {
		if err := s.Write(Record{Schema: 1, Time: Timestamp{Time: now.Add(-48 * time.Hour)}, Msg: "forged early time"}); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range bytes.Split(bytes.TrimSuffix(data, []byte("\n")), []byte("\n")) {
		if _, ok := storedAt(line); !ok {
			t.Fatalf("an appended row has no sink instant: %.80s", line)
		}
	}
	scan, err := s.ScanApplicationHistory(seekTo(t, s, now.Add(-4*time.Minute)))
	if err != nil || scan.Outcome != wire.PageOutcomePage {
		t.Fatalf("scan: %v %v", scan.Outcome, err)
	}
	for _, row := range scan.Rows {
		if row.Record.Msg == "refused at 14:02" {
			return
		}
	}
	t.Fatal("the seek passed the failure's row")
}

// Seeding at Open looks back past a final row an earlier runtime wrote without
// an instant, to the newest row that has one.
func TestSinkInstantSeedSkipsAFinalRowWithoutAnInstant(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	clock := now
	path := filepath.Join(t.TempDir(), "records.jsonl")
	s, e := seekSink(t, path, 8<<20, &clock)
	seekWrite(t, s, e, "stamped", clock)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"schema":1,"time":"2026-01-01T00:00:00.000000Z","level":0,"msg":"no instant"}` + "\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	clock = now.Add(-10 * time.Minute)
	s, e = seekSink(t, path, 8<<20, &clock)
	defer s.Close()
	seekWrite(t, s, e, "after the restart", clock)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSuffix(data, []byte("\n")), []byte("\n"))
	first, ok1 := storedAt(lines[0])
	last, ok2 := storedAt(lines[len(lines)-1])
	if len(lines) != 3 || !ok1 || !ok2 || last.Before(first) {
		t.Fatalf("instants %v then %v over %d rows", first, last, len(lines))
	}
}
