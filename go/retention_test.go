package logging

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

func retentionRecord(msg string) Record {
	return Record{Schema: 1, Time: Timestamp{Time: time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)}, Msg: msg}
}

func retentionRecordBytes(t *testing.T, msg string) int64 {
	t.Helper()
	b, err := retentionRecord(msg).Encode()
	if err != nil {
		t.Fatal(err)
	}
	return int64(len(b))
}

func retainedBytes(t *testing.T, s *FileSink) int64 {
	t.Helper()
	info, err := os.Stat(s.Path())
	if err != nil {
		t.Fatal(err)
	}
	used := info.Size()
	entries, err := os.ReadDir(s.Path() + ".history")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if _, ok := segmentBase(entry.Name()); !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		used += info.Size()
	}
	return used
}

// Reproduce with go test . -run TestRetentionSmallLimit -v -count=1.
// The fixture writes only its temporary directory and removes it automatically.
func TestRetentionSmallLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.jsonl")
	line := retentionRecordBytes(t, "record-000")
	limit := 12 * line
	s, err := OpenRetainedFileSink(path, LogsRetention{MaxUse: limit})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, name := range []string{"asks.jsonl", "jobs.jsonl", "data.jsonl"} {
		if err := os.WriteFile(filepath.Join(filepath.Dir(path), name), []byte("owner state"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(path+".history", "other.jsonl"), []byte("unowned"), 0600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := s.Write(retentionRecord(fmt.Sprintf("record-%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	first, err := s.Read("", 1, 65536)
	if err != nil || len(first.Records) != 1 || first.Records[0].Msg != "record-000" {
		t.Fatalf("first page: %+v %v", first, err)
	}
	if err := s.Write(retentionRecord("record-002")); err != nil {
		t.Fatal(err)
	}
	next, err := s.Read(first.Next, 256, 65536)
	if err != nil || next.Outcome != wire.PageOutcomePage || len(next.Records) != 2 || next.Records[0].Msg != "record-001" || next.Records[1].Msg != "record-002" {
		t.Fatalf("retained cursor across rotation: %+v %v", next, err)
	}
	for i := 3; i < 200; i++ {
		if err := s.Write(retentionRecord(fmt.Sprintf("record-%03d", i))); err != nil {
			t.Fatal(err)
		}
		if used := retainedBytes(t, s); used > limit {
			t.Fatalf("write %d left %d bytes, limit %d", i, used, limit)
		}
	}
	gap, err := s.Read(first.Next, 256, 65536)
	if err != nil || gap.Outcome != wire.PageOutcomeGap || len(gap.Records) != 0 || gap.Next != first.Next {
		t.Fatalf("removed history cursor: %+v %v", gap, err)
	}
	retained, err := s.Read("", 256, 65536)
	if err != nil || retained.Outcome != wire.PageOutcomePage || len(retained.Records) != 12 || retained.Records[11].Msg != "record-199" || !retained.AtEnd {
		t.Fatalf("latest bounded history: %+v %v", retained, err)
	}
	for _, name := range []string{"asks.jsonl", "jobs.jsonl", "data.jsonl"} {
		data, err := os.ReadFile(filepath.Join(filepath.Dir(path), name))
		if err != nil || string(data) != "owner state" {
			t.Fatalf("other storage %s changed: %q %v", name, data, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(path+".history", "other.jsonl"))
	if err != nil || string(data) != "unowned" {
		t.Fatalf("unowned file changed: %q %v", data, err)
	}
	t.Logf("wrote 200 records; retained %d records in %d bytes under logs_max_use=%d; old cursor=%s", len(retained.Records), retainedBytes(t, s), limit, gap.Outcome)
}

func TestRetentionRestartOwnerAndInterruptedRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.jsonl")
	policy := LogsRetention{MaxUse: 12 * retentionRecordBytes(t, "first")}
	s, err := OpenRetainedFileSink(path, policy)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if second, err := OpenRetainedFileSink(path, policy); !errors.Is(err, ErrRetentionOwner) || second != nil {
		t.Fatalf("second writer: %v %v", second, err)
	}
	for _, msg := range []string{"first", "second", "third"} {
		if err := s.Write(retentionRecord(msg)); err != nil {
			t.Fatal(err)
		}
	}
	end, err := s.Read(EndCursor, 1, 65536)
	if err != nil || end.Outcome != wire.PageOutcomePage {
		t.Fatal(end, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Model a process ending between active-file rename and replacement
	// creation. Reopening must derive the offset from the completed segment.
	name := fmt.Sprintf("%020d.jsonl", s.retention.base)
	if err := os.Rename(path, filepath.Join(path+".history", name)); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenRetainedFileSink(path, policy)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	stale, _ := reopened.Read(end.Next, 1, 65536)
	if stale.Outcome != wire.PageOutcomeGap || stale.Next != end.Next {
		t.Fatalf("restart reused cursor: %+v", stale)
	}
	page, err := reopened.Read("", 256, 65536)
	if err != nil || len(page.Records) != 3 || page.Records[2].Msg != "third" {
		t.Fatalf("restart retained history: %+v %v", page, err)
	}
	if err := reopened.Write(retentionRecord("after-restart")); err != nil {
		t.Fatal(err)
	}
	page, err = reopened.Read(page.Next, 256, 65536)
	if err != nil || len(page.Records) != 1 || page.Records[0].Msg != "after-restart" {
		t.Fatalf("recovered writer: %+v %v", page, err)
	}
}

func TestRetentionAgeBoundaryAndActiveProtection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.jsonl")
	s, err := OpenRetainedFileSink(path, LogsRetention{MaxUse: 1 << 20, MaxRetention: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	base := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	s.retention.now = func() time.Time { return base }
	start, _ := s.Read(EndCursor, 1, 65536)
	if err := s.Write(retentionRecord("expires")); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, base, base); err != nil {
		t.Fatal(err)
	}
	s.retention.now = func() time.Time { return base.Add(time.Hour - time.Nanosecond) }
	before, _ := s.f.Stat()
	if err := s.Collect(); err != nil {
		t.Fatal(err)
	}
	page, err := s.Read("", 256, 65536)
	after, _ := s.f.Stat()
	if err != nil || len(page.Records) != 1 || !os.SameFile(before, after) {
		t.Fatalf("age before boundary/active protection: %+v %v", page, err)
	}
	s.retention.now = func() time.Time { return base.Add(time.Hour) }
	if err := s.Collect(); err != nil {
		t.Fatal(err)
	}
	page, err = s.Read("", 256, 65536)
	if err != nil || len(page.Records) != 0 || !page.AtEnd || retainedBytes(t, s) != 0 {
		t.Fatalf("age at boundary: %+v %v", page, err)
	}
	gap, _ := s.Read(start.Next, 256, 65536)
	if gap.Outcome != wire.PageOutcomeGap || gap.Next != start.Next {
		t.Fatalf("expired continuation: %+v", gap)
	}
	active, err := s.f.Stat()
	if err != nil || !active.Mode().IsRegular() {
		t.Fatalf("active writer lost: %v %v", active, err)
	}
	if err := s.Write(retentionRecord("still-writable")); err != nil {
		t.Fatal(err)
	}
}

func TestRetentionPolicyBoundsAndFailures(t *testing.T) {
	for _, policy := range []LogsRetention{{}, {MaxUse: -1}, {MaxUse: 1000, MaxRetention: -time.Second}} {
		if s, err := OpenRetainedFileSink(filepath.Join(t.TempDir(), "records"), policy); !errors.Is(err, ErrRetentionPolicy) || s != nil {
			t.Fatalf("bad policy %+v: %v %v", policy, s, err)
		}
	}
	path := filepath.Join(t.TempDir(), "records.jsonl")
	line := retentionRecordBytes(t, "fits")
	s, err := OpenRetainedFileSink(path, LogsRetention{MaxUse: line})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Write(retentionRecord("fits")); err != nil || retainedBytes(t, s) != line {
		t.Fatalf("exact byte boundary: %v", err)
	}
	if err := s.Write(retentionRecord("too large")); !errors.Is(err, ErrRetentionRecord) || retainedBytes(t, s) != line {
		t.Fatalf("oversize record did not preserve history: %v", err)
	}
	if err := s.SetRetention(LogsRetention{MaxUse: line - 1}); err != nil || retainedBytes(t, s) != 0 {
		t.Fatalf("smaller policy: %v", err)
	}
	if err := s.SetRetention(LogsRetention{}); !errors.Is(err, ErrRetentionPolicy) {
		t.Fatal(err)
	}
	if err := s.Write(retentionRecord("fits")); !errors.Is(err, ErrRetentionRecord) {
		t.Fatalf("invalid policy replaced earlier value: %v", err)
	}
	if err := s.SetRetention(LogsRetention{MaxUse: 12 * line}); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(retentionRecord("fits")); err != nil {
		t.Fatal(err)
	}
	if err := s.f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Collect(); err == nil {
		t.Fatalf("collection I/O error hidden: %v", err)
	}
	if err := s.Write(retentionRecord("fits")); err == nil {
		t.Fatalf("writer I/O error hidden: %v", err)
	}
	page, err := s.Read("", 1, 65536)
	if err != nil || page.Outcome != wire.PageOutcomeUnavailable || page.Next != "" {
		t.Fatalf("reader I/O error: %+v %v", page, err)
	}
}

func TestRetentionDoesNotRemoveReplacedSegments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.jsonl")
	line := retentionRecordBytes(t, "record")
	s, err := OpenRetainedFileSink(path, LogsRetention{MaxUse: 12 * line})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 3; i++ {
		if err := s.Write(retentionRecord("record")); err != nil {
			t.Fatal(err)
		}
	}
	segment := s.retention.segments[0]
	if err := s.retention.archive.Rename(segment.name, "saved-unowned"); err != nil {
		t.Fatal(err)
	}
	if err := s.retention.archive.Mkdir(segment.name, 0700); err != nil {
		t.Fatal(err)
	}
	if err := s.SetRetention(LogsRetention{MaxUse: line}); err == nil {
		t.Fatal("collection hid replaced segment")
	}
	if err := s.Write(retentionRecord("record")); err == nil {
		t.Fatal("writer grew history after failed collection")
	}
	if info, err := s.retention.archive.Lstat(segment.name); err != nil || !info.IsDir() {
		t.Fatalf("collector removed replacement directory: %v %v", info, err)
	}
	if err := s.retention.archive.Remove(segment.name); err != nil {
		t.Fatal(err)
	}
	if err := s.retention.archive.Rename("saved-unowned", segment.name); err != nil {
		t.Fatal(err)
	}
	if err := s.Collect(); err != nil || retainedBytes(t, s) > line {
		t.Fatalf("collection did not recover: %v", err)
	}
}

func TestRetentionConcurrentWritersReadersAndObservation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.jsonl")
	line := retentionRecordBytes(t, "000/000")
	limit := 12 * line
	s, err := OpenRetainedFileSink(path, LogsRetention{MaxUse: limit})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var writers, readers sync.WaitGroup
	done := make(chan struct{})
	errs := make(chan error, 8)
	for i := 0; i < 3; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			cursor := ""
			for {
				select {
				case <-done:
					return
				default:
				}
				page, err := s.Read(cursor, 1, 65536)
				if err != nil {
					errs <- err
					return
				}
				switch page.Outcome {
				case wire.PageOutcomePage:
					cursor = page.Next
				case wire.PageOutcomeGap:
					if page.Next != cursor || len(page.Records) != 0 {
						errs <- fmt.Errorf("gap advanced cursor")
						return
					}
					cursor = ""
				default:
					errs <- fmt.Errorf("concurrent read: %s", page.Outcome)
					return
				}
			}
		}()
	}
	for i := 0; i < 4; i++ {
		writers.Add(1)
		go func(writer int) {
			defer writers.Done()
			for j := 0; j < 100; j++ {
				if err := s.Write(retentionRecord(fmt.Sprintf("%03d/%03d", writer, j))); err != nil {
					errs <- err
					return
				}
			}
		}(i)
	}
	writers.Wait()
	close(done)
	readers.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if used := retainedBytes(t, s); used > limit {
		t.Fatalf("concurrent writes left %d bytes under %d", used, limit)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan wire.Page, 1)
	go func() {
		page, _ := s.ObserveContext(ctx, EndCursor, 1, 65536, 30000)
		result <- page
	}()
	waitObservers(t, s, 1)
	if err := s.Write(retentionRecord("observe")); err != nil {
		t.Fatal(err)
	}
	select {
	case page := <-result:
		if page.Outcome != wire.PageOutcomePage || len(page.Records) != 1 || page.Records[0].Msg != "observe" {
			t.Fatalf("rotation notification: %+v", page)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("retained write did not notify observer")
	}
	join := s.MaintainRetention(ctx, nil)
	cancel()
	join()
}

func TestRetentionDefaultAndUnsafeEntries(t *testing.T) {
	for _, test := range []struct {
		total uint64
		want  int64
	}{{1000, 100}, {uint64(100 << 30), 4 << 30}, {^uint64(0), 4 << 30}} {
		policy, err := defaultLogsRetention(test.total)
		if err != nil || policy.MaxUse != test.want || policy.MaxRetention != 0 {
			t.Fatalf("default(%d): %+v %v", test.total, policy, err)
		}
	}
	path := filepath.Join(t.TempDir(), "records.jsonl")
	s, err := OpenDefaultRetainedFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := DefaultLogsRetention(path)
	if err != nil || s.retention.policy != policy {
		t.Fatalf("default provider: %+v %v", policy, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(path+".history", "00000000000000000000.jsonl"), 0700); err != nil {
		t.Fatal(err)
	}
	if opened, err := OpenRetainedFileSink(path, policy); err == nil || opened != nil {
		t.Fatalf("opened directory segment: %v %v", opened, err)
	}
	if _, err := os.Stat(filepath.Join(path+".history", "00000000000000000000.jsonl")); errors.Is(err, fs.ErrNotExist) {
		t.Fatal("unsafe segment entry removed")
	}
	// Name matching is confined to generated segment basenames.
	for _, name := range []string{"1.jsonl", "../00000000000000000001.jsonl", strings.Repeat("9", 20) + ".jsonl", "asks.jsonl"} {
		if _, ok := segmentBase(name); ok {
			t.Fatalf("accepted unrelated segment %q", name)
		}
	}
}

func TestRetentionPageBudgetsCorruptionAndMonthlyRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.jsonl")
	line := retentionRecordBytes(t, "record-1")
	encoded, err := retentionRecord("record-1").Encode()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := wire.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	pageBytes := int64(len(wire.Encode(decoded)))
	s, err := OpenRetainedFileSink(path, LogsRetention{MaxUse: 12 * line})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, msg := range []string{"record-1", "record-2", "record-3"} {
		if err := s.Write(retentionRecord(msg)); err != nil {
			t.Fatal(err)
		}
	}
	page, err := s.Read("", 256, pageBytes)
	if err != nil || len(page.Records) != 1 || page.AtEnd {
		t.Fatalf("cross-segment byte budget: %+v %v", page, err)
	}
	cursor := page.Next
	page, err = s.Read(cursor, 2, 2*pageBytes)
	if err != nil || len(page.Records) != 2 || !page.AtEnd || page.Records[1].Msg != "record-3" {
		t.Fatalf("cross-segment continuation: %+v %v", page, err)
	}
	for _, request := range []struct {
		cursor         string
		records, bytes int64
		outcome        wire.PageOutcome
	}{
		{cursor, 0, line, wire.PageOutcomeInvalidRequest},
		{cursor, 1, 1, wire.PageOutcomeRecordTooLarge},
		{s.historyEpoch + ":1", 1, 65536, wire.PageOutcomeGap},
	} {
		page, err := s.Read(request.cursor, request.records, request.bytes)
		if err != nil || page.Outcome != request.outcome || page.Next != request.cursor || len(page.Records) != 0 {
			t.Fatalf("refusal %+v: %+v %v", request, page, err)
		}
	}
	// A later corrupt segment refuses the whole page without delivering a
	// partial prefix or advancing the original continuation.
	segment := s.retention.segments[1]
	if err := s.retention.archive.WriteFile(segment.name, []byte(strings.Repeat("x", int(segment.info.Size())-1)+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	page, err = s.Read("", 256, 65536)
	if err != nil || page.Outcome != wire.PageOutcomeCorrupt || len(page.Records) != 0 || page.Next != "" {
		t.Fatalf("corrupt segment did not refuse whole page: %+v %v", page, err)
	}

	monthlyPath := filepath.Join(t.TempDir(), "records.jsonl")
	monthly, err := OpenRetainedFileSink(monthlyPath, LogsRetention{MaxUse: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer monthly.Close()
	base := time.Date(2026, 10, 31, 23, 0, 0, 0, time.UTC)
	monthly.retention.now = func() time.Time { return base }
	if err := monthly.Write(retentionRecord("october")); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(monthlyPath, base, base); err != nil {
		t.Fatal(err)
	}
	monthly.retention.now = func() time.Time { return base.Add(2 * time.Hour) }
	if err := monthly.Write(retentionRecord("november")); err != nil {
		t.Fatal(err)
	}
	if len(monthly.retention.segments) != 1 {
		t.Fatalf("monthly rotation produced %d segments", len(monthly.retention.segments))
	}
	page, err = monthly.Read("", 256, 65536)
	if err != nil || len(page.Records) != 2 || page.Records[0].Msg != "october" || page.Records[1].Msg != "november" {
		t.Fatalf("monthly retained history: %+v %v", page, err)
	}
}

func TestRetentionAgeDoesNotAssumeMonotonicClock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.jsonl")
	line := retentionRecordBytes(t, "first01")
	s, err := OpenRetainedFileSink(path, LogsRetention{MaxUse: 12 * line, MaxRetention: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	base := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	s.retention.now = func() time.Time { return base }
	for _, record := range []struct {
		msg string
		at  time.Time
	}{
		{"first01", base.Add(2 * time.Hour)},
		{"expired", base.Add(-2 * time.Hour)},
		{"latest0", base},
	} {
		if err := s.Write(retentionRecord(record.msg)); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, record.at, record.at); err != nil {
			t.Fatal(err)
		}
	}
	page, err := s.Read("", 256, 65536)
	if err != nil || len(page.Records) != 2 || page.Records[0].Msg != "first01" || page.Records[1].Msg != "latest0" {
		t.Fatalf("age left expired segment shielded by future mtime: %+v %v", page, err)
	}
	first, _ := s.Read("", 1, 65536)
	gap, err := s.Read(first.Next, 256, 65536)
	if err != nil || gap.Outcome != wire.PageOutcomeGap || gap.Next != first.Next || len(gap.Records) != 0 {
		t.Fatalf("continuation crossed age removal silently: %+v %v", gap, err)
	}
}

func TestRetentionOwnerProcessExit(t *testing.T) {
	const helper = "OA_LOG_RETENTION_TEST_HELPER"
	if path := os.Getenv(helper); path != "" {
		s, err := OpenRetainedFileSink(path, LogsRetention{MaxUse: 4096})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if err := s.Write(retentionRecord("child-record")); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintln(os.Stdout, "ready")
		time.Sleep(time.Hour)
		return
	}
	path := filepath.Join(t.TempDir(), "records.jsonl")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestRetentionOwnerProcessExit$")
	cmd.Env = append(os.Environ(), helper+"="+path)
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		cancel()
		if !waited {
			cmd.Wait()
		}
	}()
	line, err := bufio.NewReader(output).ReadString('\n')
	if err != nil || strings.TrimSpace(line) != "ready" {
		t.Fatalf("fixture child readiness: %q %v", line, err)
	}
	if s, err := OpenRetainedFileSink(path, LogsRetention{MaxUse: 4096}); !errors.Is(err, ErrRetentionOwner) || s != nil {
		t.Fatalf("cross-process writer protection: %v %v", s, err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("killed fixture child exited cleanly")
	}
	waited = true
	s, err := OpenRetainedFileSink(path, LogsRetention{MaxUse: 4096})
	if err != nil {
		t.Fatalf("dead writer lock survived process exit: %v", err)
	}
	defer s.Close()
	page, err := s.Read("", 256, 65536)
	if err != nil || len(page.Records) != 1 || page.Records[0].Msg != "child-record" {
		t.Fatalf("process-exit recovery: %+v %v", page, err)
	}
}
