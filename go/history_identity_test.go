package logging

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/openabstractions/abstraction-identity/fileid"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

// Reproduce with go test . -run TestHistorySegment -v -count=1.
// The fixtures write only their temporary directories.

// sealedHistory opens a retained sink whose writes have sealed at least one
// segment into the archive.
func sealedHistory(t *testing.T) *FileSink {
	t.Helper()
	path := filepath.Join(t.TempDir(), "records.jsonl")
	line := retentionRecordBytes(t, "record")
	s, err := OpenRetainedFileSink(path, LogsRetention{MaxUse: 12 * line})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	for i := 0; i < 3; i++ {
		if err := s.Write(retentionRecord("record")); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.retention.segments) == 0 {
		t.Fatal("the writes sealed no segment")
	}
	return s
}

// replaceKeepingSizeAndTime moves the file at path aside to aside and writes
// a new file at path with its exact bytes and modification time, so only
// file identity tells the two apart.
func replaceKeepingSizeAndTime(t *testing.T, path, aside string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, aside); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, before.ModTime(), before.ModTime()); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("replacement differs by size or time: %v %v %v", before, after, err)
	}
}

func TestHistorySegmentKeysComeFromTheIdentityOwner(t *testing.T) {
	s := sealedHistory(t)
	segments, outcome := s.historySegmentsLocked()
	if outcome != wire.PageOutcomePage {
		t.Fatalf("listing: %v", outcome)
	}
	for i, segment := range segments {
		f, err := s.openHistorySegmentLocked(segment, i == len(segments)-1)
		if err != nil {
			t.Fatal(err)
		}
		key, err := fileid.FileKeyOf(f)
		f.Close()
		if err != nil || segment.key == "" || segment.key != key {
			t.Fatalf("segment %s key %q, identity owner %q: %v", segment.name, segment.key, key, err)
		}
	}
}

func TestHistorySegmentReplacedWithEqualSizeAndTimeIsAGap(t *testing.T) {
	s := sealedHistory(t)
	segment := s.retention.segments[0]
	archive := s.Path() + ".history"
	replaceKeepingSizeAndTime(t, filepath.Join(archive, segment.name), filepath.Join(archive, "saved-unowned"))

	if page, err := s.Read("", 256, 65536); err != nil || page.Outcome != wire.PageOutcomeGap || len(page.Records) != 0 {
		t.Fatalf("read of a replaced segment: %+v %v", page, err)
	}
	if _, outcome := s.SeekApplicationHistory(time.Time{}); outcome != wire.PageOutcomeGap {
		t.Fatalf("seek into a replaced segment: %v", outcome)
	}
	if err := s.SetRetention(LogsRetention{MaxUse: retentionRecordBytes(t, "record")}); err == nil {
		t.Fatal("the collector removed a replaced segment")
	}
	if _, err := os.Stat(filepath.Join(archive, segment.name)); err != nil {
		t.Fatalf("the replacement is gone: %v", err)
	}
}

func TestHistorySegmentHardLinkAliasReadsOn(t *testing.T) {
	s := sealedHistory(t)
	want, err := s.Read("", 256, 65536)
	if err != nil || want.Outcome != wire.PageOutcomePage || len(want.Records) == 0 {
		t.Fatalf("first read: %+v %v", want, err)
	}
	segment := s.retention.segments[0]
	archive := s.Path() + ".history"
	name, aside := filepath.Join(archive, segment.name), filepath.Join(archive, "saved-alias")
	if err := os.Rename(name, aside); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(aside, name); err != nil {
		t.Skipf("no hard link on this volume: %v", err)
	}
	got, err := s.Read("", 256, 65536)
	if err != nil || got.Outcome != wire.PageOutcomePage || len(got.Records) != len(want.Records) {
		t.Fatalf("read through a second name of the same file: %+v %v", got, err)
	}
}

func TestHistoryActiveNameReplacedWithEqualSizeAndTimeIsAGap(t *testing.T) {
	s := sealedHistory(t)
	replaceKeepingSizeAndTime(t, s.Path(), s.Path()+".moved")

	if page, err := s.Read("", 256, 65536); err != nil || page.Outcome != wire.PageOutcomeGap {
		t.Fatalf("read with a replaced active name: %+v %v", page, err)
	}
	if err := s.Write(retentionRecord("record")); err == nil {
		t.Fatal("the writer appended after its active name was replaced")
	}
}
