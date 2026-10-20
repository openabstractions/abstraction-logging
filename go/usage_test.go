package logging

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

func TestUsageSnapshotsRetainedBytesBudgetAndAppendOldest(t *testing.T) {
	s, err := OpenRetainedFileSink(filepath.Join(t.TempDir(), "records"), LogsRetention{MaxUse: 1 << 20, MaxRetention: 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	empty, err := s.Usage()
	if err != nil || empty.Outcome != wire.HistoryUsageOutcomeAvailable || empty.Usage == nil || empty.Usage.RetainedBytes != 0 || empty.Usage.Oldest != "" {
		t.Fatalf("empty: %+v %v", empty, err)
	}
	first := retentionRecord("first")
	if err := s.Write(first); err != nil {
		t.Fatal(err)
	}
	second := retentionRecord("second")
	second.Time.Time = first.Time.Add(-time.Hour)
	if err := s.Write(second); err != nil {
		t.Fatal(err)
	}
	got, err := s.Usage()
	page, readErr := s.Read("", 1, 65536)
	if err != nil || readErr != nil || got.Usage == nil || len(page.Records) != 1 {
		t.Fatalf("usage/read: %+v %v %v", got, err, readErr)
	}
	if got.Usage.RetainedBytes != retainedBytes(t, s) || got.Usage.MaxUse == nil || *got.Usage.MaxUse != 1<<20 || got.Usage.MaxRetentionMs == nil || *got.Usage.MaxRetentionMs != (24*time.Hour).Milliseconds() || got.Usage.Oldest != page.Records[0].Time {
		t.Fatalf("snapshot: %+v", got.Usage)
	}
	priorBudget := got.Usage.MaxUse
	if err := s.SetRetention(LogsRetention{MaxUse: 2 << 20}); err != nil {
		t.Fatal(err)
	}
	if *priorBudget != 1<<20 {
		t.Fatal("an active policy update changed an already returned snapshot")
	}
	got, err = s.Usage()
	if err != nil || got.Usage == nil || got.Usage.MaxUse == nil || *got.Usage.MaxUse != 2<<20 || got.Usage.MaxRetentionMs != nil {
		t.Fatalf("active policy: %+v %v", got, err)
	}
}

func TestUsageWirePreservesUnlimitedAndZeroLimits(t *testing.T) {
	seven := int64(7)
	for _, limit := range []*int64{nil, new(int64), &seven} {
		value := wire.HistoryUsage{MaxUse: limit, MaxRetentionMs: limit}
		data, err := wire.ToJSON(&value)
		if err != nil {
			t.Fatal(err)
		}
		var decoded wire.HistoryUsage
		if err := wire.FromJSON(data, &decoded); err != nil {
			t.Fatal(err)
		}
		if limit == nil {
			if decoded.MaxUse != nil || decoded.MaxRetentionMs != nil {
				t.Fatalf("unlimited gained a limit: %s", data)
			}
		} else if decoded.MaxUse == nil || decoded.MaxRetentionMs == nil || *decoded.MaxUse != *limit || *decoded.MaxRetentionMs != *limit {
			t.Fatalf("present limit lost its value: %s", data)
		}
	}
}

func TestUsagePlainHistoryHasUnknownBudget(t *testing.T) {
	s, err := OpenFileSink(filepath.Join(t.TempDir(), "records"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.Usage()
	if err != nil || got.Outcome != wire.HistoryUsageOutcomeUnavailable || got.Usage != nil {
		t.Fatalf("plain: %+v %v", got, err)
	}
}

func TestUsageUnrepresentableAgeNeverBecomesZero(t *testing.T) {
	s, err := OpenRetainedFileSink(filepath.Join(t.TempDir(), "records"), LogsRetention{MaxUse: 1 << 20, MaxRetention: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.Usage()
	if err != nil || got.Outcome != wire.HistoryUsageOutcomeUnavailable || got.Usage != nil {
		t.Fatalf("fractional ms: %+v %v", got, err)
	}
}

func TestUsageCorruptFirstRetainedRowExposesNoSnapshot(t *testing.T) {
	s, err := OpenRetainedFileSink(filepath.Join(t.TempDir(), "records"), LogsRetention{MaxUse: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Write(retentionRecord("first")); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	f, openErr := s.retention.root.OpenFile(s.retention.name, os.O_WRONLY, 0)
	if openErr == nil {
		_, err = f.WriteAt([]byte("!"), 0)
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	} else {
		err = openErr
	}
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Usage()
	if err != nil || got.Outcome != wire.HistoryUsageOutcomeCorrupt || got.Usage != nil {
		t.Fatalf("corrupt: %+v %v", got, err)
	}
}
