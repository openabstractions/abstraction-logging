package logging

import (
	"errors"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestResolvedRetentionZeroSizeAndUnlimited(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records")
	s, err := OpenResolvedRetainedFileSink(path, ResolvedLogsRetention{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	for _, msg := range []string{"first", "second"} {
		if err = s.Write(retentionRecord(msg)); err != nil {
			t.Fatal(err)
		}
	}
	page, err := s.Read("", 1, 65536)
	if err != nil || len(page.Records) != 1 {
		t.Fatalf("page: %+v %v", page, err)
	}
	usage, err := s.Usage()
	if err != nil || usage.Usage == nil || usage.Usage.MaxUse != nil || usage.Usage.MaxRetentionMs != nil {
		t.Fatalf("unlimited: %+v %v", usage, err)
	}
	zero := int64(0)
	if err = s.SetResolvedRetention(ResolvedLogsRetention{MaxUse: &zero}); err != nil {
		t.Fatal(err)
	}
	if retainedBytes(t, s) != 0 {
		t.Fatal("zero cap retained bytes")
	}
	if err = s.Write(retentionRecord("keep nothing")); err != nil {
		t.Fatalf("zero write: %v", err)
	}
	// All native write variants share the keep-nothing retention owner.
	emitter := Emitter{Version: 1, Account: "owned", Program: filepath.Join(t.TempDir(), "writer"), Key: "captured"}
	for _, write := range []func() error{
		func() error { return s.WriteAttributed(retentionRecord("attributed"), emitter) },
		func() error { return s.WriteAttributedExact(retentionRecord("exact"), emitter, 65536) },
	} {
		if err = write(); err != nil {
			t.Fatal(err)
		}
		if retainedBytes(t, s) != 0 {
			t.Fatal("zero cap retained native write bytes")
		}
	}
	gap, err := s.Read(page.Next, 1, 65536)
	if err != nil || gap.Outcome != wire.PageOutcomeGap {
		t.Fatalf("gap: %+v %v", gap, err)
	}
	usage, err = s.Usage()
	if err != nil || usage.Usage == nil || usage.Usage.MaxUse == nil || *usage.Usage.MaxUse != 0 || usage.Usage.RetainedBytes != 0 {
		t.Fatalf("zero usage: %+v %v", usage, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenResolvedRetainedFileSink(path, ResolvedLogsRetention{MaxUse: &zero})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Write(retentionRecord("keep nothing after restart")); err != nil {
		t.Fatalf("restart zero: %v", err)
	}
	if retainedBytes(t, s) != 0 {
		t.Fatal("restart zero cap retained bytes")
	}
	if err = s.SetResolvedRetention(ResolvedLogsRetention{}); err != nil {
		t.Fatal(err)
	}
	if err = s.Write(retentionRecord("unlimited again")); err != nil {
		t.Fatal(err)
	}
}

func TestResolvedRetentionZeroAgeExpiresOnWrite(t *testing.T) {
	age := time.Duration(0)
	s, err := OpenResolvedRetainedFileSink(filepath.Join(t.TempDir(), "records"), ResolvedLogsRetention{MaxRetention: &age})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Write(retentionRecord("immediately expired")); err != nil {
		t.Fatal(err)
	}
	if retainedBytes(t, s) != 0 {
		t.Fatal("zero age retained newly written bytes")
	}
	if err = s.Collect(); err != nil {
		t.Fatal(err)
	}
	page, err := s.Read("", 1, 65536)
	if err != nil || len(page.Records) != 0 {
		t.Fatalf("zero age read: %+v %v", page, err)
	}
	usage, err := s.Usage()
	if err != nil || usage.Usage == nil || usage.Usage.MaxRetentionMs == nil || *usage.Usage.MaxRetentionMs != 0 || usage.Usage.MaxUse != nil {
		t.Fatalf("zero age usage: %+v %v", usage, err)
	}
	if err = s.SetResolvedRetention(ResolvedLogsRetention{}); err != nil {
		t.Fatal(err)
	}
	if err = s.Write(retentionRecord("kept")); err != nil {
		t.Fatal(err)
	}
	if retainedBytes(t, s) == 0 {
		t.Fatal("unlimited age retained no bytes")
	}
	if err = s.SetResolvedRetention(ResolvedLogsRetention{MaxRetention: &age}); err != nil {
		t.Fatal(err)
	}
	if retainedBytes(t, s) != 0 {
		t.Fatal("zero age update retained bytes")
	}
}

func TestResolvedRetentionCopiesLimitsAndRejectsNegative(t *testing.T) {
	size, age := int64(4096), time.Hour
	s, err := OpenResolvedRetainedFileSink(filepath.Join(t.TempDir(), "records"), ResolvedLogsRetention{MaxUse: &size, MaxRetention: &age})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	size = 0
	age = 0
	usage, err := s.Usage()
	if err != nil || usage.Usage == nil || *usage.Usage.MaxUse != 4096 || *usage.Usage.MaxRetentionMs != time.Hour.Milliseconds() {
		t.Fatalf("aliased policy: %+v %v", usage, err)
	}
	negative, negativeAge := int64(-1), -time.Second
	for _, p := range []ResolvedLogsRetention{{MaxUse: &negative}, {MaxRetention: &negativeAge}} {
		if opened, err := OpenResolvedRetainedFileSink(filepath.Join(t.TempDir(), "bad"), p); !errors.Is(err, ErrRetentionPolicy) || opened != nil {
			t.Fatalf("negative open: %v", err)
		}
		if err := s.SetResolvedRetention(p); !errors.Is(err, ErrRetentionPolicy) {
			t.Fatalf("negative update: %v", err)
		}
	}
	if err = s.Write(retentionRecord("prior policy remains")); err != nil {
		t.Fatal(err)
	}
}

func TestResolvedRetentionZeroOnOpenCollectsExistingBytes(t *testing.T) {
	for _, ageBound := range []bool{false, true} {
		t.Run(map[bool]string{false: "size", true: "age"}[ageBound], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "records")
			// Even an interrupted one-byte append is retained physical history.
			if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
				t.Fatal(err)
			}
			zero, age := int64(0), time.Duration(0)
			policy := ResolvedLogsRetention{MaxUse: &zero}
			if ageBound {
				policy = ResolvedLogsRetention{MaxRetention: &age}
			}
			s, err := OpenResolvedRetainedFileSink(path, policy)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if retainedBytes(t, s) != 0 {
				t.Fatal("open retained bytes under zero bound")
			}
			usage, err := s.Usage()
			if err != nil || usage.Outcome != wire.HistoryUsageOutcomeAvailable || usage.Usage == nil || usage.Usage.RetainedBytes != 0 {
				t.Fatalf("open usage: %+v %v", usage, err)
			}
		})
	}
}

func TestResolvedRetentionZeroAgeOverridesPositiveSizeAdmission(t *testing.T) {
	one, age := int64(1), time.Duration(0)
	s, err := OpenResolvedRetainedFileSink(filepath.Join(t.TempDir(), "records"), ResolvedLogsRetention{MaxUse: &one, MaxRetention: &age})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	emitter := Emitter{Version: 1, Account: "owned", Program: filepath.Join(t.TempDir(), "writer"), Key: "captured"}
	for _, write := range []func() error{
		func() error { return s.Write(retentionRecord("ordinary")) },
		func() error { return s.WriteAttributed(retentionRecord("attributed"), emitter) },
		func() error { return s.WriteAttributedExact(retentionRecord("exact"), emitter, 65536) },
	} {
		if err = write(); err != nil {
			t.Fatal(err)
		}
		if retainedBytes(t, s) != 0 {
			t.Fatal("zero age retained mixed-limit write")
		}
	}
	usage, err := s.Usage()
	if err != nil || usage.Usage == nil || usage.Usage.MaxUse == nil || *usage.Usage.MaxUse != 1 || usage.Usage.MaxRetentionMs == nil || *usage.Usage.MaxRetentionMs != 0 {
		t.Fatalf("mixed limits: %+v %v", usage, err)
	}
	if err = s.SetResolvedRetention(ResolvedLogsRetention{MaxUse: &one}); err != nil {
		t.Fatal(err)
	}
	if err = s.Write(retentionRecord("positive-only oversized")); !errors.Is(err, ErrRetentionRecord) {
		t.Fatalf("positive size lost admission: %v", err)
	}
}
