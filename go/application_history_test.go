package logging

import (
	"bytes"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOwnedEmitterSurvivesPayloadShrinkAndLegacyProjection(t *testing.T) {
	s, err := OpenRetainedFileSink(filepath.Join(t.TempDir(), "rows"), LogsRetention{MaxUse: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.MaxLine = 160
	emitter := Emitter{Version: 1, Account: "owned", Program: filepath.Join(t.TempDir(), "emitter"), Key: "captured-key"}
	if err = s.WriteAttributed(retentionRecord(strings.Repeat("payload", 1000)), emitter); err != nil {
		t.Fatal(err)
	}
	scan, err := s.ScanApplicationHistory("")
	if err != nil || scan.Outcome != wire.PageOutcomePage || len(scan.Rows) != 1 {
		t.Fatalf("scan: %+v %v", scan, err)
	}
	row := scan.Rows[0]
	if row.Emitter == nil || *row.Emitter != emitter || row.InvalidEmitter || row.Record.Attrs["logging.truncated"] != "true" {
		t.Fatalf("attribution lost with shrink: %+v", row)
	}
	plain, err := s.Read("", 1, 65536)
	if err != nil || len(plain.Records) != 1 {
		t.Fatalf("legacy: %+v %v", plain, err)
	}
	if bytes.Contains(wire.Encode(&plain.Records[0]), []byte("_oa_emitter")) {
		t.Fatal("private emitter escaped public record")
	}
}

func TestApplicationPhysicalScanBoundsExcludedRows(t *testing.T) {
	s, err := OpenRetainedFileSink(filepath.Join(t.TempDir(), "rows"), LogsRetention{MaxUse: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < MaxHistoryScanRows+1; i++ {
		if err = s.Write(retentionRecord("legacy")); err != nil {
			t.Fatal(err)
		}
	}
	first, err := s.ScanApplicationHistory("")
	if err != nil || len(first.Rows) != MaxHistoryScanRows || first.AtEnd || first.Next == first.Start {
		t.Fatalf("bounded progress: %+v %v", first, err)
	}
	for _, row := range first.Rows {
		if row.Emitter != nil || row.InvalidEmitter {
			t.Fatal("legacy became attributed")
		}
	}
	second, err := s.ScanApplicationHistory(first.Next)
	if err != nil || len(second.Rows) != 1 || !second.AtEnd {
		t.Fatalf("remaining: %+v %v", second, err)
	}
}

func TestApplicationTargetRejectsCallerCapturedKeyAndQuietlyEquatesAliases(t *testing.T) {
	program := filepath.Join(t.TempDir(), "app")
	if err := os.WriteFile(program, []byte("image"), 0600); err != nil {
		t.Fatal(err)
	}
	requested := wire.EncodeApplicationLogResource(&wire.ApplicationLogResource{Program: program})
	prepared, err := PrepareApplicationLogTarget(requested)
	if err != nil || !ValidateApplicationLogTarget(prepared) {
		t.Fatalf("prepare: %s %v", prepared, err)
	}
	if _, err = PrepareApplicationLogTarget(wire.EncodeApplicationLogResource(&wire.ApplicationLogResource{Program: program, CapturedFileKey: "caller-forged"})); err == nil {
		t.Fatal("caller key admitted")
	}
	a := wire.ApplicationLogResource{Program: program, CapturedFileKey: "historical"}
	b := wire.ApplicationLogResource{Program: filepath.Join(filepath.Dir(program), "alias"), CapturedFileKey: "historical"}
	if !EqualApplicationLogTargets(EncodeApplicationResource(a), EncodeApplicationResource(b)) {
		t.Fatal("retained keys did not compare")
	}
	b.CapturedFileKey = "other"
	if EqualApplicationLogTargets(EncodeApplicationResource(a), EncodeApplicationResource(b)) {
		t.Fatal("distinct historical images matched")
	}
	// Quiet comparison keeps captured evidence even after today's image changes.
	if err = os.Remove(program); err != nil {
		t.Fatal(err)
	}
	if !ValidateApplicationLogTarget(prepared) {
		t.Fatal("historical target required filesystem")
	}
	if ValidateApplicationLogTarget([]byte(`{"program":"relative"}`)) {
		t.Fatal("relative target admitted")
	}
	// Identity's path vocabulary allows other ASCII controls. Their canonical
	// escaping must still fit this owning profile's shared target byte bound.
	oversized := wire.ApplicationLogResource{Program: filepath.Join(filepath.Dir(program), strings.Repeat("\x01", 3500))}
	if len(wire.EncodeApplicationLogResource(&oversized)) <= MaxApplicationLogTargetBytes {
		t.Fatal("target-size fixture too short")
	}
	if ValidApplicationResource(oversized) || ValidateApplicationLogTarget(wire.EncodeApplicationLogResource(&oversized)) {
		t.Fatal("oversized canonical target admitted")
	}
	if _, err := PrepareApplicationLogTarget(wire.EncodeApplicationLogResource(&oversized)); err == nil {
		t.Fatal("oversized requested target prepared")
	}
}

func TestApplicationPhysicalByteBudgetKeepsWholeContinuationRow(t *testing.T) {
	s, err := OpenRetainedFileSink(filepath.Join(t.TempDir(), "rows"), LogsRetention{MaxUse: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.MaxLine = 65536
	for i := 0; i < 24; i++ {
		if err = s.Write(retentionRecord(strings.Repeat("x", 50000))); err != nil {
			t.Fatal(err)
		}
	}
	first, err := s.ScanApplicationHistory("")
	if err != nil || first.Outcome != wire.PageOutcomePage || first.AtEnd || len(first.Rows) == 0 {
		t.Fatalf("physical limit: %+v %v", first, err)
	}
	second, err := s.ScanApplicationHistory(first.Next)
	if err != nil || second.Outcome != wire.PageOutcomePage || !second.AtEnd || len(first.Rows)+len(second.Rows) != 24 {
		t.Fatalf("split row skipped/corrupt: first=%d second=%+v %v", len(first.Rows), second, err)
	}
}

func TestSharedScanPreservesLegacyQuotaBeforeLaterCorruptRow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rows")
	s, err := OpenFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	first := retentionRecord("first")
	encoded, err := first.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Write(first); err != nil {
		t.Fatal(err)
	}
	if err = s.Write(retentionRecord(strings.Repeat("second", 100))); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteString("corrupt\n"); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	// Native and wire record encodings are compatible; use the public quota.
	record, err := wire.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	page, err := s.Read("", 256, int64(len(wire.Encode(record))))
	if err != nil || page.Outcome != wire.PageOutcomePage || len(page.Records) != 1 || page.Records[0].Msg != "first" || page.AtEnd {
		t.Fatalf("legacy read-ahead changed: %+v %v", page, err)
	}
}
