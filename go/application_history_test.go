package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

type auditShortWriter struct{ calls int }

func (w *auditShortWriter) Write(b []byte) (int, error) { w.calls++; return len(b) - 1, nil }
func TestOwnedExactAppendShortWriteIsNotSuccess(t *testing.T) {
	w := &auditShortWriter{}
	n, err := writeLine(w, []byte("complete\n"))
	if n != 8 || !errors.Is(err, io.ErrShortWrite) || w.calls != 1 {
		t.Fatalf("short append: n=%d err=%v calls=%d", n, err, w.calls)
	}
}

func TestOwnedExactCallRecordRetainsRevision(t *testing.T) {
	s, err := OpenRetainedFileSink(filepath.Join(t.TempDir(), "rows"), LogsRetention{MaxUse: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.MaxLine = 65536
	emitter := Emitter{Version: 1, Account: "owned", Program: filepath.Join(t.TempDir(), "gateway"), Key: "captured"}
	call, err := json.Marshal(map[string]string{"subject": emitter.Program, "application": "fixture", "instance": "instance", "command": "app_apply", "revision_before": strings.Repeat("r", 65536), "revision_after": "", "outcome": "conflict", "reason": "revision", "idempotency_key": "key", "arguments_digest": "sha256:" + strings.Repeat("0", 64), "result_digest": "sha256:" + strings.Repeat("1", 64)})
	if err != nil {
		t.Fatal(err)
	}
	r := retentionRecord("abstraction.facade/appmodel@1")
	r.Attrs = map[string]string{"operation": "application.command", "call_record": string(call), "regarding": emitter.Program}
	if err = s.WriteAttributedExact(r, emitter, 4<<20); err != nil {
		t.Fatal(err)
	}
	line, err := os.ReadFile(s.Path())
	if err != nil {
		t.Fatal(err)
	}
	got, stamp, invalid, err := decodeStoredRow(line, true)
	if err != nil || invalid || stamp == nil || *stamp != emitter || got.Attrs["call_record"] != string(call) {
		t.Fatalf("exact audit lost: bytes=%d stamp=%v invalid=%v err=%v", len(line), stamp, invalid, err)
	}
}

func TestOwnedExactEnvelopeBoundaryAndOrdinaryShrink(t *testing.T) {
	s, err := OpenRetainedFileSink(filepath.Join(t.TempDir(), "rows"), LogsRetention{MaxUse: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.MaxLine = 512
	e := Emitter{Version: 1, Account: "owned", Program: filepath.Join(t.TempDir(), "gateway"), Key: "captured"}
	r := retentionRecord("audit")
	r.Attrs = map[string]string{"call_record": strings.Repeat("<&>\"\\\r\n", 10000), "regarding": e.Program}
	want, err := s.EncodeAttributedExact(r, e, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.EncodeAttributedExact(r, e, len(want)-1); !errors.Is(err, ErrLineCap) {
		t.Fatalf("envelope boundary: %v", err)
	}
	if err = s.WriteAttributedExact(r, e, len(want)-1); !errors.Is(err, ErrLineCap) {
		t.Fatalf("write boundary: %v", err)
	}
	if b, _ := os.ReadFile(s.Path()); len(b) != 0 {
		t.Fatal("rejected row wrote bytes")
	}
	w, err := WithExactEmitter(s, e, len(want))
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Write(r); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(s.Path())
	if err != nil || !sameExceptInstant(b, want) {
		t.Fatalf("encoder/append diverged: %v", err)
	}
	got, stamp, bad, err := decodeStoredRow(b, true)
	if err != nil || bad || stamp == nil || *stamp != e || !reflect.DeepEqual(got.Attrs, r.Attrs) {
		t.Fatalf("exact values/attribution: %v", err)
	}
	if err = s.WriteAttributed(r, e); err != nil {
		t.Fatal(err)
	}
	b, err = os.ReadFile(s.Path())
	if err != nil {
		t.Fatal(err)
	}
	ordinary, stamp, bad, err := decodeStoredRow(b[len(want):], true)
	if err != nil || bad || stamp == nil || *stamp != e || ordinary.Attrs["logging.truncated"] != "true" || ordinary.Attrs["call_record"] != "" {
		t.Fatalf("ordinary shrink changed: %+v %v", ordinary, err)
	}
	shared, err := OpenFileSink(filepath.Join(t.TempDir(), "shared"))
	if err != nil {
		t.Fatal(err)
	}
	defer shared.Close()
	if _, err = shared.EncodeAttributedExact(r, e, len(want)); err == nil {
		t.Fatal("shared writer accepted exact owned mode")
	}
	if _, err = WithExactEmitter(s, Emitter{}, len(want)); err == nil {
		t.Fatal("invalid emitter accepted")
	}
	if _, err = WithExactEmitter(s, e, 0); err == nil {
		t.Fatal("unbounded exact mode accepted")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.EncodeAttributedExact(r, e, len(want)); err == nil {
		t.Fatal("closed fit accepted")
	}
	if err = w.Write(r); err == nil {
		t.Fatal("closed write accepted")
	}
}

func TestOwnedExactNearFrameEscapingAndPhysicalWorkBound(t *testing.T) {
	s, err := OpenRetainedFileSink(filepath.Join(t.TempDir(), "rows"), LogsRetention{MaxUse: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := Emitter{Version: 1, Account: "owned", Program: filepath.Join(t.TempDir(), "gateway"), Key: "captured"}
	// Literal HTML-sensitive text is legal JSON in each complete near-1MiB
	// native frame. It expands in the CallRecord and again in the log attribute.
	text := strings.Repeat("<>&", ((1<<20)-1024)/3)
	request := []byte(`{"expected_revision":"` + text + `"}`)
	reply := []byte(`{"revision":"` + text + `"}`)
	if len(request) >= 1<<20 || len(reply) >= 1<<20 || !json.Valid(request) || !json.Valid(reply) {
		t.Fatal("frame fixture outside actual budgets")
	}
	call, err := json.Marshal(map[string]string{"revision_before": text, "revision_after": text, "subject": e.Program, "outcome": "conflict"})
	if err != nil {
		t.Fatal(err)
	}
	r := retentionRecord("audit")
	r.Attrs = map[string]string{"call_record": string(call), "regarding": e.Program}
	// This cap follows this fixture's actual authoritative encoder, including
	// the emitter/newline. Gate owns the general production cap derivation.
	line, err := s.EncodeAttributedExact(r, e, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(line) <= 2*(1<<20) {
		t.Fatal("nested escaping counterexample too small")
	}
	if err = s.WriteAttributedExact(r, e, len(line)); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(s.Path())
	if err != nil || !sameExceptInstant(b, line) {
		t.Fatalf("near-frame bytes changed: %v", err)
	}
	got, stamp, bad, err := decodeStoredRow(b, true)
	if err != nil || bad || stamp == nil || *stamp != e || got.Attrs["call_record"] != string(call) {
		t.Fatal("near-frame record changed")
	}
	scan, err := s.ScanApplicationHistory("")
	if err != nil || scan.Outcome != wire.PageOutcomeRecordTooLarge || len(scan.Rows) != 0 || scan.AtEnd {
		t.Fatalf("physical bound waived/corrupt: %+v %v", scan, err)
	}
	p, err := s.Read("", 256, 65536)
	if err != nil || p.Outcome != wire.PageOutcomeRecordTooLarge || p.Next != "" {
		t.Fatalf("public bound changed: %+v %v", p, err)
	}
}

func TestOwnedExactLargeRowsRotationAndContinuation(t *testing.T) {
	s, err := OpenRetainedFileSink(filepath.Join(t.TempDir(), "rows"), LogsRetention{MaxUse: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := Emitter{Version: 1, Account: "owned", Program: filepath.Join(t.TempDir(), "gateway")}
	r := retentionRecord("audit")
	r.Attrs = map[string]string{"call_record": strings.Repeat("x", 200000)}
	for i := 0; i < 7; i++ {
		if err = s.WriteAttributedExact(r, e, MaxHistoryScanBytes); err != nil {
			t.Fatal(err)
		}
	}
	first, err := s.ScanApplicationHistory("")
	if err != nil || first.Outcome != wire.PageOutcomePage || len(first.Rows) == 0 || first.AtEnd || first.Next == first.Start {
		t.Fatalf("first: %+v %v", first, err)
	}
	second, err := s.ScanApplicationHistory(first.Next)
	if err != nil || second.Outcome != wire.PageOutcomePage || !second.AtEnd || len(first.Rows)+len(second.Rows) != 7 {
		t.Fatalf("whole-row continuation: %+v %v", second, err)
	}
	for _, row := range append(first.Rows, second.Rows...) {
		if row.Emitter == nil || *row.Emitter != e || row.Record.Attrs["call_record"] != r.Attrs["call_record"] {
			t.Fatal("rotation changed row")
		}
	}
	p, err := s.Read("", 1, 65536)
	if err != nil || p.Outcome != wire.PageOutcomeRecordTooLarge {
		t.Fatalf("included oversized row: %+v %v", p, err)
	}
	// A size fit does not reserve storage or override the existing retention cap.
	s.retention.policy.MaxUse = 100
	if err = s.WriteAttributedExact(r, e, MaxHistoryScanBytes); !errors.Is(err, ErrRetentionRecord) {
		t.Fatalf("retention waived: %v", err)
	}
}

func TestOwnedExactConcurrentAppendAndUnavailableFile(t *testing.T) {
	s, err := OpenRetainedFileSink(filepath.Join(t.TempDir(), "rows"), LogsRetention{MaxUse: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := Emitter{Version: 1, Account: "owned", Program: filepath.Join(t.TempDir(), "gateway")}
	r := retentionRecord("audit")
	r.Attrs = map[string]string{"call_record": strings.Repeat("x", 70000)}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.WriteAttributedExact(r, e, MaxHistoryScanBytes); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	scan, err := s.ScanApplicationHistory("")
	if err != nil || scan.Outcome != wire.PageOutcomePage || len(scan.Rows) != 8 {
		t.Fatalf("serialized append: %+v %v", scan, err)
	}
	for _, row := range scan.Rows {
		if row.Emitter == nil || *row.Emitter != e || row.Record.Attrs["call_record"] != r.Attrs["call_record"] {
			t.Fatal("interleaved exact row")
		}
	}
	// Encoding can fit while the actual file is unavailable. Append must still
	// report that I/O failure; the fit never promises durable delivery.
	if err = s.f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.EncodeAttributedExact(r, e, MaxHistoryScanBytes); err != nil {
		t.Fatal(err)
	}
	if err = s.WriteAttributedExact(r, e, MaxHistoryScanBytes); err == nil {
		t.Fatal("unavailable file reported success")
	}
}

func TestOwnedExactPhysicalScanBoundaryIncludesEnvelope(t *testing.T) {
	s, err := OpenRetainedFileSink(filepath.Join(t.TempDir(), "rows"), LogsRetention{MaxUse: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := Emitter{Version: 1, Account: "owned", Program: filepath.Join(t.TempDir(), "gateway")}
	r := retentionRecord("audit")
	r.Attrs = map[string]string{"call_record": ""}
	empty, err := s.EncodeAttributedExact(r, e, MaxHistoryScanBytes)
	if err != nil {
		t.Fatal(err)
	}
	r.Attrs["call_record"] = strings.Repeat("x", MaxHistoryScanBytes-len(empty))
	line, err := s.EncodeAttributedExact(r, e, MaxHistoryScanBytes)
	if err != nil || len(line) != MaxHistoryScanBytes {
		t.Fatalf("boundary: %d %v", len(line), err)
	}
	if err = s.WriteAttributedExact(r, e, MaxHistoryScanBytes); err != nil {
		t.Fatal(err)
	}
	scan, err := s.ScanApplicationHistory("")
	if err != nil || scan.Outcome != wire.PageOutcomePage || len(scan.Rows) != 1 || !scan.AtEnd {
		t.Fatalf("complete boundary row: %+v %v", scan, err)
	}
}

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

// sameExceptInstant compares an appended row with its encoder's output: only
// the sink-written instant, filled in under the append lock, may differ.
func sameExceptInstant(appended, encoded []byte) bool {
	if len(appended) != len(encoded) || !bytes.HasPrefix(encoded, atPlaceholder) {
		return false
	}
	if _, ok := storedAt(appended); !ok {
		return false
	}
	masked := append([]byte(nil), appended...)
	copy(masked[len(atKey):], atZero)
	return bytes.Equal(masked, encoded)
}
