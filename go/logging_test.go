package logging

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func readAll(t *testing.T, path string) []Record {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []Record
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		r, err := DecodeRecord([]byte(line))
		if err != nil {
			t.Fatalf("undecodable line %q: %v", line, err)
		}
		out = append(out, r)
	}
	return out
}

// The integration an application actually performs: one line, and every
// slog call in the program and its libraries lands in a shared sink.
func TestSlogGoesToTheSinkUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.jsonl")
	sink, err := OpenFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(NewHandler(sink, &Options{Program: "modelget", Job: "job-1"}))

	log.Info("downloading", "url", "https://example.invalid/x.gguf", "bytes", 328597408)
	log.With("phase", "verify").Warn("digest mismatch")
	sink.Close()

	got := readAll(t, path)
	if len(got) != 2 {
		t.Fatalf("wrote %d records, want 2", len(got))
	}
	if got[0].Msg != "downloading" || got[0].Level != LevelInfo {
		t.Fatalf("first record wrong: %+v", got[0])
	}
	if got[0].Job != "job-1" {
		t.Fatal("the job id was lost; a shared sink without it cannot answer 'what happened to my download'")
	}
	// The count must survive as a count, not as a float.
	if got[0].Attrs["bytes"] != "328597408" {
		t.Fatalf("bytes = %q; JSON numbers are how an int64 quietly becomes a float64", got[0].Attrs["bytes"])
	}
	if got[1].Attrs["phase"] != "verify" || got[1].Level != LevelWarn {
		t.Fatalf("second record wrong: %+v", got[1])
	}
	claim, ok := got[0].Identity.Claimed()
	if !ok || claim.Program != "modelget" {
		t.Fatalf("hop-0 claim = %+v", got[0].Identity)
	}
	if claim.Verified {
		t.Fatal("a self-description marked itself verified")
	}
}

// Nothing vouched for these lines, and the record must say so rather than
// leaving a reader to assume the Source field means something.
func TestFileRecordsAreUntrusted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.jsonl")
	sink, _ := OpenFileSink(path)
	slog.New(NewHandler(sink, &Options{Program: "anything"})).Info("hello")
	sink.Close()

	got := readAll(t, path)
	if got[0].Trusted() {
		t.Fatal("a record written straight to a file claims to be verified")
	}
}

// Groups have to flatten, because the reason a shared sink is worth having is
// that `grep job=` works on it.
func TestGroupsFlattenToDottedKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.jsonl")
	sink, _ := OpenFileSink(path)
	log := slog.New(NewHandler(sink, &Options{Program: "p"}))
	log.WithGroup("http").With("status", 200).Info("done", slog.Group("peer", "addr", "1.2.3.4"))
	sink.Close()

	got := readAll(t, path)[0]
	if got.Attrs["http.status"] != "200" {
		t.Fatalf("attrs = %v", got.Attrs)
	}
	if got.Attrs["http.peer.addr"] != "1.2.3.4" {
		t.Fatalf("nested group did not flatten: %v", got.Attrs)
	}
}

// An oversized line would break the one property that lets several machines
// append to one file. It must shrink and say so, not vanish and not corrupt.
func TestOversizedRecordIsTruncatedNotDropped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.jsonl")
	sink, _ := OpenFileSink(path)
	sink.MaxLine = 512
	slog.New(NewHandler(sink, &Options{Program: "p"})).
		Info(strings.Repeat("x", 8000), "attr", strings.Repeat("y", 4000))
	sink.Close()

	b, _ := os.ReadFile(path)
	if len(b) > 512 {
		t.Fatalf("line is %d bytes, over the cap; concurrent writers could interleave halves", len(b))
	}
	got := readAll(t, path)[0]
	if got.Attrs["logging.truncated"] != "true" {
		t.Fatalf("truncation was not recorded: %+v", got)
	}
}

// Levels have to survive a language that does not have the same ones.
func TestLevelNamesBetweenLandmarks(t *testing.T) {
	for _, c := range []struct {
		l    Level
		want string
	}{
		{LevelInfo, "INFO"}, {LevelWarn, "WARN"}, {LevelError, "ERROR"}, {LevelDebug, "DEBUG"},
		{LevelError + 4, "ERROR+4"}, // Python CRITICAL has nowhere else to go
		{LevelInfo + 2, "INFO+2"},
	} {
		if got := c.l.String(); got != c.want {
			t.Errorf("Level(%d) = %q, want %q", c.l, got, c.want)
		}
	}
}

// Timestamps must serialise identically in every language. The job layer shipped
// a bug here because Go trimmed trailing zeros and Python did not.
func TestTimestampIsFixedWidth(t *testing.T) {
	ts := At(time.Date(2026, 9, 2, 18, 4, 5, 220000000, time.UTC))
	b, err := ts.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `"2026-09-02T18:04:05.220000Z"` {
		t.Fatalf("got %s — six digits always, or two implementations disagree about one instant", b)
	}
}

// A sink nobody configured must work and must not decide anything.
func TestUnconfiguredIsAWorkingNoop(t *testing.T) {
	os.Unsetenv(EnvSink)
	os.Unsetenv(EnvService)
	s := Auto("p")
	if _, ok := s.(DiscardSink); !ok {
		t.Fatalf("unconfigured Auto returned %T, want DiscardSink", s)
	}
	if err := s.Write(Record{Msg: "x"}); err != nil {
		t.Fatalf("discarding must not fail: %v", err)
	}
	if SeparationOf(s) != SeparationNone {
		t.Fatal("a discard sink must not claim to separate anything")
	}
}

// A daemon nobody configured must still be heard.
func TestUnconfiguredDefaultSpeaksOnStderr(t *testing.T) {
	os.Unsetenv(EnvSink)
	os.Unsetenv(EnvService)
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr := os.Stderr
	os.Stderr = w
	slog.New(Default("p")).Info("audible")
	os.Stderr = stderr
	w.Close()
	out, _ := io.ReadAll(r)
	if !strings.Contains(string(out), "audible") {
		t.Fatalf("stderr got %q", out)
	}
	t.Setenv(EnvSink, filepath.Join(t.TempDir(), "a.jsonl"))
	h, ok := Default("p").(*Handler)
	if !ok {
		t.Fatal("a configured sink was not used")
	}
	h.sink.(*FileSink).Close()
}

// A world-writable file separates nothing, whatever the path convention says.
//
// Skipped on Windows on purpose: os.FileMode cannot see an ACL there, so this
// assertion would be testing a number Windows never set. See
// separation_windows.go.
func TestSeparationReflectsTheModeNotTheName(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("file mode is not how Windows enforces access; Separation() reports none there by design")
	}
	dir := t.TempDir()
	path := PerUser(dir, "alice")
	sink, err := OpenFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	sink.Write(Record{Schema: SchemaVersion, Msg: "x"})

	if err := os.Chmod(path, 0o666); err != nil {
		t.Skip("cannot chmod here")
	}
	if got := sink.Separation(); got != SeparationNone {
		t.Fatalf("a 0666 file reports %s separation", got)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Skip("cannot chmod here")
	}
	if got := sink.Separation(); got != SeparationOwner {
		t.Fatalf("a 0600 file reports %s separation", got)
	}
}
