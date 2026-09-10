package logging

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestLogOversizeEmptyMessage(t *testing.T) {
	p := filepath.Join(t.TempDir(), "log")
	s, err := OpenFileSink(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.MaxLine = 256
	if err := s.Write(Record{Job: strings.Repeat("x", 1024)}); err != nil {
		return
	}
	b, _ := os.ReadFile(p)
	if len(b) > s.MaxLine {
		t.Fatalf("successful write exceeds MaxLine: %d > %d", len(b), s.MaxLine)
	}
}

func TestLogOversizeOneByteMessage(t *testing.T) {
	s, err := OpenFileSink(filepath.Join(t.TempDir(), "log"))
	if err != nil {
		t.Fatal(err)
	}
	s.MaxLine = 256
	done := make(chan error, 1)
	go func() { done <- s.Write(Record{Msg: "x", Job: strings.Repeat("x", 1024)}) }()
	// A bounded hang detector, not a timing assumption. The defect under test is
	// non-termination, which cannot be observed by waiting for it to finish; 250ms
	// is orders of magnitude above the dozen encodes a terminating shrink costs.
	select {
	case <-done:
		s.Close()
	case <-time.After(250 * time.Millisecond):
		s.Close()
		t.Fatal("Write never shrinks a one-byte message when metadata exceeds MaxLine")
	}
}

// TestShrinkStepsStrictlyNarrow is the termination proof in test form. The
// ladder is finite and every step after the mark narrows the frame; the message
// steps each remove at least one byte. Nothing else can make Write loop.
func TestShrinkStepsStrictlyNarrow(t *testing.T) {
	records := []Record{
		{Msg: strings.Repeat("x", 9000)},
		{Msg: "x", Job: strings.Repeat("x", 1024)},
		{Job: strings.Repeat("x", 1024), Identity: Identity{Claim("a"), Claim("b")}},
		{Msg: strings.Repeat("の", 500), Attrs: map[string]string{"k": strings.Repeat("v", 4000)}},
		{},
	}
	for _, r := range records {
		stages, err := degraded(r)
		if err != nil {
			t.Fatal(err)
		}
		prev := -1
		for i, st := range stages {
			n, err := frame(st)
			if err != nil {
				t.Fatal(err)
			}
			if i > 0 && n >= prev {
				t.Fatalf("shed step %d did not narrow the frame: %d after %d", i, n, prev)
			}
			prev = n
			for msg := st.Msg; msg != ""; {
				next := cut(msg, len(msg)/2)
				if len(next) >= len(msg) {
					t.Fatalf("message step did not shorten: %d after %d", len(next), len(msg))
				}
				msg = next
			}
		}
	}
}

// TestShrinkCutsOnRuneBoundary uses runes three bytes wide, at lengths and caps
// where halving lands inside one. Go's encoder substitutes U+FFFD for a split
// rune rather than refusing it, so the corruption is silent here and would be
// invalid JSON from a binding that writes the bytes it was given.
func TestShrinkCutsOnRuneBoundary(t *testing.T) {
	for _, c := range []struct{ runes, max int }{{100, 150}, {107, 150}, {107, 163}, {500, 256}} {
		msg := strings.Repeat("の", c.runes)
		p := filepath.Join(t.TempDir(), "log")
		s, err := OpenFileSink(p)
		if err != nil {
			t.Fatal(err)
		}
		s.MaxLine = c.max
		err = s.Write(Record{Msg: msg})
		s.Close()
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) > c.max {
			t.Fatalf("%d runes, cap %d: line is %d bytes", c.runes, c.max, len(b))
		}
		if !utf8.Valid(b) {
			t.Fatalf("%d runes, cap %d: line is not valid UTF-8", c.runes, c.max)
		}
		got, err := DecodeRecord(b[:len(b)-1])
		if err != nil {
			t.Fatal(err)
		}
		if got.Msg == "" || !strings.HasPrefix(msg, got.Msg) {
			t.Fatalf("%d runes, cap %d: shrunk message is not a prefix of the original: %q", c.runes, c.max, got.Msg)
		}
	}
}

func TestShrinkNeverExceedsCap(t *testing.T) {
	records := []Record{
		{},
		{Msg: "x", Job: strings.Repeat("x", 1024)},
		{Job: strings.Repeat("x", 4096)},
		{Msg: strings.Repeat("の", 4000), Job: strings.Repeat("x", 2048), Identity: Identity{Claim("a")}},
		{Attrs: map[string]string{"k": strings.Repeat("v", 8000)}},
	}
	for _, max := range []int{120, 128, 256, 512, DefaultMaxLine} {
		for i, r := range records {
			p := filepath.Join(t.TempDir(), "log")
			s, err := OpenFileSink(p)
			if err != nil {
				t.Fatal(err)
			}
			s.MaxLine = max
			err = s.Write(r)
			s.Close()
			b, readErr := os.ReadFile(p)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if err != nil {
				if !errors.Is(err, ErrLineCap) || max >= 256 {
					t.Fatalf("cap %d record %d: %v", max, i, err)
				}
				if len(b) != 0 {
					t.Fatalf("cap %d record %d: refused write left %d bytes", max, i, len(b))
				}
				continue
			}
			if len(b) > max {
				t.Fatalf("cap %d record %d: wrote %d bytes", max, i, len(b))
			}
			if !utf8.Valid(b) {
				t.Fatalf("cap %d record %d: line is not valid UTF-8", max, i)
			}
		}
	}
}

func TestCapBelowSmallestRecordRefuses(t *testing.T) {
	p := filepath.Join(t.TempDir(), "log")
	s, err := OpenFileSink(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.MaxLine = 32
	err = s.Write(Record{Msg: "x"})
	if !errors.Is(err, ErrLineCap) {
		t.Fatalf("want ErrLineCap, got %v", err)
	}
	b, _ := os.ReadFile(p)
	if len(b) != 0 {
		t.Fatalf("refused write left %d bytes behind", len(b))
	}
}
