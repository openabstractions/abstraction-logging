package logging

import (
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestHistoryBoundsContinuationAndGap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history")
	s, err := OpenFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	write := func(msg string) {
		t.Helper()
		if err := s.Write(Record{Schema: 1, Time: Timestamp{time.Now().UTC()}, Msg: msg, Attrs: map[string]string{"unicode": "日本語\n"}}); err != nil {
			t.Fatal(err)
		}
	}
	write("first")
	write("second")
	a, err := s.Read("", 1, 65536)
	if err != nil || a.Outcome != wire.PageOutcomePage || len(a.Records) != 1 || a.Records[0].Msg != "first" || a.AtEnd {
		t.Fatalf("first: %+v %v", a, err)
	}
	b, _ := s.Read(a.Next, 1, 65536)
	if len(b.Records) != 1 || b.Records[0].Msg != "second" || !b.AtEnd {
		t.Fatalf("second: %+v", b)
	}
	empty, _ := s.Read(b.Next, 1, 65536)
	if len(empty.Records) != 0 || empty.Next != b.Next || !empty.AtEnd {
		t.Fatalf("end: %+v", empty)
	}
	write("later")
	later, _ := s.Read(b.Next, 10, 65536)
	if len(later.Records) != 1 || later.Records[0].Msg != "later" {
		t.Fatalf("append: %+v", later)
	}
	for _, cursor := range []string{"foreign:0", a.Next + "x"} {
		p, _ := s.Read(cursor, 1, 65536)
		if p.Outcome != wire.PageOutcomeGap || p.Next != cursor || len(p.Records) != 0 {
			t.Fatalf("gap: %+v", p)
		}
	}
	for _, limits := range [][2]int64{{0, 100}, {257, 100}, {1, 0}, {1, 65537}} {
		p, _ := s.Read(a.Next, limits[0], limits[1])
		if p.Outcome != wire.PageOutcomeInvalidRequest || p.Next != a.Next {
			t.Fatalf("limits: %+v", p)
		}
	}
	tiny, _ := s.Read(a.Next, 1, 1)
	if tiny.Outcome != wire.PageOutcomeRecordTooLarge || tiny.Next != a.Next {
		t.Fatalf("tiny: %+v", tiny)
	}
	s.Close()
	reopened, err := OpenFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	stale, _ := reopened.Read(b.Next, 1, 65536)
	if stale.Outcome != wire.PageOutcomeGap {
		t.Fatalf("restart reused cursor: %+v", stale)
	}
	retained, _ := reopened.Read("", 10, 65536)
	if len(retained.Records) != 3 {
		t.Fatalf("retained: %+v", retained)
	}
}

func TestHistoryCorruptionAndOversizeDoNotAdvance(t *testing.T) {
	for _, data := range []string{"partial", "{bad}\n", string(make([]byte, 70000)) + "\n"} {
		t.Run(data[:3], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "history")
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			s, err := OpenFileSink(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			p, err := s.Read("", 256, 65536)
			if err != nil || (p.Outcome != wire.PageOutcomeCorrupt && p.Outcome != wire.PageOutcomeRecordTooLarge) || len(p.Records) != 0 || p.Next != "" {
				t.Fatalf("unexpected: %+v %v", p, err)
			}
		})
	}
}
