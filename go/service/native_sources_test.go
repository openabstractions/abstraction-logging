package service

import (
	"bytes"
	"testing"

	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

func TestCappedOutputBoundsRetainedNativeCommandOutput(t *testing.T) {
	buffer := &cappedOutput{limit: 5}
	if n, err := buffer.Write([]byte("abcd")); n != 4 || err != nil {
		t.Fatalf("first write %d, %v", n, err)
	}
	if n, err := buffer.Write([]byte("efgh")); n != 4 || err != nil {
		t.Fatalf("overflow write %d, %v", n, err)
	}
	if !buffer.exceeded || !bytes.Equal(buffer.Bytes(), []byte("abcde")) {
		t.Fatalf("retained output %q, exceeded=%t", buffer.Bytes(), buffer.exceeded)
	}
}

func TestBoundedNativePageHonorsEncodedBytesAndCursor(t *testing.T) {
	first := SourceEntry{Record: wire.Record{Schema: 1, Time: "2026-09-29T10:11:12Z", Msg: "first"}}
	second := SourceEntry{Record: wire.Record{Schema: 1, Time: "2026-09-29T10:11:13Z", Msg: "second"}}
	firstRecord := wire.SourceRecord{SourceID: "linux/system-journal", Record: first.Record}
	firstBytes := int64(wire.SourceRecordEncodedSize(&firstRecord))
	page := boundedNativePage("linux/system-journal", []nativeSourceEntry{
		{entry: first, cursor: "c1"}, {entry: second, cursor: "c2"},
	}, "old", 2, firstBytes)
	if page.Outcome != wire.PageOutcomePage || len(page.Entries) != 1 || page.Next != "c1" || page.AtEnd {
		t.Fatalf("bounded page %+v", page)
	}
	large := boundedNativePage("linux/system-journal", []nativeSourceEntry{{entry: first, cursor: "c1"}}, "old", 2, 1)
	if large.Outcome != wire.PageOutcomeRecordTooLarge || len(large.Entries) != 0 || large.Next != "old" {
		t.Fatalf("oversized first entry %+v", large)
	}
}

func TestNativeSourceRegistryKeepsAllStableIDs(t *testing.T) {
	adapters := NativeSourceAdapters()
	service, err := NewSourceService(adapters...)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"linux/system-journal", "linux/user-journal", "macos/oslog", "windows/application", "windows/security", "windows/system"}
	got := service.IDs()
	if len(got) != len(want) {
		t.Fatalf("source catalogue IDs %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("source catalogue IDs %v, want %v", got, want)
		}
	}
}
