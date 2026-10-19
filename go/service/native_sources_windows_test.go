//go:build windows

package service

import (
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf16"

	"golang.org/x/sys/windows"
)

type fixtureWindowsRunner struct {
	outputs map[string][]byte
	args    [][]string
}

func (r *fixtureWindowsRunner) CommandPath(string) (string, error) {
	return "C:\\Windows\\System32\\wevtutil.exe", nil
}
func (r *fixtureWindowsRunner) Run(_ context.Context, _ string, args ...string) ([]byte, error) {
	r.args = append(r.args, append([]string(nil), args...))
	return r.outputs[strings.Join(args, " ")], nil
}

func windowsEventFixture(id int, message string) string {
	return fmt.Sprintf(`<Event><System><Provider Name="fixture-provider"/><EventID>7</EventID><Level>3</Level><Task>9</Task><TimeCreated SystemTime="2026-09-29T10:11:12.0000000Z"/><EventRecordID>%d</EventRecordID><Channel>Application</Channel><Execution ProcessID="42"/><Security UserID="S-1-fixture"/></System><EventData><Data Name="Message">%s</Data></EventData></Event>`, id, message)
}

func windowsEventsFixture(events ...string) []byte {
	return []byte("<Events>" + strings.Join(events, "") + "</Events>")
}

func TestWindowsEventSourceUsesFixedChannelBoundAndAnchor(t *testing.T) {
	runner := &fixtureWindowsRunner{outputs: map[string][]byte{}}
	source := &windowsEventSource{id: "windows/application", channel: "Application", runner: runner, path: "wevtutil.exe"}
	firstArgs := []string{"qe", "Application", "/c:2", "/rd:false", "/f:xml", "/e:Events", "/uni:true"}
	runner.outputs[strings.Join(firstArgs, " ")] = windowsEventsFixture(windowsEventFixture(1, "one"), windowsEventFixture(2, "two"))
	page, err := source.Read(context.Background(), "", 1, 65536)
	if err != nil || page.Outcome.String() != "page" || len(page.Entries) != 1 || page.Entries[0].Record.Msg != "Windows event 7: one" || page.AtEnd {
		t.Fatalf("first page %+v, %v", page, err)
	}
	id, hash, err := decodeWindowsEventCursor(page.Next)
	if err != nil || id != 1 || len(hash) != 64 {
		t.Fatalf("cursor %q: %d %q %v", page.Next, id, hash, err)
	}
	anchorArgs := []string{"qe", "Application", "/q:*[System[EventRecordID=1]]", "/c:1", "/rd:false", "/f:xml", "/e:Events", "/uni:true"}
	nextArgs := []string{"qe", "Application", "/q:*[System[EventRecordID>1]]", "/c:2", "/rd:false", "/f:xml", "/e:Events", "/uni:true"}
	runner.outputs[strings.Join(anchorArgs, " ")] = windowsEventsFixture(windowsEventFixture(1, "one"))
	runner.outputs[strings.Join(nextArgs, " ")] = windowsEventsFixture(windowsEventFixture(2, "two"))
	page, err = source.Read(context.Background(), page.Next, 1, 65536)
	if err != nil || len(page.Entries) != 1 || page.Entries[0].Record.Msg != "Windows event 7: two" || !page.AtEnd {
		t.Fatalf("continued page %+v, %v", page, err)
	}
	if strings.Contains(strings.Join(runner.args[0], " "), "/q:") {
		t.Fatalf("untrusted query on initial read: %v", runner.args[0])
	}
}

func TestWindowsCurrentCursorUsesNewestSingleEventOrEmptyBaseline(t *testing.T) {
	runner := &fixtureWindowsRunner{outputs: map[string][]byte{}}
	source := &windowsEventSource{id: "windows/application", channel: "Application", runner: runner, path: "wevtutil.exe"}
	args := []string{"qe", "Application", "/c:1", "/rd:true", "/f:xml", "/e:Events", "/uni:true"}
	runner.outputs[strings.Join(args, " ")] = windowsEventsFixture(windowsEventFixture(37, "latest"))
	cursor, err := source.CurrentCursor(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, _, err := decodeWindowsEventCursor(cursor)
	if err != nil || id != 37 {
		t.Fatalf("current cursor %q, id %d, err %v", cursor, id, err)
	}
	if len(runner.args) != 1 || strings.Join(runner.args[0], " ") != strings.Join(args, " ") {
		t.Fatalf("current position query was not reverse and bounded: %v", runner.args)
	}
	runner.outputs[strings.Join(args, " ")] = windowsEventsFixture()
	empty, err := source.CurrentCursor(context.Background())
	if err != nil || empty != "" {
		t.Fatalf("empty current cursor %q, %v", empty, err)
	}
}

func TestWindowsEventOutputDecodesUTF16AndChangedAnchorGaps(t *testing.T) {
	text := "<Events></Events>"
	units := utf16.Encode([]rune(text))
	encoded := []byte{0xff, 0xfe}
	for _, unit := range units {
		encoded = append(encoded, byte(unit), byte(unit>>8))
	}
	decoded, err := decodeWindowsCommandOutput(encoded)
	if err != nil || string(decoded) != text {
		t.Fatalf("decoded %q, %v", decoded, err)
	}
	fixture := windowsEventFixture(3, "x")
	source := &windowsEventSource{id: "windows/application", channel: "Application", runner: &fixtureWindowsRunner{outputs: map[string][]byte{}}, path: "wevtutil.exe"}
	item := windowsEvents{}
	if err := xml.Unmarshal(windowsEventsFixture(fixture), &item); err != nil {
		t.Fatal(err)
	}
	cursor := item.Events[0].cursor()
	anchorArgs := []string{"qe", "Application", "/q:*[System[EventRecordID=3]]", "/c:1", "/rd:false", "/f:xml", "/e:Events", "/uni:true"}
	runner := source.runner.(*fixtureWindowsRunner)
	runner.outputs[strings.Join(anchorArgs, " ")] = windowsEventsFixture(windowsEventFixture(3, "changed"))
	page, err := source.Read(context.Background(), cursor, 1, 65536)
	if err != nil || page.Outcome.String() != "gap" || len(page.Entries) != 0 {
		t.Fatalf("changed anchor %+v, %v", page, err)
	}
}

func TestWindowsNativeCommandIgnoresPathDecoy(t *testing.T) {
	decoyDir := t.TempDir()
	decoy := filepath.Join(decoyDir, "wevtutil.exe")
	if err := os.WriteFile(decoy, []byte("decoy"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", decoyDir)
	got, err := nativeCommandPath("wevtutil.exe")
	if err != nil {
		t.Fatal(err)
	}
	systemDir, err := windows.GetSystemDirectory()
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(systemDir, "wevtutil.exe")
	if got != want || got == decoy {
		t.Fatalf("native command path %q, want trusted system path %q", got, want)
	}
}
