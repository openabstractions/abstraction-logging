package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	logging "github.com/openabstractions/abstraction-logging/go"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"golang.org/x/sys/windows"
)

func TestApplicationHistoryFixtureCapturesShortAlias(t *testing.T) {
	folder := filepath.Join(t.TempDir(), "application history long program directory")
	if err := os.Mkdir(folder, 0700); err != nil {
		t.Fatal(err)
	}
	ptr, err := windows.UTF16PtrFromString(folder)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]uint16, windows.MAX_LONG_PATH)
	n, err := windows.GetShortPathName(ptr, &buf[0], uint32(len(buf)))
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 || n >= uint32(len(buf)) {
		t.Fatal("short path did not fit")
	}
	// The image is deliberately absent: there is no file key to hide a
	// mismatch between the raw DOS alias and the captured canonical name.
	program := filepath.Join(windows.UTF16ToString(buf[:n]), "not-installed.exe")
	captured, err := logging.PrepareApplicationResource(program)
	if err != nil {
		t.Fatal(err)
	}
	if captured.Program == program {
		t.Skip("filesystem provides no distinct resolvable DOS alias")
	}
	if captured.CapturedFileKey != "" {
		t.Fatal("fixture image unexpectedly exists")
	}
	sink, reader, _, account := applicationTestHost(t, appPermit())
	appWrite(t, sink, account, program, "selected")
	scan, err := sink.ScanApplicationHistory("")
	if err != nil || len(scan.Rows) != 1 || scan.Rows[0].Emitter == nil {
		t.Fatalf("fixture scan: %+v %v", scan, err)
	}
	if got := scan.Rows[0].Emitter.Resource(); got != captured {
		t.Fatalf("fixture emitter was not captured: got %+v want %+v", got, captured)
	}
	page, err := reader.ReadContext(context.Background(), wire.ApplicationLogTarget{
		Selection: wire.ApplicationLogSelectionOneApp, Program: program,
	}, "", 1, 512)
	if err != nil || page.Outcome != wire.HistoryPageOutcomePage || len(page.Records) != 1 || page.Records[0].Msg != "selected" || !page.AtEnd {
		t.Fatalf("short alias selected page: %+v %v", page, err)
	}
}
