package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/openabstractions/abstraction-identity/placetest"
	logging "github.com/openabstractions/abstraction-logging/go"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

func TestApplicationHistoryFixtureCapturesShortAlias(t *testing.T) {
	folder := filepath.Join(t.TempDir(), "application history long program directory")
	if err := os.Mkdir(folder, 0700); err != nil {
		t.Fatal(err)
	}
	short, err := placetest.ShortName(folder)
	if err != nil {
		t.Skip("filesystem provides no distinct resolvable DOS alias: " + err.Error())
	}
	// The image is deliberately absent: there is no file key to hide a
	// mismatch between the raw DOS alias and the captured canonical name.
	program := filepath.Join(short, "not-installed.exe")
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
