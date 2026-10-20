package client

import (
	"github.com/openabstractions/abstraction-logging/go/internal/personfolders"
	"os"
	"path/filepath"
	"strings"
	"testing"

	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

func TestApplicationPermissionTargetLeavesCaptureToOwnerAndBoundsEncoding(t *testing.T) {
	program := filepath.Join(t.TempDir(), "requested")
	target, err := ApplicationLogPermissionTarget(program)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := wire.DecodeApplicationLogResource(target)
	if err != nil || doc.Program != program || doc.CapturedFileKey != "" {
		t.Fatalf("request gained captured authority: %+v %v", doc, err)
	}
	oversized := filepath.Join(t.TempDir(), strings.Repeat("\x01", 4000))
	if _, err := ApplicationLogPermissionTarget(oversized); err == nil {
		t.Fatal("escaped target exceeded owning canonical bound")
	}
}

// TestMain confines per-user folders to the canonical owned test fixture.
func TestMain(m *testing.M) { os.Exit(personfolders.Main(m)) }
