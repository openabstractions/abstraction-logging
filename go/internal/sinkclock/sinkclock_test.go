package sinkclock

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSetRestoresTheClockItReplaced(t *testing.T) {
	first := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	second := first.Add(time.Hour)
	restoreFirst := Set(func() time.Time { return first })
	restoreSecond := Set(func() time.Time { return second })
	if !Current().Equal(second) {
		t.Fatal("the newer clock is not current")
	}
	restoreSecond()
	if !Current().Equal(first) {
		t.Fatal("restore did not bring back the clock it replaced")
	}
	restoreFirst()
	if Current().Sub(time.Now()).Abs() > time.Minute {
		t.Fatal("restore did not bring back the real clock")
	}
}

// Set is a test seam: only test files in this module may call it.
func TestSetIsCalledOnlyFromTests(t *testing.T) {
	module, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(module, "go.mod")); err != nil {
		t.Fatalf("module root not found at %s: %v", module, err)
	}
	err = filepath.WalkDir(module, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), "sinkclock.Set(") {
			t.Errorf("%s calls sinkclock.Set outside a test", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
