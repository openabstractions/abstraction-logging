//go:build windows

package service

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

// TestProtectedRunnerEnvironment is LOG-N1's elevated child: it sees the
// fixed SystemRoot, windir, PATH and LC_ALL and nothing of this process's
// environment.
func TestProtectedRunnerEnvironment(t *testing.T) {
	t.Setenv("OA_PROBE_PLANTED", "from-the-parent")
	t.Setenv("PATH", t.TempDir())
	system, err := windows.GetSystemDirectory()
	if err != nil {
		t.Fatal(err)
	}
	root, err := windows.GetSystemWindowsDirectory()
	if err != nil {
		t.Fatal(err)
	}
	out, err := protectedCommandRunner{}.Run(context.Background(), filepath.Join(system, "cmd.exe"), "/d", "/c", "set")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]string{}
	for _, line := range strings.Split(strings.ReplaceAll(string(out), "\r\n", "\n"), "\n") {
		if key, value, ok := strings.Cut(line, "="); ok && key != "" {
			seen[strings.ToUpper(key)] = value
		}
	}
	want := map[string]string{"SYSTEMROOT": root, "WINDIR": root, "PATH": system, "LC_ALL": "C"}
	for key, value := range want {
		if !strings.EqualFold(seen[key], value) {
			t.Fatalf("%s=%q, want %q (all: %v)", key, seen[key], value, seen)
		}
	}
	for key := range seen {
		if _, fixed := want[key]; !fixed && key != "PROMPT" && key != "COMSPEC" && key != "PATHEXT" {
			t.Fatalf("the child inherited %s (all: %v)", key, seen)
		}
	}
	if path, err := (protectedCommandRunner{}).CommandPath("wevtutil.exe"); err != nil || !strings.EqualFold(filepath.Dir(path), system) {
		t.Fatalf("wevtutil path %q %v", path, err)
	}
	if _, err := (protectedCommandRunner{}).CommandPath("cmd.exe"); err == nil {
		t.Fatal("the protected runner resolved another command")
	}
}
