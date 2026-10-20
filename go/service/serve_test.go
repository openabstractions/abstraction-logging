package service

import (
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/openabstractions/abstraction-logging/go/client"
)

// stdoutOf runs f with os.Stdout redirected and returns what f wrote there.
func stdoutOf(t *testing.T, f func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stdout
	os.Stdout = w
	func() {
		defer func() { os.Stdout = saved }()
		f()
	}()
	w.Close()
	out, _ := io.ReadAll(r)
	r.Close()
	return string(out)
}

func TestServeHelp(t *testing.T) {
	t.Setenv(client.EnvEndpoint, "fixture-help-endpoint")
	out := stdoutOf(t, func() {
		if err := Serve([]string{"--help"}); err != nil {
			t.Fatal(err)
		}
	})
	first, _, _ := strings.Cut(out, "\n")
	if first != "Usage: openabstractions serve logging [options]" {
		t.Fatalf("first line = %q", first)
	}
	if endpoint := client.DefaultEndpoint(); endpoint != "" && strings.Contains(out, endpoint) {
		t.Fatalf("usage names this machine's resolved endpoint: %s", out)
	}
}

func TestServeBadFlagReturnsErrUsage(t *testing.T) {
	err := Serve([]string{"--not-a-real-flag"})
	if !errors.Is(err, ErrUsage) {
		t.Fatalf("err = %v, want errors.Is(err, ErrUsage)", err)
	}
}
