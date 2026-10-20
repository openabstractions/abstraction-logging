package client

import (
	"context"
	"errors"
	"github.com/openabstractions/abstraction-facade/go-core/bootstrap"
	"net"
	"os"
	"runtime"
	"testing"
)

func TestDefaultEndpointRefusesAmbientAndRetainsExplicitChoice(t *testing.T) {
	t.Setenv("OA_TEST_ALLOW_INSTALLED_RUNTIME", "")
	t.Setenv(EnvEndpoint, "")
	if endpoint, err := checkedDefaultEndpoint(); endpoint != "" || !errors.Is(err, bootstrap.ErrNoTrustedInstallation) {
		t.Fatalf("ambient endpoint: %q %v", endpoint, err)
	}
	if got := DefaultEndpoint(); got != "" {
		t.Fatal("ambient address escaped refusal", got)
	}
	t.Setenv(EnvEndpoint, "explicit-owned-endpoint")
	if got := DefaultEndpoint(); got != "explicit-owned-endpoint" {
		t.Fatal(got)
	}
}
func TestDiscoverRetainsDefaultRefusalBeforeIO(t *testing.T) {
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		t.Skip("Unix socket default refusal; Windows SID and installed Mac naming have separate controls")
	}
	t.Setenv(EnvEndpoint, "")
	base := t.TempDir()
	if err := os.Chmod(base, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_RUNTIME_DIR", base)
	if DefaultEndpoint() != "" {
		t.Fatal("refused namespace returned a fallback")
	}
	c := Discover()
	called := false
	c.transport.Dialer = func(context.Context, string) (net.Conn, error) {
		called = true
		return nil, errors.New("unexpected dial")
	}
	if err := os.Chmod(base, 0o700); err != nil {
		t.Fatal(err)
	}
	err := c.Write(Record{})
	if err == nil || called {
		t.Fatalf("lost resolution error or dialed: %v %v", err, called)
	}
}

func TestHistoryClientsRetainDefaultRefusal(t *testing.T) {
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		t.Skip("Unix namespace receiving")
	}
	t.Setenv(EnvEndpoint, "")
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_RUNTIME_DIR", dir)
	reader := NewReader(DefaultEndpoint())
	observer := NewObserver(DefaultEndpoint())
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadContext(context.Background(), "", 1, 1024); !errors.Is(err, bootstrap.ErrNoTrustedInstallation) {
		t.Fatal("reader lost default refusal")
	}
	if _, err := observer.ObserveContext(context.Background(), "", 1, 1024, 0); !errors.Is(err, bootstrap.ErrNoTrustedInstallation) {
		t.Fatal("observer lost default refusal")
	}
}
