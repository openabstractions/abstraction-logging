package client

import (
	"errors"
	"testing"

	"github.com/openabstractions/abstraction-facade/go-core/bootstrap"
)

func TestDefaultLoggingClientRefusesInstalledEndpointWithoutSeams(t *testing.T) {
	t.Setenv("OA_TEST_ALLOW_INSTALLED_RUNTIME", "")
	t.Setenv(EnvEndpoint, "")
	if DefaultEndpoint() != "" {
		t.Fatal("ambient logging address escaped refusal")
	}
	for _, client := range []*Client{Discover(), New("")} {
		if err := client.Write(Record{}); !errors.Is(err, bootstrap.ErrNoTrustedInstallation) {
			t.Fatalf("ambient logging write: %v", err)
		}
	}
}
