package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	identity "github.com/openabstractions/abstraction-identity"
	"github.com/openabstractions/abstraction-identity/listen"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	client "github.com/openabstractions/abstraction-logging/go/client"
)

func TestRecoveryApplicationRefusalsAreTyped(t *testing.T) {
	for _, test := range []struct {
		name, outcome string
		policy        error
	}{
		{"denied", "not_permitted", errors.New("blocked")},
		{"outage", "unavailable", ErrHistoryPolicyUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy := appPermit()
			policy.Check = func(context.Context, *identity.Peer, ApplicationHistoryQuery) ([]bool, error) {
				return nil, test.policy
			}
			_, reader, observer, _ := applicationTestHost(t, policy)
			target := wire.ApplicationLogTarget{Selection: wire.ApplicationLogSelectionAllApps}
			page, err := reader.ReadContext(context.Background(), target, "", 1, 65536)
			if err != nil || page.Outcome.String() != test.outcome || len(page.Records) != 0 || page.Next != "" || page.AtEnd {
				t.Fatalf("read refusal: %+v, %v", page, err)
			}
			page, err = observer.ObserveContext(context.Background(), target, "", 1, 65536, 0)
			if err != nil || page.Outcome.String() != test.outcome || len(page.Records) != 0 || page.Next != "" || page.AtEnd {
				t.Fatalf("observe refusal: %+v, %v", page, err)
			}
		})
	}
	_, reader, _, _ := applicationTestHost(t, appPermit())
	page, err := reader.ReadContext(context.Background(), wire.ApplicationLogTarget{Selection: wire.ApplicationLogSelectionAllApps}, "unchanged", 0, 65536)
	if err != nil || page.Outcome.String() != "invalid" || len(page.Records) != 0 || page.Next != "unchanged" || page.AtEnd {
		t.Fatalf("invalid bounds: %+v, %v", page, err)
	}
}

func recoverySourceHost(t *testing.T, adapter SourceAdapter, policy SourcePolicy) *client.SourceReader {
	t.Helper()
	if runtime.GOOS == "darwin" {
		t.Skip("socket lacks Program proof")
	}
	endpoint := listen.Endpoint(fmt.Sprintf("lr-%d-%d", os.Getpid(), appTestID.Add(1)))
	host, err := Listen(endpoint, discardSink{})
	if err != nil {
		t.Fatal(err)
	}
	if err = host.EnableSourceReader([]SourceAdapter{adapter}, policy); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- host.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("source host did not stop")
		}
	})
	return client.NewSourceReaderWithTransport(listen.FrameClient{Endpoint: endpoint, Timeout: time.Second, MaxFrame: 1 << 20})
}

func TestRecoverySourcePolicyAndBoundsRefusalsAreTyped(t *testing.T) {
	for _, test := range []struct {
		name, outcome string
		policy        error
	}{
		{"denied", "not_permitted", errors.New("blocked")},
		{"outage", "unavailable", ErrSourcePolicyUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := &sourceFixture{id: "windows/application"}
			reader := recoverySourceHost(t, fixture, func(context.Context, *identity.Peer, string, string) error { return test.policy })
			page, err := reader.ReadContext(context.Background(), fixture.id, "unchanged", 1, 65536)
			if err != nil || page.Outcome.String() != test.outcome || len(page.Records) != 0 || page.Next != "unchanged" || page.AtEnd {
				t.Fatalf("read refusal: %+v, %v", page, err)
			}
			page, err = reader.StartAtEndContext(context.Background(), fixture.id)
			if err != nil || page.Outcome.String() != test.outcome || len(page.Records) != 0 || page.Next != "" || page.AtEnd {
				t.Fatalf("end refusal: %+v, %v", page, err)
			}
			if fixture.describe.Load() != 0 || fixture.read.Load() != 0 || fixture.endCalls.Load() != 0 {
				t.Fatal("refused call accessed native provider")
			}
		})
	}
	fixture := &sourceFixture{id: "windows/application"}
	reader := recoverySourceHost(t, fixture, func(context.Context, *identity.Peer, string, string) error { return nil })
	page, err := reader.ReadContext(context.Background(), fixture.id, "unchanged", 0, 65536)
	if err != nil || page.Outcome.String() != "invalid" || page.Next != "unchanged" || len(page.Records) != 0 || page.AtEnd || fixture.read.Load() != 0 {
		t.Fatalf("invalid bounds: %+v, %v", page, err)
	}
}

type descriptiveSourceFixture struct {
	*sourceFixture
	descriptor wire.SourceDescriptor
}

func (f descriptiveSourceFixture) Describe(context.Context) (wire.SourceDescriptor, error) {
	return f.descriptor, nil
}

func TestRecoveryFutureSourceConstraintSurvivesCatalog(t *testing.T) {
	descriptor, err := wire.DecodeSourceDescriptor([]byte(`{"id":"windows/application","availability":"available","limitations":["future_constraint"]}`))
	if err != nil {
		t.Fatalf("descriptive word refused: %v", err)
	}
	fixture := descriptiveSourceFixture{sourceFixture: &sourceFixture{id: descriptor.ID}, descriptor: *descriptor}
	service, err := NewSourceService(fixture)
	if err != nil {
		t.Fatal(err)
	}
	current := service.Describe(context.Background(), fixture.ID())
	if current.Availability != wire.SourceAvailabilityAvailable || !bytes.Contains(wire.EncodeSourceDescriptor(&current), []byte(`"future_constraint"`)) {
		t.Fatalf("service replaced future constraint: %+v", current)
	}
	reader := recoverySourceHost(t, fixture, func(context.Context, *identity.Peer, string, string) error { return nil })
	catalog, err := reader.SourcesContext(context.Background())
	if err != nil || len(catalog.Sources) != 1 || !bytes.Contains(wire.EncodeSourceCatalog(&catalog), []byte(`"future_constraint"`)) {
		t.Fatalf("client lost future constraint: %+v, %v", catalog, err)
	}
	if _, err := wire.DecodeSourceDescriptor([]byte(`{"id":"windows/application","availability":"future_state","limitations":[]}`)); err == nil {
		t.Fatal("unknown availability selected usability")
	}
}
