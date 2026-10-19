package service

import (
	"context"
	"testing"

	identity "github.com/openabstractions/abstraction-identity"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

func TestProviderPageOutcomeConversion(t *testing.T) {
	for _, outcome := range wire.PageOutcomeValues() {
		converted := historyOutcome(outcome)
		back, ok := providerOutcome(converted)
		if !ok || back != outcome {
			t.Fatalf("provider %s converted to %s and %s", outcome, converted, back)
		}
	}
	if historyOutcome(wire.PageOutcome(999)) != wire.HistoryPageOutcomeUnavailable {
		t.Fatal("unknown internal outcome selected success")
	}
	for _, outcome := range []wire.HistoryPageOutcome{wire.HistoryPageOutcomeForbidden, wire.HistoryPageOutcomeNotPermitted, wire.HistoryPageOutcomeOSAuthorizationRequired, wire.HistoryPageOutcomeOSAuthorizationPending} {
		if _, ok := providerOutcome(outcome); ok {
			t.Fatalf("receiving policy outcome %s became native provider state", outcome)
		}
	}
	page := NativePage{Outcome: wire.PageOutcomeInvalidRequest, Next: "same"}
	data, err := encodeProtectedPage(ProtectedSourceID, page)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeProtectedPage(ProtectedSourceID, data)
	if err != nil || decoded.Outcome != page.Outcome || decoded.Next != page.Next {
		t.Fatalf("helper conversion %+v, %v", decoded, err)
	}
}

func TestSourceCatalogUnavailableIsTyped(t *testing.T) {
	catalog, err := (&receiver{}).Sources()
	if err != nil || catalog.Outcome != wire.SourceCatalogOutcomeUnavailable || len(catalog.Sources) != 0 {
		t.Fatalf("unconfigured catalogue %+v, %v", catalog, err)
	}
	fixture := &sourceFixture{id: "windows/application"}
	reader := recoverySourceHost(t, fixture, func(context.Context, *identity.Peer, string, string) error { return ErrSourcePolicyUnavailable })
	catalog, err = reader.SourcesContext(context.Background())
	if err != nil || catalog.Outcome != wire.SourceCatalogOutcomeUnavailable || len(catalog.Sources) != 0 || fixture.describe.Load() != 0 {
		t.Fatalf("policy outage %+v, %v", catalog, err)
	}
}
