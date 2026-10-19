package client

import (
	"testing"

	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

func TestValidateSourcePageBoundAndProvenance(t *testing.T) {
	page := wire.SourcePage{Outcome: wire.HistoryPageOutcomePage, Records: []wire.SourceRecord{{
		SourceID: "linux/system-journal",
		Record:   wire.Record{Schema: 1, Time: "2026-09-29T10:11:12.000000Z", Msg: "fixture"},
	}}, Next: "opaque-next", AtEnd: true}
	if _, err := validateSourcePage(page, "linux/system-journal", "", 1, 65536); err != nil {
		t.Fatal(err)
	}
	page.Records[0].Record.Identity = []wire.Attestation{{By: "identity/linux", Verified: true, Hop: 1}}
	if _, err := validateSourcePage(page, "linux/system-journal", "", 1, 65536); err == nil {
		t.Fatal("promoted source record identity accepted")
	}
	page.Records[0].Record.Identity = nil
	page.Records[0].SourceID = "linux/user-journal"
	if _, err := validateSourcePage(page, "linux/system-journal", "", 1, 65536); err == nil {
		t.Fatal("cross-source row accepted")
	}
}

func TestValidateSourcePageRefusalPreservesCursor(t *testing.T) {
	page := wire.SourcePage{Outcome: wire.HistoryPageOutcomeGap, Records: []wire.SourceRecord{}, Next: "same-cursor"}
	if _, err := validateSourcePage(page, "windows/application", "same-cursor", 1, 65536); err != nil {
		t.Fatal(err)
	}
	page.Next = "advanced"
	if _, err := validateSourcePage(page, "windows/application", "same-cursor", 1, 65536); err == nil {
		t.Fatal("refusal advanced cursor")
	}
}
