package client

import (
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"testing"
)

func TestHistoryRejectsNonadvancingRecords(t *testing.T) {
	p := wire.Page{Outcome: wire.PageOutcomePage, Records: []wire.Record{{Schema: 1, Time: "2026-09-13T00:00:00Z", Msg: "test"}}, Next: "cursor", AtEnd: true}
	if _, err := validateHistoryPage(p, "cursor", 1, 65536); err == nil {
		t.Fatal("nonadvancing records accepted")
	}
	p.Next = "next"
	if _, err := validateHistoryPage(p, "cursor", 1, 65536); err != nil {
		t.Fatal(err)
	}
	p.Records = nil
	p.Next = "cursor"
	if _, err := validateHistoryPage(p, "cursor", 1, 65536); err != nil {
		t.Fatal("empty observed end refused", err)
	}
}
