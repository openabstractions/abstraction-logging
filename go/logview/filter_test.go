package logview

import (
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"testing"
	"time"
)

func TestRecordFilterExactTagLevelTimeAndLoss(t *testing.T) {
	now := time.Now().UTC()
	f, err := Parse("warn", "stage=ready", now.Add(-time.Second).Format(time.RFC3339Nano), now.Add(time.Second).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	r := wire.Record{Time: now.Format(time.RFC3339Nano), Level: 4, Attrs: map[string]string{"stage": "ready"}}
	if !f.Keep(r, "") {
		t.Fatal("matching row hidden")
	}
	r.Attrs["stage"] = "ready-later"
	if f.Keep(r, "") {
		t.Fatal("prefix tag accepted")
	}
	if !f.Keep(wire.Record{Msg: "loss"}, "loss") {
		t.Fatal("loss hidden")
	}
	for _, args := range [][4]string{{"invalid", "", "", ""}, {"", "", "bad", ""}, {"", "", "2026-11-03T12:00:00Z", "2026-11-03T11:00:00Z"}} {
		if _, err := Parse(args[0], args[1], args[2], args[3]); err == nil {
			t.Fatal(args)
		}
	}
}

func TestContractFilterNarrowsInferenceWithoutHidingLoss(t *testing.T) {
	f, err := ParseContract("inference", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range []string{"abstraction.inference/chat@3", "abstraction.inference/image@2"} {
		if !f.Keep(wire.Record{Msg: msg}, "loss") {
			t.Fatal(msg)
		}
	}
	for _, msg := range []string{"abstraction.inferenceX/chat@3", "abstraction.logging/reader@2", "inference"} {
		if f.Keep(wire.Record{Msg: msg}, "loss") {
			t.Fatal(msg)
		}
	}
	if !f.Keep(wire.Record{Msg: "loss"}, "loss") {
		t.Fatal("loss hidden")
	}
	if _, err := ParseContract("inference/../../logging", "", "", "", ""); err == nil {
		t.Fatal("malformed contract accepted")
	}
}
