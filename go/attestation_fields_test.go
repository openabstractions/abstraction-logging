package logging

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

// testdata/records/attestation-fields.jsonl is read by the Go, Python and C++
// readers. program belongs to the writer's claim; exe to each attester's stamp.
func TestAttestationFieldsReadAlike(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "testdata", "records", "attestation-fields.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	line := bytes.TrimRight(data, "\n")
	type hop struct {
		by, program, user, exe string
		verified               bool
		uid                    int64
	}
	want := []hop{
		{"self", "ComfyUI", "", "", false, -1},
		{"identity/windows", "", "S-1-5-21-1-2-3-1001", `C:\Users\oa\Python312\python.exe`, true, -1},
		{"so_peercred", "", "oa", "/usr/bin/python3.14", true, 1000},
	}

	record, err := DecodeRecord(line)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Identity) != len(want) {
		t.Fatalf("provider reader: %+v", record.Identity)
	}
	for i, a := range record.Identity {
		got := hop{a.By, a.Program, a.User, a.Exe, a.Verified, int64(a.UID)}
		if got != want[i] || a.Hop != i {
			t.Fatalf("provider reader hop %d: %+v", i, a)
		}
	}

	generated, err := wire.Decode(line)
	if err != nil {
		t.Fatal(err)
	}
	for i, a := range generated.Identity {
		got := hop{a.By, a.Program, a.User, a.Exe, a.Verified, a.Uid}
		if got != want[i] || a.Hop != int64(i) {
			t.Fatalf("generated reader hop %d: %+v", i, a)
		}
	}
	again, err := wire.Decode(bytes.TrimRight(wire.Encode(generated), "\n"))
	if err != nil || again.Identity[1].Exe != want[1].exe || again.Identity[1].Program != "" || again.Identity[0].Program != "ComfyUI" {
		t.Fatalf("generated round trip: %+v %v", again, err)
	}
}
