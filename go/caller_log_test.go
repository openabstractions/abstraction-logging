package logging

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// A private subject stamp decodes only in its canonical form with a valid
// program; anything else marks the stamp invalid, which a reader reports as
// corrupt (LOG-P5, LOG-P9).
func TestStoredSubjectStampIsCanonicalOrInvalid(t *testing.T) {
	program := filepath.Join(t.TempDir(), "runtime")
	if err := os.WriteFile(program, []byte("runtime"), 0o600); err != nil {
		t.Fatal(err)
	}
	resource, err := PrepareApplicationResource(program)
	if err != nil {
		t.Fatal(err)
	}
	emitter, _ := json.Marshal(Emitter{Version: 1, Account: "account", Program: resource.Program, Key: resource.CapturedFileKey})
	caller := filepath.Join(t.TempDir(), "caller.exe")
	canonical, _ := json.Marshal(caller)
	row := func(subject string) []byte {
		line := `{"schema":1,"time":"2026-10-31T12:00:00.000000Z","level":0,"msg":"m","_oa_emitter":` + string(emitter)
		if subject != "" {
			line += `,"_oa_subject":` + subject
		}
		return []byte(line + "}\n")
	}
	_, e, invalid, err := decodeStoredRow(row(string(canonical)), true)
	if err != nil || invalid || e == nil || !SameCaller(e.Subject, caller) {
		t.Fatalf("canonical subject: %+v %v %v", e, invalid, err)
	}
	if _, e, invalid, err := decodeStoredRow(row(""), true); err != nil || invalid || e == nil || e.Subject != "" {
		t.Fatalf("no subject: %+v %v %v", e, invalid, err)
	}
	for _, forged := range []string{`""`, `"relative.exe"`, `7`, `{"program":"x"}`, fmt.Sprintf("\"\\u%04x", canonical[1]) + string(canonical[2:])} {
		if _, e, invalid, err := decodeStoredRow(row(forged), true); err != nil || !invalid || e != nil {
			t.Fatalf("forged subject %s: %+v %v %v", forged, e, invalid, err)
		}
	}
}
