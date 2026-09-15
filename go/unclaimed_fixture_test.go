package logging

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
)

// testdata/records/unclaimed-hop0.jsonl is the line a receiving service writes
// for a record that arrived with no writer claim [LOG-I16]. The Python and C++
// readers decode the same file.
func TestUnclaimedFixtureIsWhatTheServiceWrites(t *testing.T) {
	bare := Record{Schema: SchemaVersion, Time: At(time.Unix(0, 0).UTC()), Msg: "no writer claim"}
	stamp := Attestation{By: "identity/windows", Verified: true, User: "S-1-5-21-1-2-3-1001",
		Exe: `C:\Users\oa\Python312\python.exe`, UID: Unknown, GID: Unknown, PID: 4242}
	line, err := (&Server{}).Accept(bare, stamp, nil).Encode()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("..", "testdata", "records", "unclaimed-hop0.jsonl")
	committed, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(bytes.TrimRight(committed, "\n"), bytes.TrimRight(line, "\n")) {
		t.Fatalf("%s differs from the service's line (%v); expected:\n%s", path, err, line)
	}

	generated, err := wire.Decode(bytes.TrimRight(committed, "\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(generated.Identity) != 2 {
		t.Fatalf("generated reader chain %+v", generated.Identity)
	}
	first, second := generated.Identity[0], generated.Identity[1]
	if first.By != ByUnclaimed || first.Verified || first.Hop != 0 || first.Uid != -1 || first.Gid != -1 || first.Pid != -1 || first.Program != "" || first.Exe != "" {
		t.Fatalf("generated reader hop 0 %+v", first)
	}
	if second.By != "identity/windows" || !second.Verified || second.Hop != 1 || second.Exe != stamp.Exe {
		t.Fatalf("generated reader hop 1 %+v", second)
	}
	if generated.Attrs[AttrWriterClaim] != "absent" {
		t.Fatalf("generated reader attrs %+v", generated.Attrs)
	}
}
