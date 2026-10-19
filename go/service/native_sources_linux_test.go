//go:build linux

package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

type fixtureNativeRunner struct {
	path    string
	outputs map[string][]byte
	err     map[string]error
	args    [][]string
}

func (r *fixtureNativeRunner) CommandPath(string) (string, error) {
	if r.path == "" {
		return "", fmt.Errorf("not found")
	}
	return r.path, nil
}

func TestLinuxNativeCommandIgnoresPathDecoy(t *testing.T) {
	decoyDir := t.TempDir()
	decoy := decoyDir + "/journalctl"
	if err := os.WriteFile(decoy, []byte("decoy"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", decoyDir)
	got, err := nativeCommandPath("journalctl")
	if err == nil && got == decoy {
		t.Fatalf("native command selected PATH decoy %q", got)
	}
	if err != nil && got != "" {
		t.Fatalf("unsupported command path %q with error %v", got, err)
	}
}

func TestNativeCommandPinsEnglishLocaleForStableDiagnostics(t *testing.T) {
	env := nativeCommandEnvironment()
	count := 0
	for _, value := range env {
		if strings.HasPrefix(value, "LC_ALL=") {
			count++
			if value != "LC_ALL=C" {
				t.Fatalf("locale override = %q", value)
			}
		}
	}
	if count != 1 {
		t.Fatalf("locale override count = %d", count)
	}
}
func (r *fixtureNativeRunner) Run(_ context.Context, _ string, args ...string) ([]byte, error) {
	r.args = append(r.args, append([]string(nil), args...))
	key := strings.Join(args, " ")
	return r.outputs[key], r.err[key]
}

func journalFixture(cursor string, micros int64, message string) string {
	value := map[string]string{"__CURSOR": cursor, "__REALTIME_TIMESTAMP": fmt.Sprint(micros), "PRIORITY": "4", "MESSAGE": message, "_TRANSPORT": "fixture", "_PID": "23"}
	data, _ := json.Marshal(value)
	return string(data)
}

func TestJournalSourceReadsOldestBoundedPagesAndChecksAnchor(t *testing.T) {
	runner := &fixtureNativeRunner{path: "/usr/bin/journalctl", outputs: map[string][]byte{}}
	source := &journalSource{id: "linux/system-journal", runner: runner}
	if descriptor, err := source.Describe(context.Background()); err != nil || descriptor.Availability.String() != "available" {
		t.Fatalf("descriptor %+v, %v", descriptor, err)
	}
	firstArgs := []string{"--no-pager", "--quiet", "--output=json", "--utc", "--system", "--lines=+3", "--show-cursor"}
	runner.outputs[strings.Join(firstArgs, " ")] = []byte(journalFixture("c1", 1_000_000, "one") + "\n" + journalFixture("c2", 2_000_000, "two") + "\n" + journalFixture("c3", 3_000_000, "three") + "\n-- cursor: c3\n")
	page, err := source.Read(context.Background(), "", 2, 65536)
	if err != nil || page.Outcome.String() != "page" || len(page.Entries) != 2 || page.Entries[0].Record.Msg != "one" || page.Entries[1].Record.Msg != "two" || page.Next != "c2" || page.AtEnd {
		t.Fatalf("first bounded page %+v, %v", page, err)
	}
	anchorArgs := []string{"--no-pager", "--quiet", "--output=json", "--utc", "--system", "--cursor=c2", "--lines=+1"}
	nextArgs := []string{"--no-pager", "--quiet", "--output=json", "--utc", "--system", "--after-cursor=c2", "--lines=+3", "--show-cursor"}
	runner.outputs[strings.Join(anchorArgs, " ")] = []byte(journalFixture("c2", 2_000_000, "two") + "\n")
	runner.outputs[strings.Join(nextArgs, " ")] = []byte(journalFixture("c3", 3_000_000, "three") + "\n-- cursor: c3\n")
	page, err = source.Read(context.Background(), "c2", 2, 65536)
	if err != nil || len(page.Entries) != 1 || page.Entries[0].Record.Msg != "three" || !page.AtEnd {
		t.Fatalf("continued page %+v, %v", page, err)
	}
	if !strings.Contains(strings.Join(runner.args[len(runner.args)-1], " "), "--lines=+3") {
		t.Fatalf("did not request oldest bounded events: %v", runner.args[len(runner.args)-1])
	}
}

func TestJournalSourceReportsGapWhenAnchorWasLost(t *testing.T) {
	runner := &fixtureNativeRunner{path: "/usr/bin/journalctl", outputs: map[string][]byte{}}
	source := &journalSource{id: "linux/user-journal", user: true, runner: runner, path: "/usr/bin/journalctl"}
	anchorArgs := []string{"--no-pager", "--quiet", "--output=json", "--utc", "--user", "--cursor=old", "--lines=+1"}
	runner.outputs[strings.Join(anchorArgs, " ")] = []byte(journalFixture("new", 2_000_000, "new") + "\n")
	page, err := source.Read(context.Background(), "old", 10, 65536)
	if err != nil || page.Outcome.String() != "gap" || len(page.Entries) != 0 || len(runner.args) != 1 {
		t.Fatalf("lost anchor %+v, %v; calls=%v", page, err, runner.args)
	}
	if !strings.Contains(strings.Join(runner.args[0], " "), "--user") {
		t.Fatalf("user source used wrong journal scope: %v", runner.args[0])
	}
}

func TestJournalCurrentCursorUsesBoundedLatestEntryOrEmptyBaseline(t *testing.T) {
	runner := &fixtureNativeRunner{path: "/usr/bin/journalctl", outputs: map[string][]byte{}}
	source := &journalSource{id: "linux/system-journal", runner: runner, path: runner.path}
	args := []string{"--no-pager", "--quiet", "--output=json", "--utc", "--system", "--lines=1", "--reverse", "--show-cursor"}
	runner.outputs[strings.Join(args, " ")] = []byte(journalFixture("latest", 2_000_000, "latest") + "\n-- cursor: latest\n")
	cursor, err := source.CurrentCursor(context.Background())
	if err != nil || cursor != "latest" {
		t.Fatalf("latest baseline cursor %q, %v", cursor, err)
	}
	if got := strings.Join(runner.args[0], " "); got != strings.Join(args, " ") {
		t.Fatalf("unbounded or non-tail native query: %s", got)
	}
	runner.outputs[strings.Join(args, " ")] = nil
	cursor, err = source.CurrentCursor(context.Background())
	if err != nil || cursor != "" {
		t.Fatalf("empty baseline cursor %q, %v", cursor, err)
	}
}

func TestJournalSourceReportsGapWhenNativeCursorSeekFails(t *testing.T) {
	runner := &fixtureNativeRunner{path: "/usr/bin/journalctl", outputs: map[string][]byte{}, err: map[string]error{}}
	source := &journalSource{id: "linux/user-journal", user: true, runner: runner, path: "/usr/bin/journalctl"}
	anchorArgs := []string{"--no-pager", "--quiet", "--output=json", "--utc", "--user", "--cursor=old", "--lines=+1"}
	runner.err[strings.Join(anchorArgs, " ")] = fmt.Errorf("journalctl: exit status 1: Failed to seek to cursor: No such file or directory")
	page, err := source.Read(context.Background(), "old", 10, 65536)
	if err != nil || page.Outcome.String() != "gap" || len(page.Entries) != 0 {
		t.Fatalf("missing native cursor %+v, %v", page, err)
	}
}

func TestJournalSourceKeepsPermissionFailureDistinctFromMissingCursor(t *testing.T) {
	runner := &fixtureNativeRunner{path: "/usr/bin/journalctl", outputs: map[string][]byte{}, err: map[string]error{}}
	source := &journalSource{id: "linux/user-journal", user: true, runner: runner, path: "/usr/bin/journalctl"}
	anchorArgs := []string{"--no-pager", "--quiet", "--output=json", "--utc", "--user", "--cursor=old", "--lines=+1"}
	runner.err[strings.Join(anchorArgs, " ")] = fmt.Errorf("journalctl: exit status 1: Failed to seek to cursor: Permission denied")
	_, err := source.Read(context.Background(), "old", 10, 65536)
	if !errors.Is(err, ErrSourcePermissionDenied) {
		t.Fatalf("permission failure = %v, want ErrSourcePermissionDenied", err)
	}
}
