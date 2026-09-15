package logging

import (
	"context"
	"log/slog"
	"testing"
)

// Level equals slog.Level: the same numbers and the same names [LOG-R3, LOG-R4].
// This fails if either scale changes.
func TestLevelScaleIsSlogLevel(t *testing.T) {
	for _, pair := range []struct {
		ours   Level
		theirs slog.Level
	}{{LevelDebug, slog.LevelDebug}, {LevelInfo, slog.LevelInfo}, {LevelWarn, slog.LevelWarn}, {LevelError, slog.LevelError}} {
		if int(pair.ours) != int(pair.theirs) {
			t.Fatalf("landmark %v is %d here and %d in slog", pair.theirs, int(pair.ours), int(pair.theirs))
		}
	}
	if int(LevelDebug) != -4 || int(LevelInfo) != 0 || int(LevelWarn) != 4 || int(LevelError) != 8 {
		t.Fatal("the contract's landmarks moved: debug -4, info 0, warn 4, error 8")
	}
	for n := -16; n <= 16; n++ {
		if got, want := Level(n).String(), slog.Level(n).String(); got != want {
			t.Fatalf("level %d is named %q here and %q in slog", n, got, want)
		}
	}
	h := NewHandler(DiscardSink{}, &Options{Level: LevelWarn})
	for n := -16; n <= 16; n++ {
		if got, want := h.Enabled(context.Background(), slog.Level(n)), n >= int(LevelWarn); got != want {
			t.Fatalf("Enabled(%d) = %v at minimum WARN", n, got)
		}
	}
}
