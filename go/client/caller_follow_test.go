package client

import (
	"context"
	"errors"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"testing"
)

func TestCallerFollowBoundedReadContinuationAndGap(t *testing.T) {
	calls := 0
	read := func(_ context.Context, cursor string) (ApplicationLogPage, error) {
		calls++
		if calls == 1 {
			return ApplicationLogPage{Outcome: wire.HistoryPageOutcomePage, Next: "next", AtEnd: true}, nil
		}
		if cursor != "next" {
			t.Fatal(cursor)
		}
		return ApplicationLogPage{Outcome: wire.HistoryPageOutcomeGap, Next: cursor}, nil
	}
	page, err := observeCallerPages(t.Context(), "first", 500, read)
	if err != nil || page.Outcome != wire.HistoryPageOutcomeGap || page.Next != "next" || calls != 2 {
		t.Fatal(page, err, calls)
	}
}
func TestCallerFollowNoWaitAndCancellation(t *testing.T) {
	calls := 0
	read := func(context.Context, string) (ApplicationLogPage, error) {
		calls++
		return ApplicationLogPage{Outcome: wire.HistoryPageOutcomePage, Next: "same", AtEnd: true}, nil
	}
	if page, err := observeCallerPages(t.Context(), "same", 0, read); err != nil || page.Next != "same" || calls != 1 {
		t.Fatal(page, err, calls)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := observeCallerPages(ctx, "same", 500, read); !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatal(err, calls)
	}
	if _, err := observeCallerPages(t.Context(), "same", 30001, read); err == nil {
		t.Fatal("unbounded wait")
	}
}

func TestCallerFollowReadDeadlineNeverFabricatesAPage(t *testing.T) {
	page, err := observeCallerPages(t.Context(), "same", 5, func(ctx context.Context, _ string) (ApplicationLogPage, error) {
		<-ctx.Done()
		return ApplicationLogPage{}, ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) || page.Next != "" {
		t.Fatal(page, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, err = observeCallerPages(ctx, "same", 500, func(context.Context, string) (ApplicationLogPage, error) {
		cancel()
		return ApplicationLogPage{Outcome: wire.HistoryPageOutcomePage, Next: "same", AtEnd: true}, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
