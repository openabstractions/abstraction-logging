package client

import (
	"context"
	"errors"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"time"
)

// ObserveAboutCallerContext is a native Go convenience for applications that
// follow caller records. It rereads ReadAboutCaller with the same since query
// and returned cursor for at most waitMS. It adds no IPC method or authority;
// every read retains the service's caller binding and policy verdict.
func (c *ApplicationReader) ObserveAboutCallerContext(ctx context.Context, since time.Time, cursor string, maxRecords, maxBytes, waitMS int64) (ApplicationLogPage, error) {
	return observeCallerPages(ctx, cursor, waitMS, func(ctx context.Context, cursor string) (ApplicationLogPage, error) {
		return c.ReadAboutCallerContext(ctx, since, cursor, maxRecords, maxBytes)
	})
}

func observeCallerPages(ctx context.Context, cursor string, waitMS int64, read func(context.Context, string) (ApplicationLogPage, error)) (ApplicationLogPage, error) {
	if waitMS < 0 || waitMS > 30000 {
		return ApplicationLogPage{}, errors.New("logging: wait_ms outside 0..30000")
	}
	if err := ctx.Err(); err != nil {
		return ApplicationLogPage{}, err
	}
	if waitMS == 0 {
		return read(ctx, cursor)
	}
	budget, cancel := context.WithTimeout(ctx, time.Duration(waitMS)*time.Millisecond)
	defer cancel()
	var last ApplicationLogPage
	haveLast := false
	for {
		page, err := read(budget, cursor)
		if err != nil {
			if ctx.Err() != nil {
				return ApplicationLogPage{}, ctx.Err()
			}
			if errors.Is(budget.Err(), context.DeadlineExceeded) && haveLast {
				return last, nil
			}
			return ApplicationLogPage{}, err
		}
		if page.Outcome != wire.HistoryPageOutcomePage || len(page.Records) > 0 || !page.AtEnd {
			return page, nil
		}
		last = page
		haveLast = true
		cursor = page.Next
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ApplicationLogPage{}, ctx.Err()
		case <-budget.Done():
			timer.Stop()
			if ctx.Err() != nil {
				return ApplicationLogPage{}, ctx.Err()
			}
			return last, nil
		case <-timer.C:
		}
	}
}
