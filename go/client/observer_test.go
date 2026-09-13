package client

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestObserverCanceledBeforeIOAndIndependentBudget(t *testing.T) {
	c := NewObserver("nonexistent")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e := c.ObserveContext(ctx, "", 1, 65536, 30000)
	if !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	copy, e := c.WithTimeout(31 * time.Second)
	if e != nil || copy.transport.Timeout != 31*time.Second || c.transport.Timeout != 2*time.Second {
		t.Fatalf("budget copy %v", e)
	}
	for _, v := range []time.Duration{0, -1, 36 * time.Second} {
		if _, e := c.WithTimeout(v); e == nil {
			t.Fatal("invalid timeout accepted")
		}
	}
}
