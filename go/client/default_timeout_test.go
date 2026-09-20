package client

import (
	"testing"
	"time"

	"github.com/openabstractions/abstraction-identity/listen"
)

// Every client waits DefaultTimeout, two seconds, when its transport names no
// timeout, and keeps a timeout the transport names. A service that consults a
// policy before answering answers within this wait (CONTRACT.md, client wait).
func TestClientsWaitTheDocumentedDefault(t *testing.T) {
	if DefaultTimeout != 2*time.Second {
		t.Fatalf("DefaultTimeout is %v; CONTRACT.md documents two seconds", DefaultTimeout)
	}
	for name, got := range map[string]time.Duration{
		"sink":     New("endpoint").transport.Timeout,
		"reader":   NewReader("endpoint").transport.Timeout,
		"observer": NewObserver("endpoint").transport.Timeout,
	} {
		if got != DefaultTimeout {
			t.Fatalf("%s waits %v, want DefaultTimeout", name, got)
		}
	}
	if got := NewWithTransport(listen.FrameClient{Endpoint: "endpoint", Timeout: time.Second}).transport.Timeout; got != time.Second {
		t.Fatalf("an explicit timeout became %v", got)
	}
}
