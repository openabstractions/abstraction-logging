package service

import (
	"context"
	"fmt"
	"github.com/openabstractions/abstraction-identity/listen"
	logging "github.com/openabstractions/abstraction-logging/go"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	client "github.com/openabstractions/abstraction-logging/go/client"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

type observingSink struct {
	*logging.FileSink
	entered chan struct{}
	once    sync.Once
}

func (s *observingSink) ObserveContext(ctx context.Context, c string, n, b, w int64) (wire.Page, error) {
	s.once.Do(func() { close(s.entered) })
	return s.FileSink.ObserveContext(ctx, c, n, b, w)
}

type writeOnly struct{}

func (writeOnly) Write(logging.Record) error { return nil }
func observationHost(t *testing.T, out logging.Sink, denied bool) (*Host, string, <-chan error) {
	t.Helper()
	if runtime.GOOS == "darwin" {
		t.Skip("current Program proof limitation")
	}
	endpoint := listen.Endpoint(fmt.Sprintf("lo-%d-%d", os.Getpid(), time.Now().UnixNano()))
	h, e := Listen(endpoint, out)
	if e != nil {
		t.Fatal(e)
	}
	if denied {
		h.owner = "another-account"
	}
	done := make(chan error, 1)
	go func() { done <- h.Serve(context.Background()) }()
	t.Cleanup(func() {
		h.Close()
		select {
		case e := <-done:
			if e != nil {
				t.Error(e)
			}
		case <-time.After(3 * time.Second):
			t.Error("observation host did not drain")
		}
	})
	return h, endpoint, done
}
func TestObservationGeneratedServiceWakeAndShutdown(t *testing.T) {
	f, e := logging.OpenFileSink(filepath.Join(t.TempDir(), "history"))
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	s := &observingSink{FileSink: f, entered: make(chan struct{})}
	h, endpoint, _ := observationHost(t, s, false)
	if !h.ObservationAvailable() {
		t.Fatal("missing advertisement")
	}
	observer, e := client.NewObserver(endpoint).WithTimeout(35 * time.Second)
	if e != nil {
		t.Fatal(e)
	}
	type result struct {
		p wire.Page
		e error
	}
	done := make(chan result, 1)
	go func() {
		p, e := observer.ObserveContext(context.Background(), "", 1, 65536, 30000)
		done <- result{p, e}
	}()
	select {
	case <-s.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("observer not entered")
	}
	if e = client.New(endpoint).Log(1, "observed IPC", nil); e != nil {
		t.Fatal(e)
	}
	var p wire.Page
	select {
	case r := <-done:
		p = r.p
		if r.e != nil || len(p.Records) != 1 || p.Records[0].Msg != "observed IPC" {
			t.Fatalf("observe %+v %v", p, r.e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no notification")
	}
	// Same reusable observer gets a fresh budget; host cancellation drains its wait.
	go func() {
		p, e := observer.ObserveContext(context.Background(), p.Next, 1, 65536, 30000)
		done <- result{p, e}
	}()
	h.Close()
	select {
	case r := <-done:
		if r.e == nil {
			t.Fatalf("shutdown returned success %+v", r.p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown left wait alive")
	}
}
func TestObservationUnsupportedAndUnauthorized(t *testing.T) {
	h, endpoint, _ := observationHost(t, writeOnly{}, false)
	if h.ObservationAvailable() {
		t.Fatal("advertised unsupported provider")
	}
	p, e := client.NewObserver(endpoint).ObserveContext(context.Background(), "", 1, 65536, 0)
	if e != nil || p.Outcome != wire.PageOutcomeUnsupported {
		t.Fatalf("unsupported %+v %v", p, e)
	}
	p, e = client.NewReader(endpoint).ReadContext(context.Background(), "", 1, 65536)
	if e != nil || p.Outcome != wire.PageOutcomeUnavailable {
		t.Fatalf("legacy read changed %+v %v", p, e)
	}
	f, e := logging.OpenFileSink(filepath.Join(t.TempDir(), "private"))
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	s := &observingSink{FileSink: f, entered: make(chan struct{})}
	_, endpoint, _ = observationHost(t, s, true)
	p, e = client.NewObserver(endpoint).ObserveContext(context.Background(), "", 1, 65536, 30000)
	refused, ok := e.(*wire.ServiceError)
	if !ok || refused.Code != "wrong_user" || len(p.Records) != 0 {
		t.Fatalf("authority %+v %v", p, e)
	}
	select {
	case <-s.entered:
		t.Fatal("unauthorized provider access")
	default:
	}
}
