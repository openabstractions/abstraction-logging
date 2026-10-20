package service

import (
	"context"
	"errors"
	"fmt"
	identity "github.com/openabstractions/abstraction-identity"
	"github.com/openabstractions/abstraction-identity/listen"
	logging "github.com/openabstractions/abstraction-logging/go"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	loggingclient "github.com/openabstractions/abstraction-logging/go/client"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// This published native provider implements the old Read signature only.
// Adding Usage to the generated service must preserve its existing Read path.
type legacyReadOnlyHistory struct {
	logging.Sink
	reader interface {
		Read(string, int64, int64) (wire.Page, error)
	}
}

func (s legacyReadOnlyHistory) Read(cursor string, records, bytes int64) (wire.Page, error) {
	return s.reader.Read(cursor, records, bytes)
}

func requireHistoryProgramProof(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "darwin" {
		t.Skip("current Program proof limitation")
	}
}

func TestUsageThroughGeneratedServiceRechecksReadAuthority(t *testing.T) {
	requireHistoryProgramProof(t)
	sink, err := logging.OpenRetainedFileSink(filepath.Join(t.TempDir(), "usage"), logging.LogsRetention{MaxUse: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	host, err := Listen(listen.Endpoint(fmt.Sprintf("lu-%d", os.Getpid())), sink)
	if err != nil {
		t.Fatal(err)
	}
	var allowed atomic.Bool
	allowed.Store(true)
	if err := host.EnableHistoryPolicy(func(context.Context, *identity.Peer) error {
		if !allowed.Load() {
			return errors.New("revoked")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- host.Serve(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("host did not stop")
		}
	}()
	reader := loggingclient.NewReader(listen.Endpoint(fmt.Sprintf("lu-%d", os.Getpid())))
	got, err := reader.UsageContext(ctx)
	if err != nil || got.Usage == nil || got.Usage.MaxUse == nil || *got.Usage.MaxUse != 4096 || got.Usage.MaxRetentionMs != nil || got.Usage.RetainedBytes != 0 {
		t.Fatalf("usage: %+v %v", got, err)
	}
	allowed.Store(false)
	got, err = reader.UsageContext(ctx)
	var refused *wire.ServiceError
	if !errors.As(err, &refused) || refused.Code != CodeForbidden || got.Usage != nil {
		t.Fatalf("revoked usage: %+v %v", got, err)
	}
}

func TestHistoryThroughGeneratedService(t *testing.T) {
	requireHistoryProgramProof(t)
	for _, denied := range []bool{false, true} {
		t.Run(fmt.Sprint(denied), func(t *testing.T) {
			sink, err := logging.OpenFileSink(filepath.Join(t.TempDir(), "private-history"))
			if err != nil {
				t.Fatal(err)
			}
			defer sink.Close()
			if err := sink.Write(logging.Record{Schema: 1, Time: logging.Timestamp{Time: time.Now().UTC()}, Msg: "retained 日本語\n"}); err != nil {
				t.Fatal(err)
			}
			endpoint := listen.Endpoint(fmt.Sprintf("lh-%d-%t", os.Getpid(), denied))
			host, err := Listen(endpoint, legacyReadOnlyHistory{Sink: sink, reader: sink})
			if err != nil {
				t.Fatal(err)
			}
			if !host.HistoryAvailable() {
				t.Fatal("legacy Read-only provider lost advertised history")
			}
			if denied {
				host.owner = "not-the-current-account"
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- host.Serve(ctx) }()
			defer func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(3 * time.Second):
					t.Error("host did not stop")
				}
			}()
			client := wire.NewHistoryReaderClient(listen.FrameClient{Endpoint: endpoint, Timeout: time.Second, MaxFrame: 1 << 20})
			page, err := client.Read("", 1, 65536)
			usage, usageErr := client.Usage()
			if denied {
				if refused, ok := usageErr.(*wire.ServiceError); !ok || refused.Code != "wrong_user" {
					t.Fatalf("usage account: %+v %v", usage, usageErr)
				}
				refused, ok := err.(*wire.ServiceError)
				if !ok || refused.Code != "wrong_user" {
					t.Fatalf("wrong account: %+v %v", page, err)
				}
				return
			}
			if usageErr != nil || usage.Outcome != wire.HistoryUsageOutcomeUnavailable || usage.Usage != nil {
				t.Fatalf("legacy provider usage: %+v %v", usage, usageErr)
			}
			if err != nil || page.Outcome != wire.PageOutcomePage || len(page.Records) != 1 || page.Records[0].Msg != "retained 日本語\n" || !page.AtEnd {
				t.Fatalf("read: %+v %v", page, err)
			}
			if page.Next == "" {
				t.Fatal("missing cursor")
			}
			invalid, err := client.Read(page.Next, 257, 65536)
			if err != nil || invalid.Outcome != wire.PageOutcomeInvalidRequest || invalid.Next != page.Next {
				t.Fatalf("limits: %+v %v", invalid, err)
			}
			gap, err := client.Read("foreign:0", 1, 65536)
			if err != nil || gap.Outcome != wire.PageOutcomeGap {
				t.Fatalf("gap: %+v %v", gap, err)
			}
			// The end word through the service and the validating reader client.
			reader := loggingclient.NewReaderWithTransport(listen.FrameClient{Endpoint: endpoint, Timeout: time.Second, MaxFrame: 1 << 20})
			end, err := reader.ReadContext(context.Background(), logging.EndCursor, 256, 65536)
			if err != nil || end.Outcome != wire.PageOutcomePage || len(end.Records) != 0 || !end.AtEnd || end.Next != page.Next {
				t.Fatalf("end: %+v %v, want the continuation %q", end, err, page.Next)
			}
		})
	}
}
