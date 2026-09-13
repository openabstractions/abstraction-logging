package service

import (
	"context"
	"fmt"
	"github.com/openabstractions/abstraction-identity/listen"
	logging "github.com/openabstractions/abstraction-logging/go"
	wire "github.com/openabstractions/abstraction-logging/go/abstraction/logging"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestHistoryThroughGeneratedService(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("current Program proof limitation")
	}
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
			host, err := Listen(endpoint, sink)
			if err != nil {
				t.Fatal(err)
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
			if denied {
				refused, ok := err.(*wire.ServiceError)
				if !ok || refused.Code != "wrong_user" {
					t.Fatalf("wrong account: %+v %v", page, err)
				}
				return
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
		})
	}
}
