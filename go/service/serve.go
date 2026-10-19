package service

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"

	logging "github.com/openabstractions/abstraction-logging/go"
	"github.com/openabstractions/abstraction-logging/go/client"
)

const loggingUsage = `Usage: openabstractions serve logging [options]

Receives identity-bound structured log records and appends them to a file.

  --endpoint EP   framed logging endpoint (default: the installed runtime's
                  pipe for this service)
  --out PATH      service-owned log file (default: user cache/
                  openabstractions/logging/records.jsonl)

Exit codes: 0 a clean stop, 1 startup failure, 2 usage error.
`

func isHelp(arg string) bool { return arg == "--help" || arg == "-h" || arg == "help" }

// ErrUsage wraps an error Serve returns for a bad flag or argument, after it
// has already printed loggingUsage. A caller that hosts several
// capabilities tells this apart from a startup failure with
// errors.Is(err, ErrUsage).
var ErrUsage = errors.New("logging: usage error")

// Serve is the logging capability hosted by `openabstractions serve logging`.
func Serve(args []string) error {
	if len(args) > 0 && isHelp(args[0]) {
		_, err := io.WriteString(os.Stdout, loggingUsage)
		return err
	}
	flags := flag.NewFlagSet("logging", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	// loggingUsage already documents every option; the fallback on a
	// genuine parse error prints it once, not a second time as
	// flag.PrintDefaults' own single-dash listing.
	var usageErr error
	flags.Usage = func() { _, usageErr = fmt.Fprint(flags.Output(), loggingUsage) }
	endpoint := flags.String("endpoint", client.DefaultEndpoint(), "framed logging endpoint")
	out := flags.String("out", "", "service-owned log file (default: user cache/openabstractions/logging/records.jsonl)")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return usageErr
		}
		return errors.Join(fmt.Errorf("%w: %v", ErrUsage, err), usageErr)
	}
	if flags.NArg() != 0 {
		//unchecked: stderr is diagnostic only; ErrUsage below preserves the failed invocation
		fmt.Fprint(os.Stderr, loggingUsage)
		return fmt.Errorf("%w: logging: unexpected arguments", ErrUsage)
	}
	if *out == "" {
		cache, err := os.UserCacheDir()
		if err != nil {
			return err
		}
		*out = filepath.Join(cache, "openabstractions", "logging", "records.jsonl")
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0700); err != nil {
		return err
	}
	sink, err := logging.OpenDefaultRetainedFileSink(*out)
	if err != nil {
		return err
	}
	defer sink.Close()
	host, err := Listen(*endpoint, sink)
	if err != nil {
		return err
	}
	defer host.Close()
	host.OnError = func(err error) { fmt.Fprintln(os.Stderr, "logging:", err) }
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	joinRetention := sink.MaintainRetention(ctx, host.OnError)
	defer func() { cancel(); joinRetention() }()
	fmt.Fprintln(os.Stdout, "logging: listening", *endpoint)
	return host.Serve(ctx)
}
