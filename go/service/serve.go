package service

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"

	logging "github.com/openabstractions/abstraction-logging/go"
	"github.com/openabstractions/abstraction-logging/go/client"
)

// Serve is the logging capability hosted by `openabstractions serve logging`.
func Serve(args []string) error {
	flags := flag.NewFlagSet("logging", flag.ContinueOnError)
	endpoint := flags.String("endpoint", client.DefaultEndpoint(), "framed logging endpoint")
	out := flags.String("out", "", "service-owned log file (default: user cache/openabstractions/logging/records.jsonl)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("logging: unexpected arguments")
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
	sink, err := logging.OpenFileSink(*out)
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
	fmt.Fprintln(os.Stdout, "logging: listening", *endpoint)
	return host.Serve(ctx)
}
