package logging

import (
	"log/slog"

	"github.com/openabstractions/abstraction-identity/listen"
)

// NewResolvedNativeHandler is NewResolvedHandler with a native handler, for
// the fallback tests that serve their own resolver [LOG-S15].
func NewResolvedNativeHandler(program, endpoint string, server listen.ServerExpectation, native slog.Handler) *Handler {
	return newAdoptedHandler(resolvedSinkFor(endpoint, server), &Options{Program: program, Native: native})
}
