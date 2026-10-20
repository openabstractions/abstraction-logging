package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"
)

// ResolveDiagnostics resolves a native provider's handler within two seconds
// using its supplied resolver. Production callers pass Resolve to select the
// authenticated installed runtime. A resolution failure keeps the caller's
// explicit stderr text diagnostics. The returned closer drains an adopted
// handler once within five seconds and reports shutdown errors to stderr.
func ResolveDiagnostics(program string, stderr io.Writer, resolve func(context.Context, string) (*Handler, error)) (slog.Handler, func()) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if handler, err := resolve(ctx, program); err == nil {
		var once sync.Once
		return handler, func() {
			once.Do(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if _, err := handler.Close(ctx); err != nil {
					//unchecked: shutdown failure is already reported; stderr has no further diagnostic channel
					fmt.Fprintln(stderr, "logging shutdown:", err)
				}
			})
		}
	}
	return slog.NewTextHandler(stderr, nil), func() {}
}
